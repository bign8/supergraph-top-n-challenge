package main

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	_ "github.com/lib/pq"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const DSN = `postgres://postgres:postgrespassword@[::]:7432?sslmode=disable`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	db, err := sql.Open(`postgres`, DSN)
	check(err)
	query, err := db.Prepare(`SELECT id FROM posts WHERE thread_id = $1 LIMIT $2;`)
	check(err)
	p := &processor{stmt: query}

	l, err := net.Listen(`tcp`, `:8001`) // TODO: test UDP
	check(err)
	log.Printf(`posts on %v`, l.Addr())
	for {
		conn, err := l.Accept()
		check(err)
		log.Printf(`received connection: %v`, conn.RemoteAddr())
		go func() {
			check(p.processBatch(conn))
		}()
	}
}

type processor struct {
	stmt *sql.Stmt
}

/*
Frame Format: (all values are uint32)
Input: [limit][thread_id]
Output: [length][post1_id][post2_id]...[postN_id]
*/

// TODO: batch fetch sets of post IDs

func (p processor) process(conn net.Conn) error {
	defer conn.Close()
	reader := gob.NewDecoder(conn)
	writer := gob.NewEncoder(conn)
	for {

		// parse input
		var args [2]uint32
		if err := reader.Decode(&args); err == io.EOF {
			log.Printf(`closing connection: %v`, conn.RemoteAddr())
			return nil
		} else if err != nil {
			return fmt.Errorf(`decode: %w`, err)
		}
		limit := args[0]
		threadID := args[1]

		// query database
		out, err := p.fetchThread(threadID, limit)
		if err != nil {
			return fmt.Errorf(`fetchThread: %w`, err)
		}

		// write result
		if err := writer.Encode(out); err != nil {
			return fmt.Errorf(`encode: %w`, err)
		}
	}
}

func (p processor) fetchThread(threadID, limit uint32) ([]uint32, error) {
	rows, err := p.stmt.Query(threadID, limit)
	if err != nil {
		return nil, fmt.Errorf(`query: %w`, err)
	}
	output := make([]uint32, 0, limit)
	for rows.Next() {
		var id uint32
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf(`scan: %w`, err)
		}
		output = append(output, id)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf(`close: %w`, err)
	}
	return output, nil
}

type PostsRequest struct {
	Limit   uint32
	Threads []uint32
}

type PostsResponse struct {
	Posts map[uint32][]uint32
}

func (p processor) processBatch(conn net.Conn) error {
	defer conn.Close()
	reader := gob.NewDecoder(conn)
	writer := gob.NewEncoder(conn)

	for {

		// parse input
		var args PostsRequest
		if err := reader.Decode(&args); err == io.EOF {
			log.Printf(`closing connection: %v`, conn.RemoteAddr())
			return nil
		} else if err != nil {
			return fmt.Errorf(`decode: %w`, err)
		}

		// query database
		var wg sync.WaitGroup
		var stage sync.Map
		wg.Add(int(len(args.Threads)))
		for _, threadID := range args.Threads {
			go func(threadID uint32) {
				defer wg.Done()
				out, err := p.fetchThread(threadID, args.Limit)
				if err != nil {
					panic(err)
				}
				stage.Store(threadID, out)
			}(threadID)
		}
		wg.Wait()

		// serialize
		result := PostsResponse{
			Posts: make(map[uint32][]uint32, int(args.Limit)),
		}
		stage.Range(func(key, value any) bool {
			result.Posts[key.(uint32)] = value.([]uint32)
			return true
		})

		// write result
		if err := writer.Encode(result); err != nil {
			return fmt.Errorf(`encode: %w`, err)
		}
	}
}
