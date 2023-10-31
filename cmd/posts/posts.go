package main

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/lib/pq"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

const DSN = `postgres://postgres:postgrespassword@[::]:7432?sslmode=disable`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	p, err := newProcessor()
	check(err)
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
	threadPostsViaRows  *sql.Stmt // SLOW: see usage in posts_test
	threadPostsViaArray *sql.Stmt // SLOW: see usage in posts_test
	multiThreadPosts    *sql.Stmt
}

func newProcessor() (*processor, error) {
	db, err := sql.Open(`postgres`, DSN)
	check(err)
	fetchThreadPostsViaRows, err := db.Prepare(`SELECT id FROM posts WHERE thread_id = $1 LIMIT $2;`)
	check(err)
	fetchThreadPostsViaArray, err := db.Prepare(`SELECT ARRAY(SELECT id FROM posts WHERE thread_id = $1 LIMIT $2);`)
	check(err)
	fetchMultiThreadPosts, err := db.Prepare(`SELECT thread_id, (array_agg(id))[1:$2] FROM posts
		WHERE thread_id = ANY($1) GROUP BY thread_id;`)
	check(err)
	return &processor{
		threadPostsViaRows:  fetchThreadPostsViaRows,
		threadPostsViaArray: fetchThreadPostsViaArray,
		multiThreadPosts:    fetchMultiThreadPosts,
	}, nil
}

type PostsRequest struct {
	Limit   int32
	Threads []int32
}

type PostsResponse struct {
	Posts map[int32][]int32
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

		result := p.fetchBatchMulti(args)

		// write result
		if err := writer.Encode(result); err != nil {
			return fmt.Errorf(`encode: %w`, err)
		}
	}
}

// TODO: measure (used in processBatch)
func (p processor) fetchBatchMulti(args PostsRequest) PostsResponse {
	// start := time.Now()
	result := PostsResponse{
		Posts: make(map[int32][]int32, len(args.Threads)),
	}

	// convert unsigned => signed (domain => postgres)
	threads := make([]int32, len(args.Threads))
	for i, threadID := range args.Threads {
		threads[i] = int32(threadID)
	}

	rows, err := p.multiThreadPosts.Query(pq.Int32Array(threads), args.Limit)
	check(err)
	var (
		thread      int32
		postsSigned pq.Int32Array
	)
	for rows.Next() {
		check(rows.Scan(&thread, &postsSigned))
		result.Posts[thread] = postsSigned
	}
	check(rows.Err())

	// log.Printf(`fetching %d posts of %d threads took %s`, args.Limit, len(args.Threads), time.Since(start))
	return result
}
