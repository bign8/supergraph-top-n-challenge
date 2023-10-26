package main

import (
	"database/sql"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"

	_ "github.com/lib/pq"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const DSN = `postgres://postgres:postgrespassword@[::]:8432?sslmode=disable`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	db, err := sql.Open(`postgres`, DSN)
	check(err)
	query, err := db.Prepare(`SELECT id FROM threads LIMIT $1;`)
	check(err)
	p := &processor{stmt: query}

	l, err := net.Listen(`tcp`, `:8002`) // TODO: test UDP
	check(err)
	log.Printf(`threads on %v`, l.Addr())
	for {
		conn, err := l.Accept()
		check(err)
		log.Printf(`received connection: %v`, conn.RemoteAddr())
		go func() {
			check(p.process(conn))
		}()
	}
}

type processor struct {
	stmt *sql.Stmt
}

/*
Frame Format: (all values are uint32)
Input: [limit]
Output: gob([thread1_id][thread2_id]...[threadN_id])
// TODO: length should == limit
*/

func (p processor) process(conn net.Conn) error {
	defer conn.Close()
	reader := gob.NewDecoder(conn)
	writer := gob.NewEncoder(conn)
	for {

		// parse input
		var limit uint32
		if err := reader.Decode(&limit); err == io.EOF {
			log.Printf(`closing connection: %v`, conn.RemoteAddr())
			return nil
		} else if err != nil {
			return fmt.Errorf(`decode: %w`, err)
		}
		log.Printf(`got %d`, limit)

		// query database
		rows, err := p.stmt.Query(limit)
		if err != nil {
			return fmt.Errorf(`query: %w`, err)
		}
		output := make([]uint32, 0, limit)
		for rows.Next() {
			var id uint32
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf(`scan: %w`, err)
			}
			output = append(output, id)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf(`close: %w`, err)
		}

		// write result
		if err := writer.Encode(output); err != nil {
			return fmt.Errorf(`encode: %w`, err)
		}
	}
}
