package main

import (
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/lib/pq"
	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"go.opentelemetry.io/otel"

	"github.com/bign8/supergraph-top-n-challenge/lib/env"
	"github.com/bign8/supergraph-top-n-challenge/lib/tracing"
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const DSN = `postgres://postgres:postgrespassword@[::]:8432?sslmode=disable`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	tracing.Init(`threads`)
	p, err := newProcessor()
	check(err)
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
	rowsParser *sql.Stmt
	arrParser  *sql.Stmt
}

func newProcessor() (*processor, error) {
	db, err := otelsql.Open(`postgres`, env.Default(`THREADS_DSN`, DSN))
	if err != nil {
		return nil, fmt.Errorf(`sql.Open: %w`, err)
	}
	rowsParser, err := db.Prepare(`SELECT id FROM threads LIMIT $1;`)
	if err != nil {
		return nil, fmt.Errorf(`db.Prepare(rows): %w`, err)
	}
	arrParser, err := db.Prepare(`SELECT ARRAY_AGG(a.id) FROM (SELECT id FROM threads LIMIT $1) a;`)
	if err != nil {
		return nil, fmt.Errorf(`db.Prepare(arr): %w`, err)
	}
	return &processor{
		rowsParser: rowsParser,
		arrParser:  arrParser,
	}, nil
}

/*
Frame Format: (all values are int32)
Input: [limit]
Output: gob([thread1_id][thread2_id]...[threadN_id])
*/

func (p processor) process(conn net.Conn) error {
	defer conn.Close()
	reader := gob.NewDecoder(conn)
	writer := gob.NewEncoder(conn)
	for {

		// parse input
		var limit int32
		if err := reader.Decode(&limit); err == io.EOF {
			log.Printf(`closing connection: %v`, conn.RemoteAddr())
			return nil
		} else if err != nil {
			return fmt.Errorf(`decode: %w`, err)
		}

		// TODO: propagate span properties from request headers
		ctx, span := otel.Tracer(``).Start(context.Background(), `processRequest`)
		log.Printf(`got %d`, limit)

		// query database
		output, err := p.processRows(ctx, limit)
		if err != nil {
			span.RecordError(err)
			span.End()
			return fmt.Errorf(`process: %w`, err)
		}

		// write result
		if err := writer.Encode(output); err != nil {
			span.RecordError(err)
			span.End()
			return fmt.Errorf(`encode: %w`, err)
		}
		span.End()
	}
}

func (p processor) processRows(ctx context.Context, limit int32) ([]int32, error) {
	rows, err := p.rowsParser.QueryContext(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf(`query: %w`, err)
	}
	output := make([]int32, 0, limit)
	for rows.Next() {
		var id int32
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

// randomly is slower than `processRows` even though parsing is faster + easier
func (p processor) processArray(ctx context.Context, limit int32) ([]int32, error) {
	var list pq.Int32Array
	if err := p.arrParser.QueryRowContext(ctx, limit).Scan(&list); err != nil {
		return nil, fmt.Errorf(`query/scan: %w`, err)
	}
	return list, nil
}
