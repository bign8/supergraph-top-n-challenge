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
	"go.opentelemetry.io/otel/propagation"

	"github.com/bign8/supergraph-top-n-challenge/lib/env"
	"github.com/bign8/supergraph-top-n-challenge/lib/tracing"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

const DSN = `postgres://postgres:postgrespassword@[::]:7432?sslmode=disable`

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	tracing.Init(`posts`)
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
	ctx, span := otel.Tracer(``).Start(context.Background(), `startup`)
	defer span.End()
	db, err := otelsql.Open(`postgres`, env.Default(`POSTS_DSN`, DSN))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf(`sql.Open: %w`, err)
	}
	fetchThreadPostsViaRows, err := db.PrepareContext(ctx, `SELECT id FROM posts WHERE thread_id = $1 LIMIT $2;`)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf(`prepare(rows): %w`, err)
	}
	fetchThreadPostsViaArray, err := db.PrepareContext(ctx, `SELECT ARRAY(SELECT id FROM posts WHERE thread_id = $1 LIMIT $2);`)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf(`prepare(array): %w`, err)
	}
	fetchMultiThreadPosts, err := db.PrepareContext(ctx, `SELECT thread_id, (array_agg(id))[1:$2] FROM posts
		WHERE thread_id = ANY($1) GROUP BY thread_id;`)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf(`prepare(multi): %w`, err)
	}
	return &processor{
		threadPostsViaRows:  fetchThreadPostsViaRows,
		threadPostsViaArray: fetchThreadPostsViaArray,
		multiThreadPosts:    fetchMultiThreadPosts,
	}, nil
}

type PostsRequest struct {
	Limit   int32
	Threads []int32
	Headers map[string]string
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

		// TODO: propagate span properties from request headers
		ctx := context.Background() // gotta start somewhere!
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(args.Headers))
		ctx, span := otel.Tracer(``).Start(ctx, `processBatch`)

		result := p.fetchBatchMulti(ctx, args)

		// write result
		if err := writer.Encode(result); err != nil {
			span.RecordError(err)
			span.End()
			return fmt.Errorf(`encode: %w`, err)
		}
		span.End()
	}
}

// TODO: measure (used in processBatch)
func (p processor) fetchBatchMulti(ctx context.Context, args PostsRequest) PostsResponse {
	// start := time.Now()
	result := PostsResponse{
		Posts: make(map[int32][]int32, len(args.Threads)),
	}

	// convert unsigned => signed (domain => postgres)
	threads := make([]int32, len(args.Threads))
	for i, threadID := range args.Threads {
		threads[i] = int32(threadID)
	}

	rows, err := p.multiThreadPosts.QueryContext(ctx, pq.Int32Array(threads), args.Limit)
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
