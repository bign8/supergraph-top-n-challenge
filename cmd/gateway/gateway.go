package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	dataloader "github.com/graph-gophers/dataloader/v7"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/handler"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/bign8/supergraph-top-n-challenge/lib/domain"
	"github.com/bign8/supergraph-top-n-challenge/lib/env"
	"github.com/bign8/supergraph-top-n-challenge/lib/tracing"
)

type Identified struct {
	ID int32 `json:"id"`
}

func resolveThreads(p graphql.ResolveParams) (any, error) {
	limit := p.Args[`limit`].(int)
	request := &domain.ThreadsRequest{
		Limit:   int32(limit),
		Headers: make(map[string]string, 2),
	}

	ctx, span := otel.Tracer(``).Start(p.Context, `resolveThreads`)
	defer span.End()

	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(request.Headers))

	// TODO: connection pooling
	conn, err := net.Dial(`tcp`, env.Default(`THREADS_HOST`, `[::]:8002`))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf(`dial: %w`, err)
	}

	// make request
	if err := gob.NewEncoder(conn).Encode(request); err != nil {
		return nil, fmt.Errorf(`gob encode: %w`, err)
	}

	// process response
	var res []int32
	if err := gob.NewDecoder(conn).Decode(&res); err != nil {
		return nil, fmt.Errorf(`gob decode: %w`, err)
	}

	// convert to model
	output := make([]Identified, limit)
	for i, id := range res {
		output[i].ID = id
	}
	return output, conn.Close()
}

type client struct {
	conn io.Closer
	enc  *gob.Encoder
	dec  *gob.Decoder
}

type PostRequest struct {
	Limit  int32
	Thread int32
}

func (c client) postsBatch(ctx context.Context, limit int32, threads []int32) (map[int32][]int32, error) {
	req := domain.PostsRequest{
		Limit:   limit,
		Threads: threads,
		Headers: make(map[string]string, 2),
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(req.Headers))
	if err := c.enc.Encode(req); err != nil {
		return nil, fmt.Errorf(`encode: %w`, err)
	}
	var res domain.PostsResponse
	if err := c.dec.Decode(&res); err != nil {
		return nil, fmt.Errorf(`decode: %w`, err)
	}
	return res.Posts, nil
}

var pool = sync.Pool{
	New: func() any {
		conn, err := net.Dial(`tcp`, env.Default(`POSTS_HOST`, `[::]:8001`))
		if err != nil {
			panic(err)
		}
		log.Printf(`New connection to posts: %v`, conn.LocalAddr())

		c := &client{
			conn: conn,
			enc:  gob.NewEncoder(conn),
			dec:  gob.NewDecoder(conn),
		}
		// runtime.SetFinalizer(c, c.conn.Close)

		return c
	},
}

func resolvePostsBatch(p graphql.ResolveParams) (any, error) {
	limit := p.Args[`limit`].(int)
	thread := p.Source.(Identified)

	thunk := loader.Load(p.Context, PostRequest{
		Limit:  int32(limit),
		Thread: thread.ID,
	})

	return func() (any, error) {
		posts, err := thunk()
		out := make([]Identified, len(posts))
		for i, postID := range posts {
			out[i].ID = postID
		}
		return out, err
	}, nil
}

var (
	ID    = &graphql.Field{Type: graphql.Int}
	Limit = graphql.FieldConfigArgument{
		`limit`: &graphql.ArgumentConfig{
			Type: graphql.Int,
		},
	}
	schema = graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: `RootQuery`,
			Fields: graphql.Fields{
				`threads`: &graphql.Field{
					Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
						Name: `Thread`,
						Fields: graphql.Fields{
							`id`: ID,
							`posts`: &graphql.Field{
								Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
									Name:   `Post`,
									Fields: graphql.Fields{`id`: ID},
								})),
								Args:    Limit,
								Resolve: resolvePostsBatch,
							},
						},
					})),
					Args:    Limit,
					Resolve: resolveThreads,
				},
			},
		}),
	}
)

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type tracer struct{}

func (t tracer) TraceQuery(ctx context.Context, queryString, operationName string) (context.Context, graphql.TraceQueryFinishFunc) {
	ctx, span := otel.Tracer(``).Start(ctx, operationName)
	return ctx, func(fe []gqlerrors.FormattedError) {
		// TODO: span errors
		span.End()
	}
}

func (t tracer) TraceField(ctx context.Context, fieldName, typeName string) (context.Context, graphql.TraceFieldFinishFunc) {
	if fieldName == `id` && typeName == `Int` {
		return ctx, func(fe []gqlerrors.FormattedError) { /* noop for IDs */ }
	}

	ctx, span := otel.Tracer(``).Start(ctx, fieldName+`.`+typeName)
	return ctx, func(fe []gqlerrors.FormattedError) {
		span.End()
	}
}

func loadBatch(ctx context.Context, keys []PostRequest) []*dataloader.Result[[]int32] {

	ctx, span := otel.Tracer(``).Start(ctx, `loadBatch`)
	defer span.End()

	limit := keys[0].Limit
	threads := make([]int32, len(keys))
	for i, req := range keys {
		if req.Limit != limit {
			panic(`non-equal limits!`)
		}
		threads[i] = req.Thread
	}
	res := make([]*dataloader.Result[[]int32], len(keys))

	conn := pool.Get().(*client)
	defer pool.Put(conn)

	data, err := conn.postsBatch(ctx, limit, threads)
	if err != nil {
		for i := range res {
			res[i] = &dataloader.Result[[]int32]{Error: err}
		}
		return res
	}

	for i, req := range keys {
		res[i] = &dataloader.Result[[]int32]{
			Data: data[req.Thread],
		}
	}

	return res
}

var loader = dataloader.NewBatchedLoader(
	loadBatch,
	dataloader.WithWait[PostRequest, []int32](time.Millisecond),
	// dataloader.WithBatchCapacity[PostRequest, []int32](10),
	dataloader.WithClearCacheOnBatch[PostRequest, []int32](),
	// dataloader.WithTracer[PostRequest, []int32](t),
	// TODO dataloader.WithTracer(t))
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	tracing.Init(`gateway`)
	schema, err := graphql.NewSchema(schema)
	check(err)

	h := handler.New(&handler.Config{
		Schema:     &schema,
		Playground: true,
		Tracer:     &tracer{},
	})
	mux := http.DefaultServeMux
	mux.Handle(`/graphql`, h)
	mux.Handle(`/`, http.RedirectHandler(`/graphql`, http.StatusSeeOther))
	server := http.Server{
		Addr:    `[::]:8000`,
		Handler: measure(mux),
	}
	log.Printf(`gateway on %v`, server.Addr)
	check(server.ListenAndServe())
}

func measure(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx, span := otel.Tracer(``).Start(r.Context(), r.Method+` `+r.URL.Path)
		defer span.End()
		h.ServeHTTP(w, r.WithContext(ctx))
		log.Printf(`%s %s %s`, r.Method, r.URL.Path, time.Since(start).Round(time.Nanosecond*100))
	}
}
