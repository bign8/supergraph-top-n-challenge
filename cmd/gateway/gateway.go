package main

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	dataloader "github.com/graph-gophers/dataloader/v7"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/handler"

	"github.com/bign8/supergraph-top-n-challenge/lib/env"
	"github.com/bign8/supergraph-top-n-challenge/lib/tracing"
)

type Identified struct {
	ID int32 `json:"id"`
}

func resolveThreads(p graphql.ResolveParams) (any, error) {
	limit := p.Args[`limit`].(int)

	// TODO: connection pooling
	conn, err := net.Dial(`tcp`, env.Default(`THREADS_HOST`, `[::]:8002`))
	if err != nil {
		return nil, fmt.Errorf(`dial: %w`, err)
	}

	// make request
	if err := gob.NewEncoder(conn).Encode(int32(limit)); err != nil {
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

type PostsRequest struct {
	Limit   int32
	Threads []int32
}

type PostsResponse struct {
	Posts map[int32][]int32
}

func (c client) postsBatch(limit int32, threads []int32) (map[int32][]int32, error) {
	if err := c.enc.Encode(PostsRequest{Limit: limit, Threads: threads}); err != nil {
		return nil, fmt.Errorf(`encode: %w`, err)
	}
	var res PostsResponse
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
	loader := p.Context.Value(loaderKey).(func(context.Context, PostRequest) dataloader.Thunk[[]int32])

	thunk := loader(p.Context, PostRequest{
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

type tracer struct {
	// TODO
}

type traceWapper struct {
	Data       any `json:"data"`
	Errors     any `json:"errors,omitempty"`
	Extensions struct {
		Tracing tracingExtension `json:"tracing"`
	} `json:"extensions,omitempty"`
}

type tracingExtension struct {
	Version int       `json:"version"`
	Start   time.Time `json:"startTime"`
	End     time.Time `json:"endTime"`
	// Duration int `json:"duration"`
	// TODO: parsing
	// TODO: validation
	Execution struct {
		Resolvers []*resolverTrace `json:"resolvers"`
	} `json:"execution"`
}

type resolverTrace struct {
	Path     []any `json:"path"`
	Start    uint  `json:"startOffset"`
	Duration uint  `json:"duration"`
}

type ctxKey string

const traceKey = ctxKey(`tracer`)
const parentKey = ctxKey(`parent`)
const loaderKey = ctxKey(`loader`)

func (t tracer) wrap(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			h.ServeHTTP(w, r)
			return
		}

		ext := &tracingExtension{
			Version: 1,
			Start:   time.Now(),
		}

		// TODO: and a trace recorder to the context
		// TODO: record JSON response
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), traceKey, ext)))

		ext.End = time.Now()
		// ext.Duration = int(ext.End.Sub(ext.Start))

		// copy over response meta
		res := rec.Result()
		for key, values := range res.Header {
			w.Header()[key] = values
		}
		w.WriteHeader(res.StatusCode)

		// parse response
		var wrap traceWapper
		if err := json.NewDecoder(res.Body).Decode(&wrap); err != nil {
			panic(err)
		}
		if err := res.Body.Close(); err != nil {
			panic(err)
		}

		// TODO: embed traces from context
		wrap.Extensions.Tracing = *ext

		// TODO: write output
		if err := json.NewEncoder(w).Encode(wrap); err != nil {
			panic(err)
		}
	}
}

func (t tracer) TraceQuery(ctx context.Context, queryString, operationName string) (context.Context, graphql.TraceQueryFinishFunc) {

	log.Printf(`traceQuery: start %q`, operationName)
	ctx = context.WithValue(ctx, parentKey, []any{operationName})

	return ctx, func(fe []gqlerrors.FormattedError) {

		log.Printf(`traceQuery: end %q`, operationName)

	}
}

func (t tracer) TraceField(ctx context.Context, fieldName, typeName string) (context.Context, graphql.TraceFieldFinishFunc) {
	if fieldName == `id` && typeName == `Int` {
		return ctx, func(fe []gqlerrors.FormattedError) { /* noop for IDs */ }
	}

	ext := ctx.Value(traceKey).(*tracingExtension)
	parents := append(ctx.Value(parentKey).([]any), fieldName, typeName)
	ctx = context.WithValue(ctx, parentKey, parents)

	start := time.Now()
	span := &resolverTrace{
		Path:  parents,
		Start: uint(time.Since(ext.Start)), // TODO: based on `start`
	}
	ext.Execution.Resolvers = append(ext.Execution.Resolvers, span)

	return ctx, func(fe []gqlerrors.FormattedError) {
		span.Duration = uint(time.Since(start))
	}
}

// func (t tracer) TraceBatch() {
// 	// TODO
// }

func loadBatch(ctx context.Context, keys []PostRequest) []*dataloader.Result[[]int32] {
	// time.Sleep(20 * time.Millisecond) // TODO: remove

	ext := ctx.Value(traceKey).(*tracingExtension)
	start := time.Now()
	span := &resolverTrace{
		Path:  []any{`batch`, `Post`},
		Start: uint(time.Since(ext.Start)),
	}
	ext.Execution.Resolvers = append(ext.Execution.Resolvers, span)

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

	data, err := conn.postsBatch(limit, threads)
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

	span.Duration = uint(time.Since(start))

	return res
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	tracing.Init(`gateway`)
	schema, err := graphql.NewSchema(schema)
	check(err)

	t := &tracer{}

	loader := dataloader.NewBatchedLoader(
		loadBatch,
		dataloader.WithWait[PostRequest, []int32](time.Millisecond),
		// dataloader.WithBatchCapacity[PostRequest, []int32](10),
		dataloader.WithClearCacheOnBatch[PostRequest, []int32](),
		// dataloader.WithTracer[PostRequest, []int32](t),
		// TODO dataloader.WithTracer(t))
	)

	h := handler.New(&handler.Config{
		Schema:     &schema,
		Playground: true,
		Tracer:     t,
	})
	mux := http.DefaultServeMux
	mux.Handle(`/graphql`, t.wrap(h))
	mux.Handle(`/`, http.RedirectHandler(`/graphql`, http.StatusSeeOther))
	server := http.Server{
		Addr: `[::]:8000`,
		BaseContext: func(l net.Listener) context.Context {
			ctx := context.Background()
			ctx = context.WithValue(ctx, loaderKey, loader.Load)
			return ctx
		},
		Handler: measure(mux),
	}
	log.Printf(`gateway on %v`, server.Addr)
	check(server.ListenAndServe())
}

func measure(h http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.ServeHTTP(w, r)
		log.Printf(`%s %s %s`, r.Method, r.URL.Path, time.Since(start).Round(time.Nanosecond*100))
	}
}
