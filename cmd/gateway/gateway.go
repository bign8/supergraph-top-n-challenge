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

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/handler"
)

type Identified struct {
	ID uint32 `json:"id"`
}

func resolveThreads(p graphql.ResolveParams) (any, error) {
	limit := p.Args[`limit`].(int)

	// TODO: connection pooling
	conn, err := net.Dial(`tcp`, `[::]:8002`)
	if err != nil {
		return nil, fmt.Errorf(`dial: %w`, err)
	}

	// make request
	if err := gob.NewEncoder(conn).Encode(uint32(limit)); err != nil {
		return nil, fmt.Errorf(`gob encode: %w`, err)
	}

	// process response
	var res []uint32
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

func (c client) posts(threadID, limit uint32) ([]Identified, error) {
	// time.Sleep(time.Millisecond * 20)
	if err := c.enc.Encode([2]uint32{limit, threadID}); err != nil {
		return nil, fmt.Errorf(`encode: %w`, err)
	}
	var res []uint32
	if err := c.dec.Decode(&res); err != nil {
		return nil, fmt.Errorf(`decode: %w`, err)
	}
	output := make([]Identified, len(res))
	for i, id := range res {
		output[i].ID = id
	}
	return output, nil
}

var pool = sync.Pool{
	New: func() any {
		conn, err := net.Dial(`tcp`, `[::]:8001`)
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

func resolvePosts(p graphql.ResolveParams) (any, error) {
	limit := p.Args[`limit`].(int)
	thread := p.Source.(Identified)

	conn := pool.Get().(*client)
	defer pool.Put(conn)

	return conn.posts(thread.ID, uint32(limit))
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
								Resolve: resolvePosts,
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

	return ctx, func(fe []gqlerrors.FormattedError) {

		log.Printf(`traceQuery: end %q`, operationName)

	}
}

func (t tracer) TraceField(ctx context.Context, fieldName, typeName string) (context.Context, graphql.TraceFieldFinishFunc) {
	if fieldName == `id` && typeName == `Int` {
		return ctx, func(fe []gqlerrors.FormattedError) { /* noop for IDs */ }
	}

	ext := ctx.Value(traceKey).(*tracingExtension)

	start := time.Now()
	span := &resolverTrace{
		Path:  []any{fieldName, typeName},
		Start: uint(time.Since(ext.Start)), // TODO: based on `start`
	}
	ext.Execution.Resolvers = append(ext.Execution.Resolvers, span)

	return ctx, func(fe []gqlerrors.FormattedError) {
		span.Duration = uint(time.Since(start))
	}
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	schema, err := graphql.NewSchema(schema)
	check(err)
	t := &tracer{}
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

			// TODO: dataloader

			return context.Background()
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
