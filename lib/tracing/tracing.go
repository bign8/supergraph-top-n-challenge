package tracing

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
)

func Init(service string) {
	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithInsecure(),
	)
	exporter, err := otlptrace.New(context.Background(), client)
	if err != nil {
		log.Fatal(`creating OTLP trace exporter: %w`, err)
	}
	tp := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()), // for debugging
		trace.WithBatcher(
			exporter,
			trace.WithBatchTimeout(time.Second), // for debugging
		),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(service),
			semconv.ServiceVersion(`0.0.1`),
		)),
	)

	otel.SetTracerProvider(tp)

	// by default propagate spans!
	otel.SetTextMapPropagator(propagation.TraceContext{})
}
