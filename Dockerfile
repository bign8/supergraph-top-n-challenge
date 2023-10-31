FROM golang:1.21.3-alpine
ARG COMPONENT
RUN echo "Hi ${COMPONENT}"
WORKDIR /mnt
ENV CGOENABLED=0
ENV GOBUILD='go build -v -trimpath'
RUN ${GOBUILD} \
    net/http/httptest \
    encoding/gob
COPY go.* ./
RUN go mod download -x
RUN ${GOBUILD} \
    go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc \
    go.opentelemetry.io/otel/semconv/v1.20.0 \
    github.com/graph-gophers/dataloader/v7 \
    github.com/graphql-go/graphql \
    github.com/graphql-go/handler \
    github.com/lib/pq
COPY cmd ./cmd
COPY lib ./lib
RUN ${GOBUILD} -o /app ./cmd/${COMPONENT}

FROM scratch
COPY --from=0 /app /app
CMD [ "/app" ]
