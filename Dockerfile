FROM golang:1.21.3-alpine
ARG COMPONENT
RUN echo "Hi ${COMPONENT}"
WORKDIR /mnt
ENV CGOENABLED=0
ENV GOBUILD='go build -v -trimpath'
RUN ${GOBUILD} net/http
COPY go.* ./
RUN go mod download
COPY cmd ./cmd
RUN ${GOBUILD} -o /app ./cmd/${COMPONENT}

FROM scratch
COPY --from=0 /app /app
CMD [ "/app" ]
