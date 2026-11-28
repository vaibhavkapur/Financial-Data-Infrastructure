FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY src ./src
RUN CGO_ENABLED=0 go build -o /tickdb ./src/cmd/tickdb

FROM alpine:3.20
WORKDIR /var/lib/tickdb
COPY --from=builder /tickdb /usr/local/bin/tickdb
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/tickdb"]
