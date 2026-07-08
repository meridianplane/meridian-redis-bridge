# Contributing

## Development

```bash
go build ./cmd/meridian
go test ./...
```

## Before submitting a PR

- `go test -race -count=1 ./...` passes
- `go vet ./...` is clean
- New features include tests
- Proto changes (`proto/meridian.proto`) include regenerated `.pb.go` files

## Code generation

```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/meridian.proto
```

## E2E testing

Run the full docker-compose demo:

```bash
cd examples
docker compose up --build
redis-cli -p 6381 SET k v
redis-cli -p 6382 GET k   # → "v"
```

## Project structure

| Directory | Purpose |
|---|---|
| `cmd/meridian` | Entry point |
| `internal/config` | JSON config loading |
| `internal/proxy` | Dispatcher, backend, forwarder, auth |
| `internal/sync` | gRPC replication server + follower |
| `internal/wal` | Write-ahead log |
| `internal/resp` | Redis serialization protocol |
| `internal/server` | RESP frontend |
| `proto` | Protobuf definitions |
| `e2e` | End-to-end tests |
| `examples` | Docker Compose demo |
