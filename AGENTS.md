# AGENTS.md

Go webhook delivery service (sender-side): Postgres ledger + Redis Streams dispatch.

## Commands
- Test: `go test ./... -race` (integration tests need RELAY_TEST_DATABASE_URL / RELAY_TEST_REDIS_URL — disposable instances, the suite truncates them)
- Lint: `golangci-lint run`
- Run all-in-one: `go run ./cmd/relay all`
- Full stack: `docker compose -f deploy/docker-compose.yml up --build`
- Load test: `make load` · chaos test: `make chaos`

## Architecture
- `internal/domain` is pure (no I/O). SQL lives only in `internal/store`. Redis only in `internal/queue`.
- Deliveries table doubles as the transactional outbox; Redis is disposable (sweeper rebuilds it).
- Commits: Leonidas Augusto Amaro <augustoamaro@proton.me> only, English, conventional.
