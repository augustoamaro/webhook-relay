# webhook-relay

A webhook delivery service that doesn't lose your events — Postgres ledger, Redis Streams dispatch, Standard Webhooks signing, and a chaos test to prove it.

[![CI](https://github.com/augustoamaro/webhook-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/augustoamaro/webhook-relay/actions/workflows/ci.yml)
[![Go 1.26](https://img.shields.io/badge/go-1.26-blue.svg)](https://go.dev/)
[![License MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

---

## Chaos proof: 14,399/14,399 delivered, zero lost

The signature end-to-end test (`make chaos`) runs `loadtest/steady.js` against a live compose stack and issues a `FLUSHALL` against Redis 45 seconds into the load. Redis holds only delivery pointers — the authoritative ledger is Postgres. The sweeper detects the gap, rebuilds the dispatch queue from the `deliveries` table, idempotent worker claims absorb any duplicates, and every delivery reaches `succeeded`.

Measured on a dev machine (podman), 10→100→100→0 req/s ramp over 2m45s:

```
k6 summary
  checks...........: 100.00%  14399/14399
  http_req_duration: avg=10.35ms  p(90)=12.18ms  p(95)=15.36ms  max=70.16ms
  p(95)<100ms threshold: PASSED

>>> CHAOS: flushing Redis mid-load
OK

>>> draining and reconciling...
deliveries: pending=14399 delivering=0 failed=0 succeeded=0 dead=0
deliveries: pending=8201 delivering=0 failed=0 succeeded=6198 dead=0
...
deliveries: pending=0   delivering=0 failed=0 succeeded=14399 dead=0
RECONCILED: every delivery reached a terminal state — zero lost
```

Why it works: `IngestMessage` commits message + fan-out deliveries in a single Postgres transaction before touching Redis. The optimistic Redis enqueue is best-effort. The sweeper runs every second, scans `deliveries` for rows that are due (`status IN ('pending','failed') AND next_attempt_at <= now()`) or whose worker lease has expired (`status = 'delivering' AND claimed_at < now()-60s`), skipping any row re-enqueued in the last 30 seconds, and pushes them back into the Redis stream. A `FLUSHALL` empties the queue; the sweeper notices on the next tick and fills it back. Redis is intentionally disposable.

Delivery throughput: 2 worker replicas drained the ~14k backlog (accumulated during the 2m45s ingest burst) in ~3.5 minutes, sustaining ~41 deliveries/s. Workers process stream batches sequentially today — per-message concurrency is a documented follow-up.

---

## Architecture

```
POST /messages
  │  one Postgres tx:
  │    INSERT messages
  │    INSERT deliveries (one per matching enabled endpoint)
  ▼
[commit] ──► best-effort XADD to Redis Stream "deliveries"
               │
               │  Sweeper (the guarantee)
               │  scans due deliveries every 1s, re-enqueues anything missing
               ▼
Workers  XREADGROUP("deliveries", group="workers", N replicas)
  1. Atomic claim:  UPDATE deliveries SET status='delivering', claimed_at=now
                    WHERE status IN ('pending','failed') AND next_attempt_at <= now
                    → race loser is a no-op (makes duplicate enqueues safe)
  2. Sign (Standard Webhooks: HMAC-SHA256) + POST, 10s timeout
  3. Record delivery_attempt; decide:
       2xx → succeeded;  reset endpoint consecutive_failures
       else → schedule retry (backoff) or mark dead
  4. XACK
  Crash recovery: XAUTOCLAIM reclaims entries idle > 60s;
                  PG lease (claimed_at older than 60s) re-opens the window.
```

The `deliveries` table **is** the transactional outbox — created in the same transaction as its `message`, scanned by the sweeper. There is no separate outbox table. Redis holds only stream entry IDs pointing at delivery ULIDs; losing it loses nothing that the sweeper cannot reconstruct.

### Module map

| Package | Role |
|---|---|
| `internal/domain` | Pure types, constants, retry policy, filter matching — no I/O |
| `internal/signer` | Standard Webhooks HMAC-SHA256 — pure |
| `internal/netguard` | SSRF egress guard — dial-time check, no I/O beyond `net.Dialer` |
| `internal/store` | pgx v5, hand-written SQL, embedded goose migrations |
| `internal/queue` | Redis Streams: enqueue / consume / XAUTOCLAIM |
| `internal/dispatch` | Worker loop: claim → sign → POST → record → decide |
| `internal/sweep` | Sweeper: due-delivery scan + enqueue |
| `internal/api` | HTTP handlers, admin/app-key auth middleware |
| `internal/obs` | OTel SDK wiring (traces, metrics, HTTP middleware) |
| `cmd/relay` | One binary with subcommands: `api`, `worker`, `sweep`, `all` |
| `cmd/demo-receiver` | Deliberately flaky receiver: configurable fail rate + latency jitter, verifies signatures |
| `cmd/reconcile` | Post-chaos reconciliation script: waits for drain, asserts zero stuck deliveries |

---

## Reliability semantics

**At-least-once delivery.** In the crash window — worker has POSTed but dies before recording the result — the delivery can be attempted twice. This is documented semantics, not a bug. Receivers deduplicate on the `webhook-id` header (the stable message ULID). The alternative (exactly-once) requires 2PC with the receiver and is not worth the complexity.

**Retry backoff with ±20% jitter.** 8 attempts total (1 initial + 7 retries). Base delays: `5s → 30s → 2m → 10m → 30m → 2h → 5h`. Each delay is multiplied by a factor in `[0.8, 1.2)`. After 8 failed attempts the delivery becomes `dead` (DLQ). The schedule lives in `internal/domain/retry.go` and is table-tested.

**No per-endpoint ordering guarantee — deliberate.** Strict FIFO ordering kills throughput and requires a per-endpoint serial queue. Svix and Standard Webhooks both leave ordering undefined. Receivers use `webhook-timestamp` to reconcile ordering if needed.

**Per-endpoint circuit breaker.** After 20 consecutive failures, the endpoint is auto-disabled (`status = 'disabled'`), new fan-out skips it, and its in-flight pending deliveries become `dead` with `dead_reason = 'endpoint_disabled'`. Manual re-enable (`PATCH .../endpoints/{id}` with `{"status":"enabled"}`) followed by redrive restores flow. The threshold is `domain.BreakerThreshold = 20`.

**SSRF egress guard.** `internal/netguard` installs a `net.Dialer.Control` hook that rejects private, loopback, and link-local destinations at dial time — after DNS resolution, so DNS-rebinding cannot bypass it. Redirect following is disabled. The guard is on by default; set `RELAY_ALLOW_PRIVATE_DESTINATIONS=true` only in controlled environments (the demo compose sets it to reach the `demo-receiver` container).

---

## Standard Webhooks signing

Implements the open [Standard Webhooks](https://www.standardwebhooks.com) spec. Every delivery carries three headers:

| Header | Value |
|---|---|
| `webhook-id` | The stable message ULID — use this for deduplication |
| `webhook-timestamp` | Unix seconds of the signing instant |
| `webhook-signature` | `v1,<base64(HMAC-SHA256)>` |

The signed string is `{webhook-id}.{webhook-timestamp}.{payload-bytes}`. Endpoint secrets are `whsec_<base64>` (24 random bytes). The signer lives in `internal/signer/signer.go` and is verified against the spec's published test vectors in `internal/signer/signer_test.go`.

Receivers get replay protection (check `webhook-timestamp` within a tolerance window) and deduplication (`webhook-id`) without additional infrastructure.

---

## Run it

**Prerequisites:** Docker (or Podman) and `docker compose` (or `podman compose`). `k6` for load/chaos tests.

```bash
make up
# API     → http://localhost:8080
# Grafana → http://localhost:3000  (anonymous admin, no login)
```

Podman users: override the compose command for any target.

```bash
make up COMPOSE="podman compose -f deploy/docker-compose.yml"
```

### Quickstart

```bash
ADMIN="dev-admin-key"
BASE="http://localhost:8080"

# 1. Create an application (admin) — api_key returned once
APP=$(curl -s -X POST $BASE/api/v1/applications \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d '{"name":"my-app"}')
APP_ID=$(echo $APP | jq -r .id)
APP_KEY=$(echo $APP | jq -r .api_key)

# 2. Register an endpoint (admin) — whsec_ secret returned once
EP=$(curl -s -X POST $BASE/api/v1/applications/$APP_ID/endpoints \
  -H "Authorization: Bearer $ADMIN" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://my.receiver.example/hook","event_types":["order.created"]}')

# 3. Send a message (app key)
curl -s -X POST $BASE/api/v1/applications/$APP_ID/messages \
  -H "Authorization: Bearer $APP_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: order-42" \
  -d '{"event_type":"order.created","payload":{"order_id":42}}'
# → 202 Accepted (new) or 200 OK (duplicate idempotency key)

# 4. Check delivery status
MSG_ID=<id from step 3>
curl -s $BASE/api/v1/applications/$APP_ID/messages/$MSG_ID \
  -H "Authorization: Bearer $APP_KEY"

# 5. Browse the DLQ
curl -s "$BASE/api/v1/applications/$APP_ID/deliveries?status=dead" \
  -H "Authorization: Bearer $APP_KEY"

# 6. Redrive all dead deliveries for an endpoint
curl -s -X POST $BASE/api/v1/applications/$APP_ID/deliveries/redrive \
  -H "Authorization: Bearer $APP_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"endpoint_id\":\"$EP_ID\"}"
```

The compose stack includes a deliberately flaky demo receiver (`cmd/demo-receiver`, `FAIL_RATE=0.2`, `MAX_DELAY_MS=200`) that verifies signatures and logs every delivery — useful for watching retries and DLQ in real time. Grafana comes up at `:3000` with a provisioned dashboard showing ingest rate, deliveries/s by outcome, p95 attempt latency, queue depth, and DLQ size.

![webhook-relay Grafana dashboard under load — delivery rate by outcome, attempt and end-to-end p95, API request rate and latency, success ratio](docs/media/dashboard.png)

_Provisioned dashboard under `make load`: the 20% flaky demo receiver drives the ~0.8 success ratio while retries and the DLQ absorb the rest._

---

## Load & chaos test results

Measured on a dev machine (podman). `loadtest/steady.js` ramps 10→100→100→0 req/s over 2m45s against 2 worker replicas + 1 sweeper.

| Metric | Value |
|---|---|
| Total messages ingested | 14,399 |
| Ingest acceptance rate | 100% (k6 `checks rate>0.99` passed) |
| Ingest p50 | ~10ms |
| Ingest p90 | 12.18ms |
| Ingest p95 | 15.36ms |
| Ingest max | 70.16ms |
| p95 < 100ms threshold | PASSED |
| Redis FLUSHALL injected at | t=45s (mid-load) |
| Interrupted ingest iterations | 0 (Postgres commit is unaffected by Redis) |
| Deliveries succeeded | 14,399 |
| Deliveries dead / lost | 0 |
| Delivery throughput (drain) | ~41/s across 2 worker replicas |
| Backlog drain time | ~3.5 minutes |

**Reproduce:**

```bash
make load     # steady load only
make chaos    # load + FLUSHALL at t=45s + reconciliation
```

`make chaos` runs `loadtest/steady.js` in the background, issues the FLUSHALL after 45 seconds, waits for k6 to finish, then runs `cmd/reconcile` which polls the `deliveries` table until every row is terminal and prints the final counts. Exit 0 = clean; exit 1 = stuck deliveries.

---

## API reference

All endpoints are under `:8080`. Auth: admin routes require `Authorization: Bearer <RELAY_ADMIN_API_KEY>`; message routes require the per-application key.

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/api/v1/applications` | admin | Create application — returns `api_key` (once) |
| `POST` | `/api/v1/applications/{app}/endpoints` | admin | Create endpoint — returns `whsec_` secret (once) |
| `GET` | `/api/v1/applications/{app}/endpoints` | admin | List endpoints |
| `PATCH` | `/api/v1/applications/{app}/endpoints/{ep}` | admin | Set `status` to `enabled` or `disabled` |
| `POST` | `/api/v1/applications/{app}/messages` | app key | Ingest event — `202` new, `200` duplicate `Idempotency-Key` |
| `GET` | `/api/v1/applications/{app}/messages/{msg}` | app key | Message status + per-delivery attempt ledger |
| `GET` | `/api/v1/applications/{app}/deliveries?status=dead` | app key | List deliveries by status (default: `dead`); newest first, capped at 100, with total count |
| `POST` | `/api/v1/applications/{app}/deliveries/redrive` | app key | Redrive by `delivery_ids`, `endpoint_id`, or `since`/`until` time range |
| `GET` | `/healthz` | none | Liveness — always 200 if the process is up |
| `GET` | `/readyz` | none | Readiness — checks Postgres + Redis connectivity |

Ingest body: `{"event_type":"<string>","payload":<json>}`. Optional `Idempotency-Key` header scoped per application. Payload cap: 64 KB (configurable via `RELAY_MAX_PAYLOAD_BYTES`).

Redrive resets `status → pending`, `next_attempt_at = now`, `attempt_count = 0` (fresh retry budget); the `delivery_attempt` history is preserved.

### Configuration

All environment variables use the `RELAY_` prefix.

| Variable | Default | Purpose |
|---|---|---|
| `RELAY_DATABASE_URL` | required | Postgres connection URL |
| `RELAY_REDIS_URL` | required | Redis connection URL |
| `RELAY_ADMIN_API_KEY` | required | Admin bearer token |
| `RELAY_ALLOW_PRIVATE_DESTINATIONS` | `false` | Disable SSRF guard (demo only) |
| `RELAY_HTTP_TIMEOUT_MS` | `10000` | Per-attempt HTTP timeout |
| `RELAY_MAX_PAYLOAD_BYTES` | `65536` | Ingest payload cap |
| `RELAY_SWEEP_INTERVAL_MS` | `1000` | Sweeper cadence |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Standard OTel OTLP export URL |

---

## Testing

Three layers, all with the race detector enabled.

**Unit** (`internal/domain`, `internal/signer`, `internal/netguard`): retry policy table-tests, Standard Webhooks signer against the spec's published test vectors, SSRF guard IP classification, event-type filter matching, circuit-breaker state transitions. No I/O.

**Integration** (real Postgres + Redis): claim race with two workers (one wins, one is a no-op), `XAUTOCLAIM` reclaim path, idempotent ingest, full pipeline against an `httptest.Server` receiver, redrive flow, goose migrations. Gated by environment variables; the suite truncates all tables on setup.

**E2E / chaos**: `make chaos` as described above.

```bash
# Unit + integration (integration requires env vars)
go test ./... -race -p 1

# Integration env vars
export RELAY_TEST_DATABASE_URL="postgres://relay:relay@localhost:5432/relay_test"
export RELAY_TEST_REDIS_URL="redis://localhost:6379/1"

# Lint
golangci-lint run

# Full chaos proof
make up && make chaos
```

CI (`.github/workflows/ci.yml`) runs `gofmt`, `golangci-lint` (v2), `go vet`, unit tests with `-race`, integration tests against Postgres + Redis service containers with `-race`, builds both Docker images, brings up compose, and runs a k6 smoke test.

---

## Limitations and follow-ups

These are known design trade-offs, not gaps:

- **Payload fidelity.** Payloads are stored as `jsonb` in Postgres, which normalizes whitespace and may reorder keys. The delivered body can differ byte-for-byte from the original submission. The HMAC is computed over the delivered bytes so signatures remain valid, but receivers that hash the raw body for their own purposes will see a different value.

- **Per-message delivery concurrency.** Workers consume stream batches sequentially within each replica — ~41 deliveries/s across 2 replicas in the load test. Adding goroutine-per-message dispatch within a worker is the obvious next step for higher throughput.

- **Single sweeper.** One sweeper process is fine at this scale. Multi-node coordination (Postgres advisory locks) is the straightforward extension and is documented but not implemented.

- **No endpoint secret rotation.** Rotation requires a multi-secret signing window so that in-flight deliveries signed with the old key still verify. Documented follow-up.

- **No wildcard event filters.** `endpoint.event_types` does exact-match; `order.*` patterns are a v2 item.

- **No message/attempt retention TTL.** All history accumulates. A purge job keyed on `created_at` is straightforward to add.

- **No per-endpoint rate limiting or concurrency caps.** All endpoints compete equally for worker capacity.

- **Idempotency-key TTL eviction.** Keys are stored indefinitely; production use would add a TTL-based cleanup.

---

## License

MIT. See [LICENSE](LICENSE).
