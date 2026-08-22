# Queuemaxxing 💪

Queuemaxxing is a durable, single-node HTTP queue that combines priority, delay, and process-wide FIFO or LIFO ordering. It runs as one Go process, stores its own data in a local write-ahead log, and does not depend on a database or another queue.

## Requirements

| Requirement | Implementation |
|---|---|
| FIFO or LIFO | Selected for the process with `-discipline fifo` or `-discipline lifo` |
| Priority | Higher integer priority is delivered first |
| Delay | Each message can set an initial delivery delay |
| Combined behavior | Priority always wins; FIFO/LIFO breaks equal-priority ties |
| Durable storage | Every state change is appended to a checksummed WAL and `fsync`ed |
| Restart recovery | Startup replays the WAL and restores messages, leases, attempts, and idempotency keys |
| WAL compaction | Optional crash-safe current-state snapshot replacement at startup |
| Concurrency | Concurrent HTTP requests are safe; one mutex serializes queue state and WAL writes |
| Retries | Bounded exponential backoff with an explicit-delay override |
| Dead letters | Exhausted messages are stored durably and can be inspected or replayed |
| Long polling | Consumers can wait up to 20 seconds for eligible work |
| Lease extension | A worker can durably extend its current delivery lease |
| No external store | Storage is implemented directly with a local file |
| Example application | The same `queuemaxxing` binary exercises the complete HTTP API |

One process owns one logical queue and one WAL. Run another process with another listen address and WAL path when you need another queue.

## Quick start

Requires Go 1.25 or later.

```sh
make build
./bin/queuemaxxing serve -addr 127.0.0.1:8080 -discipline fifo -data data/queue.wal
```

In another terminal:

```sh
# Enqueue JSON. Producer retries using the same key will not create duplicates.
./bin/queuemaxxing put -priority 10 -idempotency-key email-123 '{"task":"send-email"}'

# Enqueue a lower-priority message with a five-second delay.
./bin/queuemaxxing put -priority 1 -delay 5s '{"task":"cleanup"}'

./bin/queuemaxxing stats
./bin/queuemaxxing reserve -visibility 30s -wait 20s

# Set a slow job's lease deadline to 60 seconds from server time.
./bin/queuemaxxing extend -visibility 60s MESSAGE_ID RECEIPT

./bin/queuemaxxing ack MESSAGE_ID RECEIPT

# Return a reserved message using automatic exponential backoff.
./bin/queuemaxxing nack MESSAGE_ID RECEIPT

# Or override the retry delay, including 0s for immediate retry.
./bin/queuemaxxing nack -delay 10s MESSAGE_ID RECEIPT

./bin/queuemaxxing dead list
./bin/queuemaxxing dead replay DEAD_LETTER_ID
```

For priority LIFO, use a different WAL:

```sh
./bin/queuemaxxing serve -addr 127.0.0.1:8081 -discipline lifo -data data/lifo.wal
```

The discipline is stored in the WAL. Restart a WAL with the same discipline.

Client commands use `http://localhost:8080` by default. Put `-url` before the command to target another process, for example `./bin/queuemaxxing -url http://127.0.0.1:8081 stats`. The older flag-only server invocation and optional `qmctl` compatibility binary remain available; `make build-compat` builds both.

### Server configuration

| Flag | Default | Purpose |
|---|---|---|
| `-addr` | `:8080` | HTTP listen address |
| `-data` | `data/queue.wal` | WAL path |
| `-discipline` | `fifo` | Equal-priority ordering: `fifo` or `lifo` |
| `-idempotency-retention` | `24h` | Producer-key deduplication window |
| `-max-attempts` | `5` | Reservations allowed before dead-lettering |
| `-retry-base-delay` | `1s` | Initial automatic retry delay |
| `-retry-max-delay` | `5m` | Automatic retry delay cap |
| `-compact-on-start` | `false` | Replace WAL history with a current-state snapshot before listening |

## Queue semantics

A message is eligible when its initial or retry delay has passed and it has no active delivery lease. Reserve selects an eligible message in this order:

1. Higher priority first.
2. For equal priority, lower sequence first in FIFO mode.
3. For equal priority, higher sequence first in LIFO mode.

A delayed high-priority message does not block a lower-priority message that is ready now.

Ready messages live in a priority max-heap. Delayed and leased messages live in a time-ordered min-heap. Enqueue, reserve, ack, nack, and lease extension therefore update the selection indexes in `O(log n)` time. Queue statistics still scan live messages in `O(n)` time.

### Delivery lifecycle

Reserve creates a random receipt and hides the message until `lease_until`:

```text
enqueue → ready → reserve → ack
                    ├────→ extend lease → reserved
                    └────→ nack/lease expiry → retry delay → ready
                                      └──────→ max attempts → dead letter
                                                                    └→ replay → new message
```

- `ack` permanently completes the message.
- `nack` schedules exponential backoff unless the request explicitly supplies a delay.
- An expired lease schedules the same automatic backoff. Immediate reserves advance due transitions synchronously; a long poll also wakes at the next scheduled deadline. There is no scheduler running when no consumer is reserving.
- A worker may extend its current unexpired lease. Extension preserves its receipt and attempt count and is durable across restarts.
- Every reservation increments `attempts` and issues a new receipt.
- Attempts count successful reservations, including the first delivery.
- At five attempts by default, `nack` or lease expiry moves the message atomically into durable dead-letter state.
- A stale, replaced, or expired receipt returns `409 Conflict` and never changes durable state.

Automatic backoff is deterministic:

```text
min(retry_base_delay * 2^(attempts-1), retry_max_delay)
```

There is no jitter in this initial implementation. Retry configuration is process-wide and persisted in the WAL; restarting with a different policy is rejected.

Delivery is **at least once**. Consumers must make their handlers idempotent because work can complete even when its acknowledgement is lost.

### Producer idempotency

`POST /v1/messages` accepts an optional `Idempotency-Key` header, limited to 128 bytes.

- Repeating an active key with the same body, priority, and delay returns the original message with `Idempotency-Replayed: true`.
- Reusing the key with different input returns `409 Conflict`.
- The key remains valid after the message is acknowledged.
- Keys survive restarts and expire after 24 hours by default.
- `-idempotency-retention` changes the process-wide retention period.

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/messages` | Enqueue a message |
| `POST` | `/v1/messages/reserve` | Reserve the next eligible message |
| `POST` | `/v1/messages/{id}/ack` | Complete a reserved message |
| `POST` | `/v1/messages/{id}/nack` | Return a reserved message |
| `POST` | `/v1/messages/{id}/lease` | Extend a reserved message's lease |
| `GET` | `/v1/dead-letters?limit=100` | Inspect exhausted messages |
| `POST` | `/v1/dead-letters/{id}/replay` | Replay one exhausted message |
| `GET` | `/v1/stats` | Count ready, delayed, and in-flight messages |
| `GET` | `/healthz` | Process health check |

Requests are JSON, reject unknown fields, and are limited to 1 MiB.

### Enqueue

```http
POST /v1/messages
Content-Type: application/json
Idempotency-Key: email-123

{"body":{"task":"send-email"},"priority":10,"delay_seconds":5}
```

Returns `201 Created` with the persisted message.

### Reserve

```http
POST /v1/messages/reserve
Content-Type: application/json

{"visibility_timeout_seconds":30,"wait_timeout_seconds":20}
```

The default visibility timeout is 30 seconds. `wait_timeout_seconds` defaults to `0` for immediate polling and is capped at 20 seconds. A long poll returns as soon as work becomes eligible or queue state changes. The server returns `204 No Content` when the wait expires without a ready message. Otherwise it returns the message, receipt, attempt count, and lease deadline.

### Extend a lease

```http
POST /v1/messages/{id}/lease
Content-Type: application/json

{"receipt":"...","visibility_timeout_seconds":60}
```

The receipt must identify the current unexpired delivery. The new deadline is calculated from server time and must be later than the existing deadline. A successful extension keeps the same receipt and attempt count; stale or expired receipts return `409 Conflict`.

### Ack

```http
POST /v1/messages/{id}/ack
Content-Type: application/json

{"receipt":"..."}
```

### Nack

```http
POST /v1/messages/{id}/nack
Content-Type: application/json

{"receipt":"..."}
```

Omitting `delay_seconds` uses automatic backoff. Supplying it overrides backoff; an explicit `0` requests immediate retry. The response status is `retry_scheduled` or `dead_lettered`.

### Dead letters

```http
GET /v1/dead-letters?limit=100
```

The default limit is 100 and the maximum is 1000. Results are ordered by dead-letter time and original sequence.

```http
POST /v1/dead-letters/{id}/replay
Content-Type: application/json

{"delay_seconds":0}
```

Replay atomically removes the dead letter and creates a live message with a new ID, zero attempts, the same body and priority, and `original_message_id` pointing to the exhausted message.

## Durability and recovery

The default data file is `data/queue.wal`. This is a custom binary event log, not a SQLite WAL or another database format. Each record contains:

```text
4-byte payload length | 4-byte CRC32 | JSON event
```

The WAL records queue and retry configuration, enqueue, reserve, lease extension, ack, retry scheduling, dead-letter, and dead-letter replay events. Legacy `nack` records remain readable. An idempotent enqueue includes its key, request fingerprint, original response, and expiration in the same event as the message.

For each mutation, the queue:

1. Builds the event.
2. Appends it to the WAL.
3. Calls `fsync`.
4. Updates in-memory state.
5. Returns success to the client.

Startup replays valid events to reconstruct the queue. If a crash leaves a partial final operational record, startup truncates that incomplete tail. A checksum failure in a complete record stops startup instead of silently discarding data.

`-compact-on-start` rewrites accumulated history as a versioned, committed snapshot of current durable state before the server listens. The snapshot retains live messages and active leases, dead letters, unexpired idempotency responses, retry configuration, discipline, and the sequence high-water mark. It deliberately drops completed history and expired producer keys.

Replacement is stop-the-world and crash-safe on supported local Linux and macOS filesystems:

1. Write a private `0600` sibling temp file.
2. Validate and `fsync` the complete snapshot.
3. Rename it over the active WAL without removing the active path first.
4. Switch appends to the already-open replacement descriptor.
5. `fsync` the parent directory before accepting mutations again.

A definite failure before rename leaves the old WAL writable. A directory-sync failure after rename is durability-ambiguous: the process keeps read-only diagnostics available, returns `503 Service Unavailable` from `/healthz`, rejects mutations, and requires restart. Compacted WALs require a snapshot-aware binary; downgrading to the initial event-only binary is unsupported.

Compaction is explicit rather than automatic. Stop the process and restart once with `-compact-on-start` when history growth warrants it. A process crash can leave `.queue.wal.compact-*.tmp` siblings; startup ignores them. Inspect and remove those exact files manually only while the queue is stopped. NFS and other network filesystems are outside the crash-safety claim.

## Concurrency model

Go's HTTP server accepts concurrent requests. A single mutex protects message state, heap membership, and WAL mutation. This prevents two consumers from receiving the same active lease and keeps durable and in-memory state in the same order.

Long polls capture a queue-change generation channel while holding that mutex, then wait outside it on a state change, request cancellation, the client deadline, or the next scheduled-message deadline. This avoids lost wakeups without holding the queue lock or maintaining a persistent waiter registry. A state change broadcasts to all waiters; only the mutex winner can reserve a particular message.

The tradeoff is write throughput: mutations are serialized and each one performs an `fsync`. This design favors simple, inspectable correctness over maximum single-node throughput.

## Design boundaries

This submission intentionally has a narrow scope:

- One queue per process and WAL.
- One machine and one disk; no replication or automatic failover.
- At-least-once delivery, not exactly once.
- Startup-only, stop-the-world WAL compaction; no automatic threshold or HTTP admin trigger.
- No batch endpoints, waiter fairness guarantee, or persistent background scheduler yet. Batch variants are mechanically straightforward but intentionally omitted from the initial API.
- No retry jitter or background lease-expiry timer yet.
- No built-in authentication, TLS termination, rate limiting, tracing, or metrics backend.

The last group belongs at the deployment boundary rather than in the queue core. Do not expose the server directly to an untrusted network; bind it to localhost or place it behind a trusted proxy or service mesh.

## Assignment questions

### How do you handle replay messages?

The lease model handles operational replay. If a consumer crashes or misses its acknowledgement deadline, the next reserve advances the expired lease into deterministic exponential backoff. Explicit `nack` does the same or accepts an override. Each retry receives a new receipt; exhausted messages move to durable dead-letter state after five reservations by default.

Operators can inspect dead letters and replay one intentionally. Replay creates a new message ID and preserves the exhausted ID as `original_message_id`, avoiding ambiguity with stale receipts and consumer deduplication state.

Producer retries use `Idempotency-Key`, which prevents a lost enqueue response from creating a duplicate message. Slow consumers can extend an unexpired lease so ordinary processing time does not create an avoidable retry.

### How would you refactor the queue into Pub/Sub?

I would keep one immutable message log and replace destructive queue state with independent subscription state:

- Publish writes each payload once.
- Every subscription owns its own cursor, leases, and acknowledgement state.
- One subscriber cannot remove data needed by another subscriber.
- Retention removes a log segment only after every durable subscription passes it or its retention window expires.
- Priority and delay become subscription delivery policies rather than properties of one destructive consumer group.

### What would you add with more time?

In order:

1. Power-loss and filesystem fault validation beyond syscall-level and in-process fault tests.
2. Automatic compaction thresholds or segmented online compaction.
3. Retry jitter and an optional background deadline scheduler.
4. Batch endpoints; these reuse the existing single-message state transitions and are intentionally omitted for now.
5. Message quotas, waiter limits, and explicit capacity limits.
6. Replication and failover if the project moves beyond its single-node goal.

### Why choose this over SQS, RabbitMQ, or Pulsar?

Choose Queuemaxxing when the constraint is a small, transparent, dependency-free queue on one host: local development, an appliance, an edge node, or a service where running a broker is disproportionate. It combines priority, delay, and FIFO/LIFO in one API, ships as ordinary Go binaries, and uses an inspectable local event log.

Choose an incumbent when you need stronger operational guarantees. SQS provides managed durability and scale. RabbitMQ provides mature routing and administration. Pulsar provides replicated logs, retention, multi-tenancy, and streaming. Queuemaxxing does not claim those guarantees.

## Development

```sh
make test
make race
make fidelity
```

`make fidelity` additionally runs the compiled server over real TCP, terminates it with `SIGKILL`, reopens the same WAL, exercises a concurrent producer/consumer/lease-extension soak, and checks waiter cleanup. The default suite also includes an independent randomized state-machine oracle and exact WAL append fault injection.

The test suite covers ordering, delay, bounded retries, backoff, dead-letter replay, stale receipts, durable lease extension, long-poll wakeups and cancellation, concurrent expiry, failed writes, restart recovery, torn WAL tails, idempotency, heap membership, snapshot validation, WAL replacement failure boundaries, compaction state equivalence, and the HTTP/CLI lifecycle. The queue package also includes a scan-versus-heap selection benchmark:

```sh
go test ./internal/queue -run '^$' -bench '^BenchmarkReserveSelection$' -benchmem
```
