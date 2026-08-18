# Queuemaxxing 💪

A deliberately small HTTP queue with composable priority, delay, and FIFO/LIFO ordering. It is a single Go process with no runtime dependencies and no external database or queue.

This is an initial design pass, not a production replacement for SQS, RabbitMQ, or Pulsar.

## Semantics

- Higher integer priority is delivered first.
- `fifo` or `lifo` breaks ties between messages with the same priority.
- A delayed message is invisible until `visible_at`.
- A reserved message is invisible for its lease. It must be acked or nacked.
- An expired lease makes the message eligible again: delivery is **at least once**.
- Every mutation is appended to a checksummed WAL and `fsync`ed before the HTTP call succeeds.
- A mutex serializes selection and WAL mutation, so concurrent consumers cannot hold the same active lease.

Durability is local to one machine and one disk. There is no replication or high availability in this pass.

## Run it

Requires Go 1.25+.

```sh
make build
./bin/queuemaxxing -discipline fifo -data data/queue.wal
```

The discipline is persisted in the WAL. Restart with the same value.

In another shell, use the included sample client:

```sh
./bin/qmctl put -priority 10 -delay 5s '{"task":"send-email"}'
./bin/qmctl put -priority 1 '{"task":"cleanup"}'
./bin/qmctl stats
./bin/qmctl get -visibility 30s
./bin/qmctl ack MESSAGE_ID RECEIPT
# or return work to the queue:
./bin/qmctl nack -delay 10s MESSAGE_ID RECEIPT
```

To get priority LIFO behavior instead:

```sh
./bin/queuemaxxing -discipline lifo -data data/lifo.wal
```

## HTTP API

### Enqueue

```http
POST /v1/messages
Content-Type: application/json

{"body":{"task":"send-email"},"priority":10,"delay_seconds":5}
```

### Reserve

```http
POST /v1/messages/reserve
Content-Type: application/json

{"visibility_timeout_seconds":30}
```

Returns `204` when no message is ready. A successful response contains `id`, `body`, `priority`, `attempts`, `receipt`, and `lease_until`.

### Ack or nack

```http
POST /v1/messages/{id}/ack
{"receipt":"..."}
```

```http
POST /v1/messages/{id}/nack
{"receipt":"...","delay_seconds":10}
```

Receipts change on every reservation. An old receipt gets `409 Conflict`, which prevents a slow consumer from acknowledging a newer consumer's lease.

### Inspect

```http
GET /v1/stats
GET /healthz
```

## Storage design

The WAL is a sequence of length-prefixed JSON events with a CRC32 checksum. Each enqueue, reserve, ack, and nack is persisted and synced before in-memory state changes. Startup replays those events. A partial final frame from a crash is truncated; checksum failure in a complete frame stops startup instead of silently losing data.

This keeps the implementation inspectable and meets the “no separate queue or database” constraint. Its obvious tradeoffs are an ever-growing log, `O(n)` message selection, one writer, and no replication.

## Additional questions

### How do you handle replay messages?

The implemented lease/receipt model handles operational redelivery. If a consumer crashes or never acknowledges, its lease expires and the message is reserved again with `attempts + 1` and a new receipt. Explicit `nack` can replay immediately or after another delay. Consumers still need idempotency because the guarantee is at least once.

For intentional historical replay, I would add immutable topic offsets or a dead-letter/archive log rather than overload the live work queue. A replay request would copy selected records back into the live queue with a new message ID plus `original_message_id` and `replay_reason` metadata.

### How would you refactor this into Pub/Sub?

Keep one immutable append-only message log and replace the single destructive queue state with a cursor plus lease state per subscription. Publishing appends once. Each subscription independently applies its own priority/delay policy and advances only when that subscriber acknowledges. Add retention so old log segments are removed only after every durable subscription passes them (or its retention window expires). Fan-out then costs one payload write, not one copy per subscriber.

### What would you add with more time?

In roughly this order:

1. WAL compaction/snapshots and bounded disk usage.
2. Long polling and batch enqueue/reserve/ack.
3. Dead-letter policy, max attempts, retention, and explicit replay tooling.
4. Idempotency keys and message size/count quotas.
5. Metrics, tracing, auth/TLS, rate limits, and an admin UI.
6. A heap/index for ready and delayed messages instead of `O(n)` scans.
7. Replicated consensus, failover, and online backups if the scope becomes distributed.

### Why choose this over SQS, RabbitMQ, or Pulsar?

Choose it only when a tiny, transparent, zero-dependency, single-node queue is the feature: local development, an appliance, an edge host, or a small service where operating a broker is disproportionate. Priority + delay + FIFO/LIFO are available through one compact API, the on-disk format is inspectable, and deployment is one static binary.

Choose an incumbent for serious production workloads. SQS provides managed durability and scale; RabbitMQ has mature routing and operational tooling; Pulsar provides distributed logs, retention, and multi-tenant streaming. This first pass intentionally does not compete with those guarantees.

## Validate

```sh
make test
make race
```
