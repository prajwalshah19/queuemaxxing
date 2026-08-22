# Queue refinement specification

## 1. Status and decision summary

Status: Implemented and verified locally. Indexed scheduling, producer idempotency, bounded retries/dead letters, lease extension, long polling, crash-safe compaction, and one-entrypoint packaging are complete.

Decisions:

- One process owns exactly one logical queue and one WAL.
- FIFO/LIFO discipline is process-wide and persisted in the WAL.
- Named queues are out of scope for this submission. Multiple queues require multiple processes and WAL paths.
- Delivery remains at least once.
- Consumer retries use a persisted process-wide policy with five attempts, one-second base delay, five-minute maximum delay, and durable dead-letter replay.
- Ready-message max-heap and scheduled-message min-heap indexing are implemented.
- Producer idempotency is implemented with an optional 128-byte key, canonical request fingerprint, configurable 24-hour default retention, durable restart replay, and conflict detection.
- Long polling, lease extension, and crash-safe compaction are implemented. Batch operations are deliberately omitted as straightforward API expansion.
- Authentication, TLS termination, distributed tracing, rate limiting, and a full metrics stack are acknowledged production integrations, not submission-critical queue logic.
- `queuemaxxing` now supplies both `serve` and client commands; the standalone `qmctl` remains an optional compatibility binary.

Blocking decisions: none.

## 2. Problem and evidence

The current implementation proves the core queue mechanics but has deliberate scaling and reliability gaps:

- `cmd/queuemaxxing/main.go` opens one `queue.Queue` and mounts one HTTP handler.
- `internal/httpapi/api.go` contains no queue name in any route.
- `internal/queue/queue.go` protects one in-memory message map and one WAL with one mutex.
- Before the current refinement, `Queue.Reserve` scanned the complete message map for every delivery; it now uses ready and scheduled heaps.
- `internal/queue/wal.go` appends and syncs every event but never compacts the log.
- Retry/DLQ transitions are durable and explicit; due lease-expiry transitions are currently advanced by reserve calls rather than a background scheduler.

## 3. Goals and non-goals

Goals:

- Preserve priority + delay + FIFO/LIFO behavior.
- Make transient producer and consumer failures safe and explicit.
- Bound steady-state selection cost and WAL growth.
- Preserve restart durability through every new state transition.
- Keep deployment dependency-free and single-node.
- Keep the API small enough to explain completely in the README.

Non-goals:

- Multiple named queues in one process.
- Replication, consensus, or high availability.
- Exactly-once processing.
- Broker federation, routing exchanges, or Pub/Sub in this implementation.
- Building an identity system, TLS proxy, or observability platform into the queue.

## 4. Existing system and constraints

- Go standard library only.
- One process, one queue, one WAL.
- Every acknowledged mutation must be durable before the HTTP response succeeds.
- The queue may not delegate storage to another database or queue.
- The current WAL format is a custom length-prefixed, CRC32-protected event log.
- FIFO/LIFO affects equal-priority ordering; higher priority always wins.
- Delayed or actively leased messages are not eligible for delivery.
- Receipts fence stale consumers after a later reservation replaces the lease.

## 5. Options considered

### Named queues

Option A: one queue per process.

- Preserves the existing API and WAL model.
- Makes configuration, failure containment, compaction, and explanation simple.
- Multiple queues require multiple processes/ports or an external supervisor.
- Selected for this submission.

Option B: multiple named queues per process.

- Adds queue names to every route and every durable event.
- Requires per-queue capacity, fairness, stats, deletion, configuration, and recovery semantics.
- Creates ambiguity because FIFO/LIFO is process-wide while data is queue-scoped.
- Rejected as scope without improving the core assignment.

### Selection index

Option A: scan all messages on reserve.

- Minimal code and already correct.
- `O(n)` reserve and stats behavior degrades with queue depth.
- Removed from the reserve path by the indexed implementation.

Option B: message map plus ready and scheduled heaps.

- Ready max-heap key: priority descending, then sequence ascending for FIFO or descending for LIFO.
- Scheduled min-heap key: next eligibility timestamp for delays, nacks, retry backoff, and active lease expiry.
- `O(log n)` enqueue, reserve, nack, lease extension, expiry, and retry scheduling.
- Selected.

Option C: one FIFO/LIFO deque per priority plus a time heap.

- Fast ready operations, but priority cardinality and queue bookkeeping are more complex.
- Offers little benefit over a ready heap for this workload.
- Rejected for the initial submission.

### WAL compaction

Option A: stop-the-world live-state rewrite.

- Under the queue mutex, write the current durable state to a temporary generation, sync it, atomically rename it over the old WAL, sync the parent directory, and reopen for append.
- Simple recovery story and no separate storage engine.
- Temporarily blocks queue operations during compaction.
- Selected for the initial submission.

Option B: snapshot plus WAL segments.

- Supports short pauses and large queues by rotating segments and snapshotting concurrently.
- Requires generation manifests, segment retention, and more crash states.
- Deferred until scale requires it.

### Retry policy

Option A: retry forever immediately after lease expiry.

- Matches current behavior.
- Poison messages can loop indefinitely and consume the queue.
- Rejected.

Option B: bounded exponential retries plus internal dead-letter state.

- Attempts count reservations, including the initial delivery.
- A failed attempt is an explicit nack or an expired lease.
- Retry delay is `min(base_delay * 2^(attempt-1), max_delay)` unless nack supplies an override.
- After `max_attempts`, the message moves durably to dead-letter state.
- Selected. Jitter is deferred to keep tests and behavior deterministic.

## 6. Chosen architecture

The process owns:

- One queue configuration.
- One message map keyed by message ID.
- One ready heap.
- One scheduled heap.
- One dead-letter map.
- One bounded producer-idempotency index.
- One WAL writer and compactor.
- One condition variable or notification channel used by long-polling consumers.

Durable events expand to include:

- `enqueue`, optionally containing the atomic idempotency key, request hash, and expiration
- `retry_policy`
- `reserve`
- `ack`
- legacy `nack` events remain readable
- `extend_lease`
- `retry_scheduled`
- `dead_letter`
- `dead_letter_replay`

Long polling waits until one of these occurs:

- A ready message exists.
- A delayed/retrying/expired-lease message becomes eligible.
- A producer enqueues a newly eligible message.
- The request context is cancelled.
- The configured wait timeout expires.

Compaction writes one self-contained representation of current queue, lease, retry, dead-letter, sequence, and unexpired idempotency state. Historical acked events are discarded.

## 7. Contracts and invariants

### External contracts

- `POST /v1/messages` accepts an optional `Idempotency-Key` header.
- Keys are limited to 128 bytes and retained for 24 hours by default; retention is configurable process-wide.
- Repeating a non-expired key with the same canonical body, priority, and delay returns the original result with `Idempotency-Replayed: true` and does not append to the WAL.
- Reusing a key with different input returns `409 Conflict`.
- Ack does not remove the idempotency record; expiration permits a new enqueue.
- `POST /v1/messages/reserve` accepts visibility timeout, long-poll wait time, and maximum batch size.
- Reserve returns at most the requested batch size; each message receives a distinct receipt.
- Lease extension requires the current message ID and receipt.
- Ack and nack require the current receipt.
- A stale receipt always returns `409 Conflict` and never changes durable state.
- Exhausted messages leave the live queue and enter internal dead-letter state atomically.
- Dead-letter replay creates a new live message ID and retains the original ID as metadata.

### Important invariants

| Invariant | Why | Observation | Falsification | Test layer |
|---|---|---|---|---|
| One WAL belongs to one configured queue discipline | Prevents ordering changes after restart | Restart reports the persisted discipline | Opening a FIFO WAL as LIFO succeeds | Integration |
| No two unexpired deliveries of one message have valid receipts | Prevents concurrent ownership | Only the newest receipt can mutate the message | Two consumers can both ack the same lease generation | Concurrency scenario |
| An HTTP success implies its state transition survives restart | Meets durability requirement | Restart exposes the acknowledged state | A successful enqueue disappears | Crash scenario |
| Priority dominates sequence; sequence only breaks equal-priority ties | Preserves queue contract | Delivery order matches the comparator | Lower priority is delivered while higher priority is ready | Property test |
| Delayed, retrying, and actively leased messages are never in the ready heap | Prevents early delivery | Reserve returns no such message | A message arrives before eligibility | Property test |
| A message is either live, leased, scheduled, or dead-lettered, never in conflicting states | Prevents duplication and loss | State inspection has one owner | One ID exists in ready and dead-letter state | Property test |
| Compaction produces state equivalent to replaying the pre-compaction WAL | Prevents compaction data loss | Pre/post restart snapshots match | Any live lease, retry, or key changes after compaction | Crash integration |
| A WAL sync ambiguity stops further mutation | Prevents ghost events and sequence reuse | Subsequent mutations return unavailable | Queue continues after uncertain durability | Failure-injection test |
| One active idempotency key and request produce one message ID | Prevents producer retry duplicates | Concurrent callers receive the original ID | Two enqueue events are written for the same active key | Concurrency scenario |
| Conflicting input for an active idempotency key never mutates state | Prevents accidental key reuse | HTTP returns 409 and WAL size is unchanged | Conflict inserts a message or event | Contract test |

## 8. Failure handling and observability

WAL errors fall into two categories:

- Definite failure before any bytes are written: return an error; state remains unchanged.
- Ambiguous failure after a write or during sync: mark the queue poisoned, reject further mutations with `503 Service Unavailable`, and require restart/recovery.

The queue must not decrement and reuse sequence numbers after an ambiguous append.

Crash-safe compaction protocol:

1. Hold the mutation mutex.
2. Write a complete replacement generation to a sibling temporary file.
3. Sync the replacement file.
4. Atomically rename it over the active WAL.
5. Sync the parent directory so the rename is durable.
6. Open the new generation for append, then close the old descriptor.
7. Ignore or clean stale temporary generations during startup.

The README must state that the submission omits built-in auth, TLS, rate limiting, tracing, and a metrics backend. It must also state that production deployments need those controls at the process or proxy boundary.

Minimum built-in diagnostics remain structured startup/shutdown/error logging, health, and queue state counts. This is enough to diagnose terminal storage failure without turning observability integrations into the project.

## 9. Security and privacy

- WAL and temporary files remain mode `0600`; data directories remain `0700` where created by the process.
- Message bodies are opaque and must not be logged.
- Receipts and idempotency keys must not be logged.
- The queue does not provide authentication or encryption in transit; bind to localhost by default and document that external exposure requires a trusted TLS/auth proxy.
- Compaction must preserve file permissions.

## 10. TDD and verification plan

Add tests in risk order:

1. Retry and dead-letter state-machine table tests.
2. Stale-receipt tests across nack, expiry, lease extension, and retry.
3. Heap-order property tests against the existing scan comparator.
4. Deterministic long-poll wake, timeout, and cancellation tests using an injected clock.
5. Batch atomicity and partial-failure contract tests.
6. Idempotency same-request, conflicting-request, restart, ack, expiry, WAL-no-op, and concurrency tests — implemented locally.
7. Compaction equivalence tests with ready, delayed, leased, retrying, dead-lettered, and deduplicated state.
8. Failure-injection tests at write, sync, rename, directory sync, reopen, and close boundaries.
9. Concurrent producer/consumer/lease-extension/compaction race tests.
10. Live process-kill tests around enqueue, reserve, retry, dead-letter, and compaction.

Required verification evidence:

```sh
go test ./...
go test -race ./...
go vet ./...
```

Add benchmarks for enqueue, reserve, ack, and startup replay at representative queue depths before claiming the index or compaction work improves behavior.

## 11. Implementation slices

### Slice 1: producer idempotency — implemented locally

- Add optional `Idempotency-Key` enqueue semantics and a durable in-memory lookup index.
- Tests cover canonical replay, conflicts, restart after ack, expiry, WAL no-op, failed WAL append, API behavior, CLI propagation, and concurrent producers.
- The WAL change is additive: old enqueue events remain readable.

### Slice 2: bounded consumer retries — implemented locally

- Delivered maximum attempts, exponential backoff, explicit-delay override, stale-receipt fencing, dead-letter transition, inspection, and atomic replay with a new ID.
- Added deterministic clock/restart/event-count/concurrency harnesses plus queue, HTTP, and CLI contracts.
- Verified with unit, WAL-failure, restart, live-process restart, race, and vet gates.
- Existing messages and legacy nack events remain readable through additive WAL events.

### Slice 3: indexed scheduling — implemented locally

- Replace scans with ready and scheduled heaps while keeping external behavior unchanged.
- First failing test: property comparison between heap and reference scan ordering.
- Likely files: new `internal/queue/index.go`, queue integration changes and tests.
- Verify with property tests, race tests, and depth benchmarks.
- Rollback: remove the index and restore scan selection; WAL format remains compatible.

### Slice 4: long polling and lease extension — implemented locally

- Add cancellable waiting and durable lease extension.
- First failing tests: deterministic wake/cancel/extension tests.
- Likely files: queue notifier/timer abstraction, API, CLI, tests.
- Verify with HTTP integration and restart tests.

### Slice 5: batches — deliberately omitted

- Batch enqueue, reserve, ack, and nack reuse the existing single-message state transitions.
- Exact partial-failure semantics would expand the API without exercising new queue fundamentals, so the README acknowledges the omission.

### Slice 6: crash-safe compaction — implemented locally

- Add stop-the-world live-state rewrite and failure injection.
- First failing test: pre/post-compaction state equivalence.
- Likely files: WAL abstraction, injectable filesystem operations, compaction tests.
- Verify every crash boundary plus live process restart.

### Slice 7: README omissions and operational boundaries

- Document single-queue/process scope, at-least-once behavior, omitted infrastructure integrations, and measured limits.
- No feature expansion.

### Slice 8: one-entrypoint packaging — implemented locally

- Merge server and client commands into one `queuemaxxing` binary with `serve`, `put`, `reserve`, `ack`, `nack`, `stats`, and dead-letter commands.
- Provide one obvious local quickstart and optional container packaging.
- Verify from a clean checkout using only the documented commands.

## 12. Rollout, migration, and rollback

- Prefer additive WAL event types until compaction is implemented.
- Persist a WAL format version before shipping new events.
- A new binary must either replay the old format or fail with a clear unsupported-version error.
- Heap indexing is in-memory and reversible without WAL migration.
- Compaction is the first irreversible history-removal feature; preserve a backup generation until the new WAL has reopened successfully.
- Do not push or submit refinement work until all required verification commands pass and the README matches actual behavior.

## 13. Open questions and UNVERIFIED assumptions

- Whether batch ack/nack is atomic or returns per-item results is undecided.
- Representative benchmark depth and acceptable latency are **UNVERIFIED** and must be chosen before performance claims.
- Directory sync portability across supported operating systems is **UNVERIFIED** and needs a capability test or platform-specific implementation.

## 14. Source references

- `cmd/queuemaxxing/main.go`
- `cmd/qmctl/main.go`
- `internal/httpapi/api.go`
- `internal/queue/queue.go`
- `internal/queue/wal.go`
- `internal/queue/queue_test.go`
- `internal/httpapi/api_test.go`
- `README.md`

## Readiness check

- Goals map to contracts and planned tests: yes.
- Invariants are observable and falsifiable: yes.
- Live filesystem assumptions have a capability gate: required, not yet implemented.
- Retry rules, defaults, and dead-letter contracts are implemented and verified: yes.
- Destructive compaction boundaries are explicit: yes.
- First implementation slice is small and executable: yes.
- Unresolved decisions are visible: yes.
