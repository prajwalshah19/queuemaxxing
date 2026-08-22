# Retry and dead-letter mini specification

## 1. Status and decision summary

Status: Implemented locally and verified on 2026-08-21.

Verification completed:

- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `go vet ./...`
- Live process restart with a scheduled retry: hidden before the persisted timestamp and redelivered afterward with the same ID and incremented attempts.

Decisions:

- Delivery remains at least once.
- Attempts count successful reservations, including the initial delivery.
- A failed attempt is either an explicit `nack` or an expired visibility lease.
- Retry is bounded by a process-wide persisted policy: 5 attempts, 1 second base delay, and 5 minute maximum delay by default.
- Automatic retry delay is `min(base_delay * 2^(attempts-1), max_delay)` with overflow-safe saturation and no jitter.
- An explicit nack delay overrides automatic backoff, including an explicit zero for immediate retry, but never bypasses `max_attempts`.
- Exhausted messages move atomically into an internal dead-letter state stored in the same WAL.
- Dead-letter replay removes the dead letter and creates a new live message ID with attempts reset to zero and the original ID retained as metadata.
- Lease expiry is advanced by reserve calls in this slice. A background timer is deferred to long polling.

## 2. Existing behavior and problem

Current behavior:

- `Queue.Reserve` increments `Attempts` and places the message in the scheduled min-heap until its lease expires.
- `promoteEligible` currently moves an expired lease directly back to the ready max-heap.
- `Queue.Nack` clears the receipt and uses only the caller-supplied delay.
- No maximum-attempt, backoff, or dead-letter state exists.
- An expired receipt can currently ack or nack until another reservation replaces it.

Consequences:

- Poison messages retry forever.
- Lease expiry retries immediately and can create a hot loop.
- Operators cannot inspect or intentionally replay exhausted work.
- Retry state is not explicit enough for future WAL compaction to preserve.

## 3. Goals and non-goals

Goals:

- Bound consumer retries.
- Apply deterministic exponential backoff.
- Make retry and dead-letter transitions durable before returning success.
- Fence expired and stale receipts.
- Preserve priority, delay, FIFO/LIFO, heap consistency, restart durability, and producer idempotency.
- Provide minimal HTTP and CLI operations for dead-letter inspection and replay.

Non-goals:

- Retry jitter.
- Per-message retry policies.
- A separate dead-letter process, database, queue, or WAL.
- Background retry scheduling before long polling is implemented.
- Bulk dead-letter replay.
- Automatic classification of permanent versus transient consumer errors.

## 4. Options considered

### Retry policy

1. Retry forever immediately: current behavior; rejected because poison messages never terminate.
2. Bounded fixed delay: simple but treats every retry identically; rejected because it creates unnecessary repeated load during longer failures.
3. Bounded deterministic exponential backoff: selected because it is small, predictable, and straightforward to test. Jitter can be added later if synchronized retries become material.

### Dead-letter storage

1. Separate WAL: rejected because moving a message would require a cross-file atomic transaction.
2. Same WAL plus an internal dead-letter map: selected because one event can atomically transfer ownership from live state to dead-letter state.

### Replay identity

1. Reuse the old message ID: rejected because consumer deduplication and stale receipt history become ambiguous.
2. Create a new ID and retain `original_message_id`: selected because replay is a new delivery lifecycle with traceable provenance.

## 5. External contracts

### Process configuration

Add server flags:

```text
-max-attempts 5
-retry-base-delay 1s
-retry-max-delay 5m
```

Validation:

- `max_attempts >= 1`
- `base_delay > 0`
- `max_delay >= base_delay`

The policy is persisted once as a `retry_policy` WAL event. Opening a WAL with a different requested policy fails clearly, matching the existing persisted-discipline behavior. Opening an older WAL without a policy appends the requested/default policy after successful replay.

### Nack

```http
POST /v1/messages/{id}/nack
Content-Type: application/json

{}
```

An omitted `delay_seconds` applies automatic backoff. A provided value overrides it:

```json
{"receipt":"...","delay_seconds":0}
```

Response before exhaustion:

```json
{"status":"retry_scheduled","visible_at":"...","attempts":2}
```

Response at exhaustion:

```json
{"status":"dead_lettered","attempts":5}
```

### Dead-letter inspection

```http
GET /v1/dead-letters?limit=100
```

- Default limit: 100.
- Maximum limit: 1000.
- Stable order: `dead_lettered_at` ascending, then original sequence ascending.
- Message bodies are returned because this is the queue's explicit inspection API; they must never be written to logs.

### Dead-letter replay

```http
POST /v1/dead-letters/{id}/replay
Content-Type: application/json

{"delay_seconds":0}
```

The response is `201 Created` with a new message:

- New random ID and sequence.
- Same body and priority.
- Attempts reset to zero.
- `original_message_id` set to the dead-letter message ID.
- The dead-letter entry is removed atomically with creation of the live message.

Unknown dead-letter IDs return `404 Not Found`.

## 6. State and WAL changes

### Queue state

Add:

```go
type RetryPolicy struct {
    MaxAttempts int
    BaseDelay   time.Duration
    MaxDelay    time.Duration
}

type DeadLetter struct {
    Message
    DeadLetteredAt time.Time
    Reason         string
    Sequence       uint64
}
```

`Queue` gains:

```go
retryPolicy RetryPolicy
deadLetters map[string]*DeadLetter
```

`Message` gains optional `OriginalMessageID` metadata. `Stats` gains `DeadLetters`.

### WAL events

Add:

```text
retry_policy
retry_scheduled
dead_letter
dead_letter_replay
```

`retry_scheduled` contains message ID, current receipt, retry timestamp, attempt count, and reason (`nack` or `lease_expired`).

`dead_letter` contains message ID, current receipt, dead-letter timestamp, attempt count, and reason (`max_attempts`).

`dead_letter_replay` contains the old dead-letter ID and complete new stored message. It is one event so replay cannot remove the dead letter without creating its replacement.

Existing `enqueue`, `reserve`, `ack`, `nack`, and WAL files remain readable. New WALs are not readable by the old binary after `retry_policy` or another new event is written; rollback therefore requires the pre-upgrade binary plus a pre-upgrade WAL copy.

## 7. State transitions

### Explicit nack

Under the queue mutex:

1. Verify the message exists, the receipt is current, and the lease has not expired.
2. If `Attempts >= MaxAttempts`, append `dead_letter`, then remove the message from its heap/live map and add it to `deadLetters`.
3. Otherwise calculate or apply the retry delay, append `retry_scheduled`, clear the receipt and lease, set `VisibleAt`, and place the message in the scheduled or ready heap.
4. Mutate memory only after the WAL append and sync succeed.

### Lease expiry

Replace `promoteEligible(now)` with an error-returning scheduled-state advance:

1. Ordinary delayed messages whose `VisibleAt <= now` move to the ready heap without a WAL event.
2. Messages with an expired receipt are failed exactly once.
3. Before exhaustion, append `retry_scheduled` and schedule backoff.
4. At exhaustion, append `dead_letter` and move out of live state.
5. `Reserve` continues advancing due entries until it can return a ready message, no message, or a WAL error.

### Ack and nack fencing

Both operations require:

```text
receipt matches AND now < lease_until
```

Expired or replaced receipts return `409 Conflict` and do not mutate state. The next reserve call advances the expired lease into retry or dead-letter state.

## 8. Invariants

| Invariant | Why | Observation | Smallest falsification | Test layer |
|---|---|---|---|---|
| Attempts increment exactly once per successful reservation | Prevents premature exhaustion | Returned and replayed attempts increase by one | One reserve increments twice | Unit + restart |
| A failed non-final attempt is live only as a scheduled retry | Prevents duplicate ownership | ID exists once in scheduled heap/map | ID is both ready and scheduled | State-machine/property |
| A final failure is dead-lettered and not live | Prevents infinite retry and duplicate ownership | ID exists only in dead-letter map | ID remains in a heap after dead-lettering | State-machine/property |
| No retry is delivered before `VisibleAt` | Enforces backoff | Reserve returns no message before injected clock reaches timestamp | Early delivery | Deterministic clock |
| Expired and replaced receipts cannot ack or nack | Fences stale consumers | Operation returns 409 and WAL size is unchanged | Expired receipt mutates state | Contract |
| Every successful retry/dead-letter transition survives restart | Meets durability contract | Reopened state equals pre-close state | Scheduled retry disappears or dead letter revives | Restart integration |
| A WAL append failure leaves indexes and maps unchanged | Prevents half-transitions | Before/after state snapshot matches | Heap changes despite error | Failure path |
| Dead-letter replay atomically removes old ID and creates one new ID | Prevents loss or double replay | Restart shows only the new live message | Neither or both ownership states remain | Restart integration |
| Retry does not change priority or FIFO/LIFO sequence semantics | Preserves queue contract | Reference ordering and heap ordering agree | Retried lower-priority item jumps a ready higher-priority item | Property |
| Concurrent discovery of one expired lease creates one transition | Prevents duplicate WAL events | One retry/dead-letter event exists | Two transitions for one lease generation | Race scenario |

## 9. Failure behavior

- Append/sync succeeds: apply the exact transition in memory and return success.
- Definite WAL failure: return `500`, leave the message, receipt, maps, and heaps unchanged.
- Ambiguous sync failure: remains part of the later crash-safety/poisoned-queue slice; this feature must not claim protection from that case.
- Dead-letter replay WAL failure leaves the original dead letter intact.
- Backoff calculation saturates at `MaxDelay` without integer overflow.
- Restart rejects corrupt or impossible events, including retry events with the wrong receipt or attempts.

## 10. TDD harness and test order

Add `internal/queue/harness_test.go` with a real temporary WAL, injected clock, reopen operation, state snapshot, WAL event counter, and concurrent start barrier.

First failing tests:

1. `TestRetryBackoffTable`
2. `TestNackDelayOverride`
3. `TestLeaseExpirySchedulesRetry`
4. `TestMaxAttemptsMovesMessageToDeadLetter`
5. `TestExpiredReceiptCannotAckOrNack`
6. `TestScheduledRetrySurvivesRestart`
7. `TestDeadLetterSurvivesRestart`
8. `TestDeadLetterReplayCreatesNewIdentityAtomically`
9. `TestRetryPreservesPriorityAndDiscipline`
10. `TestConcurrentLeaseExpiryWritesOneTransition`
11. `TestRetryWALFailureDoesNotMutateState`
12. `TestRetryPolicyPersistsAndRejectsMismatch`
13. HTTP contract tests for nack, inspection, replay, status, and errors
14. CLI request-construction tests for nack and dead-letter commands

Required evidence:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

Add one live process restart scenario that leaves a message in retry delay, restarts the binary, and confirms it is delivered only after the persisted retry timestamp.

## 11. Changes by file

- `internal/queue/retry.go`: retry policy validation, overflow-safe backoff, nack/lease-expiry failure transition, dead-letter transition, and replay logic.
- `internal/queue/queue.go`: add durable types/events/state, make reserve advance scheduled failures, fence expired receipts, extend stats, and replay new events.
- `internal/queue/index.go`: replace unconditional scheduled-to-ready promotion with hooks that distinguish ordinary delay from expired leases.
- `internal/queue/harness_test.go`: reusable injected-clock/restart/event-count/concurrency harness.
- `internal/queue/retry_test.go`: invariant, state-machine, failure-path, restart, and race tests.
- `internal/httpapi/api.go`: nullable nack delay, dead-letter list/replay endpoints, result/error mapping.
- `internal/httpapi/api_test.go`: HTTP contracts without network sockets.
- `cmd/queuemaxxing/main.go`: retry-policy flags passed into `queue.Config`.
- `cmd/qmctl/main.go`: nack automatic/override semantics plus `dead list` and `dead replay` commands.
- `cmd/qmctl/main_test.go`: outbound request and flag tests.
- `README.md`: retry formula, defaults, DLQ API/CLI, at-least-once/idempotent-consumer requirement, and omitted jitter/background scheduler.
- `docs/specs/2026-08-21-queue-refinement.md`: mark retries/DLQ complete only after all gates pass.
- `internal/queue/wal.go`: no framing change expected; only the serialized event schema expands.
- `internal/queue/idempotency.go`: no behavioral change expected.

## 12. Implementation slices

1. Add the harness, retry policy, and backoff table tests.
2. Implement explicit nack retry/dead-letter transitions and restart replay.
3. Implement lease-expiry retry/dead-letter transitions and stale-receipt fencing.
4. Implement dead-letter inspection and atomic replay.
5. Add HTTP and CLI surfaces.
6. Run race, restart, and live-process gates; update README and parent refinement status.

Each slice stops safely with additive WAL events. Do not commit or push until the full retry/DLQ contract passes.

## 13. Open questions and UNVERIFIED assumptions

- The default values `5 attempts / 1s base / 5m max` are selected for this submission, not derived from workload measurements.
- Acceptable dead-letter list latency and maximum dead-letter cardinality are **UNVERIFIED**; the first implementation sorts the in-memory map because this is an administrative path.
- Whether long polling should advance retries without an active reserve request is deferred to the long-polling design.
- Full protection from post-write `fsync` ambiguity is deferred to the crash-safety slice and must remain documented.

## 14. Readiness check

- Every goal maps to a contract and test: yes.
- Ordering, retry, receipt, durability, and replay invariants are falsifiable: yes.
- Retry and idempotency interaction is explicit: yes; dead-letter replay does not reuse producer idempotency keys.
- Destructive transitions are single-event and WAL-first: yes.
- The first implementation slice is small and executable: yes.
- Blocking decisions: none; defaults are explicit and configurable.
- Implementation and required verification gates: complete locally.
