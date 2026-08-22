# Lease extension and long-polling specification

## 1. Status and decision summary

Status: Implemented and verified locally on 2026-08-22.

Decisions:

- Existing immediate reserve behavior remains the default.
- `POST /v1/messages/reserve` gains `wait_timeout_seconds`; `0` means do not wait.
- Long polls wait at most 20 seconds and return `204 No Content` on a normal wait timeout.
- A generation channel plus a per-request timer provides wakeups without a background scheduler.
- Long polls wake for queue mutations and for the next scheduled eligibility deadline.
- Waiting is ephemeral. Waiters and open HTTP requests are not persisted.
- Lease extension sets a new deadline from server time: `now + visibility_timeout_seconds`.
- An extension must move the deadline later; it never shortens a lease.
- Extension requires the current, unexpired receipt.
- Extension does not change the receipt or increment delivery attempts.
- Every successful extension is a new `extend_lease` WAL event and survives restart.
- Lost extension responses may be retried. A retry can move the deadline slightly later, but can never shorten it or create another delivery attempt.
- Retry/DLQ behavior from `2026-08-21-retries-dead-letter.md` lands first. Long polling then drives lease-expiry retry transitions when their scheduled deadlines arrive.

## 2. Problem and evidence

### Current reserve behavior

`Queue.Reserve` in `internal/queue/queue.go`:

1. Locks the queue.
2. Promotes scheduled messages whose deadline has passed.
3. Returns `nil` immediately when the ready heap is empty.
4. Otherwise writes a durable reserve event and returns a leased message.

The HTTP handler maps `nil` to `204 No Content`. An idle consumer must therefore send repeated reserve requests. This creates avoidable HTTP work and forces clients to choose between latency and aggressive polling.

### Current lease behavior

A reservation sets `Receipt` and `LeaseUntil`, then places the message in the scheduled min-heap. The worker can only ack or nack. A job that takes longer than its initial visibility timeout can become eligible while the original worker is still processing it, causing concurrent duplicate work.

### Relevant planned retry behavior

The retry specification changes expired leases from immediate redelivery into a durable retry or dead-letter transition. Long polling must use that state transition rather than moving every expired lease directly into the ready heap.

## 3. Goals and non-goals

Goals:

- Let consumers wait efficiently for the best eligible message.
- Wake consumers when a new message arrives or scheduled work becomes eligible.
- Honor HTTP cancellation without leaking waiters or timer goroutines.
- Avoid lost wakeups between checking the heaps and starting to wait.
- Let active workers extend valid leases durably.
- Preserve priority, delay, FIFO/LIFO, retry, receipt fencing, and restart semantics.
- Keep the implementation single-process, dependency-free, and explainable.

Non-goals:

- FIFO fairness between waiting consumers.
- A persistent waiter registry.
- Server push, WebSockets, or streaming responses.
- A background retry scheduler when there are no reserve requests.
- Per-message visibility limits or lease policies.
- Lease shortening or early release; nack already performs release.
- Exactly-once delivery.
- Batch reserve or batch lease extension.

## 4. Existing system and constraints

- One process owns one queue, ready max-heap, scheduled min-heap, message map, and WAL.
- One mutex protects heap membership, state transitions, and WAL mutation.
- Queue time is currently injected as `now func() time.Time`; deterministic waiting also requires injectable timers.
- Every successful mutation is appended and synced before memory changes.
- The current server `WriteTimeout` is 10 seconds, shorter than the selected 20-second maximum long poll.
- HTTP request bodies are decoded before queue operations begin.
- Receipts are random lease-generation tokens.
- The retry design will fence expired receipts and make lease expiry a durable transition.
- Message bodies, receipts, and idempotency keys must not be logged.

## 5. Options considered

### Long-poll wakeups

#### Option A: keep client-side short polling

- No server changes.
- Wastes requests while idle and makes low latency expensive.
- Rejected because it does not solve the problem.

#### Option B: `sync.Cond`

- Waiters sleep on a condition variable and producers broadcast after mutations.
- Fits the existing mutex but has no native context-cancellation or timeout selection.
- Cancellation requires extra goroutines or global broadcasts, which complicates cleanup.
- Rejected.

#### Option C: generation channel plus per-request timer

- Queue owns `changed chan struct{}` under its mutex.
- A mutation closes the current channel and replaces it with a new one.
- A waiter captures the current channel while holding the lock, unlocks, then selects on change, timer, or context cancellation.
- Capturing before unlock prevents lost wakeups.
- Broadcast wakes all waiters; losers recheck state and sleep again.
- Selected because it is small, cancellable, and needs no registration map or background goroutine.

#### Option D: explicit waiter queue and dispatcher

- Registers each waiter and wakes only selected consumers.
- Can provide fairness and avoid broadcast contention.
- Requires waiter lifecycle, cancellation removal, dispatcher ownership, and shutdown coordination.
- Rejected for the single-node submission. It remains reversible because waiter state is ephemeral.

### Scheduled-time wakeups

#### Option A: background scheduler goroutine

- One queue goroutine owns a timer for the scheduled-heap root and promotes due state proactively.
- Keeps state current without reserve traffic.
- Adds lifecycle, shutdown, timer-reset, and retry-WAL error ownership.
- Rejected for this slice.

#### Option B: one timer per long-poll request

- Each waiter sleeps until the earlier of its request deadline or the scheduled-heap root.
- The first waiter that wakes acquires the mutex and advances due state; all others recheck.
- No background lifecycle and no durable waiter state.
- Selected. The accepted cost is duplicate timers and broadcast contention when many consumers wait.

### Lease-update semantics

#### Option A: add a duration to the current deadline

- `extend_by_seconds` is easy to understand.
- Retrying a request after a lost response can extend twice.
- Rejected because mutation retries are not idempotent or naturally bounded.

#### Option B: accept an absolute client timestamp

- Repeating the same request is idempotent.
- Requires client/server clock agreement and exposes clock-skew failures.
- Rejected.

#### Option C: set visibility from server time

- `visibility_timeout_seconds` sets `LeaseUntil = server_now + duration`.
- Matches reserve terminology and needs no client clock.
- Retrying after a lost response may extend slightly further, but cannot shorten the lease or increment attempts.
- Selected.

### Lease durability

#### Option A: change only memory and the scheduled heap

- Minimal work but restart restores the old deadline and can redeliver early.
- Rejected because it violates restart durability.

#### Option B: reuse the `reserve` WAL event

- Avoids a new event type.
- Existing replay treats reserve as a new attempt and would corrupt attempt counts.
- Rejected.

#### Option C: append `extend_lease`

- Records message ID, current receipt, and new deadline.
- Replay validates the lease generation and updates only its deadline.
- Selected.

## 6. Chosen architecture

### Queue clock

Replace the current `now`-only injection with an internal clock abstraction that supports both current time and stoppable timers:

```go
type Clock interface {
    Now() time.Time
    NewTimer(time.Duration) Timer
}

type Timer interface {
    C() <-chan time.Time
    Stop() bool
}
```

Production uses `time.Now` and `time.NewTimer`. Tests use a manual clock that advances without sleeping. Existing injected-clock tests migrate to the new abstraction.

### Queue notification generation

`Queue` gains:

```go
changed chan struct{}
```

It is initialized during open. Every heap or ownership mutation calls `notifyLocked` after its WAL-backed memory transition:

```go
func (q *Queue) notifyLocked() {
    close(q.changed)
    q.changed = make(chan struct{})
}
```

Close wakes all waiters; waiters then observe `ErrClosed`. The close path must not replace the channel after setting `closed`.

Notifications are hints, not ownership grants. Every woken waiter must reacquire the mutex and re-evaluate the queue.

### Long-poll reserve loop

Keep the existing method as a compatibility wrapper:

```go
func (q *Queue) Reserve(visibility time.Duration) (*Delivery, error) {
    return q.ReserveWait(context.Background(), visibility, 0)
}
```

Add:

```go
func (q *Queue) ReserveWait(
    ctx context.Context,
    visibilityTimeout time.Duration,
    waitTimeout time.Duration,
) (*Delivery, error)
```

Algorithm:

1. Validate both durations before locking.
2. Calculate one absolute request deadline from the queue clock.
3. Lock and reject a closed queue.
4. Advance due scheduled state using the retry/DLQ state machine.
5. If a ready message exists, reserve the highest-ranked message durably and return it.
6. If `waitTimeout == 0` or the request deadline has passed, return no message.
7. Capture `q.changed` while still holding the mutex.
8. Calculate the next timer deadline as the earlier of the request deadline and scheduled-heap root.
9. Unlock and wait for the captured change channel, timer, or `ctx.Done()`.
10. Stop and drain the timer safely, then repeat.

The loop checks ready state before the wait deadline. A message eligible when the queue acquires the lock at the deadline may be returned instead of `204`.

There is no fairness guarantee among concurrent waiters. The mutex winner reserves the best eligible message; other waiters recheck and continue waiting.

### Lease extension

Add:

```go
type LeaseExtension struct {
    LeaseUntil time.Time `json:"lease_until"`
}

func (q *Queue) ExtendLease(
    id string,
    receipt string,
    visibilityTimeout time.Duration,
) (LeaseExtension, error)
```

Under the queue mutex:

1. Validate a positive timeout.
2. Look up the live message.
3. Require a non-empty receipt that matches the current receipt.
4. Require `now < LeaseUntil`; an expired lease cannot be revived.
5. Calculate `newLeaseUntil = now + visibilityTimeout` with overflow-safe duration validation.
6. Require `newLeaseUntil > current LeaseUntil`; extension never shortens or preserves the deadline.
7. Append and sync `extend_lease` with message ID, receipt, and new deadline.
8. Update `LeaseUntil` and the scheduled-heap key using `heap.Fix` or an equivalent remove/push operation.
9. Notify waiters so they replace timers based on the old earlier deadline.
10. Return the new deadline.

No new receipt is issued and `Attempts` does not change.

### WAL replay

Add event type:

```text
extend_lease {
  id,
  receipt,
  lease_until
}
```

Replay requires:

- The message exists.
- The receipt matches the active lease generation.
- The new deadline is later than the stored deadline.

Invalid extension history stops startup. Indexes are rebuilt after complete replay, so no replay-time heap mutation is needed.

### HTTP surface

Long poll:

```http
POST /v1/messages/reserve
Content-Type: application/json

{
  "visibility_timeout_seconds": 30,
  "wait_timeout_seconds": 20
}
```

- Default `wait_timeout_seconds`: `0`, preserving immediate reserve.
- Maximum: `20` seconds.
- Negative or larger values: `400 Bad Request`.
- Normal wait timeout with no ready message: `204 No Content`.
- Queue storage failure while advancing retries or reserving: `500 Internal Server Error`.
- Client cancellation before reserve linearization: no queue mutation and no response guarantee.
- Cancellation after durable reserve linearization can lose the response while leaving a valid lease; the message later retries under at-least-once semantics.

Lease extension:

```http
POST /v1/messages/{id}/lease
Content-Type: application/json

{
  "receipt": "...",
  "visibility_timeout_seconds": 60
}
```

Success:

```json
{
  "status": "lease_extended",
  "lease_until": "2026-08-21T18:01:00Z"
}
```

- Missing message, wrong receipt, or expired receipt: `409 Conflict`.
- Invalid duration or a deadline that does not extend the lease: `400 Bad Request`.
- WAL failure: `500 Internal Server Error` with no in-memory change.

### CLI surface

Extend `get`:

```sh
qmctl get -visibility 30s -wait 20s
```

Add:

```sh
qmctl extend -visibility 60s MESSAGE_ID RECEIPT
```

The single-entrypoint implementation carries the same flags and contracts in `queuemaxxing reserve` and `queuemaxxing extend`; `qmctl get` remains a compatibility alias.

### HTTP server deadlines

Increase the server `WriteTimeout` from 10 seconds to 30 seconds while maximum long poll remains 20 seconds. If maximum wait becomes configurable later, startup validation must require enough response headroom or configure per-handler write deadlines.

## 7. Contracts and invariants

| Invariant | Why | Observation | Smallest falsification | Test layer |
|---|---|---|---|---|
| `wait_timeout_seconds: 0` preserves immediate reserve behavior | Backward compatibility | Empty queue returns `204` without waiting | Existing client blocks | HTTP contract |
| A state change after an empty check cannot be missed by a waiter | Prevents avoidable timeout latency | Enqueue immediately wakes a waiter captured before the mutation | Message is ready but waiter sleeps until deadline | Deterministic concurrency |
| A long poll returns only the highest-ranked message eligible at reserve linearization | Preserves priority and FIFO/LIFO | Returned ID matches ready-heap root | Lower-ranked message is delivered | Property + integration |
| One live message creates at most one active reservation | Prevents concurrent duplicate ownership | One waiter gets `200`; others continue or time out | Two waiters receive the same ID/lease generation | Race scenario |
| Normal wait timeout performs no durable mutation | Avoids WAL noise | WAL size and state are unchanged after `204` | Timeout appends an event | Failure/contract |
| Cancellation before reserve linearization performs no durable mutation | Honors abandoned waits | WAL size and state are unchanged | Cancelled waiter creates a lease | Deterministic cancellation |
| Queue close wakes all long polls | Prevents goroutine leaks and hanging shutdown | Every waiter returns `ErrClosed` | Shutdown waits for poll timeout | Concurrency scenario |
| Only a current, unexpired receipt can extend a lease | Fences stale workers | Wrong/expired receipt returns `409` with unchanged WAL | Stale worker changes deadline | Contract + restart |
| Extension strictly increases `LeaseUntil` | Prevents lease shortening | New deadline is later than old deadline | Extension makes work eligible earlier | Unit/property |
| Extension changes neither receipt nor attempts | Preserves delivery-generation identity | Response/restart shows same values | Extension creates a new attempt or receipt | Unit + restart |
| A successful extension survives restart | Meets durability requirement | Reopened queue hides the message until extended deadline | Old deadline returns after restart | Restart integration |
| Failed extension append leaves heap and message unchanged | Prevents memory/disk divergence | State snapshot and WAL boundary do not change | Heap moves after error | Failure injection |
| Extending the scheduled-heap root wakes waiters to recompute timers | Prevents retry at the old deadline | No waiter advances the lease at the former deadline | Extended message retries early | Deterministic concurrency |
| Retry/DLQ transition and extension are linearized by the queue mutex | Resolves expiry races | Exactly one transition wins | Message is both extended and retried/dead-lettered | Race scenario |

## 8. Failure handling and observability

- Long-poll timeout is a normal empty result, not an error.
- Context cancellation is returned from the queue layer and normally produces no HTTP write because the client connection is already gone.
- Timer creation has no durable effect.
- Any retry/DLQ WAL failure discovered while a long poll advances scheduled state ends that reserve with `500` and leaves state unchanged according to the retry spec.
- Extension validates before append. A definite append failure leaves the message and scheduled heap unchanged.
- Post-write `fsync` ambiguity remains governed by the future poisoned-queue design and must not be claimed as solved here.
- Closing the queue broadcasts once, then closes the WAL after waiters can observe `closed` under the mutex.
- Do not log bodies, receipts, or idempotency keys. Error logs may include message ID and operation name.
- `GET /v1/stats` remains unchanged; waiter count is not part of the queue contract.

## 9. Security and privacy

- Long polling holds one HTTP handler goroutine and timer per waiting client.
- The 20-second cap bounds individual request lifetime but not concurrent waiter count.
- External authentication and rate limiting remain deployment responsibilities; an untrusted public listener can be exhausted by long polls.
- Lease extension can postpone retry indefinitely through repeated valid requests. This is intentional worker ownership, bounded per call by duration validation but not by total lease age.
- Receipts are capabilities and must remain in request bodies, never URLs or logs.
- No new persistent sensitive fields are added beyond the existing receipt and deadline.

## 10. TDD and verification plan

Build tests in risk order with a manual clock and real temporary WAL:

1. `TestReserveWaitReturnsReadyMessageImmediately`
2. `TestReserveWaitZeroPreservesImmediateEmptyResult`
3. `TestReserveWaitTimesOutWithoutMutation`
4. `TestEnqueueWakesReserveWait`
5. `TestDelayedMessageWakesAtEligibility`
6. `TestReserveWaitCancellationDoesNotReserve`
7. `TestCloseWakesAllReserveWaiters`
8. `TestConcurrentWaitersReserveMessageOnce`
9. `TestReserveWaitHasNoLostWakeup` with a barrier at capture/unlock
10. `TestReserveWaitPreservesPriorityAndDiscipline`
11. `TestExtendLeasePersistsNewDeadline`
12. `TestExtendLeaseDoesNotChangeReceiptOrAttempts`
13. `TestExtendLeaseRejectsWrongExpiredAndNonExtendingRequests`
14. `TestExtendLeaseWALFailureDoesNotMutateState`
15. `TestExtendLeaseSurvivesRestart`
16. `TestExtendLeaseReschedulesHeapRootAndWaitingTimer`
17. `TestExtendLeaseRaceWithAckNackAndExpiry`
18. Retry integration: expired extended lease schedules exactly one retry or dead letter
19. HTTP tests for validation, `204`, wakeup, cancellation, extension success, and errors
20. CLI request-construction tests for `get -wait` and `extend`
21. Live-process scenario: long poll wakes on enqueue; extended lease survives restart and is not returned at its old deadline

Required evidence:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

Add a goroutine-leak regression test around repeated timeout/cancel cycles. If timing-based tests remain after the manual clock lands, label them **UNVERIFIED** until stress runs show they are stable.

## 11. Implementation slices

### Dependency: bounded retries and dead letters

- Implement `docs/specs/2026-08-21-retries-dead-letter.md` first.
- Long polling must call its error-returning scheduled-state advance function.
- Safe stopping point: retry tests and race suite pass before this work begins.

### Slice 1: durable lease extension core

- First failing tests: extension validation, unchanged attempts/receipt, WAL failure, and restart.
- Add `extend_lease` replay and scheduled-heap deadline update.
- Likely files: `internal/queue/queue.go`, `internal/queue/index.go`, new `internal/queue/lease_test.go`.
- Verify queue package unit, restart, and race tests.
- Rollback requires a pre-extension WAL copy once an `extend_lease` event has been written because the old binary rejects unknown events.

### Slice 2: notification and clock foundation

- First failing tests: enqueue wakeup, cancellation, close wakeup, and no-lost-wakeup barrier.
- Add manual timer-capable clock and generation-channel notification.
- Thread notification calls through every live ownership/heap mutation, including retries, dead-letter transitions, and extension.
- Likely files: new `internal/queue/clock.go`, new `internal/queue/wait.go`, queue/index/retry integration, focused tests.
- Safe stopping point: notification exists but the existing immediate `Reserve` contract remains unchanged.

### Slice 3: queue long polling

- First failing tests: timeout, delayed eligibility, multiple waiters, ordering, and retry integration.
- Implement `ReserveWait`; retain `Reserve` as its zero-wait wrapper.
- Verify no WAL change on timeout/cancel and no duplicate reservations under race.

### Slice 4: HTTP and CLI long polling

- Add `wait_timeout_seconds`, 20-second validation, request-context propagation, and `qmctl get -wait`.
- Increase server write timeout to 30 seconds.
- First failing tests: API timeout/wakeup/cancel plus CLI request construction.

### Slice 5: HTTP and CLI lease extension

- Add `/v1/messages/{id}/lease` and `qmctl extend`.
- First failing tests: status/error mapping and outbound request construction.
- Keep receipts in JSON bodies.

### Slice 6: scenario verification and documentation

- Run unit, race, vet, leak regression, and live restart scenarios.
- Update README only after observed behavior matches this spec.
- Mark the parent refinement spec complete for these features only after every gate passes.

## 12. Rollout, migration, and rollback

- `wait_timeout_seconds` is additive and defaults to zero, so old HTTP clients keep immediate polling behavior.
- Long polling adds no durable state and can be disabled by clients without migration.
- `extend_lease` is an additive WAL event. New binaries can read existing WALs.
- After the first successful extension, an old binary cannot replay that WAL because it rejects unknown events. Keep a pre-upgrade WAL copy for binary rollback until WAL versioning is added.
- Deploy retries/DLQ first, then lease extension, then long polling.
- During rollback to a build that supports retry but not long polling, clients must return to wait `0`; no data migration is needed if no extension event was written.
- Do not commit, push, or update README claims as implemented until verification passes.

## 13. Open questions and UNVERIFIED assumptions

- The 20-second maximum wait and 30-second server write timeout are submission defaults, not workload-derived.
- Broadcast wakeups are expected to be acceptable for a single-node submission; behavior with thousands of concurrent waiters is **UNVERIFIED**. Benchmark before making scale claims.
- There is no fairness guarantee between waiting consumers. Adding one would require the rejected waiter-dispatcher design.
- A repeated extension after a lost response may extend slightly further. Adding idempotent mutation keys is deferred unless this behavior proves unacceptable.
- Total lease age is unbounded while the current worker continues to extend successfully.
- Post-write `fsync` ambiguity remains **UNVERIFIED** until poisoned-queue handling exists.
- Exact timer behavior under large wall-clock jumps is **UNVERIFIED**. The implementation must use durations derived from queue-clock deadlines and include clock-jump tests where practical.

Blocking decisions: none. The selected defaults and tradeoffs are explicit and reversible except for writing the additive WAL event.

## 14. Source references

- `internal/queue/queue.go`: current reserve, ack, nack, event replay, mutex, and clock injection.
- `internal/queue/index.go`: ready/scheduled heaps and next eligibility timestamp.
- `internal/queue/wal.go`: append-and-sync durability and replay behavior.
- `internal/httpapi/api.go`: current reserve contract and duration validation.
- `cmd/qmctl/main.go`: current `get` client surface.
- `cmd/queuemaxxing/main.go`: HTTP server deadline configuration.
- `internal/queue/queue_test.go`: injected-time, restart, concurrency, and heap tests.
- `internal/httpapi/api_test.go`: current network-free API tests.
- `docs/specs/2026-08-21-retries-dead-letter.md`: prerequisite expiry and retry state machine.
- `docs/specs/2026-08-21-queue-refinement.md`: parent design and implementation sequence.

## Readiness check

- Every goal maps to a contract or test: yes.
- Every invariant is observable and falsifiable: yes.
- Long-poll cancellation and timeout rules are explicit: yes.
- Lease-extension retry/idempotency behavior is explicit: yes.
- Retry/DLQ dependency is explicit: yes.
- Durable versus ephemeral state is explicit: yes.
- Migration and irreversible WAL boundaries are explicit: yes.
- First implementation slice is small and executable: yes.
- Unresolved assumptions are visible: yes.
- Implementation matches the selected contracts: yes.
