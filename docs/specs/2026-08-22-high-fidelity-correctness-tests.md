# High-fidelity correctness test specification

## 1. Status and decision summary

Status: Implemented and verified locally on 2026-08-22.

Decisions:

- Keep fast deterministic tests in the default `go test ./...` suite.
- Put subprocess and soak scenarios behind the `fidelity` build tag and a single `make fidelity` entrypoint.
- Add an independent randomized queue model; do not use production heap or retry helpers as the oracle.
- Refactor the WAL file field behind a narrow internal interface so tests can inject exact append failures without changing the on-disk format.
- Treat a successful frame write followed by `fsync` failure as durability-ambiguous: poison the WAL and reject later mutations until restart.
- Use the actual compiled server and real TCP for black-box crash/restart scenarios.
- Exercise the shipped `queuemaxxing` binary as both server and client through a complete retry/dead-letter/replay lifecycle and startup compaction.
- Use bounded operation counts and deterministic seeds so failures are reproducible.
- Do not claim power-loss durability from `SIGKILL`; that remains a filesystem capability boundary.

## 2. Problem and evidence

The existing suite exercises queue behavior with real temporary WALs, deterministic clocks, direct HTTP handlers, and the Go race detector. Queue package statement coverage is 82.7%. It does not currently provide:

- an independent state-machine oracle over mixed operations and restarts;
- deterministic partial-write and sync-failure injection at the WAL boundary;
- an automated compiled-process/TCP restart scenario;
- a sustained producer/consumer/lease-extension workload with waiter cleanup checks.

Before the concurrent compaction foundation landed, `wal` stored `*os.File` directly, so tests could only simulate failure by closing the complete file. The compaction foundation supplied the selected `walFile` seam; this slice reuses it for append-boundary failures rather than introducing a second abstraction.

## 3. Goals and non-goals

Goals:

- Detect semantic drift across future queue, heap, retry, lease, and replay refactors.
- Exercise durable state through repeated real WAL reopen operations.
- Falsify acknowledged-write loss after process termination.
- Verify that definite WAL failures preserve availability and ambiguous failures prevent further mutation.
- Detect duplicate active delivery, lost messages, index divergence, and waiter leaks under concurrency.
- Produce reproducible failures with the seed and operation number in the failure output.

Non-goals:

- Proving physical-media durability after power loss.
- Filesystem, kernel, or hardware fault simulation.
- Fairness or throughput benchmarking.
- Testing future compaction/replacement boundaries before that implementation exists.
- Distributed or multi-process writers against one WAL; that remains unsupported.

## 4. Existing system and constraints

- One mutex linearizes queue state and WAL appends.
- WAL frames are `length | CRC32 | JSON` and each acknowledged mutation calls `fsync`.
- Retry policy and FIFO/LIFO discipline are process-wide and persisted.
- Queue tests can inspect package-private state and use a manual clock.
- HTTP long polling is capped at 20 seconds.
- Local TCP binding may require explicit sandbox capability during verification.
- The repository has no external test dependencies.

## 5. Options considered

### Model testing

Option A: add more example tables. Easy to debug but does not cover mixed state transitions. Rejected as the only oracle.

Option B: randomized operations checked only with internal heap assertions. Finds index corruption but can share the production bug. Rejected as insufficient.

Option C: independent reference model plus internal structural assertions. Selected. The model implements ordering, leases, explicit nack delay, retry backoff, dead-letter transitions, extension, and restart expectations without calling production selection or retry helpers.

### Process testing

Option A: direct `httptest.ResponseRecorder`. Fast but does not test sockets, executable flags, timeouts, signals, or process restart. Already covered elsewhere.

Option B: test helper process linked into the test binary. Avoids a build but is not the shipped executable. Rejected.

Option C: build `bin/queuemaxxing`, run it on an ephemeral loopback port, use real HTTP, send `SIGKILL`, and restart on the same WAL. Selected.

### WAL failures

Option A: close the WAL descriptor. Existing coarse coverage; cannot select the failure boundary.

Option B: narrow internal `walFile` interface with one-shot wrappers. Selected because it preserves production bytes and aligns with the planned compaction abstraction.

### Soak placement

Option A: run the full workload in every unit-test invocation. Rejected because repeated `fsync` makes it unsuitable for the fast loop.

Option B: `fidelity` build tag and `make fidelity`. Selected. Core regression tests remain in the default suite; real-process and soak scenarios are explicit gates.

## 6. Chosen architecture

```text
make fidelity
  ├─ default unit/contract suite
  ├─ reference model → Queue API → real temp WAL → reopen
  ├─ injected walFile failures → append boundary assertions
  ├─ compiled server → real TCP → SIGKILL → same-WAL restart
  ├─ single binary → client lifecycle → startup compaction → idempotent retry
  └─ concurrent producers/consumers/extenders → drain + leak checks
```

The reference model owns logical expected state. Production queue internals are observed only after operations to compare durable fields and assert heap membership.

The WAL wrapper can fail one selected `Write`, `Sync`, or `Truncate` call. It delegates every other operation to the real file.

The subprocess test owns an isolated temporary directory and ephemeral port. It waits for `/healthz`, performs acknowledged mutations, kills without graceful shutdown, restarts with identical persisted configuration, and verifies state through HTTP only.

## 7. Contracts and invariants

| Invariant | Why | Observation | Falsification | Test layer |
|---|---|---|---|---|
| Queue state equals an independent model after every generated operation | Detects semantic drift | Canonical live/dead state matches | First differing field at seed/step | Model |
| Restart does not change modeled durable state | Protects recovery | State matches before/after reopen | Any message, receipt, attempt, or deadline changes | Model + real WAL |
| Every live message is in exactly one correct heap | Prevents loss/duplication | Internal index assertion passes | Missing or double-indexed ID | Structural |
| Definite write failure appends no event and leaves WAL writable | Preserves safe retry | Later enqueue succeeds and survives reopen | Failed event replays or later append fails | Fault injection |
| Post-write sync failure forbids later mutation | Prevents memory/disk divergence | Later mutation returns storage unavailable without growing WAL | Later success is acknowledged | Fault injection |
| Every acknowledged pre-kill HTTP mutation survives process restart | Validates executable recovery | Restarted API exposes expected state | Acknowledged message disappears | Subprocess |
| An extended lease remains hidden across `SIGKILL` restart | Protects lease durability | Immediate reserve returns `204` | Message redelivers before extended deadline | Subprocess |
| Concurrent drain delivers each message once while leases remain valid | Prevents duplicate active ownership | Unique ID set equals produced set | Duplicate or missing ID | Soak + race |
| Completed/cancelled long polls do not accumulate goroutines | Prevents resource leaks | Goroutine count returns within bounded slack | Count remains elevated | Leak scenario |

## 8. Failure handling and observability

- Model failures print discipline, seed, step, operation, expected state, and actual state.
- Subprocess failures include captured server logs.
- Test cleanup always terminates child processes and waits for them.
- WAL fault tests report the exact injected call boundary.
- A sync failure is wrapped with `ErrStorageUnavailable`; HTTP maps it to `503`.
- Pre-write and recoverably truncated short-write errors remain ordinary storage errors and do not poison the WAL.

## 9. Security and privacy

- Tests use random temporary directories with existing `0700` directory and `0600` WAL behavior.
- Child servers bind only to `127.0.0.1` on ephemeral ports.
- Test payloads and receipts are synthetic.
- No external network or service is required.

## 10. TDD and verification plan

Risk order:

1. WAL partial-write recovery and post-write sync poison tests.
2. Reference-model mixed-operation/restart test for FIFO and LIFO.
3. Compiled-server `SIGKILL`/restart lease durability test.
4. Concurrent drain with enqueue, long poll, extension, and ack.
5. Repeated timeout/cancellation waiter cleanup test.
6. Full unit, race, vet, repeated fidelity, and clean build gates.

Required commands:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
make fidelity
```

## 11. Implementation slices

1. Refactor `wal.file` to `walFile`, add terminal error semantics, and add exact boundary tests.
2. Add deterministic reference model and mixed-operation restart test.
3. Add `integration` fidelity-tagged compiled-process scenario.
4. Add fidelity-tagged concurrent soak and waiter cleanup scenarios.
5. Add Makefile target, README instructions, and full verification.

Each slice is independently removable except WAL poisoning, which is an additive safety guarantee without an on-disk migration.

## 12. Rollout, migration, and rollback

- The WAL byte format does not change.
- Existing WALs replay unchanged.
- `ErrStorageUnavailable` changes only behavior after an ambiguous in-process sync failure; restart remains the recovery action.
- Fidelity tests create isolated data and never rewrite repository fixtures.
- Removing fidelity-tagged tests does not affect production behavior.

## 13. Open questions and UNVERIFIED assumptions

- `SIGKILL` verifies process recovery from acknowledged filesystem state, not sudden power-loss persistence.
- Goroutine-count leak checks require small runtime slack because the Go runtime owns background goroutines.
- macOS results do not verify Linux filesystem behavior; CI on Linux remains a separate capability gate.
- Large-scale behavior above the selected bounded soak workload remains **UNVERIFIED**.

Blocking decisions: none.

## 14. Source references

- `internal/queue/wal.go`: current concrete file boundary and append protocol.
- `internal/queue/queue_test.go`: reference ordering, restart, torn-tail, and concurrency coverage.
- `internal/queue/wait_test.go`: manual clock and long-poll invariants.
- `internal/queue/lease_test.go`: durable extension coverage.
- `internal/httpapi/api_test.go`: handler-level contracts.
- `cmd/queuemaxxing/main.go`: executable server and timeout behavior.
- `docs/specs/2026-08-22-wal-replacement-compaction.md`: future WAL abstraction and failure taxonomy to remain compatible with.

## Readiness check

- Every goal maps to a contract or test: yes.
- Every invariant is observable and falsifiable: yes.
- Every live dependency has a capability gate: yes.
- Retry and idempotency rules are explicit: yes.
- Destructive boundaries are isolated to temporary test data: yes.
- First implementation slice is small and executable: yes.
- Unresolved assumptions are visible: yes.
