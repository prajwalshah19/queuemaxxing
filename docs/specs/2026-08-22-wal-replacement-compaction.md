# Crash-safe WAL replacement and compaction specification

## 1. Status and decision summary

Status: Ready for implementation. No production code is changed by this specification.

Decisions:

- Build one crash-safe `wal.Replace` primitive, then implement `Queue.Compact` exclusively through it.
- Compaction is stop-the-world under the existing queue mutex. It blocks mutations and reads for the duration of serialization and replacement.
- The replacement file is created in the WAL's directory, written completely, synced, renamed over the active path, and followed by a parent-directory sync.
- Keep the replacement descriptor open through rename and make it the active append descriptor. Do not rename and then reopen by path.
- A failure before rename is definite: the old WAL remains active and the queue remains writable.
- A failure after successful rename but before successful directory sync is durability-ambiguous: switch to the new descriptor, poison the WAL, reject further mutations, and require restart.
- Compaction serializes a versioned, committed, multi-frame snapshot. One giant snapshot frame is prohibited.
- Snapshot state includes discipline, retry policy, `nextSeq`, every live message and lease, every dead letter, and every unexpired idempotency record. Heap state is derived and is not serialized.
- The first shipped trigger is `-compact-on-start`. It runs after successful replay and before the HTTP listener starts.
- Do not add an HTTP compaction endpoint or automatic threshold in this slice.
- Initial crash-safe replacement support is limited to local filesystems on Linux and macOS. Other platforms return a clear unsupported error.
- Compaction is an irreversible history rewrite. The implementation does not retain a backup generation after success.

This specification supersedes the replacement sequence in `docs/specs/2026-08-21-queue-refinement.md` where it says to reopen the renamed file. Reusing the already-open replacement descriptor removes an unnecessary post-rename failure point.

## 2. Problem and evidence

`internal/queue/wal.go` currently appends one checksummed frame per mutation and calls `Sync` before returning. It never rewrites the file. Consequently:

- acknowledged messages remain in historical `enqueue`, `reserve`, and `ack` records;
- retried and replayed messages accumulate complete transition histories;
- expired idempotency records disappear from memory but remain on disk;
- WAL size and startup replay time grow with total history rather than current state.

The current WAL replacement hazards are:

- rewriting or truncating the active file in place can destroy the only durable generation;
- creating a temp file in another directory can cross filesystems and make rename fail;
- syncing only the replacement file does not make its directory entry durable on Linux;
- closing the old descriptor before the replacement is committed creates a service failure window;
- continuing after a directory-sync ambiguity can append acknowledged mutations to a generation that may not remain named after a crash;
- a single JSON snapshot can exceed the current 16 MiB WAL frame limit;
- synthesizing ordinary history cannot represent acknowledged idempotency results or dead letters without fabricating live messages.

Go documents that `os.Rename` replaces an existing non-directory path, but also warns that rename is not atomic on non-Unix platforms. Go's `File.Sync` commits a file's current contents to stable storage. Linux additionally requires syncing the containing directory to persist its directory entry. These constraints require an explicit platform and filesystem boundary rather than a generic "rename is durable" claim.

## 3. Goals and non-goals

### Goals

- Replace a WAL without an interval where the active pathname is missing.
- Ensure recovery sees either the complete old generation or a complete state-equivalent replacement.
- Prevent any acknowledged mutation after a durability-ambiguous replacement boundary.
- Reduce WAL size and future restart work to current durable state plus post-compaction history.
- Preserve all queue ordering, lease, retry, dead-letter, and idempotency semantics exactly.
- Preserve the monotonic sequence high-water mark even when no current message has that sequence.
- Keep peak memory bounded by current queue state plus `O(n)` pointer/key ordering slices, not another copy of every body.
- Make every replacement failure boundary deterministic and injectable in tests.
- Keep the implementation standard-library-only and single-process.

### Non-goals

- Online or incremental compaction.
- WAL segments, manifests, replication, or consensus.
- Automatically choosing a compaction threshold.
- A runtime HTTP admin endpoint.
- Retaining historical acknowledged messages after successful compaction.
- Binary downgrade after a snapshot has replaced the legacy event stream.
- Detecting every network, virtualized, or misconfigured filesystem with unreliable durability semantics.
- Solving general append `fsync` ambiguity; this slice adds the terminal-state foundation and applies it to replacement.
- Windows crash-safe replacement in the initial implementation.

## 4. Existing system and constraints

- `wal.Append` encodes `event` as JSON inside `length | CRC32 | payload` framing and syncs every event.
- `wal.Replay` validates frame size and CRC, truncates an incomplete final frame, and rejects corrupt complete frames.
- `Queue.mu` serializes message state, heap membership, and WAL operations.
- `Queue` reconstructs ready and scheduled heaps after replay.
- `storedMessage` contains durable sequence, receipt, lease, visibility, attempts, body, priority, and enqueue timestamp. Heap fields are unexported and therefore not serialized.
- Dead letters retain message state, original sequence, terminal timestamp, and reason.
- Idempotency records retain request hash, original response message, and expiration even after acknowledgement.
- Queue discipline and retry policy are persisted.
- One process owns one queue and one WAL. There is no enforced operating-system file lock.
- The HTTP request body limit is 1 MiB and the WAL frame limit is 16 MiB, so one record per message fits under normal API use.
- The current environment is Go 1.25 on macOS/arm64. Linux behavior must be verified separately.

Durable state:

- discipline;
- retry policy;
- `nextSeq` high-water mark;
- live `storedMessage` values, including active receipts and leases;
- dead letters;
- unexpired idempotency records.

Derived or ephemeral state:

- ready and scheduled heap arrays, indexes, and `nextEligibleAt`;
- idempotency expiry heap;
- mutex state;
- injected clock function;
- closed and in-progress flags;
- historical events that no longer affect current state.

## 5. Options considered

### Replacement protocol

#### Option A: truncate and rewrite the active WAL

- Uses no temporary disk space.
- Any crash or write failure can leave the only generation incomplete.
- Existing readers and append offsets become difficult to reason about.
- Rejected because it violates the core durability requirement.

#### Option B: write temp, close active, rename, reopen

- Easy to describe.
- Closing the active descriptor before commit creates avoidable unavailability.
- Reopening after rename adds a failure point after the pathname has changed.
- Rejected.

#### Option C: keep both descriptors open and atomically replace the pathname

- Create a sibling temp file and keep its descriptor open.
- Write and sync it, rename it over the active pathname, sync the directory, then swap the in-memory descriptor and close the obsolete one.
- On directory-sync ambiguity, the new descriptor is still known and can be installed before poisoning the WAL.
- Selected.

#### Option D: manifest plus numbered generations

- Can retain multiple generations and support more platforms.
- Requires atomic manifest updates, startup arbitration, retention rules, and more recovery states.
- Deferred until online compaction or binary rollback justifies it.

### Snapshot encoding

#### Option A: one complete JSON snapshot frame

- Minimal replay state machine.
- Exceeds `maxFrameSize` as queue depth grows and requires a second full in-memory copy.
- Rejected.

#### Option B: synthesize a shorter stream of existing operational events

- Old binaries could parse the result.
- Cannot represent an idempotency result whose message was acknowledged without recreating a live message.
- Dead letters would require fabricated reserve and receipt history.
- Rejected because it makes state restoration indirect and fragile.

#### Option C: committed multi-frame snapshot

- A versioned begin record carries configuration, `nextSeq`, and expected counts.
- One record represents each live message, dead letter, and idempotency entry.
- A final commit record carries the generation and a checksum of the exact preceding snapshot payloads.
- Startup buffers snapshot state separately and installs it only after validating the commit.
- Selected.

### Compaction concurrency

#### Option A: stop-the-world under `Queue.mu`

- Gives one exact state cut without a delta log.
- Blocks queue operations for `O(current state)` serialization and file sync.
- Selected for the initial implementation.

#### Option B: rotate WAL, snapshot concurrently, merge a delta segment

- Shorter mutation pause.
- Requires segments, generation manifests, replay ordering, and cleanup recovery.
- Deferred.

### Operational trigger

#### Option A: synchronous HTTP admin endpoint

- Convenient at runtime.
- Interacts poorly with the current 10-second server and client timeouts and exposes a long-running destructive operation without built-in authentication.
- Deferred.

#### Option B: automatic size threshold

- Bounds growth without operator action.
- Adds latency placement, background lifecycle, retry, and shutdown policy before the primitive is proven.
- Deferred.

#### Option C: `-compact-on-start`

- Runs after a complete replay and before accepting traffic.
- Keeps cancellation, HTTP deadlines, and concurrent trigger ownership out of the first slice.
- Still permits direct runtime `Queue.Compact` use by embedders under the queue mutex.
- Selected.

## 6. Chosen architecture

### Layering

```text
Queue.Compact
  ├─ capture/prune current durable state under Queue.mu
  ├─ stream versioned snapshot records in deterministic order
  └─ wal.Replace(writer)
       ├─ create sibling temp (0600)
       ├─ write framed records without per-record fsync
       ├─ fsync replacement
       ├─ rename temp over active WAL
       ├─ install already-open replacement descriptor
       ├─ fsync parent directory
       └─ close obsolete descriptor
```

`Queue.Compact` owns logical state selection and snapshot encoding. `wal.Replace` owns only byte durability, pathname replacement, descriptor handoff, cleanup, and terminal error classification. The WAL layer must not understand queue messages.

### WAL abstraction

Refactor `wal` to retain:

```go
type wal struct {
    path        string
    file        walFile
    ops         walOps
    terminalErr error
}
```

Use narrow internal interfaces for failure injection:

```go
type walFile interface {
    io.ReaderAt
    io.Writer
    io.Seeker
    Stat() (os.FileInfo, error)
    Sync() error
    Truncate(int64) error
    Close() error
}

type walOps struct {
    createTemp func(dir, pattern string) (namedWALFile, error)
    openDir    func(path string) (syncCloser, error)
    rename     func(oldPath, newPath string) error
    remove     func(path string) error
    lstat      func(path string) (os.FileInfo, error)
}
```

The implementation may adjust interface details, but tests must independently inject create, write, replacement-file sync, rename, directory sync, and obsolete-close failures.

`wal.Replace` accepts a streaming callback:

```go
func (w *wal) Replace(write func(*replacementWriter) error) (replaceResult, error)
```

`replacementWriter.Append(event)` uses the existing frame encoder but does not sync per record. `Replace` performs one replacement-file sync after the callback completes.

### Replacement protocol

Under `Queue.mu`:

1. Reject replacement if `wal.terminalErr` is already set.
2. `Lstat` the active path and reject symbolic links for compaction.
3. Open the parent directory and perform a preflight `Sync`. If unsupported, fail before creating or renaming anything.
4. Create a unique sibling file such as `.queue.wal.compact-<random>.tmp` with mode `0600` and an append-capable read/write descriptor.
5. Stream every replacement frame. Any encode or write error closes and best-effort removes the temp; the old WAL remains active.
6. Call replacement `Sync`. Failure is definite because rename has not occurred; close/remove the temp and keep the old WAL.
7. Call same-directory `Rename(tempPath, activePath)`. On supported local Unix filesystems, failure leaves the old active pathname intact; close/remove the temp and keep the old descriptor.
8. Install the replacement descriptor as `w.file` before returning from any post-rename path. No mutation may use the obsolete descriptor again.
9. Call parent-directory `Sync`.
10. If directory sync fails, set `wal.terminalErr` to a typed durability-ambiguous error, keep the replacement descriptor installed, close the obsolete descriptor, and reject every later mutation until restart.
11. If directory sync succeeds, replacement is committed. Close the obsolete descriptor. A close failure on the obsolete, previously synced generation is logged as a warning but does not undo or poison the committed replacement.

The temp descriptor remains open across rename. `Rename` changes its pathname identity without invalidating the descriptor on supported Unix systems.

### Snapshot format version 1

The compacted WAL contains:

```text
snapshot_begin
snapshot_message       repeated live-message count times
snapshot_dead_letter   repeated dead-letter count times
snapshot_idempotency   repeated idempotency count times
snapshot_commit
<ordinary events appended after compaction>
```

`snapshot_begin` contains:

```go
type snapshotBegin struct {
    Version          uint32
    Generation       string
    CreatedAt        time.Time
    Discipline       Discipline
    RetryPolicy      RetryPolicy
    NextSequence     uint64
    MessageCount     uint64
    DeadLetterCount  uint64
    IdempotencyCount uint64
}
```

State records contain complete values:

- `snapshot_message`: one `storedMessage` including sequence, attempts, receipt, lease, and timestamps;
- `snapshot_dead_letter`: one complete dead-letter snapshot including its original sequence explicitly; it must not depend on the public `DeadLetter.Sequence` JSON tag, which is currently omitted;
- `snapshot_idempotency`: key, request hash, original response `Message`, and expiration.

`snapshot_commit` contains generation, counts, and a lowercase hexadecimal SHA-256 digest. The digest covers each exact JSON payload from `snapshot_begin` through the last state record, prefixed by its big-endian payload length. It excludes WAL CRC headers and the commit payload.

The frame decoder must expose raw payload bytes to the snapshot replay state machine. Re-marshalling decoded events for verification is prohibited because schema evolution can change encoding while preserving meaning.

The frame decoder must also report a torn-tail offset without truncating immediately. Queue replay decides whether truncation is safe:

- after a complete legacy stream or committed snapshot, truncate and sync the incomplete ordinary tail as today;
- while a snapshot is still uncommitted, fail startup without truncation so recovery evidence is preserved;
- never truncate a complete frame with a CRC, JSON, snapshot-validation, or state-transition error.

Snapshot record order is deterministic:

1. live messages by sequence ascending, then ID;
2. dead letters by original sequence ascending, then ID;
3. idempotency entries by key lexical order.

Deterministic order improves reproducibility; queue delivery order still comes exclusively from stored sequence and priority.

### Snapshot replay

Startup accepts either:

- a legacy operational WAL beginning with `config`; or
- snapshot version 1 beginning with `snapshot_begin`.

For a snapshot WAL:

1. Build state in a separate `snapshotState`; do not mutate the live queue while the snapshot is incomplete.
2. Reject nested begins, ordinary events before commit, wrong generation, unsupported version, duplicate message IDs, IDs existing in both live and dead-letter state, duplicate idempotency keys, invalid policies, invalid timestamps, count mismatch, checksum mismatch, and a `NextSequence` below any stored sequence.
3. Require `snapshot_commit`. An incomplete/torn snapshot is fatal and is not truncated, even though ordinary torn final operational frames after a committed state remain recoverable by truncation.
4. Atomically install the validated snapshot state.
5. Apply ordinary events after the commit through the existing replay logic.
6. Prune idempotency entries whose expiration is not after the current queue clock.
7. Rebuild ready, scheduled, and idempotency-expiry heaps.

The compactor preserves expired leases as leases. It does not manufacture retry events; the next reserve operation advances them according to the retry contract.

### Queue compaction

```go
type CompactionResult struct {
    OldBytes        int64
    NewBytes        int64
    SizeDelta       int64 // OldBytes - NewBytes; may be negative
    Messages        int
    DeadLetters     int
    IdempotencyKeys int
}

func (q *Queue) Compact() (CompactionResult, error)
```

`Compact`:

1. Locks `Queue.mu` for the complete operation.
2. Rejects a closed or terminal WAL.
3. Reads the active WAL size.
4. Prunes expired idempotency records using the queue clock.
5. Captures counts and sorted pointer/key slices while holding the lock.
6. Streams the snapshot through `wal.Replace` without copying message bodies.
7. Returns the observed old/new sizes and state counts.

Live queue maps and heaps are not replaced during successful compaction because they already represent the snapshot state. Only the WAL descriptor changes.

### Shipped trigger

Add:

```text
-compact-on-start=false
```

When enabled, `cmd/queuemaxxing`:

1. opens and replays the queue;
2. calls `Queue.Compact`;
3. logs sizes and counts without bodies, receipts, or idempotency keys;
4. exits non-zero on failure;
5. starts the HTTP server only after success.

No API or `qmctl` change is included. A runtime admin operation can be designed later with explicit authentication, cancellation, progress, and timeout semantics.

## 7. Contracts and invariants

### External contracts

- Existing WALs open without migration and remain operational until compaction is explicitly triggered.
- `-compact-on-start` defaults to false and therefore preserves current startup behavior.
- A successful startup compaction preserves every externally visible queue state and changes only WAL history, WAL size, and future replay work.
- Startup compaction failure prevents the server from listening.
- Once a compacted snapshot replaces the WAL, binaries that do not understand snapshot version 1 cannot open it.
- A queue with a terminal replacement error serves read-only diagnostics but rejects mutation-capable operations with `ErrStorageUnavailable`; HTTP maps that error to `503 Service Unavailable`.
- Compaction does not alter attempt counts, receipts, lease deadlines, visibility timestamps, priority, sequence, dead-letter time/reason, original message IDs, or active idempotency responses/expirations.
- A successful compaction may produce a larger WAL when history is already shorter than the snapshot representation; `SizeDelta` reports the signed result without claiming bytes were reclaimed.
- Runtime callers of `Queue.Compact` block behind and then hold the queue mutex. There is no cancellation after replacement begins.
- Retrying compaction after a definite pre-rename failure is safe. After a terminal post-rename error, restart is required first.

### Invariants

| Invariant | Why | Observation | Smallest falsification | Test layer |
|---|---|---|---|---|
| Recovery after an interrupted replacement yields the old or new complete generation | Prevents queue loss | Reopen matches the pre-compaction logical snapshot | Startup observes a partial or mixed generation | Subprocess crash matrix |
| The active WAL pathname is never intentionally absent | Prevents unrecoverable startup gaps | Repeated path probes always find old or new file | Path is missing between operations | Primitive integration |
| No frame from an incomplete snapshot becomes live state | Prevents partial-state recovery | Missing commit makes open fail | Queue opens with only some messages | Replay failure test |
| A committed snapshot is state-equivalent to pre-compaction replay | Preserves queue semantics | Canonical state snapshots match before/after reopen | Any durable field differs | Property + restart |
| `nextSeq` never decreases through compaction | Prevents sequence reuse and FIFO/LIFO corruption | New enqueue sequence exceeds every historical allocation | Empty compacted queue restarts at sequence 1 | Restart regression |
| Every active lease retains its receipt, attempts, and deadline | Prevents duplicate ownership | Pre/post delivery state matches | Compaction makes a leased message ready | Restart integration |
| Every dead letter remains dead and replayable | Prevents poison-message revival or loss | List/replay matches before and after | Dead letter becomes live or disappears | Contract + restart |
| Every unexpired idempotency key returns the original response | Prevents producer duplicates | Same key produces replay after compaction/restart | Retry enqueues a second message | Contract + restart |
| Expired idempotency records are absent from the compacted generation | Bounds retained state | Snapshot count and retry behavior exclude expired key | Expired key remains deduplicated | Deterministic clock |
| A pre-rename failure leaves the old descriptor/path writable | Preserves availability on definite failure | A subsequent append succeeds and restarts | Queue becomes poisoned or event is lost | Failure injection |
| A post-rename directory-sync failure forbids all later mutation | Prevents appends to an uncertain generation | Mutations return unavailable and WAL size stays fixed | A later enqueue returns success | Failure injection + API |
| The replacement descriptor receives the first post-compaction append | Prevents writes to the unlinked old inode | Reopen sees the first new event | Event exists only through the old descriptor | Descriptor-handoff regression |
| Concurrent mutations linearize entirely before or after compaction | Prevents split generations | Reopen contains each acknowledged mutation exactly once | Mutation is missing or duplicated | Barrier + race |
| Replacement files and active WAL remain private | Protects payloads and receipts | File mode is `0600` after every boundary | Temp or active file is group/world-readable | Permission integration |
| Compaction never logs message bodies, receipts, or idempotency keys | Protects sensitive queue data | Captured logs contain only counts, sizes, generation, phase, and errors | Secret fixture appears in logs | Log-capture test |

## 8. Failure handling and observability

### Failure classification

| Boundary | Active generation | Queue after return | Cleanup | Error class |
|---|---|---|---|---|
| directory open/preflight sync | old | writable | none | definite |
| temp create | old | writable | none | definite |
| snapshot encode/write | old | writable | close/remove temp best effort | definite |
| replacement file sync | old | writable | close/remove temp best effort | definite |
| rename returns error on supported local Unix FS | old | writable | close/remove temp best effort | definite |
| rename succeeds, directory sync fails | new descriptor installed; crash may recover old or new | terminal/read-only until restart | close obsolete descriptor | ambiguous |
| directory sync succeeds | new | writable | close obsolete descriptor | committed |
| obsolete descriptor close fails after commit | new | writable | log warning | committed warning |

After an ambiguous error:

- `wal.Err()` exposes the terminal cause;
- `Append` and `Replace` return `ErrStorageUnavailable` without I/O;
- queue mutation-capable methods check terminal state before changing sequence or heaps;
- `GET /healthz` returns `503` with a generic storage-unavailable status;
- read-only stats and dead-letter inspection remain available;
- process restart reopens the active pathname and clears the in-memory terminal state only after complete replay succeeds.

Logs include operation, phase, active path, old/new byte counts, and generation ID. They exclude bodies, receipts, and idempotency keys.

### Stale temporary files

- A definite in-process failure removes only the exact temp path created by that call.
- A process crash may leave `.queue.wal.compact-*.tmp` files.
- Startup ignores these files and never chooses one over the active WAL.
- The initial implementation does not automatically delete crash leftovers because they may be useful recovery evidence.
- README documents the exact pattern for manual inspection/removal while the queue is stopped.

### Crash matrix

- Before temp sync: active old WAL.
- After temp sync but before rename: active old WAL plus harmless complete temp.
- After rename but before directory sync: after power loss, active path may resolve to old or new; both encode the same logical cut and no later mutation is permitted.
- After directory sync: active new WAL is the committed generation.
- After descriptor handoff: subsequent appends must survive restart from the active path.

Process-kill tests can verify code-phase recovery but cannot prove storage behavior across actual power loss. That remains a live hardware/filesystem gate.

## 9. Security and privacy

- Create replacement files with mode `0600`; never rely on a later chmod to close an exposure window.
- Keep the existing `0700` data-directory creation behavior.
- Reject compaction when the configured WAL pathname itself is a symbolic link. Renaming over a symlink replaces the link entry rather than its target and would violate operator expectations.
- Never place message bodies, receipts, or idempotency keys in paths, errors, generation IDs, or logs.
- Use cryptographically random generation/temp suffixes or `os.CreateTemp`; never predictable names with non-exclusive creation.
- Do not follow or recursively delete stale-temp globs.
- One-process ownership remains a trust assumption. Concurrent processes pointing at one WAL are unsupported and can invalidate replacement safety.
- Local trusted storage is required. NFS and other network filesystems are unsupported because rename failure can be ambiguous and durability depends on server behavior.
- Full-disk encryption, backup encryption, and secure deletion of discarded historical blocks are outside the application. Compaction removes logical reachability, not forensic recoverability.

## 10. TDD and verification plan

### Shared harness

Extend `internal/queue/harness_test.go` with:

- canonical durable-state snapshot excluding heap internals;
- injected `walOps` and `walFile` implementations with one-shot failures;
- named barriers before temp sync, before rename, after rename, after directory sync, and before obsolete close;
- reopen using real files;
- WAL size and raw frame readers;
- helper-process crash execution for real descriptor/path behavior.

Do not use sleeps for ordering assertions.

### First failing tests, in risk order

1. `TestWALReplaceWriteFailureKeepsOldGenerationWritable`
2. `TestWALReplaceSyncFailureKeepsOldGenerationWritable`
3. `TestWALReplaceRenameFailureKeepsOldGenerationWritable`
4. `TestWALReplaceDirectorySyncFailurePoisonsNewDescriptor`
5. `TestWALReplaceFirstAppendUsesReplacementDescriptor`
6. `TestWALReplacePreserves0600Mode`
7. `TestWALReplaceRejectsSymlinkPath`
8. `TestSnapshotRequiresCommit`
9. `TestSnapshotRejectsWrongCountsGenerationAndChecksum`
10. `TestCompactionPreservesCompleteDurableState`
11. `TestCompactionPreservesNextSequenceWhenQueueIsEmpty`
12. `TestCompactionPreservesActiveLeaseAndReceipt`
13. `TestCompactionPreservesScheduledRetry`
14. `TestCompactionPreservesDeadLetterAndReplay`
15. `TestCompactionPreservesUnexpiredIdempotencyAfterAck`
16. `TestCompactionDropsExpiredIdempotency`
17. `TestCompactionPreservesPriorityFIFOAndLIFOOrder`
18. `TestCompactionFailureBeforeRenameLeavesQueueWritable`
19. `TestCompactionDirectorySyncFailureRejectsLaterMutations`
20. `TestConcurrentMutationLinearizesAfterCompaction`
21. `TestCompactionReducesLongHistory`
22. `TestLegacyWALCompactsToSnapshotV1`
23. `TestTornOperationalTailAfterSnapshotStillRecovers`
24. `TestCompactOnStartRunsBeforeListen`
25. HTTP health test for terminal storage state

### Scenario and capability gates

Run helper processes that terminate at each replacement barrier, then reopen and compare state:

- temp created;
- snapshot written;
- replacement synced;
- rename completed;
- directory synced;
- replacement installed;
- first post-compaction append synced.

Required commands:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

Add:

```sh
go test -count=25 -run 'TestWALReplace|TestCompaction' ./internal/queue
```

Live gates:

- Linux on a local ext4 or XFS filesystem: directory `Sync`, process-kill matrix, restart, permissions.
- macOS on local APFS: directory `Sync`, process-kill matrix, restart, permissions.
- One disposable VM/filesystem power-cut test at the post-rename/pre-directory-sync boundary before claiming power-loss safety beyond syscall contracts.
- Benchmark replay time and WAL bytes before/after compaction at 1,000 and 100,000 historical events. Report measurements; do not claim a speedup from unit tests alone.

## 11. Implementation slices

### Slice 1: framing and injectable filesystem foundation

- Behavior: one shared frame encoder, raw-payload replay records, deferred torn-tail decisions, narrow file/ops interfaces, and terminal WAL error access.
- First failing tests: frame byte equivalence with current encoding and injected create/write/sync errors.
- Files: `internal/queue/wal.go`, new `internal/queue/wal_test.go` or focused replacement tests.
- Boundary: no replacement and no new on-disk records yet.
- Verification: existing WAL/restart tests plus new frame tests pass unchanged.
- Safe stop: fully reversible refactor with identical WAL bytes.

### Slice 2: crash-safe replacement primitive

- Behavior: sibling temp, file sync, rename, directory sync, open-descriptor handoff, definite/ambiguous classification, and terminal state.
- First failing tests: write/sync/rename/directory-sync matrix and first append descriptor.
- Files: new `internal/queue/wal_replace.go`, platform files for Linux/macOS and unsupported systems, WAL tests.
- Boundary: primitive exists but queue never calls it.
- Verification: injected matrix, real local-filesystem test, race test.
- Safe stop: no production WAL changes until `Replace` is invoked.

### Slice 3: snapshot version 1 writer and replay transaction

- Behavior: begin/state/commit records, raw checksum, validation, legacy-or-snapshot startup.
- First failing tests: missing commit, wrong checksum/counts, full-state decode.
- Files: new `internal/queue/snapshot.go`, `internal/queue/queue.go`, snapshot tests.
- Boundary: new binary can read snapshot files; no normal path writes one yet.
- Verification: malformed snapshot table and legacy WAL suite.
- Rollback: code-only because no production trigger emits snapshots.

### Slice 4: `Queue.Compact`

- Behavior: deterministic state streaming, idempotency pruning, result metrics, mutex linearization.
- First failing test: complete durable-state equivalence before/after compaction and restart.
- Files: new `internal/queue/compaction.go`, harness and compaction tests.
- Boundary: embedders can invoke compaction explicitly.
- Verification: state matrix, failure matrix, concurrent mutation barrier, race detector.
- Rollback: requires a pre-compaction WAL copy after the first successful invocation.

### Slice 5: startup trigger and terminal health

- Behavior: `-compact-on-start`, fail-before-listen, structured result log, `503` terminal health, mutation error mapping.
- First failing tests: startup ordering and health transition.
- Files: `cmd/queuemaxxing/main.go`, `internal/httpapi/api.go`, API/main tests.
- Boundary: no automatic or runtime HTTP compaction trigger.
- Verification: live process startup compaction and restart.
- Rollback: snapshot-aware binary required once the flag succeeds.

### Slice 6: crash/capability gates and documentation

- Behavior: subprocess phase matrix, Linux/macOS capability evidence, README operational procedure and limitations, parent-spec status update.
- First failing test: helper process killed at the first barrier.
- Files: compaction scenario tests, `README.md`, `docs/specs/2026-08-21-queue-refinement.md`.
- Verification: full tests, race, vet, repeat stress, live restart, permissions, measured size/replay benchmark.
- Safe stop: do not claim power-loss safety for a platform/filesystem without its live gate.

## 12. Rollout, migration, and rollback

### Migration

- New binary opens existing legacy WALs unchanged.
- Compaction is opt-in through `-compact-on-start=false` by default.
- First successful compaction replaces legacy history with snapshot version 1.
- Future appends continue as ordinary events after `snapshot_commit`.
- Recompaction writes a fresh snapshot generation from current state; it does not copy the previous snapshot.

### Rollout

1. Ship snapshot-read support with the compaction trigger disabled.
2. Verify legacy replay and snapshot fixture compatibility on Linux and macOS.
3. Back up the WAL while the process is stopped if binary rollback is required.
4. Enable `-compact-on-start` for one disposable WAL and verify state and size.
5. Enable for the target WAL, then keep the snapshot-aware binary for all restarts.

### Rollback

- Before first compaction: deploy the old binary normally, provided no other new event types exceed its support.
- After first compaction: old binaries reject `snapshot_begin`; restore a stopped-process pre-compaction WAL backup or use a snapshot-aware binary.
- Compaction itself does not retain a `.previous` copy because doing so defeats immediate disk reclamation. Backup ownership is explicit and external.
- A failed pre-rename compaction requires no rollback.
- A post-rename ambiguous failure requires restart with the same snapshot-aware binary; startup accepts whichever state-equivalent generation the filesystem retained.

### Storage capacity

- Peak disk use is approximately old WAL size plus compacted state size until rename succeeds.
- `ENOSPC` during write or replacement sync leaves the old WAL active.
- Operators must provision enough free space for the replacement before triggering compaction.
- Stale crash temp files count against disk until manually inspected and removed while stopped.

## 13. Open questions and **UNVERIFIED** assumptions

- Local filesystem crash behavior after successful file sync, rename, and directory sync is **UNVERIFIED** until Linux/ext4-or-XFS and macOS/APFS live gates pass.
- Actual power-loss behavior is **UNVERIFIED**; process-kill tests do not flush or discard hardware caches.
- The standard library cannot reliably identify every network filesystem. Local-storage placement remains an operator precondition.
- Directory `Sync` support on the exact deployment filesystem is **UNVERIFIED** until the startup/preflight capability test runs there.
- Stop-the-world pause at 100,000 live records is **UNVERIFIED** until benchmarked. Historical event count alone should not affect compact duration, but current state size will.
- Peak free-space requirements are **UNVERIFIED** for sparse files, copy-on-write filesystems, quotas, and container overlay filesystems.
- Compaction frequency remains an operator decision. Automatic thresholds are deferred until size and pause measurements exist.
- One-process WAL ownership is documented but not enforced with an OS lock.
- Secure erasure of discarded WAL blocks is not provided.

Highest-risk assumption: the deployment uses a local Unix filesystem whose `fsync` and same-directory rename semantics match the documented contract. This cannot be proven by mocks.

Blocking decisions: none for implementation. Linux/macOS filesystem capability gates block any broad power-loss-safety claim, not the code slices.

## 14. Source references

Repository:

- `internal/queue/wal.go`: current frame encoding, append/sync, replay, and torn-tail truncation.
- `internal/queue/queue.go`: queue mutex, durable event schema, replay, `nextSeq`, live state, and configuration.
- `internal/queue/index.go`: derived ready/scheduled heap state.
- `internal/queue/idempotency.go`: derived expiration heap and pruning.
- `internal/queue/retry.go`: dead-letter state and durable retry transitions.
- `internal/queue/harness_test.go`: real-WAL deterministic clock harness to extend.
- `internal/queue/queue_test.go`: legacy replay, torn tail, index, restart, and concurrency coverage.
- `internal/queue/retry_test.go`: retry, dead-letter, WAL-failure, and restart invariants.
- `cmd/queuemaxxing/main.go`: startup order and server lifecycle.
- `internal/httpapi/api.go`: health and storage-error mapping.
- `README.md`: current unbounded-WAL disclosure and operational boundaries.
- `docs/specs/2026-08-21-queue-refinement.md`: selected stop-the-world architecture and parent sequence.
- `docs/specs/2026-08-21-retries-dead-letter.md`: durable retry/dead-letter state that snapshots must preserve.
- `docs/specs/2026-08-21-lease-extension-long-polling.md`: future lease-extension event and long-poll concurrency requirements that snapshot replay must remain compatible with.

Primary filesystem/runtime references:

- [Go `os.Rename`](https://go.dev/pkg/os/#Rename): replacement behavior and warning that rename is not atomic on non-Unix platforms.
- [Go `os.File.Sync`](https://go.dev/pkg/os/#File.Sync): stable-storage contract used for replacement-file sync.
- [Linux `fsync(2)`](https://man7.org/linux/man-pages/man2/fsync.2.html): file sync does not necessarily persist its directory entry; directory sync is required.
- [Linux `rename(2)`](https://man7.org/linux/man-pages/man2/renameat2.2.html): atomic pathname replacement on local Linux filesystems, same-filesystem requirement, and NFS failure ambiguity.
- [POSIX `rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html): portable Unix rename contract.

## Readiness check

- Every goal maps to a contract or test: yes.
- Every invariant is observable and falsifiable: yes.
- Every live filesystem dependency has a capability or scenario gate: yes.
- Retry, lease, sequence, dead-letter, and idempotency preservation are explicit: yes.
- Destructive replacement boundaries and rollback requirements are explicit: yes.
- Incomplete snapshots cannot become live state: yes.
- The first implementation slice is small and emits no new format: yes.
- Unsupported platforms and filesystems are explicit: yes.
- Unresolved assumptions are visible: yes.
- Production code changed by this specification: no.
