package queue

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestCompactionPreservesCompleteDurableStateAcrossRestart(t *testing.T) {
	t.Parallel()

	policy := RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second}
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: policy})

	acked, err := h.q.EnqueueIdempotent(json.RawMessage(`{"state":"acked"}`), 100, 0, "producer-key")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := h.q.Reserve(time.Minute)
	if err != nil || delivery == nil || delivery.ID != acked.Message.ID {
		t.Fatalf("reserve acked message: delivery=%+v err=%v", delivery, err)
	}
	if err := h.q.Ack(delivery.ID, delivery.Receipt); err != nil {
		t.Fatal(err)
	}

	dead, err := h.q.Enqueue(json.RawMessage(`{"state":"dead"}`), 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err = h.q.Reserve(time.Minute)
	if err != nil || delivery == nil || delivery.ID != dead.ID {
		t.Fatalf("reserve dead message: delivery=%+v err=%v", delivery, err)
	}
	if _, err := h.q.NackWithOptions(delivery.ID, delivery.Receipt, nil); err != nil {
		t.Fatal(err)
	}

	leased, err := h.q.Enqueue(json.RawMessage(`{"state":"leased"}`), 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err = h.q.Reserve(time.Hour)
	if err != nil || delivery == nil || delivery.ID != leased.ID {
		t.Fatalf("reserve leased message: delivery=%+v err=%v", delivery, err)
	}
	if _, err := h.q.Enqueue(json.RawMessage(`{"state":"ready"}`), 5, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := h.q.Enqueue(json.RawMessage(`{"state":"delayed"}`), 7, time.Hour); err != nil {
		t.Fatal(err)
	}

	want := captureDurableState(h.q)
	result, err := h.q.Compact()
	if err != nil {
		t.Fatal(err)
	}
	if result.Messages != 3 || result.DeadLetters != 1 || result.IdempotencyKeys != 1 {
		t.Fatalf("compaction counts = %+v", result)
	}
	if result.OldBytes <= 0 || result.NewBytes <= 0 || result.SizeDelta != result.OldBytes-result.NewBytes {
		t.Fatalf("compaction sizes = %+v", result)
	}
	if got := captureDurableState(h.q); !reflect.DeepEqual(got, want) {
		t.Fatalf("in-memory state changed\ngot:  %#v\nwant: %#v", got, want)
	}

	h.reopen()
	if got := captureDurableState(h.q); !reflect.DeepEqual(got, want) {
		t.Fatalf("restarted state changed\ngot:  %#v\nwant: %#v", got, want)
	}

	var types []string
	if err := h.q.wal.Replay(func(e event) error {
		types = append(types, e.Type)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	wantTypes := []string{"snapshot_begin", "snapshot_message", "snapshot_message", "snapshot_message", "snapshot_dead_letter", "snapshot_idempotency", "snapshot_commit"}
	if !reflect.DeepEqual(types, wantTypes) {
		t.Fatalf("snapshot record order = %v, want %v", types, wantTypes)
	}
}

func TestCompactionPreservesNextSequenceWhenQueueIsEmpty(t *testing.T) {
	t.Parallel()

	h := newQueueHarness(t, Config{Discipline: FIFO})
	message, err := h.q.Enqueue(json.RawMessage(`{"old":true}`), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := h.q.Reserve(time.Minute)
	if err != nil || delivery == nil || delivery.ID != message.ID {
		t.Fatalf("reserve: delivery=%+v err=%v", delivery, err)
	}
	if err := h.q.Ack(delivery.ID, delivery.Receipt); err != nil {
		t.Fatal(err)
	}
	wantSequence := h.q.nextSeq + 1
	if _, err := h.q.Compact(); err != nil {
		t.Fatal(err)
	}
	h.reopen()

	newMessage, err := h.q.Enqueue(json.RawMessage(`{"new":true}`), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.q.messages[newMessage.ID].Sequence; got != wantSequence {
		t.Fatalf("new sequence = %d, want %d", got, wantSequence)
	}
}

func TestCompactionDropsExpiredIdempotency(t *testing.T) {
	t.Parallel()

	h := newQueueHarness(t, Config{Discipline: FIFO, IdempotencyRetention: time.Second})
	first, err := h.q.EnqueueIdempotent(json.RawMessage(`{"value":1}`), 0, 0, "expired-key")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := h.q.Reserve(time.Minute)
	if err != nil || delivery == nil {
		t.Fatalf("reserve: delivery=%+v err=%v", delivery, err)
	}
	if err := h.q.Ack(delivery.ID, delivery.Receipt); err != nil {
		t.Fatal(err)
	}
	h.advance(2 * time.Second)
	result, err := h.q.Compact()
	if err != nil {
		t.Fatal(err)
	}
	if result.IdempotencyKeys != 0 {
		t.Fatalf("idempotency count = %d, want 0", result.IdempotencyKeys)
	}
	h.reopen()
	second, err := h.q.EnqueueIdempotent(json.RawMessage(`{"value":1}`), 0, 0, "expired-key")
	if err != nil {
		t.Fatal(err)
	}
	if second.Replayed || second.Message.ID == first.Message.ID {
		t.Fatalf("expired idempotency result was retained: first=%+v second=%+v", first, second)
	}
}

func TestCompactionRenameFailureLeavesQueueWritable(t *testing.T) {
	t.Parallel()

	h := newQueueHarness(t, Config{Discipline: FIFO})
	rename := h.q.wal.ops.rename
	h.q.wal.ops.rename = func(string, string) error { return errors.New("injected rename failure") }
	if _, err := h.q.Compact(); err == nil {
		t.Fatal("Compact succeeded")
	}
	h.q.wal.ops.rename = rename
	message, err := h.q.Enqueue(json.RawMessage(`{"after":true}`), 0, 0)
	if err != nil {
		t.Fatalf("enqueue after definite failure: %v", err)
	}
	h.reopen()
	if h.q.messages[message.ID] == nil {
		t.Fatal("post-failure enqueue was lost on restart")
	}
}

func TestCompactionDirectorySyncFailurePoisonsMutations(t *testing.T) {
	t.Parallel()

	h := newQueueHarness(t, Config{Discipline: FIFO})
	h.q.wal.ops.openDir = func(string) (syncCloser, error) {
		return &failNthSyncCloser{failAt: 2}, nil
	}
	result, err := h.q.Compact()
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Compact error = %v, want ErrStorageUnavailable", err)
	}
	if result.NewBytes == 0 || !errors.Is(h.q.StorageError(), ErrStorageUnavailable) {
		t.Fatalf("terminal result=%+v storage error=%v", result, h.q.StorageError())
	}
	beforeSequence := h.q.nextSeq
	before, statErr := h.q.wal.file.Stat()
	if statErr != nil {
		t.Fatal(statErr)
	}
	if _, err := h.q.Enqueue(json.RawMessage(`{"forbidden":true}`), 0, 0); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("enqueue error = %v, want ErrStorageUnavailable", err)
	}
	after, statErr := h.q.wal.file.Stat()
	if statErr != nil {
		t.Fatal(statErr)
	}
	if h.q.nextSeq != beforeSequence || after.Size() != before.Size() {
		t.Fatalf("terminal mutation changed state: seq %d -> %d, bytes %d -> %d", beforeSequence, h.q.nextSeq, before.Size(), after.Size())
	}
}

func TestConcurrentMutationLinearizesAfterCompaction(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	originalRename := h.q.wal.ops.rename
	enteredRename := make(chan struct{})
	releaseRename := make(chan struct{})
	h.q.wal.ops.rename = func(oldPath, newPath string) error {
		close(enteredRename)
		<-releaseRename
		return originalRename(oldPath, newPath)
	}

	compactResult := make(chan error, 1)
	go func() {
		_, err := h.q.Compact()
		compactResult <- err
	}()
	<-enteredRename

	enqueueStarted := make(chan struct{})
	enqueueResult := make(chan EnqueueResult, 1)
	enqueueError := make(chan error, 1)
	go func() {
		close(enqueueStarted)
		result, err := h.q.EnqueueIdempotent(json.RawMessage(`{"concurrent":true}`), 0, 0, "concurrent-key")
		enqueueResult <- result
		enqueueError <- err
	}()
	<-enqueueStarted
	select {
	case err := <-enqueueError:
		t.Fatalf("enqueue completed inside compaction critical section: %v", err)
	default:
	}

	close(releaseRename)
	if err := <-compactResult; err != nil {
		t.Fatal(err)
	}
	result := <-enqueueResult
	if err := <-enqueueError; err != nil {
		t.Fatal(err)
	}
	h.reopen()
	if h.q.messages[result.Message.ID] == nil || h.q.idempotency["concurrent-key"].Message.ID != result.Message.ID {
		t.Fatal("mutation acknowledged after compaction was not replayed")
	}
}

type durableState struct {
	Discipline  Discipline
	RetryPolicy RetryPolicy
	NextSeq     uint64
	Messages    []storedMessage
	DeadLetters []DeadLetter
	Idempotency []durableIdempotency
}

type durableIdempotency struct {
	Key    string
	Record idempotencyRecord
}

func captureDurableState(q *Queue) durableState {
	q.mu.Lock()
	defer q.mu.Unlock()
	state := durableState{Discipline: q.discipline, RetryPolicy: q.retryPolicy, NextSeq: q.nextSeq}
	for _, message := range q.messages {
		copy := *message
		copy.Message = cloneMessage(message.Message)
		copy.heapKind = heapNone
		copy.heapIndex = 0
		copy.nextEligibleAt = time.Time{}
		state.Messages = append(state.Messages, copy)
	}
	sort.Slice(state.Messages, func(i, j int) bool {
		if state.Messages[i].Sequence != state.Messages[j].Sequence {
			return state.Messages[i].Sequence < state.Messages[j].Sequence
		}
		return state.Messages[i].ID < state.Messages[j].ID
	})
	for _, letter := range q.deadLetters {
		state.DeadLetters = append(state.DeadLetters, cloneDeadLetter(*letter))
	}
	sort.Slice(state.DeadLetters, func(i, j int) bool {
		if state.DeadLetters[i].Sequence != state.DeadLetters[j].Sequence {
			return state.DeadLetters[i].Sequence < state.DeadLetters[j].Sequence
		}
		return state.DeadLetters[i].ID < state.DeadLetters[j].ID
	})
	for key, record := range q.idempotency {
		record.Message = cloneMessage(record.Message)
		state.Idempotency = append(state.Idempotency, durableIdempotency{Key: key, Record: record})
	}
	sort.Slice(state.Idempotency, func(i, j int) bool { return state.Idempotency[i].Key < state.Idempotency[j].Key })
	return state
}
