package queue

import (
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnqueueIdempotencyReturnsOriginalWithoutAppending(t *testing.T) {
	t.Parallel()
	q := openTestQueue(t, FIFO)

	first, err := q.EnqueueIdempotent(json.RawMessage(`{"a":1,"b":2}`), 7, 5*time.Second, "job-123")
	if err != nil {
		t.Fatal(err)
	}
	if first.Replayed {
		t.Fatal("first enqueue was reported as replayed")
	}
	before, err := q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	second, err := q.EnqueueIdempotent(json.RawMessage(`{ "b": 2, "a": 1 }`), 7, 5*time.Second, "job-123")
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.Message.ID != first.Message.ID {
		t.Fatalf("replay = %#v, want original message %s", second, first.Message.ID)
	}
	after, err := q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("idempotent replay appended to WAL: size %d -> %d", before.Size(), after.Size())
	}
	if stats := q.Stats(); stats.Total != 1 {
		t.Fatalf("queue contains %d messages, want 1", stats.Total)
	}
	assertIndexConsistency(t, q)
}

func TestEnqueueIdempotencyRejectsConflictingInput(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		body     string
		priority int
		delay    time.Duration
	}{
		{name: "body", body: `{"job":2}`, priority: 7, delay: time.Second},
		{name: "priority", body: `{"job":1}`, priority: 8, delay: time.Second},
		{name: "delay", body: `{"job":1}`, priority: 7, delay: 2 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := openTestQueue(t, FIFO)
			first, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 7, time.Second, "same-key")
			if err != nil {
				t.Fatal(err)
			}
			before, _ := q.wal.file.Stat()

			_, err = q.EnqueueIdempotent(json.RawMessage(test.body), test.priority, test.delay, "same-key")
			if !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
			}
			after, _ := q.wal.file.Stat()
			if after.Size() != before.Size() || q.Stats().Total != 1 {
				t.Fatal("conflicting request mutated queue or WAL")
			}
			if q.messages[first.Message.ID] == nil {
				t.Fatal("conflict removed the original message")
			}
		})
	}
}

func TestEnqueueIdempotencySurvivesAckAndRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "queue.wal")
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	q, err := openWithClock(path, FIFO, clock)
	if err != nil {
		t.Fatal(err)
	}
	first, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 3, 0, "durable-key")
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := q.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(delivery.ID, delivery.Receipt); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Hour)
	reopened, err := openWithClock(path, FIFO, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	replay, err := reopened.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 3, 0, "durable-key")
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.Message.ID != first.Message.ID {
		t.Fatalf("restart replay = %#v, want original message %s", replay, first.Message.ID)
	}
	if reopened.Stats().Total != 0 {
		t.Fatal("idempotent retry reinserted an acknowledged message")
	}
}

func TestEnqueueIdempotencyExpires(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	q, err := openWithConfig(filepath.Join(t.TempDir(), "queue.wal"), Config{
		Discipline:           FIFO,
		IdempotencyRetention: time.Minute,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	first, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 0, 0, "expiring-key")
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	second, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 0, 0, "expiring-key")
	if err != nil {
		t.Fatal(err)
	}
	if second.Replayed || second.Message.ID == first.Message.ID {
		t.Fatalf("expired key returned original result: %#v", second)
	}
	if q.Stats().Total != 2 {
		t.Fatal("reusing an expired key should enqueue a new message")
	}
}

func TestIdempotencyExpirationPrunesLookupIndex(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	q, err := openWithConfig(filepath.Join(t.TempDir(), "queue.wal"), Config{
		Discipline:           FIFO,
		IdempotencyRetention: time.Minute,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	if _, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 0, 0, "expired-key"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Minute)
	if _, err := q.EnqueueIdempotent(json.RawMessage(`{"job":2}`), 0, 0, "live-key"); err != nil {
		t.Fatal(err)
	}
	if len(q.idempotency) != 1 || q.idempotency["live-key"].Message.ID == "" {
		t.Fatalf("idempotency index was not pruned: %#v", q.idempotency)
	}
	if q.idempotencyExpiry.Len() != 1 {
		t.Fatalf("expiration heap contains %d entries, want 1", q.idempotencyExpiry.Len())
	}
}

func TestConcurrentEnqueueSameIdempotencyKeyCreatesOneMessage(t *testing.T) {
	q := openTestQueue(t, FIFO)
	const callers = 32
	start := make(chan struct{})
	results := make(chan EnqueueResult, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 9, 0, "concurrent-key")
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id string
	created := 0
	for result := range results {
		if id == "" {
			id = result.Message.ID
		}
		if result.Message.ID != id {
			t.Fatalf("got message %s, want %s", result.Message.ID, id)
		}
		if !result.Replayed {
			created++
		}
	}
	if created != 1 || q.Stats().Total != 1 {
		t.Fatalf("created=%d total=%d, want 1 and 1", created, q.Stats().Total)
	}
	assertIndexConsistency(t, q)
}

func TestConcurrentEnqueueConflictingIdempotencyInputsHasOneWinner(t *testing.T) {
	q := openTestQueue(t, FIFO)
	start := make(chan struct{})
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	for _, body := range []string{`{"job":1}`, `{"job":2}`} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := q.EnqueueIdempotent(json.RawMessage(body), 0, 0, "contended-key")
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)

	succeeded := 0
	conflicted := 0
	for err := range errCh {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrIdempotencyConflict):
			conflicted++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 || q.Stats().Total != 1 {
		t.Fatalf("succeeded=%d conflicted=%d total=%d", succeeded, conflicted, q.Stats().Total)
	}
	assertIndexConsistency(t, q)
}

func TestEnqueueWithoutIdempotencyKeyCreatesDistinctMessages(t *testing.T) {
	t.Parallel()
	q := openTestQueue(t, FIFO)
	first, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 0, 0, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.Message.ID == second.Message.ID || first.Replayed || second.Replayed {
		t.Fatalf("unkeyed enqueues were deduplicated: first=%#v second=%#v", first, second)
	}
}

func TestIdempotencyWALFailureDoesNotMutateState(t *testing.T) {
	t.Parallel()
	q := openTestQueue(t, FIFO)
	if err := q.wal.file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 0, 0, "failed-key")
	if err == nil {
		t.Fatal("enqueue succeeded after WAL was closed")
	}
	if len(q.messages) != 0 || len(q.idempotency) != 0 || q.nextSeq != 0 || q.ready.Len() != 0 || q.scheduled.Len() != 0 {
		t.Fatalf("failed append mutated state: messages=%d keys=%d seq=%d ready=%d scheduled=%d",
			len(q.messages), len(q.idempotency), q.nextSeq, q.ready.Len(), q.scheduled.Len())
	}
}

func TestEnqueueRejectsOversizedIdempotencyKey(t *testing.T) {
	t.Parallel()
	q := openTestQueue(t, FIFO)
	_, err := q.EnqueueIdempotent(json.RawMessage(`{"job":1}`), 0, 0, string(make([]byte, MaxIdempotencyKeyBytes+1)))
	if !errors.Is(err, ErrInvalidIdempotencyKey) {
		t.Fatalf("error = %v, want ErrInvalidIdempotencyKey", err)
	}
}

func TestReadyHeapMatchesReferenceOrder(t *testing.T) {
	t.Parallel()
	for _, discipline := range []Discipline{FIFO, LIFO} {
		t.Run(string(discipline), func(t *testing.T) {
			ready := readyHeap{discipline: discipline}
			var reference []*storedMessage
			for sequence := uint64(1); sequence <= 500; sequence++ {
				m := &storedMessage{
					Message:  Message{ID: fmt.Sprintf("message-%03d", sequence), Priority: int(sequence*17%11) - 5},
					Sequence: sequence,
				}
				heap.Push(&ready, m)
				reference = append(reference, m)
			}
			sort.Slice(reference, func(i, j int) bool {
				return beforeForDiscipline(discipline, reference[i], reference[j])
			})

			for i, want := range reference {
				got := heap.Pop(&ready).(*storedMessage)
				if got.ID != want.ID {
					t.Fatalf("pop %d = %s, want %s", i, got.ID, want.ID)
				}
				if got.heapKind != heapNone || got.heapIndex != -1 {
					t.Fatalf("popped message retains heap membership: kind=%d index=%d", got.heapKind, got.heapIndex)
				}
			}
		})
	}
}

func TestIndexesTrackMessageLifecycle(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	q, err := openWithClock(filepath.Join(t.TempDir(), "queue.wal"), FIFO, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })

	enqueue(t, q, `"ready"`, 1, 0)
	enqueue(t, q, `"delayed"`, 10, time.Minute)
	assertIndexConsistency(t, q)
	if q.ready.Len() != 1 || q.scheduled.Len() != 1 {
		t.Fatalf("initial heaps = ready:%d scheduled:%d", q.ready.Len(), q.scheduled.Len())
	}

	d, err := q.Reserve(30 * time.Second)
	if err != nil || d == nil || string(d.Body) != `"ready"` {
		t.Fatalf("reserve = %#v, %v", d, err)
	}
	assertIndexConsistency(t, q)
	if q.ready.Len() != 0 || q.scheduled.Len() != 2 {
		t.Fatalf("leased heaps = ready:%d scheduled:%d", q.ready.Len(), q.scheduled.Len())
	}

	if err := q.Nack(d.ID, d.Receipt, 0); err != nil {
		t.Fatal(err)
	}
	assertIndexConsistency(t, q)
	if q.ready.Len() != 1 || q.scheduled.Len() != 1 {
		t.Fatalf("nacked heaps = ready:%d scheduled:%d", q.ready.Len(), q.scheduled.Len())
	}

	d, err = q.Reserve(30 * time.Second)
	if err != nil || d == nil {
		t.Fatalf("second reserve = %#v, %v", d, err)
	}
	if err := q.Ack(d.ID, d.Receipt); err != nil {
		t.Fatal(err)
	}
	assertIndexConsistency(t, q)
	if q.ready.Len() != 0 || q.scheduled.Len() != 1 {
		t.Fatalf("acked heaps = ready:%d scheduled:%d", q.ready.Len(), q.scheduled.Len())
	}

	now = now.Add(time.Minute)
	d, err = q.Reserve(30 * time.Second)
	if err != nil || d == nil || string(d.Body) != `"delayed"` {
		t.Fatalf("delayed reserve = %#v, %v", d, err)
	}
	assertIndexConsistency(t, q)
}

func TestOrderCombinesPriorityAndDiscipline(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		discipline Discipline
		want       []string
	}{
		{name: "FIFO", discipline: FIFO, want: []string{`"high-old"`, `"high-new"`, `"low"`}},
		{name: "LIFO", discipline: LIFO, want: []string{`"high-new"`, `"high-old"`, `"low"`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := openTestQueue(t, test.discipline)
			enqueue(t, q, `"low"`, 1, 0)
			enqueue(t, q, `"high-old"`, 10, 0)
			enqueue(t, q, `"high-new"`, 10, 0)

			for _, want := range test.want {
				d, err := q.Reserve(time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if d == nil || string(d.Body) != want {
					t.Fatalf("got delivery %#v, want body %s", d, want)
				}
				if err := q.Ack(d.ID, d.Receipt); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDelayAndNack(t *testing.T) {
	t.Parallel()
	q := openTestQueue(t, FIFO)
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	q.clock = functionClock{now: func() time.Time { return now }}
	enqueue(t, q, `{"job":1}`, 0, time.Minute)

	if got, err := q.Reserve(time.Minute); err != nil || got != nil {
		t.Fatalf("early reserve = %#v, %v", got, err)
	}
	now = now.Add(time.Minute)
	d, err := q.Reserve(time.Minute)
	if err != nil || d == nil {
		t.Fatalf("reserve = %#v, %v", d, err)
	}
	if err := q.Nack(d.ID, d.Receipt, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if got, _ := q.Reserve(time.Minute); got != nil {
		t.Fatal("nacked message was visible before its delay")
	}
	now = now.Add(2 * time.Minute)
	replayed, err := q.Reserve(time.Minute)
	if err != nil || replayed == nil || replayed.Attempts != 2 {
		t.Fatalf("redelivery = %#v, %v", replayed, err)
	}
}

func TestRestartPreservesMessagesAndLease(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "queue.wal")
	now := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	q, err := openWithClock(path, FIFO, clock)
	if err != nil {
		t.Fatal(err)
	}
	enqueue(t, q, `"durable"`, 2, 0)
	d, err := q.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	now = now.Add(30 * time.Second)
	reopened, err := openWithClock(path, FIFO, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if got, _ := reopened.Reserve(time.Minute); got != nil {
		t.Fatal("leased message became visible after restart")
	}
	now = now.Add(31 * time.Second)
	replayed, err := reopened.Reserve(time.Minute)
	if err != nil || replayed == nil || replayed.ID != d.ID || replayed.Attempts != 2 {
		t.Fatalf("redelivery = %#v, %v", replayed, err)
	}
}

func TestConcurrentConsumersDeliverEachMessageOncePerLease(t *testing.T) {
	q := openTestQueue(t, FIFO)
	const count = 400
	for i := 0; i < count; i++ {
		enqueue(t, q, fmt.Sprintf(`{"n":%d}`, i), i%5, 0)
	}

	var processed atomic.Int64
	seen := make(map[string]bool)
	var seenMu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				d, err := q.Reserve(time.Hour)
				if err != nil {
					t.Error(err)
					return
				}
				if d == nil {
					return
				}
				seenMu.Lock()
				if seen[d.ID] {
					t.Errorf("duplicate concurrent delivery for %s", d.ID)
				}
				seen[d.ID] = true
				seenMu.Unlock()
				if err := q.Ack(d.ID, d.Receipt); err != nil {
					t.Error(err)
					return
				}
				processed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := processed.Load(); got != count {
		t.Fatalf("processed %d messages, want %d", got, count)
	}
}

func TestWALTruncatesTornTail(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "queue.wal")
	q, err := Open(path, FIFO)
	if err != nil {
		t.Fatal(err)
	}
	enqueue(t, q, `"safe"`, 0, 0)
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	reopened, err := Open(path, FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	after, _ := os.Stat(path)
	if after.Size() != before.Size() {
		t.Fatalf("WAL size after recovery = %d, want %d", after.Size(), before.Size())
	}
	d, err := reopened.Reserve(time.Minute)
	if err != nil || d == nil || string(d.Body) != `"safe"` {
		t.Fatalf("recovered delivery = %#v, %v", d, err)
	}
}

func TestRejectsDisciplineChange(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "queue.wal")
	q, err := Open(path, FIFO)
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Close()
	if _, err := Open(path, LIFO); err == nil {
		t.Fatal("expected persisted discipline mismatch")
	}
}

func openTestQueue(t *testing.T, discipline Discipline) *Queue {
	t.Helper()
	q, err := Open(filepath.Join(t.TempDir(), "queue.wal"), discipline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func enqueue(t *testing.T, q *Queue, body string, priority int, delay time.Duration) Message {
	t.Helper()
	m, err := q.Enqueue(json.RawMessage(body), priority, delay)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func assertIndexConsistency(t *testing.T, q *Queue) {
	t.Helper()
	seen := make(map[string]bool, len(q.messages))
	for index, m := range q.ready.items {
		if m.heapKind != heapReady || m.heapIndex != index {
			t.Fatalf("ready message %s has kind=%d index=%d, want kind=%d index=%d", m.ID, m.heapKind, m.heapIndex, heapReady, index)
		}
		seen[m.ID] = true
	}
	for index, m := range q.scheduled.items {
		if m.heapKind != heapScheduled || m.heapIndex != index || m.nextEligibleAt.IsZero() {
			t.Fatalf("scheduled message %s has kind=%d index=%d eligible=%s", m.ID, m.heapKind, m.heapIndex, m.nextEligibleAt)
		}
		if seen[m.ID] {
			t.Fatalf("message %s exists in both heaps", m.ID)
		}
		seen[m.ID] = true
	}
	if len(seen) != len(q.messages) {
		t.Fatalf("indexed %d messages, queue contains %d", len(seen), len(q.messages))
	}
	for id, m := range q.messages {
		if !seen[id] || m.heapKind == heapNone {
			t.Fatalf("message %s is not indexed", id)
		}
	}
}

var benchmarkSelected *storedMessage

func BenchmarkReserveSelection(b *testing.B) {
	for _, depth := range []int{100, 10_000} {
		messages := make([]*storedMessage, 0, depth)
		for sequence := 1; sequence <= depth; sequence++ {
			messages = append(messages, &storedMessage{
				Message:  Message{ID: fmt.Sprintf("message-%d", sequence), Priority: sequence % 17},
				Sequence: uint64(sequence),
			})
		}

		b.Run(fmt.Sprintf("heap/depth_%d", depth), func(b *testing.B) {
			ready := readyHeap{discipline: FIFO}
			for _, m := range messages {
				heap.Push(&ready, m)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				selected := heap.Pop(&ready).(*storedMessage)
				heap.Push(&ready, selected)
				benchmarkSelected = selected
			}
		})

		b.Run(fmt.Sprintf("scan/depth_%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				selected := messages[0]
				for _, candidate := range messages[1:] {
					if beforeForDiscipline(FIFO, candidate, selected) {
						selected = candidate
					}
				}
				benchmarkSelected = selected
			}
		})
	}
}
