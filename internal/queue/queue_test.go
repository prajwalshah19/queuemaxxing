package queue

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
	q.now = func() time.Time { return now }
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
	q, err := Open(path, FIFO)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return base }
	enqueue(t, q, `"durable"`, 2, 0)
	d, err := q.Reserve(time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, FIFO)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	reopened.now = func() time.Time { return base.Add(30 * time.Second) }
	if got, _ := reopened.Reserve(time.Minute); got != nil {
		t.Fatal("leased message became visible after restart")
	}
	reopened.now = func() time.Time { return base.Add(61 * time.Second) }
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
				d, err := q.Reserve(time.Minute)
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
