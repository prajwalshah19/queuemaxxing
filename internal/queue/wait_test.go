package queue

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestReserveWaitReturnsImmediatelyWhenReady(t *testing.T) {
	q, clock := openManualClockQueue(t)
	m := enqueue(t, q, `{"job":1}`, 0, 0)
	delivery, err := q.ReserveWait(context.Background(), time.Minute, 20*time.Second)
	if err != nil || delivery == nil || delivery.ID != m.ID {
		t.Fatalf("reserve wait = %#v, %v", delivery, err)
	}
	if clock.timerCount() != 0 {
		t.Fatalf("immediate reserve created %d timers", clock.timerCount())
	}
}

func TestReserveWaitTimeoutDoesNotMutateWAL(t *testing.T) {
	q, clock := openManualClockQueue(t)
	before, err := q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan reserveWaitResult, 1)
	go reserveInto(q, context.Background(), 10*time.Second, result)
	clock.waitForTimer(t)
	clock.Advance(10 * time.Second)
	got := waitForReserveResult(t, result)
	if got.err != nil || got.delivery != nil {
		t.Fatalf("timed reserve = %#v, %v", got.delivery, got.err)
	}
	after, err := q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("timeout changed WAL size: %d -> %d", before.Size(), after.Size())
	}
}

func TestEnqueueWakesReserveWaitWithoutLostWakeup(t *testing.T) {
	q, clock := openManualClockQueue(t)
	result := make(chan reserveWaitResult, 1)
	go reserveInto(q, context.Background(), time.Minute, result)
	clock.waitForTimer(t)
	m := enqueue(t, q, `{"job":1}`, 0, 0)
	got := waitForReserveResult(t, result)
	if got.err != nil || got.delivery == nil || got.delivery.ID != m.ID {
		t.Fatalf("woken reserve = %#v, %v", got.delivery, got.err)
	}
}

func TestDelayedMessageWakesReserveWaitAtEligibility(t *testing.T) {
	q, clock := openManualClockQueue(t)
	m := enqueue(t, q, `{"job":1}`, 0, 10*time.Second)
	result := make(chan reserveWaitResult, 1)
	go reserveInto(q, context.Background(), time.Minute, result)
	clock.waitForTimer(t)
	clock.Advance(10 * time.Second)
	got := waitForReserveResult(t, result)
	if got.err != nil || got.delivery == nil || got.delivery.ID != m.ID {
		t.Fatalf("delayed reserve = %#v, %v", got.delivery, got.err)
	}
}

func TestReserveWaitCancellationDoesNotReserve(t *testing.T) {
	q, clock := openManualClockQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan reserveWaitResult, 1)
	go reserveInto(q, ctx, time.Minute, result)
	clock.waitForTimer(t)
	cancel()
	got := waitForReserveResult(t, result)
	if !errors.Is(got.err, context.Canceled) || got.delivery != nil {
		t.Fatalf("cancelled reserve = %#v, %v", got.delivery, got.err)
	}
	if countWALEvents(t, q, "reserve") != 0 {
		t.Fatal("cancelled wait created a reservation")
	}
}

func TestCloseWakesAllReserveWaiters(t *testing.T) {
	q, clock := openManualClockQueue(t)
	results := make(chan reserveWaitResult, 4)
	for range 4 {
		go reserveInto(q, context.Background(), time.Minute, results)
	}
	for range 4 {
		clock.waitForTimer(t)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		got := waitForReserveResult(t, results)
		if !errors.Is(got.err, ErrClosed) || got.delivery != nil {
			t.Fatalf("closed reserve = %#v, %v", got.delivery, got.err)
		}
	}
}

func TestConcurrentReserveWaitersCreateOneReservation(t *testing.T) {
	q, clock := openManualClockQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const waiterCount = 8
	results := make(chan reserveWaitResult, waiterCount)
	for range waiterCount {
		go reserveInto(q, ctx, time.Minute, results)
	}
	for range waiterCount {
		clock.waitForTimer(t)
	}
	m := enqueue(t, q, `{"job":1}`, 0, 0)

	first := waitForReserveResult(t, results)
	if first.err != nil || first.delivery == nil || first.delivery.ID != m.ID {
		t.Fatalf("first reserve = %#v, %v", first.delivery, first.err)
	}
	cancel()
	for range waiterCount - 1 {
		got := waitForReserveResult(t, results)
		if got.delivery != nil || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("losing waiter = %#v, %v", got.delivery, got.err)
		}
	}
	if got := countWALEvents(t, q, "reserve"); got != 1 {
		t.Fatalf("reserve events = %d, want 1", got)
	}
}

func TestLeaseExtensionMovesReserveWaitTimer(t *testing.T) {
	q, clock := openManualClockQueue(t)
	enqueue(t, q, `{"job":1}`, 0, 0)
	delivery, err := q.Reserve(30 * time.Second)
	if err != nil || delivery == nil {
		t.Fatalf("reserve = %#v, %v", delivery, err)
	}

	result := make(chan reserveWaitResult, 1)
	go reserveInto(q, context.Background(), 2*time.Minute, result)
	clock.waitForTimer(t)
	if _, err := q.ExtendLease(delivery.ID, delivery.Receipt, time.Minute); err != nil {
		t.Fatal(err)
	}
	clock.waitForTimer(t)

	clock.Advance(30 * time.Second)
	select {
	case got := <-result:
		t.Fatalf("wait returned at old lease deadline: %#v, %v", got.delivery, got.err)
	default:
	}

	clock.Advance(30 * time.Second)
	clock.waitForTimer(t) // retry backoff after the extended lease expires
	clock.Advance(time.Second)
	got := waitForReserveResult(t, result)
	if got.err != nil || got.delivery == nil || got.delivery.ID != delivery.ID || got.delivery.Attempts != 2 {
		t.Fatalf("redelivery after extension = %#v, %v", got.delivery, got.err)
	}
}

type reserveWaitResult struct {
	delivery *Delivery
	err      error
}

func reserveInto(q *Queue, ctx context.Context, wait time.Duration, result chan<- reserveWaitResult) {
	delivery, err := q.ReserveWait(ctx, time.Minute, wait)
	result <- reserveWaitResult{delivery: delivery, err: err}
}

func waitForReserveResult(t *testing.T, result <-chan reserveWaitResult) reserveWaitResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reserve result")
		return reserveWaitResult{}
	}
}

func openManualClockQueue(t *testing.T) (*Queue, *manualQueueClock) {
	t.Helper()
	clock := newManualQueueClock(time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC))
	q, err := openWithConfigAndClock(filepath.Join(t.TempDir(), "queue.wal"), Config{Discipline: FIFO}, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q, clock
}

type manualQueueClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  map[*manualQueueTimer]struct{}
	created chan struct{}
}

func newManualQueueClock(now time.Time) *manualQueueClock {
	return &manualQueueClock{
		now:     now,
		timers:  make(map[*manualQueueTimer]struct{}),
		created: make(chan struct{}, 128),
	}
}

func (c *manualQueueClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualQueueClock) NewTimer(d time.Duration) queueTimer {
	timer := &manualQueueTimer{clock: c, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	timer.deadline = c.now.Add(d)
	if d <= 0 {
		timer.fired = true
		timer.ch <- c.now
	} else {
		c.timers[timer] = struct{}{}
	}
	c.mu.Unlock()
	c.created <- struct{}{}
	return timer
}

func (c *manualQueueClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	for timer := range c.timers {
		if timer.deadline.After(c.now) {
			continue
		}
		delete(c.timers, timer)
		timer.fired = true
		timer.ch <- c.now
	}
	c.mu.Unlock()
}

func (c *manualQueueClock) waitForTimer(t *testing.T) {
	t.Helper()
	select {
	case <-c.created:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for timer creation")
	}
}

func (c *manualQueueClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

type manualQueueTimer struct {
	clock    *manualQueueClock
	ch       chan time.Time
	deadline time.Time
	fired    bool
	stopped  bool
}

func (t *manualQueueTimer) C() <-chan time.Time { return t.ch }

func (t *manualQueueTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	delete(t.clock.timers, t)
	return true
}
