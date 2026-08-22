//go:build fidelity

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentProducerConsumerLeaseSoak(t *testing.T) {
	const (
		producerCount = 8
		perProducer   = 30
		consumerCount = 12
		totalMessages = producerCount * perProducer
	)
	path := filepath.Join(t.TempDir(), "queue.wal")
	config := Config{
		Discipline:  FIFO,
		RetryPolicy: RetryPolicy{MaxAttempts: 5, BaseDelay: 10 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
	}
	q, err := OpenWithConfig(path, config)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var consumed atomic.Int64
	var producerWG sync.WaitGroup
	var consumerWG sync.WaitGroup
	var setsMu sync.Mutex
	produced := make(map[string]struct{}, totalMessages)
	seen := make(map[string]struct{}, totalMessages)
	errs := make(chan error, totalMessages)

	for range consumerCount {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			for {
				if consumed.Load() >= totalMessages {
					return
				}
				delivery, err := q.ReserveWait(ctx, 5*time.Second, 100*time.Millisecond)
				if err != nil {
					if errors.Is(err, context.Canceled) && consumed.Load() >= totalMessages {
						return
					}
					errs <- fmt.Errorf("reserve: %w", err)
					return
				}
				if delivery == nil {
					continue
				}

				setsMu.Lock()
				_, duplicate := seen[delivery.ID]
				seen[delivery.ID] = struct{}{}
				setsMu.Unlock()
				if duplicate {
					errs <- fmt.Errorf("duplicate active delivery for %s", delivery.ID)
					return
				}
				if delivery.Attempts != 1 {
					errs <- fmt.Errorf("message %s delivered at attempt %d", delivery.ID, delivery.Attempts)
					return
				}
				if delivery.Priority%3 == 0 {
					if _, err := q.ExtendLease(delivery.ID, delivery.Receipt, 10*time.Second); err != nil {
						errs <- fmt.Errorf("extend %s: %w", delivery.ID, err)
						return
					}
				}
				if err := q.Ack(delivery.ID, delivery.Receipt); err != nil {
					errs <- fmt.Errorf("ack %s: %w", delivery.ID, err)
					return
				}
				if consumed.Add(1) == totalMessages {
					cancel()
					return
				}
			}
		}()
	}

	for producer := range producerCount {
		producerWG.Add(1)
		go func() {
			defer producerWG.Done()
			for item := range perProducer {
				body := json.RawMessage(fmt.Sprintf(`{"producer":%d,"item":%d}`, producer, item))
				message, err := q.Enqueue(body, (producer+item)%7-3, time.Duration((producer+item)%3)*time.Millisecond)
				if err != nil {
					errs <- fmt.Errorf("enqueue producer=%d item=%d: %w", producer, item, err)
					return
				}
				setsMu.Lock()
				produced[message.ID] = struct{}{}
				setsMu.Unlock()
			}
		}()
	}

	producerWG.Wait()
	consumerWG.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	if t.Failed() {
		_ = q.Close()
		return
	}
	if consumed.Load() != totalMessages {
		t.Fatalf("consumed %d messages, want %d", consumed.Load(), totalMessages)
	}
	setsMu.Lock()
	if len(produced) != totalMessages || len(seen) != totalMessages {
		t.Fatalf("produced/seen = %d/%d, want %d/%d", len(produced), len(seen), totalMessages, totalMessages)
	}
	for id := range produced {
		if _, ok := seen[id]; !ok {
			t.Fatalf("produced message %s was not delivered", id)
		}
	}
	setsMu.Unlock()
	if stats := q.Stats(); stats.Total != 0 || stats.DeadLetters != 0 {
		t.Fatalf("queue did not drain: %#v", stats)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithConfig(path, config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if stats := reopened.Stats(); stats.Total != 0 || stats.DeadLetters != 0 {
		t.Fatalf("drained state did not survive restart: %#v", stats)
	}
}

func TestReserveWaitTimeoutAndCancellationDoNotLeakGoroutines(t *testing.T) {
	q, err := Open(filepath.Join(t.TempDir(), "queue.wal"), FIFO)
	if err != nil {
		t.Fatal(err)
	}
	baseline := runtime.NumGoroutine()
	const waiterCount = 300
	var waiters sync.WaitGroup
	cancels := make([]context.CancelFunc, 0, waiterCount/2)
	errs := make(chan error, waiterCount)

	for index := range waiterCount {
		waiters.Add(1)
		if index%2 == 0 {
			ctx, cancel := context.WithCancel(context.Background())
			cancels = append(cancels, cancel)
			go func() {
				defer waiters.Done()
				_, err := q.ReserveWait(ctx, time.Minute, time.Second)
				if !errors.Is(err, context.Canceled) {
					errs <- fmt.Errorf("cancelled waiter returned %v", err)
				}
			}()
			continue
		}
		go func() {
			defer waiters.Done()
			delivery, err := q.ReserveWait(context.Background(), time.Minute, 5*time.Millisecond)
			if err != nil || delivery != nil {
				errs <- fmt.Errorf("timed waiter returned %#v, %v", delivery, err)
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	for _, cancel := range cancels {
		cancel()
	}
	done := make(chan struct{})
	go func() {
		waiters.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("waiters did not exit")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		current := runtime.NumGoroutine()
		if current <= baseline+12 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines after waiter cleanup = %d, baseline = %d", current, baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
