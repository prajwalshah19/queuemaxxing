package queue

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRetryBackoffTable(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 10, BaseDelay: time.Second, MaxDelay: 5 * time.Second}
	for _, test := range []struct {
		attempts uint64
		want     time.Duration
	}{
		{attempts: 1, want: time.Second},
		{attempts: 2, want: 2 * time.Second},
		{attempts: 3, want: 4 * time.Second},
		{attempts: 4, want: 5 * time.Second},
		{attempts: 64, want: 5 * time.Second},
	} {
		if got := retryBackoff(policy, test.attempts); got != test.want {
			t.Fatalf("attempts %d: backoff = %s, want %s", test.attempts, got, test.want)
		}
	}
}

func TestNackDelayOverride(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, err := h.q.Reserve(time.Minute)
	if err != nil || d == nil {
		t.Fatalf("reserve = %#v, %v", d, err)
	}
	override := time.Duration(0)
	result, err := h.q.NackWithOptions(d.ID, d.Receipt, &override)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != RetryScheduled || result.VisibleAt == nil || !result.VisibleAt.Equal(h.now) || result.Attempts != 1 {
		t.Fatalf("nack result = %#v", result)
	}
	redelivery, err := h.q.Reserve(time.Minute)
	if err != nil || redelivery == nil || redelivery.Attempts != 2 {
		t.Fatalf("immediate redelivery = %#v, %v", redelivery, err)
	}
}

func TestLeaseExpirySchedulesRetry(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, err := h.q.Reserve(10 * time.Second)
	if err != nil || d == nil {
		t.Fatalf("reserve = %#v, %v", d, err)
	}
	h.advance(10 * time.Second)
	if got, err := h.q.Reserve(time.Minute); err != nil || got != nil {
		t.Fatalf("reserve at lease expiry = %#v, %v", got, err)
	}
	if h.countEvents("retry_scheduled") != 1 {
		t.Fatal("lease expiry did not persist exactly one retry transition")
	}
	h.advance(time.Second)
	redelivery, err := h.q.Reserve(time.Minute)
	if err != nil || redelivery == nil || redelivery.ID != d.ID || redelivery.Attempts != 2 {
		t.Fatalf("redelivery = %#v, %v", redelivery, err)
	}
}

func TestMaxAttemptsMovesMessageToDeadLetter(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(2)})
	m := enqueue(t, h.q, `{"job":1}`, 7, 0)

	first, _ := h.q.Reserve(time.Minute)
	zero := time.Duration(0)
	if _, err := h.q.NackWithOptions(first.ID, first.Receipt, &zero); err != nil {
		t.Fatal(err)
	}
	second, _ := h.q.Reserve(time.Minute)
	result, err := h.q.NackWithOptions(second.ID, second.Receipt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != DeadLettered || result.Attempts != 2 {
		t.Fatalf("result = %#v", result)
	}
	if h.q.Stats().Total != 0 || h.q.Stats().DeadLetters != 1 {
		t.Fatalf("stats = %#v", h.q.Stats())
	}
	letters, err := h.q.ListDeadLetters(100)
	if err != nil || len(letters) != 1 || letters[0].ID != m.ID || letters[0].Reason != "max_attempts" {
		t.Fatalf("dead letters = %#v, %v", letters, err)
	}
	assertIndexConsistency(t, h.q)
}

func TestExpiredReceiptCannotAckOrNack(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, _ := h.q.Reserve(time.Second)
	h.advance(time.Second)
	before, _ := h.q.wal.file.Stat()
	if err := h.q.Ack(d.ID, d.Receipt); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("ack error = %v", err)
	}
	if _, err := h.q.NackWithOptions(d.ID, d.Receipt, nil); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("nack error = %v", err)
	}
	after, _ := h.q.wal.file.Stat()
	if after.Size() != before.Size() {
		t.Fatalf("stale receipt changed WAL size: %d -> %d", before.Size(), after.Size())
	}
}

func TestScheduledRetrySurvivesRestart(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	m := enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, _ := h.q.Reserve(time.Minute)
	result, err := h.q.NackWithOptions(d.ID, d.Receipt, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.reopen()
	if got, err := h.q.Reserve(time.Minute); err != nil || got != nil {
		t.Fatalf("early reserve = %#v, %v", got, err)
	}
	h.advance(time.Second)
	got, err := h.q.Reserve(time.Minute)
	if err != nil || got == nil || got.ID != m.ID || result.VisibleAt == nil || !result.VisibleAt.Equal(h.now) {
		t.Fatalf("restarted redelivery = %#v, %v", got, err)
	}
}

func TestDeadLetterSurvivesRestart(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(1)})
	m := enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, _ := h.q.Reserve(time.Minute)
	if _, err := h.q.NackWithOptions(d.ID, d.Receipt, nil); err != nil {
		t.Fatal(err)
	}
	h.reopen()
	letters, err := h.q.ListDeadLetters(100)
	if err != nil || len(letters) != 1 || letters[0].ID != m.ID || letters[0].Attempts != 1 {
		t.Fatalf("dead letters after restart = %#v, %v", letters, err)
	}
}

func TestDeadLetterReplayCreatesNewIdentityAtomically(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(1)})
	original := enqueue(t, h.q, `{"job":1}`, 9, 0)
	d, _ := h.q.Reserve(time.Minute)
	if _, err := h.q.NackWithOptions(d.ID, d.Receipt, nil); err != nil {
		t.Fatal(err)
	}
	replayed, err := h.q.ReplayDeadLetter(original.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID == original.ID || replayed.OriginalMessageID != original.ID || replayed.Attempts != 0 {
		t.Fatalf("replayed message = %#v", replayed)
	}
	h.reopen()
	letters, _ := h.q.ListDeadLetters(100)
	if len(letters) != 0 {
		t.Fatalf("dead letter remained after replay: %#v", letters)
	}
	got, err := h.q.Reserve(time.Minute)
	if err != nil || got == nil || got.ID != replayed.ID || got.Priority != 9 || string(got.Body) != `{"job":1}` {
		t.Fatalf("replayed delivery = %#v, %v", got, err)
	}
}

func TestConcurrentLeaseExpiryWritesOneTransition(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	enqueue(t, h.q, `{"job":1}`, 0, 0)
	if _, err := h.q.Reserve(time.Second); err != nil {
		t.Fatal(err)
	}
	h.advance(time.Second)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := h.q.Reserve(time.Minute); err != nil {
				t.Errorf("reserve: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := h.countEvents("retry_scheduled"); got != 1 {
		t.Fatalf("retry_scheduled events = %d, want 1", got)
	}
}

func TestLeaseExpiryAtMaxAttemptsDeadLetters(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(1)})
	m := enqueue(t, h.q, `{"job":1}`, 0, 0)
	if _, err := h.q.Reserve(time.Second); err != nil {
		t.Fatal(err)
	}
	h.advance(time.Second)
	if got, err := h.q.Reserve(time.Minute); err != nil || got != nil {
		t.Fatalf("reserve = %#v, %v", got, err)
	}
	letters, err := h.q.ListDeadLetters(100)
	if err != nil || len(letters) != 1 || letters[0].ID != m.ID {
		t.Fatalf("dead letters = %#v, %v", letters, err)
	}
}

func TestLeaseExpiryWALFailureDoesNotMutateState(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	m := enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, _ := h.q.Reserve(time.Second)
	h.advance(time.Second)
	if err := h.q.wal.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.q.Reserve(time.Minute); err == nil {
		t.Fatal("reserve succeeded with a closed WAL")
	}
	stored := h.q.messages[m.ID]
	if stored == nil || stored.Receipt != d.Receipt || stored.Attempts != 1 || stored.heapKind != heapScheduled || len(h.q.deadLetters) != 0 {
		t.Fatalf("failed lease transition mutated state: %#v", stored)
	}
}

func TestRetryPreservesPriorityAndDiscipline(t *testing.T) {
	for _, test := range []struct {
		discipline Discipline
		want       []string
	}{
		{discipline: FIFO, want: []string{`"high"`, `"retry"`, `"peer"`}},
		{discipline: LIFO, want: []string{`"high"`, `"peer"`, `"retry"`}},
	} {
		t.Run(string(test.discipline), func(t *testing.T) {
			h := newQueueHarness(t, Config{Discipline: test.discipline, RetryPolicy: testRetryPolicy(3)})
			enqueue(t, h.q, `"retry"`, 5, 0)
			first, err := h.q.Reserve(time.Minute)
			if err != nil || first == nil {
				t.Fatalf("first reserve = %#v, %v", first, err)
			}
			enqueue(t, h.q, `"peer"`, 5, 0)
			enqueue(t, h.q, `"high"`, 10, 0)
			zero := time.Duration(0)
			if _, err := h.q.NackWithOptions(first.ID, first.Receipt, &zero); err != nil {
				t.Fatal(err)
			}
			for _, want := range test.want {
				d, err := h.q.Reserve(time.Minute)
				if err != nil || d == nil || string(d.Body) != want {
					t.Fatalf("delivery = %#v, %v; want %s", d, err, want)
				}
				if err := h.q.Ack(d.ID, d.Receipt); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRetryWALFailureDoesNotMutateState(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	m := enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, _ := h.q.Reserve(time.Minute)
	if err := h.q.wal.file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := h.q.NackWithOptions(d.ID, d.Receipt, nil)
	if err == nil {
		t.Fatal("nack succeeded with a closed WAL")
	}
	stored := h.q.messages[m.ID]
	if stored == nil || stored.Receipt != d.Receipt || stored.heapKind != heapScheduled || len(h.q.deadLetters) != 0 {
		t.Fatalf("failed retry mutated state: %#v", stored)
	}
}

func TestRetryPolicyPersistsAndRejectsMismatch(t *testing.T) {
	path := t.TempDir() + "/queue.wal"
	policy := testRetryPolicy(3)
	q, err := OpenWithConfig(path, Config{Discipline: FIFO, RetryPolicy: policy})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithConfig(path, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(4)}); err == nil {
		t.Fatal("expected persisted retry policy mismatch")
	}
}

func TestDeadLetterReplayWALFailureLeavesOriginal(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(1)})
	m := enqueue(t, h.q, `{"job":1}`, 0, 0)
	d, _ := h.q.Reserve(time.Minute)
	if _, err := h.q.NackWithOptions(d.ID, d.Receipt, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.q.wal.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.q.ReplayDeadLetter(m.ID, 0); err == nil {
		t.Fatal("replay succeeded with a closed WAL")
	}
	if len(h.q.deadLetters) != 1 || h.q.messages[m.ID] != nil || h.q.nextSeq != 1 {
		t.Fatalf("failed replay mutated state: dead=%d live=%d seq=%d", len(h.q.deadLetters), len(h.q.messages), h.q.nextSeq)
	}
}

func TestOlderWALReceivesRetryPolicy(t *testing.T) {
	path := t.TempDir() + "/queue.wal"
	w, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(event{Type: "config", Discipline: FIFO}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	q, err := Open(path, FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if q.retryPolicy != DefaultRetryPolicy() || countWALEvents(t, q, "retry_policy") != 1 {
		t.Fatalf("retry policy = %#v", q.retryPolicy)
	}
}

func TestReplayDeadLetterUnknown(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	if _, err := h.q.ReplayDeadLetter("missing", 0); !errors.Is(err, ErrDeadLetterNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	for _, policy := range []RetryPolicy{
		{MaxAttempts: -1, BaseDelay: time.Second, MaxDelay: time.Second},
		{MaxAttempts: 1, BaseDelay: -time.Second, MaxDelay: time.Second},
		{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Millisecond},
	} {
		if _, err := OpenWithConfig(t.TempDir()+"/queue.wal", Config{Discipline: FIFO, RetryPolicy: policy}); err == nil {
			t.Fatalf("accepted invalid policy %#v", policy)
		}
	}
}

func TestDeadLetterListIsStable(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(1)})
	for _, body := range []string{`"first"`, `"second"`} {
		enqueue(t, h.q, body, 0, 0)
		d, _ := h.q.Reserve(time.Minute)
		if _, err := h.q.NackWithOptions(d.ID, d.Receipt, nil); err != nil {
			t.Fatal(err)
		}
	}
	letters, err := h.q.ListDeadLetters(1)
	if err != nil || len(letters) != 1 || string(letters[0].Body) != `"first"` {
		t.Fatalf("letters = %#v, %v", letters, err)
	}
}

func testRetryPolicy(maxAttempts int) RetryPolicy {
	return RetryPolicy{MaxAttempts: maxAttempts, BaseDelay: time.Second, MaxDelay: time.Minute}
}

func countWALEvents(t *testing.T, q *Queue, eventType string) int {
	t.Helper()
	count := 0
	if err := q.wal.Replay(func(e event) error {
		if e.Type == eventType {
			count++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return count
}
