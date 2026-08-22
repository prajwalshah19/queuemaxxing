package queue

import (
	"errors"
	"testing"
	"time"
)

func TestExtendLeasePersistsWithoutChangingDeliveryGeneration(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO, RetryPolicy: testRetryPolicy(3)})
	enqueue(t, h.q, `{"job":1}`, 0, 0)
	delivery, err := h.q.Reserve(30 * time.Second)
	if err != nil || delivery == nil {
		t.Fatalf("reserve = %#v, %v", delivery, err)
	}

	h.advance(10 * time.Second)
	extension, err := h.q.ExtendLease(delivery.ID, delivery.Receipt, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	wantDeadline := h.now.Add(time.Minute)
	if !extension.LeaseUntil.Equal(wantDeadline) {
		t.Fatalf("lease deadline = %s, want %s", extension.LeaseUntil, wantDeadline)
	}
	stored := h.q.messages[delivery.ID]
	if stored.Receipt != delivery.Receipt || stored.Attempts != 1 {
		t.Fatalf("extension changed delivery generation: %#v", stored)
	}
	if got := h.countEvents("extend_lease"); got != 1 {
		t.Fatalf("extend_lease events = %d, want 1", got)
	}

	h.reopen()
	stored = h.q.messages[delivery.ID]
	if stored == nil || stored.Receipt != delivery.Receipt || stored.Attempts != 1 || !stored.LeaseUntil.Equal(wantDeadline) {
		t.Fatalf("replayed lease = %#v", stored)
	}
	h.advance(20 * time.Second)
	if got, err := h.q.Reserve(time.Minute); err != nil || got != nil {
		t.Fatalf("message returned at original deadline: %#v, %v", got, err)
	}
}

func TestExtendLeaseRejectsWrongExpiredAndNonExtendingRequests(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	enqueue(t, h.q, `{"job":1}`, 0, 0)
	delivery, err := h.q.Reserve(30 * time.Second)
	if err != nil || delivery == nil {
		t.Fatalf("reserve = %#v, %v", delivery, err)
	}
	before, err := h.q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := h.q.ExtendLease(delivery.ID, "wrong", time.Minute); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("wrong-receipt error = %v", err)
	}
	if _, err := h.q.ExtendLease(delivery.ID, delivery.Receipt, 10*time.Second); !errors.Is(err, ErrInvalidLeaseExtension) {
		t.Fatalf("non-extending error = %v", err)
	}
	if _, err := h.q.ExtendLease(delivery.ID, delivery.Receipt, 0); !errors.Is(err, ErrInvalidLeaseExtension) {
		t.Fatalf("zero-duration error = %v", err)
	}
	h.advance(30 * time.Second)
	if _, err := h.q.ExtendLease(delivery.ID, delivery.Receipt, time.Minute); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("expired-receipt error = %v", err)
	}

	after, err := h.q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("rejected extensions changed WAL size: %d -> %d", before.Size(), after.Size())
	}
}

func TestExtendLeaseWALFailureLeavesMessageAndHeapUnchanged(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	enqueue(t, h.q, `{"job":1}`, 0, 0)
	delivery, err := h.q.Reserve(30 * time.Second)
	if err != nil || delivery == nil {
		t.Fatalf("reserve = %#v, %v", delivery, err)
	}
	stored := h.q.messages[delivery.ID]
	oldDeadline := stored.LeaseUntil
	oldEligibleAt := stored.nextEligibleAt
	if err := h.q.wal.file.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := h.q.ExtendLease(delivery.ID, delivery.Receipt, time.Minute); err == nil {
		t.Fatal("extension succeeded with closed WAL")
	}
	if !stored.LeaseUntil.Equal(oldDeadline) || !stored.nextEligibleAt.Equal(oldEligibleAt) || stored.Receipt != delivery.Receipt || stored.Attempts != 1 {
		t.Fatalf("failed extension mutated state: %#v", stored)
	}
}

func TestExtendLeaseReordersScheduledHeap(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	first := enqueue(t, h.q, `"first"`, 0, 0)
	second := enqueue(t, h.q, `"second"`, 0, 0)
	firstDelivery, _ := h.q.Reserve(30 * time.Second)
	secondDelivery, _ := h.q.Reserve(time.Minute)
	if firstDelivery.ID != first.ID || secondDelivery.ID != second.ID {
		t.Fatalf("unexpected deliveries: %#v %#v", firstDelivery, secondDelivery)
	}
	if h.q.scheduled.items[0].ID != first.ID {
		t.Fatalf("scheduled root = %s, want %s", h.q.scheduled.items[0].ID, first.ID)
	}
	if _, err := h.q.ExtendLease(firstDelivery.ID, firstDelivery.Receipt, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if h.q.scheduled.items[0].ID != second.ID {
		t.Fatalf("scheduled root after extension = %s, want %s", h.q.scheduled.items[0].ID, second.ID)
	}
	assertIndexConsistency(t, h.q)
}
