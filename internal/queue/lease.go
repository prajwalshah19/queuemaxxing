package queue

import (
	"fmt"
	"time"
)

type LeaseExtension struct {
	LeaseUntil time.Time `json:"lease_until"`
}

func (q *Queue) ExtendLease(id, receipt string, visibilityTimeout time.Duration) (LeaseExtension, error) {
	if visibilityTimeout <= 0 {
		return LeaseExtension{}, fmt.Errorf("%w: visibility timeout must be positive", ErrInvalidLeaseExtension)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return LeaseExtension{}, ErrClosed
	}

	now := q.clock.Now().UTC()
	m, ok := q.messages[id]
	if !ok || receipt == "" || m.Receipt != receipt || !now.Before(m.LeaseUntil) {
		return LeaseExtension{}, ErrInvalidReceipt
	}
	newLeaseUntil := now.Add(visibilityTimeout)
	if !newLeaseUntil.After(m.LeaseUntil) {
		return LeaseExtension{}, ErrInvalidLeaseExtension
	}
	if m.heapKind != heapScheduled || m.heapIndex < 0 {
		return LeaseExtension{}, fmt.Errorf("leased message %s is missing from the scheduled index", id)
	}
	if err := q.wal.Append(event{
		Type:       "extend_lease",
		ID:         id,
		Receipt:    receipt,
		LeaseUntil: newLeaseUntil,
	}); err != nil {
		return LeaseExtension{}, err
	}

	m.LeaseUntil = newLeaseUntil
	q.reschedule(m, newLeaseUntil)
	q.notifyLocked()
	return LeaseExtension{LeaseUntil: newLeaseUntil}, nil
}
