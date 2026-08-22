package queue

import (
	"context"
	"errors"
	"time"
)

func (q *Queue) Reserve(visibilityTimeout time.Duration) (*Delivery, error) {
	return q.ReserveWait(context.Background(), visibilityTimeout, 0)
}

func (q *Queue) ReserveWait(ctx context.Context, visibilityTimeout, waitTimeout time.Duration) (*Delivery, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if visibilityTimeout <= 0 {
		return nil, errors.New("visibility timeout must be positive")
	}
	if waitTimeout < 0 {
		return nil, errors.New("wait timeout cannot be negative")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	deadline := q.clock.Now().UTC().Add(waitTimeout)
	for {
		q.mu.Lock()
		if err := q.ensureWritableLocked(); err != nil {
			q.mu.Unlock()
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			q.mu.Unlock()
			return nil, err
		}

		now := q.clock.Now().UTC()
		if err := q.advanceScheduled(now); err != nil {
			q.mu.Unlock()
			return nil, err
		}
		if q.ready.Len() > 0 {
			delivery, err := q.reserveReadyLocked(now, visibilityTimeout)
			q.mu.Unlock()
			return delivery, err
		}
		if waitTimeout == 0 || !now.Before(deadline) {
			q.mu.Unlock()
			return nil, nil
		}

		changed := q.changed
		wakeAt := deadline
		if q.scheduled.Len() > 0 && q.scheduled.items[0].nextEligibleAt.Before(wakeAt) {
			wakeAt = q.scheduled.items[0].nextEligibleAt
		}
		waitFor := wakeAt.Sub(now)
		if waitFor < 0 {
			waitFor = 0
		}
		timer := q.clock.NewTimer(waitFor)
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			stopAndDrainTimer(timer)
			return nil, ctx.Err()
		case <-changed:
			stopAndDrainTimer(timer)
		case <-timer.C():
		}
	}
}

func (q *Queue) reserveReadyLocked(now time.Time, visibilityTimeout time.Duration) (*Delivery, error) {
	selected := q.popReady()
	receipt, err := randomID()
	if err != nil {
		q.pushReady(selected)
		return nil, err
	}
	leaseUntil := now.Add(visibilityTimeout)
	if err := q.wal.Append(event{
		Type:       "reserve",
		ID:         selected.ID,
		Receipt:    receipt,
		LeaseUntil: leaseUntil,
	}); err != nil {
		q.pushReady(selected)
		return nil, err
	}
	selected.Receipt = receipt
	selected.LeaseUntil = leaseUntil
	selected.Attempts++
	q.pushScheduled(selected, leaseUntil)
	q.notifyLocked()

	return &Delivery{
		Message:    cloneMessage(selected.Message),
		Receipt:    receipt,
		LeaseUntil: leaseUntil,
	}, nil
}

func (q *Queue) notifyLocked() {
	if q.closed {
		return
	}
	close(q.changed)
	q.changed = make(chan struct{})
}

func stopAndDrainTimer(timer queueTimer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C():
	default:
	}
}
