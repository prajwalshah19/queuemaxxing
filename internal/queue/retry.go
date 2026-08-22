package queue

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

const MaxDeadLetterListLimit = 1000

const (
	DefaultMaxAttempts    = 5
	DefaultRetryBaseDelay = time.Second
	DefaultRetryMaxDelay  = 5 * time.Minute
)

type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"`
	BaseDelay   time.Duration `json:"base_delay"`
	MaxDelay    time.Duration `json:"max_delay"`
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: DefaultMaxAttempts,
		BaseDelay:   DefaultRetryBaseDelay,
		MaxDelay:    DefaultRetryMaxDelay,
	}
}

type RetryStatus string

const (
	RetryScheduled RetryStatus = "retry_scheduled"
	DeadLettered   RetryStatus = "dead_lettered"
)

type RetryResult struct {
	Status    RetryStatus `json:"status"`
	VisibleAt *time.Time  `json:"visible_at,omitempty"`
	Attempts  uint64      `json:"attempts"`
}

type DeadLetter struct {
	Message
	DeadLetteredAt time.Time `json:"dead_lettered_at"`
	Reason         string    `json:"reason"`
	Sequence       uint64    `json:"-"`
}

func validateRetryPolicy(policy RetryPolicy) error {
	if policy.MaxAttempts < 1 {
		return errors.New("max attempts must be at least 1")
	}
	if policy.BaseDelay <= 0 {
		return errors.New("retry base delay must be positive")
	}
	if policy.MaxDelay < policy.BaseDelay {
		return errors.New("retry max delay must be greater than or equal to base delay")
	}
	return nil
}

func retryBackoff(policy RetryPolicy, attempts uint64) time.Duration {
	delay := policy.BaseDelay
	for n := uint64(1); n < attempts; n++ {
		if delay >= policy.MaxDelay || delay > policy.MaxDelay/2 {
			return policy.MaxDelay
		}
		delay *= 2
	}
	if delay > policy.MaxDelay {
		return policy.MaxDelay
	}
	return delay
}

func requiresRetryPolicy(eventType string) bool {
	switch eventType {
	case "retry_scheduled", "dead_letter", "dead_letter_replay":
		return true
	default:
		return false
	}
}

// NackWithOptions fails the current delivery. A nil delayOverride applies the
// process retry policy; a non-nil value, including zero, overrides backoff.
func (q *Queue) NackWithOptions(id, receipt string, delayOverride *time.Duration) (RetryResult, error) {
	if delayOverride != nil && *delayOverride < 0 {
		return RetryResult{}, errors.New("delay cannot be negative")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return RetryResult{}, ErrClosed
	}
	m, ok := q.messages[id]
	now := q.clock.Now().UTC()
	if !ok || receipt == "" || m.Receipt != receipt || !now.Before(m.LeaseUntil) {
		return RetryResult{}, ErrInvalidReceipt
	}
	return q.failMessage(m, receipt, "nack", now, now, delayOverride)
}

func (q *Queue) failMessage(m *storedMessage, receipt, source string, failedAt, now time.Time, delayOverride *time.Duration) (RetryResult, error) {
	if m.Attempts >= uint64(q.retryPolicy.MaxAttempts) {
		deadLetteredAt := now
		e := event{
			Type:           "dead_letter",
			ID:             m.ID,
			Receipt:        receipt,
			Attempts:       m.Attempts,
			Reason:         "max_attempts",
			DeadLetteredAt: deadLetteredAt,
		}
		if err := q.wal.Append(e); err != nil {
			return RetryResult{}, err
		}
		q.removeFromIndex(m)
		q.deadLetters[m.ID] = deadLetterFromStored(m, deadLetteredAt, e.Reason)
		delete(q.messages, m.ID)
		q.notifyLocked()
		return RetryResult{Status: DeadLettered, Attempts: m.Attempts}, nil
	}

	delay := retryBackoff(q.retryPolicy, m.Attempts)
	if delayOverride != nil {
		delay = *delayOverride
	}
	visibleAt := failedAt.Add(delay)
	e := event{
		Type:      "retry_scheduled",
		ID:        m.ID,
		Receipt:   receipt,
		VisibleAt: visibleAt,
		Attempts:  m.Attempts,
		Reason:    source,
	}
	if err := q.wal.Append(e); err != nil {
		return RetryResult{}, err
	}
	q.removeFromIndex(m)
	m.Receipt = ""
	m.LeaseUntil = time.Time{}
	m.VisibleAt = visibleAt
	q.indexMessage(m, now)
	q.notifyLocked()
	return RetryResult{Status: RetryScheduled, VisibleAt: &visibleAt, Attempts: m.Attempts}, nil
}

func (q *Queue) ListDeadLetters(limit int) ([]DeadLetter, error) {
	if limit < 1 || limit > MaxDeadLetterListLimit {
		return nil, fmt.Errorf("limit must be between 1 and %d", MaxDeadLetterListLimit)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}
	letters := make([]DeadLetter, 0, len(q.deadLetters))
	for _, letter := range q.deadLetters {
		letters = append(letters, cloneDeadLetter(*letter))
	}
	sort.Slice(letters, func(i, j int) bool {
		if !letters[i].DeadLetteredAt.Equal(letters[j].DeadLetteredAt) {
			return letters[i].DeadLetteredAt.Before(letters[j].DeadLetteredAt)
		}
		return letters[i].Sequence < letters[j].Sequence
	})
	if len(letters) > limit {
		letters = letters[:limit]
	}
	return letters, nil
}

func (q *Queue) ReplayDeadLetter(id string, delay time.Duration) (Message, error) {
	if delay < 0 {
		return Message{}, errors.New("delay cannot be negative")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Message{}, ErrClosed
	}
	letter, ok := q.deadLetters[id]
	if !ok {
		return Message{}, ErrDeadLetterNotFound
	}
	now := q.clock.Now().UTC()
	newID, err := randomID()
	if err != nil {
		return Message{}, err
	}
	q.nextSeq++
	m := &storedMessage{
		Message: Message{
			ID:                newID,
			Body:              append([]byte(nil), letter.Body...),
			Priority:          letter.Priority,
			VisibleAt:         now.Add(delay),
			EnqueuedAt:        now,
			OriginalMessageID: letter.ID,
		},
		Sequence: q.nextSeq,
	}
	if err := q.wal.Append(event{Type: "dead_letter_replay", ID: id, Message: m}); err != nil {
		q.nextSeq--
		return Message{}, err
	}
	delete(q.deadLetters, id)
	q.messages[newID] = m
	q.indexMessage(m, now)
	q.notifyLocked()
	return cloneMessage(m.Message), nil
}

func (q *Queue) applyRetryEvent(e event) (bool, error) {
	switch e.Type {
	case "retry_scheduled":
		m, ok := q.messages[e.ID]
		if !ok || e.Receipt == "" || m.Receipt != e.Receipt || e.Attempts != m.Attempts || e.VisibleAt.IsZero() || (e.Reason != "nack" && e.Reason != "lease_expired") {
			return true, errors.New("invalid retry_scheduled event")
		}
		m.Receipt = ""
		m.LeaseUntil = time.Time{}
		m.VisibleAt = e.VisibleAt
		return true, nil
	case "dead_letter":
		m, ok := q.messages[e.ID]
		if !ok || e.Receipt == "" || m.Receipt != e.Receipt || e.Attempts != m.Attempts || e.DeadLetteredAt.IsZero() || e.Reason != "max_attempts" {
			return true, errors.New("invalid dead_letter event")
		}
		q.deadLetters[e.ID] = deadLetterFromStored(m, e.DeadLetteredAt, e.Reason)
		delete(q.messages, e.ID)
		return true, nil
	case "dead_letter_replay":
		letter, ok := q.deadLetters[e.ID]
		if !ok || e.Message == nil || e.Message.ID == "" || e.Message.ID == e.ID || e.Message.OriginalMessageID != e.ID || e.Message.Attempts != 0 || len(e.Message.Body) == 0 {
			return true, errors.New("invalid dead_letter_replay event")
		}
		if _, exists := q.messages[e.Message.ID]; exists {
			return true, errors.New("dead-letter replay message already exists")
		}
		copy := *e.Message
		copy.Body = append([]byte(nil), e.Message.Body...)
		delete(q.deadLetters, letter.ID)
		q.messages[copy.ID] = &copy
		if copy.Sequence > q.nextSeq {
			q.nextSeq = copy.Sequence
		}
		return true, nil
	default:
		return false, nil
	}
}

func deadLetterFromStored(m *storedMessage, at time.Time, reason string) *DeadLetter {
	return &DeadLetter{
		Message:        cloneMessage(m.Message),
		DeadLetteredAt: at,
		Reason:         reason,
		Sequence:       m.Sequence,
	}
}

func cloneDeadLetter(letter DeadLetter) DeadLetter {
	letter.Message = cloneMessage(letter.Message)
	return letter
}
