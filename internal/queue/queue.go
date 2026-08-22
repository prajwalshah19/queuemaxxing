package queue

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Discipline string

type Config struct {
	Discipline           Discipline
	IdempotencyRetention time.Duration
	RetryPolicy          RetryPolicy
}

const (
	FIFO                        Discipline = "fifo"
	LIFO                        Discipline = "lifo"
	DefaultIdempotencyRetention            = 24 * time.Hour
	MaxIdempotencyKeyBytes                 = 128
)

var (
	ErrClosed                = errors.New("queue is closed")
	ErrInvalidReceipt        = errors.New("message or receipt is no longer valid")
	ErrIdempotencyConflict   = errors.New("idempotency key was already used with different input")
	ErrInvalidIdempotencyKey = errors.New("idempotency key is invalid")
	ErrDeadLetterNotFound    = errors.New("dead letter was not found")
	ErrInvalidLeaseExtension = errors.New("lease extension must move the deadline later")
)

type Message struct {
	ID                string          `json:"id"`
	Body              json.RawMessage `json:"body"`
	Priority          int             `json:"priority"`
	VisibleAt         time.Time       `json:"visible_at"`
	EnqueuedAt        time.Time       `json:"enqueued_at"`
	Attempts          uint64          `json:"attempts"`
	OriginalMessageID string          `json:"original_message_id,omitempty"`
}

type Delivery struct {
	Message
	Receipt    string    `json:"receipt"`
	LeaseUntil time.Time `json:"lease_until"`
}

type Stats struct {
	Discipline  Discipline `json:"discipline"`
	Ready       int        `json:"ready"`
	Delayed     int        `json:"delayed"`
	InFlight    int        `json:"in_flight"`
	DeadLetters int        `json:"dead_letters"`
	Total       int        `json:"total"`
}

type storedMessage struct {
	Message
	Sequence       uint64    `json:"sequence"`
	Receipt        string    `json:"receipt,omitempty"`
	LeaseUntil     time.Time `json:"lease_until,omitempty"`
	heapKind       heapKind
	heapIndex      int
	nextEligibleAt time.Time
}

type EnqueueResult struct {
	Message  Message
	Replayed bool
}

type idempotencyRecord struct {
	RequestHash string
	Message     Message
	ExpiresAt   time.Time
}

type event struct {
	Type                 string               `json:"type"`
	Discipline           Discipline           `json:"discipline,omitempty"`
	Message              *storedMessage       `json:"message,omitempty"`
	ID                   string               `json:"id,omitempty"`
	Receipt              string               `json:"receipt,omitempty"`
	VisibleAt            time.Time            `json:"visible_at,omitempty"`
	LeaseUntil           time.Time            `json:"lease_until,omitempty"`
	IdempotencyKey       string               `json:"idempotency_key,omitempty"`
	IdempotencyHash      string               `json:"idempotency_hash,omitempty"`
	IdempotencyExpiresAt time.Time            `json:"idempotency_expires_at,omitempty"`
	RetryPolicy          *RetryPolicy         `json:"retry_policy,omitempty"`
	Attempts             uint64               `json:"attempts,omitempty"`
	Reason               string               `json:"reason,omitempty"`
	DeadLetteredAt       time.Time            `json:"dead_lettered_at,omitempty"`
	SnapshotBegin        *snapshotBegin       `json:"snapshot_begin,omitempty"`
	SnapshotDeadLetter   *snapshotDeadLetter  `json:"snapshot_dead_letter,omitempty"`
	SnapshotIdempotency  *snapshotIdempotency `json:"snapshot_idempotency,omitempty"`
	SnapshotCommit       *snapshotCommit      `json:"snapshot_commit,omitempty"`
}

type Queue struct {
	mu                   sync.Mutex
	wal                  *wal
	discipline           Discipline
	messages             map[string]*storedMessage
	ready                readyHeap
	scheduled            scheduledHeap
	idempotency          map[string]idempotencyRecord
	idempotencyExpiry    idempotencyExpiryHeap
	idempotencyRetention time.Duration
	retryPolicy          RetryPolicy
	deadLetters          map[string]*DeadLetter
	nextSeq              uint64
	clock                queueClock
	changed              chan struct{}
	closed               bool
}

func Open(path string, discipline Discipline) (*Queue, error) {
	return OpenWithConfig(path, Config{Discipline: discipline})
}

func OpenWithConfig(path string, config Config) (*Queue, error) {
	return openWithConfigAndClock(path, config, systemClock{})
}

func openWithClock(path string, discipline Discipline, now func() time.Time) (*Queue, error) {
	return openWithConfigAndClock(path, Config{Discipline: discipline}, functionClock{now: now})
}

func openWithConfig(path string, config Config, now func() time.Time) (*Queue, error) {
	return openWithConfigAndClock(path, config, functionClock{now: now})
}

func openWithConfigAndClock(path string, config Config, clock queueClock) (*Queue, error) {
	if config.Discipline != FIFO && config.Discipline != LIFO {
		return nil, fmt.Errorf("discipline must be %q or %q", FIFO, LIFO)
	}
	if config.IdempotencyRetention == 0 {
		config.IdempotencyRetention = DefaultIdempotencyRetention
	}
	if config.IdempotencyRetention < 0 {
		return nil, errors.New("idempotency retention must be positive")
	}
	if config.RetryPolicy == (RetryPolicy{}) {
		config.RetryPolicy = DefaultRetryPolicy()
	}
	if err := validateRetryPolicy(config.RetryPolicy); err != nil {
		return nil, err
	}

	w, err := openWAL(path)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		wal:                  w,
		messages:             make(map[string]*storedMessage),
		idempotency:          make(map[string]idempotencyRecord),
		idempotencyRetention: config.IdempotencyRetention,
		deadLetters:          make(map[string]*DeadLetter),
		clock:                clock,
		changed:              make(chan struct{}),
	}
	replay, err := replayQueueWAL(w, q)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	configured := replay.configured
	retryPolicyConfigured := replay.retryPolicyConfigured

	if configured {
		if q.discipline != config.Discipline {
			_ = w.Close()
			return nil, fmt.Errorf("WAL uses %s discipline, not requested %s", q.discipline, config.Discipline)
		}
		if retryPolicyConfigured && q.retryPolicy != config.RetryPolicy {
			_ = w.Close()
			return nil, fmt.Errorf("WAL uses retry policy %+v, not requested %+v", q.retryPolicy, config.RetryPolicy)
		}
		if !retryPolicyConfigured {
			policy := config.RetryPolicy
			if err := w.Append(event{Type: "retry_policy", RetryPolicy: &policy}); err != nil {
				_ = w.Close()
				return nil, err
			}
			q.retryPolicy = policy
		}
		q.pruneExpiredIdempotency(q.clock.Now().UTC())
		q.rebuildIndexes(q.clock.Now().UTC())
		return q, nil
	}

	q.discipline = config.Discipline
	if err := w.Append(event{Type: "config", Discipline: config.Discipline}); err != nil {
		_ = w.Close()
		return nil, err
	}
	policy := config.RetryPolicy
	if err := w.Append(event{Type: "retry_policy", RetryPolicy: &policy}); err != nil {
		_ = w.Close()
		return nil, err
	}
	q.retryPolicy = policy
	q.rebuildIndexes(q.clock.Now().UTC())
	return q, nil
}

func (q *Queue) Enqueue(body json.RawMessage, priority int, delay time.Duration) (Message, error) {
	result, err := q.EnqueueIdempotent(body, priority, delay, "")
	return result.Message, err
}

func (q *Queue) EnqueueIdempotent(body json.RawMessage, priority int, delay time.Duration, key string) (EnqueueResult, error) {
	if len(body) == 0 || !json.Valid(body) {
		return EnqueueResult{}, errors.New("body must be valid JSON")
	}
	if delay < 0 {
		return EnqueueResult{}, errors.New("delay cannot be negative")
	}
	if len(key) > MaxIdempotencyKeyBytes {
		return EnqueueResult{}, fmt.Errorf("%w: maximum length is %d bytes", ErrInvalidIdempotencyKey, MaxIdempotencyKeyBytes)
	}

	var requestHash string
	if key != "" {
		var err error
		requestHash, err = enqueueRequestHash(body, priority, delay)
		if err != nil {
			return EnqueueResult{}, err
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.ensureWritableLocked(); err != nil {
		return EnqueueResult{}, err
	}

	now := q.clock.Now().UTC()
	q.pruneExpiredIdempotency(now)
	if key != "" {
		if previous, ok := q.idempotency[key]; ok {
			if previous.RequestHash != requestHash {
				return EnqueueResult{}, ErrIdempotencyConflict
			}
			return EnqueueResult{Message: cloneMessage(previous.Message), Replayed: true}, nil
		}
	}

	id, err := randomID()
	if err != nil {
		return EnqueueResult{}, err
	}
	q.nextSeq++
	m := &storedMessage{
		Message: Message{
			ID:         id,
			Body:       append(json.RawMessage(nil), body...),
			Priority:   priority,
			VisibleAt:  now.Add(delay),
			EnqueuedAt: now,
		},
		Sequence: q.nextSeq,
	}
	e := event{Type: "enqueue", Message: m}
	if key != "" {
		e.IdempotencyKey = key
		e.IdempotencyHash = requestHash
		e.IdempotencyExpiresAt = now.Add(q.idempotencyRetention)
	}
	if err := q.wal.Append(e); err != nil {
		q.nextSeq--
		return EnqueueResult{}, err
	}
	q.messages[id] = m
	if key != "" {
		q.trackIdempotency(key, idempotencyRecord{
			RequestHash: requestHash,
			Message:     cloneMessage(m.Message),
			ExpiresAt:   e.IdempotencyExpiresAt,
		})
	}
	q.indexMessage(m, now)
	q.notifyLocked()
	return EnqueueResult{Message: cloneMessage(m.Message)}, nil
}

func (q *Queue) Ack(id, receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.ensureWritableLocked(); err != nil {
		return err
	}
	m, ok := q.messages[id]
	now := q.clock.Now().UTC()
	if !ok || receipt == "" || m.Receipt != receipt || !now.Before(m.LeaseUntil) {
		return ErrInvalidReceipt
	}
	if err := q.wal.Append(event{Type: "ack", ID: id, Receipt: receipt}); err != nil {
		return err
	}
	q.removeFromIndex(m)
	delete(q.messages, id)
	q.notifyLocked()
	return nil
}

func (q *Queue) Nack(id, receipt string, delay time.Duration) error {
	_, err := q.NackWithOptions(id, receipt, &delay)
	return err
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.clock.Now().UTC()
	s := Stats{Discipline: q.discipline, Total: len(q.messages), DeadLetters: len(q.deadLetters)}
	for _, m := range q.messages {
		switch {
		case m.Receipt != "":
			if m.LeaseUntil.After(now) {
				s.InFlight++
			} else {
				s.Delayed++
			}
		case m.VisibleAt.After(now):
			s.Delayed++
		default:
			s.Ready++
		}
	}
	return s
}

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	q.closed = true
	close(q.changed)
	return q.wal.Close()
}

func (q *Queue) apply(e event) error {
	switch e.Type {
	case "enqueue":
		if e.Message == nil || e.Message.ID == "" {
			return errors.New("invalid enqueue event")
		}
		if _, exists := q.messages[e.Message.ID]; exists {
			return fmt.Errorf("duplicate message %s", e.Message.ID)
		}
		copy := *e.Message
		copy.Body = append(json.RawMessage(nil), e.Message.Body...)
		q.messages[copy.ID] = &copy
		if e.IdempotencyKey != "" {
			if e.IdempotencyHash == "" || !e.IdempotencyExpiresAt.After(copy.EnqueuedAt) {
				return errors.New("invalid enqueue idempotency state")
			}
			if previous, exists := q.idempotency[e.IdempotencyKey]; exists && copy.EnqueuedAt.Before(previous.ExpiresAt) {
				return fmt.Errorf("idempotency key %q reused before expiry", e.IdempotencyKey)
			}
			q.trackIdempotency(e.IdempotencyKey, idempotencyRecord{
				RequestHash: e.IdempotencyHash,
				Message:     cloneMessage(copy.Message),
				ExpiresAt:   e.IdempotencyExpiresAt,
			})
		}
		if copy.Sequence > q.nextSeq {
			q.nextSeq = copy.Sequence
		}
	case "reserve":
		m, ok := q.messages[e.ID]
		if !ok || e.Receipt == "" {
			return errors.New("invalid reserve event")
		}
		m.Receipt = e.Receipt
		m.LeaseUntil = e.LeaseUntil
		m.Attempts++
	case "ack":
		m, ok := q.messages[e.ID]
		if !ok || m.Receipt != e.Receipt {
			return errors.New("invalid ack event")
		}
		delete(q.messages, e.ID)
	case "nack":
		m, ok := q.messages[e.ID]
		if !ok || m.Receipt != e.Receipt {
			return errors.New("invalid nack event")
		}
		m.Receipt = ""
		m.LeaseUntil = time.Time{}
		m.VisibleAt = e.VisibleAt
	case "extend_lease":
		m, ok := q.messages[e.ID]
		if !ok || e.Receipt == "" || m.Receipt != e.Receipt || e.LeaseUntil.IsZero() || !e.LeaseUntil.After(m.LeaseUntil) {
			return errors.New("invalid extend_lease event")
		}
		m.LeaseUntil = e.LeaseUntil
	default:
		handled, err := q.applyRetryEvent(e)
		if !handled {
			return fmt.Errorf("unknown event type %q", e.Type)
		}
		return err
	}
	return nil
}

func cloneMessage(m Message) Message {
	m.Body = append(json.RawMessage(nil), m.Body...)
	return m
}

func enqueueRequestHash(body json.RawMessage, priority int, delay time.Duration) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", fmt.Errorf("canonicalize message body: %w", err)
	}
	canonicalBody, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("canonicalize message body: %w", err)
	}
	envelope, err := json.Marshal(struct {
		Body       json.RawMessage `json:"body"`
		Priority   int             `json:"priority"`
		DelayNanos int64           `json:"delay_nanos"`
	}{
		Body:       canonicalBody,
		Priority:   priority,
		DelayNanos: int64(delay),
	})
	if err != nil {
		return "", fmt.Errorf("encode enqueue fingerprint: %w", err)
	}
	digest := sha256.Sum256(envelope)
	return hex.EncodeToString(digest[:]), nil
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b), nil
}
