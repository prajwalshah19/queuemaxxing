package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Discipline string

const (
	FIFO Discipline = "fifo"
	LIFO Discipline = "lifo"
)

var (
	ErrClosed         = errors.New("queue is closed")
	ErrInvalidReceipt = errors.New("message or receipt is no longer valid")
)

type Message struct {
	ID         string          `json:"id"`
	Body       json.RawMessage `json:"body"`
	Priority   int             `json:"priority"`
	VisibleAt  time.Time       `json:"visible_at"`
	EnqueuedAt time.Time       `json:"enqueued_at"`
	Attempts   uint64          `json:"attempts"`
}

type Delivery struct {
	Message
	Receipt    string    `json:"receipt"`
	LeaseUntil time.Time `json:"lease_until"`
}

type Stats struct {
	Discipline Discipline `json:"discipline"`
	Ready      int        `json:"ready"`
	Delayed    int        `json:"delayed"`
	InFlight   int        `json:"in_flight"`
	Total      int        `json:"total"`
}

type storedMessage struct {
	Message
	Sequence   uint64    `json:"sequence"`
	Receipt    string    `json:"receipt,omitempty"`
	LeaseUntil time.Time `json:"lease_until,omitempty"`
}

type event struct {
	Type       string         `json:"type"`
	Discipline Discipline     `json:"discipline,omitempty"`
	Message    *storedMessage `json:"message,omitempty"`
	ID         string         `json:"id,omitempty"`
	Receipt    string         `json:"receipt,omitempty"`
	VisibleAt  time.Time      `json:"visible_at,omitempty"`
	LeaseUntil time.Time      `json:"lease_until,omitempty"`
}

type Queue struct {
	mu         sync.Mutex
	wal        *wal
	discipline Discipline
	messages   map[string]*storedMessage
	nextSeq    uint64
	now        func() time.Time
	closed     bool
}

func Open(path string, discipline Discipline) (*Queue, error) {
	if discipline != FIFO && discipline != LIFO {
		return nil, fmt.Errorf("discipline must be %q or %q", FIFO, LIFO)
	}

	w, err := openWAL(path)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		wal:      w,
		messages: make(map[string]*storedMessage),
		now:      time.Now,
	}
	var configured bool
	if err := w.Replay(func(e event) error {
		if e.Type == "config" {
			if configured {
				return errors.New("duplicate queue configuration")
			}
			if e.Discipline != FIFO && e.Discipline != LIFO {
				return fmt.Errorf("invalid persisted discipline %q", e.Discipline)
			}
			configured = true
			q.discipline = e.Discipline
			return nil
		}
		if !configured {
			return errors.New("event encountered before queue configuration")
		}
		return q.apply(e)
	}); err != nil {
		_ = w.Close()
		return nil, err
	}

	if configured {
		if q.discipline != discipline {
			_ = w.Close()
			return nil, fmt.Errorf("WAL uses %s discipline, not requested %s", q.discipline, discipline)
		}
		return q, nil
	}

	q.discipline = discipline
	if err := w.Append(event{Type: "config", Discipline: discipline}); err != nil {
		_ = w.Close()
		return nil, err
	}
	return q, nil
}

func (q *Queue) Enqueue(body json.RawMessage, priority int, delay time.Duration) (Message, error) {
	if len(body) == 0 || !json.Valid(body) {
		return Message{}, errors.New("body must be valid JSON")
	}
	if delay < 0 {
		return Message{}, errors.New("delay cannot be negative")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return Message{}, ErrClosed
	}

	now := q.now().UTC()
	id, err := randomID()
	if err != nil {
		return Message{}, err
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
	if err := q.wal.Append(event{Type: "enqueue", Message: m}); err != nil {
		q.nextSeq--
		return Message{}, err
	}
	q.messages[id] = m
	return m.Message, nil
}

func (q *Queue) Reserve(visibilityTimeout time.Duration) (*Delivery, error) {
	if visibilityTimeout <= 0 {
		return nil, errors.New("visibility timeout must be positive")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrClosed
	}

	now := q.now().UTC()
	var selected *storedMessage
	for _, candidate := range q.messages {
		if candidate.VisibleAt.After(now) {
			continue
		}
		if candidate.Receipt != "" && candidate.LeaseUntil.After(now) {
			continue
		}
		if selected == nil || q.before(candidate, selected) {
			selected = candidate
		}
	}
	if selected == nil {
		return nil, nil
	}

	receipt, err := randomID()
	if err != nil {
		return nil, err
	}
	leaseUntil := now.Add(visibilityTimeout)
	if err := q.wal.Append(event{
		Type:       "reserve",
		ID:         selected.ID,
		Receipt:    receipt,
		LeaseUntil: leaseUntil,
	}); err != nil {
		return nil, err
	}
	selected.Receipt = receipt
	selected.LeaseUntil = leaseUntil
	selected.Attempts++

	return &Delivery{
		Message:    cloneMessage(selected.Message),
		Receipt:    receipt,
		LeaseUntil: leaseUntil,
	}, nil
}

func (q *Queue) Ack(id, receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	m, ok := q.messages[id]
	if !ok || receipt == "" || m.Receipt != receipt {
		return ErrInvalidReceipt
	}
	if err := q.wal.Append(event{Type: "ack", ID: id, Receipt: receipt}); err != nil {
		return err
	}
	delete(q.messages, id)
	return nil
}

func (q *Queue) Nack(id, receipt string, delay time.Duration) error {
	if delay < 0 {
		return errors.New("delay cannot be negative")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	m, ok := q.messages[id]
	if !ok || receipt == "" || m.Receipt != receipt {
		return ErrInvalidReceipt
	}
	visibleAt := q.now().UTC().Add(delay)
	if err := q.wal.Append(event{Type: "nack", ID: id, Receipt: receipt, VisibleAt: visibleAt}); err != nil {
		return err
	}
	m.Receipt = ""
	m.LeaseUntil = time.Time{}
	m.VisibleAt = visibleAt
	return nil
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now().UTC()
	s := Stats{Discipline: q.discipline, Total: len(q.messages)}
	for _, m := range q.messages {
		switch {
		case m.Receipt != "" && m.LeaseUntil.After(now):
			s.InFlight++
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
	return q.wal.Close()
}

func (q *Queue) before(a, b *storedMessage) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if q.discipline == LIFO {
		return a.Sequence > b.Sequence
	}
	return a.Sequence < b.Sequence
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
	default:
		return fmt.Errorf("unknown event type %q", e.Type)
	}
	return nil
}

func cloneMessage(m Message) Message {
	m.Body = append(json.RawMessage(nil), m.Body...)
	return m
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	return hex.EncodeToString(b), nil
}
