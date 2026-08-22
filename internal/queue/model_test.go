package queue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestQueueStateMachineMatchesIndependentModel(t *testing.T) {
	seeds := []int64{1, 0xC0FFEE, 20260822}
	for _, discipline := range []Discipline{FIFO, LIFO} {
		for _, seed := range seeds {
			t.Run(fmt.Sprintf("%s/seed_%d", discipline, seed), func(t *testing.T) {
				policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 4 * time.Second}
				h := newQueueHarness(t, Config{Discipline: discipline, RetryPolicy: policy})
				model := newReferenceQueue(discipline, policy)
				rng := rand.New(rand.NewSource(seed))

				for step := range 300 {
					op := runModelOperation(t, h, model, rng, step)
					assertQueueMatchesModel(t, h.q, model, h.now, seed, step, op)
				}
				h.reopen()
				assertQueueMatchesModel(t, h.q, model, h.now, seed, 300, "final restart")
			})
		}
	}
}

func runModelOperation(t *testing.T, h *queueHarness, model *referenceQueue, rng *rand.Rand, step int) string {
	t.Helper()
	choice := rng.Intn(100)
	switch {
	case choice < 24:
		priority := rng.Intn(7) - 3
		delay := time.Duration(rng.Intn(4)) * time.Second
		body := json.RawMessage(fmt.Sprintf(`{"step":%d}`, step))
		message, err := h.q.Enqueue(body, priority, delay)
		if err != nil {
			t.Fatalf("step %d enqueue: %v", step, err)
		}
		if message.ID == "" || string(message.Body) != string(body) || message.Priority != priority || !message.EnqueuedAt.Equal(h.now) || !message.VisibleAt.Equal(h.now.Add(delay)) || message.Attempts != 0 || message.OriginalMessageID != "" {
			t.Fatalf("step %d enqueue response = %#v", step, message)
		}
		model.enqueue(message.ID, body, priority, h.now, delay)
		return fmt.Sprintf("enqueue id=%s priority=%d delay=%s", message.ID, priority, delay)

	case choice < 45:
		visibility := time.Duration(1+rng.Intn(5)) * time.Second
		want := model.reserve(h.now, visibility)
		got, err := h.q.Reserve(visibility)
		if err != nil {
			t.Fatalf("step %d reserve: %v", step, err)
		}
		if want == nil {
			if got != nil {
				t.Fatalf("step %d reserve returned %s, model is empty", step, got.ID)
			}
			return fmt.Sprintf("reserve visibility=%s empty", visibility)
		}
		if got == nil || got.ID != want.ID {
			t.Fatalf("step %d reserve = %#v, model wants %s", step, got, want.ID)
		}
		if got.Receipt == "" || !got.LeaseUntil.Equal(h.now.Add(visibility)) || string(got.Body) != string(want.Body) || got.Priority != want.Priority || got.Attempts != want.Attempts+1 {
			t.Fatalf("step %d delivery = %#v, model message = %#v", step, got, want)
		}
		model.acceptDelivery(got)
		return fmt.Sprintf("reserve id=%s visibility=%s", got.ID, visibility)

	case choice < 57:
		delta := time.Duration(rng.Intn(6)) * time.Second
		h.advance(delta)
		return fmt.Sprintf("advance %s", delta)

	case choice < 68:
		m := model.randomActive(rng, h.now)
		if m == nil {
			return "ack skipped"
		}
		if err := h.q.Ack(m.ID, m.Receipt); err != nil {
			t.Fatalf("step %d ack %s: %v", step, m.ID, err)
		}
		delete(model.live, m.ID)
		return "ack id=" + m.ID

	case choice < 80:
		m := model.randomActive(rng, h.now)
		if m == nil {
			return "nack skipped"
		}
		delay := time.Duration(rng.Intn(4)) * time.Second
		if err := h.q.Nack(m.ID, m.Receipt, delay); err != nil {
			t.Fatalf("step %d nack %s: %v", step, m.ID, err)
		}
		model.nack(m, h.now, delay)
		return fmt.Sprintf("nack id=%s delay=%s", m.ID, delay)

	case choice < 90:
		m := model.randomActive(rng, h.now)
		if m == nil {
			return "extend skipped"
		}
		visibility := m.LeaseUntil.Sub(h.now) + time.Duration(1+rng.Intn(3))*time.Second
		extension, err := h.q.ExtendLease(m.ID, m.Receipt, visibility)
		if err != nil {
			t.Fatalf("step %d extend %s: %v", step, m.ID, err)
		}
		if !extension.LeaseUntil.Equal(h.now.Add(visibility)) {
			t.Fatalf("step %d extension deadline = %s, want %s", step, extension.LeaseUntil, h.now.Add(visibility))
		}
		m.LeaseUntil = extension.LeaseUntil
		return fmt.Sprintf("extend id=%s visibility=%s", m.ID, visibility)

	case choice < 96:
		h.reopen()
		return "restart"

	default:
		letter := model.randomDeadLetter(rng)
		if letter == nil {
			return "dead replay skipped"
		}
		delay := time.Duration(rng.Intn(4)) * time.Second
		message, err := h.q.ReplayDeadLetter(letter.ID, delay)
		if err != nil {
			t.Fatalf("step %d replay %s: %v", step, letter.ID, err)
		}
		if message.ID == "" || message.ID == letter.ID || string(message.Body) != string(letter.Body) || message.Priority != letter.Priority || !message.EnqueuedAt.Equal(h.now) || !message.VisibleAt.Equal(h.now.Add(delay)) || message.Attempts != 0 || message.OriginalMessageID != letter.ID {
			t.Fatalf("step %d replay response = %#v, dead letter = %#v", step, message, letter)
		}
		model.replay(letter, message.ID, h.now, delay)
		return fmt.Sprintf("dead replay id=%s new=%s delay=%s", letter.ID, message.ID, delay)
	}
}

type referenceQueue struct {
	discipline Discipline
	policy     RetryPolicy
	nextSeq    uint64
	live       map[string]*referenceMessage
	dead       map[string]*referenceDeadLetter
}

type referenceMessage struct {
	Message
	Sequence   uint64
	Receipt    string
	LeaseUntil time.Time
}

type referenceDeadLetter struct {
	Message
	Sequence       uint64
	DeadLetteredAt time.Time
	Reason         string
}

func newReferenceQueue(discipline Discipline, policy RetryPolicy) *referenceQueue {
	return &referenceQueue{
		discipline: discipline,
		policy:     policy,
		live:       make(map[string]*referenceMessage),
		dead:       make(map[string]*referenceDeadLetter),
	}
}

func (m *referenceQueue) enqueue(id string, body json.RawMessage, priority int, now time.Time, delay time.Duration) {
	m.nextSeq++
	m.live[id] = &referenceMessage{
		Message: Message{
			ID:         id,
			Body:       append(json.RawMessage(nil), body...),
			Priority:   priority,
			VisibleAt:  now.Add(delay),
			EnqueuedAt: now,
		},
		Sequence: m.nextSeq,
	}
}

func (m *referenceQueue) reserve(now time.Time, visibility time.Duration) *referenceMessage {
	m.advanceExpiredLeases(now)
	var selected *referenceMessage
	for _, candidate := range m.live {
		if candidate.Receipt != "" || candidate.VisibleAt.After(now) {
			continue
		}
		if selected == nil || m.before(candidate, selected) {
			selected = candidate
		}
	}
	return selected
}

func (m *referenceQueue) acceptDelivery(delivery *Delivery) {
	stored := m.live[delivery.ID]
	stored.Attempts++
	stored.Receipt = delivery.Receipt
	stored.LeaseUntil = delivery.LeaseUntil
}

func (m *referenceQueue) advanceExpiredLeases(now time.Time) {
	for _, message := range m.live {
		if message.Receipt == "" || now.Before(message.LeaseUntil) {
			continue
		}
		if message.Attempts >= uint64(m.policy.MaxAttempts) {
			m.dead[message.ID] = &referenceDeadLetter{
				Message:        cloneReferenceMessage(message.Message),
				Sequence:       message.Sequence,
				DeadLetteredAt: now,
				Reason:         "max_attempts",
			}
			delete(m.live, message.ID)
			continue
		}
		failedAt := message.LeaseUntil
		message.Receipt = ""
		message.LeaseUntil = time.Time{}
		message.VisibleAt = failedAt.Add(referenceBackoff(m.policy, message.Attempts))
	}
}

func (m *referenceQueue) nack(message *referenceMessage, now time.Time, delay time.Duration) {
	if message.Attempts >= uint64(m.policy.MaxAttempts) {
		m.dead[message.ID] = &referenceDeadLetter{
			Message:        cloneReferenceMessage(message.Message),
			Sequence:       message.Sequence,
			DeadLetteredAt: now,
			Reason:         "max_attempts",
		}
		delete(m.live, message.ID)
		return
	}
	message.Receipt = ""
	message.LeaseUntil = time.Time{}
	message.VisibleAt = now.Add(delay)
}

func (m *referenceQueue) replay(letter *referenceDeadLetter, id string, now time.Time, delay time.Duration) {
	delete(m.dead, letter.ID)
	m.nextSeq++
	m.live[id] = &referenceMessage{
		Message: Message{
			ID:                id,
			Body:              append(json.RawMessage(nil), letter.Body...),
			Priority:          letter.Priority,
			VisibleAt:         now.Add(delay),
			EnqueuedAt:        now,
			OriginalMessageID: letter.ID,
		},
		Sequence: m.nextSeq,
	}
}

func (m *referenceQueue) before(a, b *referenceMessage) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if m.discipline == LIFO {
		return a.Sequence > b.Sequence
	}
	return a.Sequence < b.Sequence
}

func (m *referenceQueue) randomActive(rng *rand.Rand, now time.Time) *referenceMessage {
	messages := make([]*referenceMessage, 0)
	for _, message := range m.live {
		if message.Receipt != "" && now.Before(message.LeaseUntil) {
			messages = append(messages, message)
		}
	}
	if len(messages) == 0 {
		return nil
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].Sequence < messages[j].Sequence })
	return messages[rng.Intn(len(messages))]
}

func (m *referenceQueue) randomDeadLetter(rng *rand.Rand) *referenceDeadLetter {
	letters := make([]*referenceDeadLetter, 0, len(m.dead))
	for _, letter := range m.dead {
		letters = append(letters, letter)
	}
	if len(letters) == 0 {
		return nil
	}
	sort.Slice(letters, func(i, j int) bool { return letters[i].Sequence < letters[j].Sequence })
	return letters[rng.Intn(len(letters))]
}

func referenceBackoff(policy RetryPolicy, attempts uint64) time.Duration {
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

func assertQueueMatchesModel(t *testing.T, q *Queue, model *referenceQueue, now time.Time, seed int64, step int, operation string) {
	t.Helper()
	prefix := fmt.Sprintf("seed=%d step=%d op=%s", seed, step, operation)
	if q.nextSeq != model.nextSeq {
		t.Fatalf("%s: nextSeq=%d, want %d", prefix, q.nextSeq, model.nextSeq)
	}
	if len(q.messages) != len(model.live) || len(q.deadLetters) != len(model.dead) {
		t.Fatalf("%s: live/dead=%d/%d, want %d/%d", prefix, len(q.messages), len(q.deadLetters), len(model.live), len(model.dead))
	}
	for id, want := range model.live {
		got := q.messages[id]
		if got == nil {
			t.Fatalf("%s: missing live message %s", prefix, id)
		}
		if !equalReferenceMessage(got, want) {
			t.Fatalf("%s: message %s = %#v, want %#v", prefix, id, got, want)
		}
	}
	for id, want := range model.dead {
		got := q.deadLetters[id]
		if got == nil {
			t.Fatalf("%s: missing dead letter %s", prefix, id)
		}
		if got.ID != want.ID || string(got.Body) != string(want.Body) || got.Priority != want.Priority || !got.VisibleAt.Equal(want.VisibleAt) || !got.EnqueuedAt.Equal(want.EnqueuedAt) || got.Attempts != want.Attempts || got.OriginalMessageID != want.OriginalMessageID || got.Sequence != want.Sequence || got.Reason != want.Reason || !got.DeadLetteredAt.Equal(want.DeadLetteredAt) {
			t.Fatalf("%s: dead letter %s = %#v, want %#v", prefix, id, got, want)
		}
	}
	assertIndexConsistency(t, q)
	for _, message := range q.messages {
		switch {
		case message.Receipt != "":
			if message.heapKind != heapScheduled || !message.nextEligibleAt.Equal(message.LeaseUntil) {
				t.Fatalf("%s: leased message %s has wrong index", prefix, message.ID)
			}
		case message.VisibleAt.After(now):
			if message.heapKind != heapScheduled || !message.nextEligibleAt.Equal(message.VisibleAt) {
				t.Fatalf("%s: delayed message %s has wrong index", prefix, message.ID)
			}
		default:
			if message.heapKind != heapReady && (message.heapKind != heapScheduled || !message.nextEligibleAt.Equal(message.VisibleAt)) {
				t.Fatalf("%s: eligible message %s has invalid heap state kind=%d eligible=%s", prefix, message.ID, message.heapKind, message.nextEligibleAt)
			}
		}
	}
}

func equalReferenceMessage(got *storedMessage, want *referenceMessage) bool {
	return got.ID == want.ID &&
		string(got.Body) == string(want.Body) &&
		got.Priority == want.Priority &&
		got.VisibleAt.Equal(want.VisibleAt) &&
		got.EnqueuedAt.Equal(want.EnqueuedAt) &&
		got.Attempts == want.Attempts &&
		got.OriginalMessageID == want.OriginalMessageID &&
		got.Sequence == want.Sequence &&
		got.Receipt == want.Receipt &&
		got.LeaseUntil.Equal(want.LeaseUntil)
}

func cloneReferenceMessage(message Message) Message {
	message.Body = append(json.RawMessage(nil), message.Body...)
	return message
}
