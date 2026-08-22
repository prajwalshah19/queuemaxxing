package queue

import (
	"container/heap"
	"time"
)

type heapKind uint8

const (
	heapNone heapKind = iota
	heapReady
	heapScheduled
)

// readyHeap is a max-heap by message priority. Sequence breaks equal-priority
// ties according to the process-wide FIFO/LIFO discipline.
type readyHeap struct {
	discipline Discipline
	items      []*storedMessage
}

func (h readyHeap) Len() int { return len(h.items) }

func (h readyHeap) Less(i, j int) bool {
	return beforeForDiscipline(h.discipline, h.items[i], h.items[j])
}

func (h readyHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].heapIndex = i
	h.items[j].heapIndex = j
}

func (h *readyHeap) Push(value any) {
	m := value.(*storedMessage)
	m.heapKind = heapReady
	m.heapIndex = len(h.items)
	h.items = append(h.items, m)
}

func (h *readyHeap) Pop() any {
	old := h.items
	last := len(old) - 1
	m := old[last]
	old[last] = nil
	h.items = old[:last]
	m.heapKind = heapNone
	m.heapIndex = -1
	return m
}

// scheduledHeap is a min-heap by the next time a delayed or leased message can
// become ready.
type scheduledHeap struct {
	items []*storedMessage
}

func (h scheduledHeap) Len() int { return len(h.items) }

func (h scheduledHeap) Less(i, j int) bool {
	if !h.items[i].nextEligibleAt.Equal(h.items[j].nextEligibleAt) {
		return h.items[i].nextEligibleAt.Before(h.items[j].nextEligibleAt)
	}
	return h.items[i].Sequence < h.items[j].Sequence
}

func (h scheduledHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].heapIndex = i
	h.items[j].heapIndex = j
}

func (h *scheduledHeap) Push(value any) {
	m := value.(*storedMessage)
	m.heapKind = heapScheduled
	m.heapIndex = len(h.items)
	h.items = append(h.items, m)
}

func (h *scheduledHeap) Pop() any {
	old := h.items
	last := len(old) - 1
	m := old[last]
	old[last] = nil
	h.items = old[:last]
	m.heapKind = heapNone
	m.heapIndex = -1
	m.nextEligibleAt = time.Time{}
	return m
}

func beforeForDiscipline(discipline Discipline, a, b *storedMessage) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	if discipline == LIFO {
		return a.Sequence > b.Sequence
	}
	return a.Sequence < b.Sequence
}

func (q *Queue) rebuildIndexes(now time.Time) {
	q.ready = readyHeap{discipline: q.discipline}
	q.scheduled = scheduledHeap{}
	for _, m := range q.messages {
		m.heapKind = heapNone
		m.heapIndex = -1
		m.nextEligibleAt = time.Time{}
		q.indexMessage(m, now)
	}
}

func (q *Queue) indexMessage(m *storedMessage, now time.Time) {
	switch {
	case m.Receipt != "":
		q.pushScheduled(m, m.LeaseUntil)
	case m.VisibleAt.After(now):
		q.pushScheduled(m, m.VisibleAt)
	default:
		q.pushReady(m)
	}
}

func (q *Queue) pushReady(m *storedMessage) {
	heap.Push(&q.ready, m)
}

func (q *Queue) popReady() *storedMessage {
	return heap.Pop(&q.ready).(*storedMessage)
}

func (q *Queue) pushScheduled(m *storedMessage, eligibleAt time.Time) {
	m.nextEligibleAt = eligibleAt
	heap.Push(&q.scheduled, m)
}

func (q *Queue) reschedule(m *storedMessage, eligibleAt time.Time) {
	m.nextEligibleAt = eligibleAt
	heap.Fix(&q.scheduled, m.heapIndex)
}

func (q *Queue) advanceScheduled(now time.Time) error {
	for q.scheduled.Len() > 0 {
		m := q.scheduled.items[0]
		if m.nextEligibleAt.After(now) {
			return nil
		}
		if m.Receipt != "" {
			if _, err := q.failMessage(m, m.Receipt, "lease_expired", m.LeaseUntil, now, nil); err != nil {
				return err
			}
			continue
		}
		heap.Pop(&q.scheduled)
		q.pushReady(m)
		q.notifyLocked()
	}
	return nil
}

func (q *Queue) removeFromIndex(m *storedMessage) {
	switch m.heapKind {
	case heapReady:
		heap.Remove(&q.ready, m.heapIndex)
	case heapScheduled:
		heap.Remove(&q.scheduled, m.heapIndex)
	}
}
