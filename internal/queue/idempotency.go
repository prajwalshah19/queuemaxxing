package queue

import (
	"container/heap"
	"time"
)

type idempotencyExpiry struct {
	Key       string
	ExpiresAt time.Time
}

type idempotencyExpiryHeap []idempotencyExpiry

func (h idempotencyExpiryHeap) Len() int { return len(h) }

func (h idempotencyExpiryHeap) Less(i, j int) bool {
	if !h[i].ExpiresAt.Equal(h[j].ExpiresAt) {
		return h[i].ExpiresAt.Before(h[j].ExpiresAt)
	}
	return h[i].Key < h[j].Key
}

func (h idempotencyExpiryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *idempotencyExpiryHeap) Push(value any) {
	*h = append(*h, value.(idempotencyExpiry))
}

func (h *idempotencyExpiryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = idempotencyExpiry{}
	*h = old[:last]
	return value
}

func (q *Queue) trackIdempotency(key string, record idempotencyRecord) {
	q.idempotency[key] = record
	heap.Push(&q.idempotencyExpiry, idempotencyExpiry{Key: key, ExpiresAt: record.ExpiresAt})
}

func (q *Queue) pruneExpiredIdempotency(now time.Time) {
	for q.idempotencyExpiry.Len() > 0 {
		next := q.idempotencyExpiry[0]
		if next.ExpiresAt.After(now) {
			return
		}
		heap.Pop(&q.idempotencyExpiry)
		record, exists := q.idempotency[next.Key]
		if exists && record.ExpiresAt.Equal(next.ExpiresAt) {
			delete(q.idempotency, next.Key)
		}
	}
}
