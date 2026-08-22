package queue

import (
	"path/filepath"
	"testing"
	"time"
)

type queueHarness struct {
	t      *testing.T
	path   string
	now    time.Time
	config Config
	q      *Queue
}

func newQueueHarness(t *testing.T, config Config) *queueHarness {
	t.Helper()
	h := &queueHarness{
		t:      t,
		path:   filepath.Join(t.TempDir(), "queue.wal"),
		now:    time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		config: config,
	}
	h.open()
	t.Cleanup(func() {
		if h.q != nil {
			_ = h.q.Close()
		}
	})
	return h
}

func (h *queueHarness) open() {
	h.t.Helper()
	q, err := openWithConfig(h.path, h.config, func() time.Time { return h.now })
	if err != nil {
		h.t.Fatal(err)
	}
	h.q = q
}

func (h *queueHarness) reopen() {
	h.t.Helper()
	if err := h.q.Close(); err != nil {
		h.t.Fatal(err)
	}
	h.open()
}

func (h *queueHarness) advance(d time.Duration) {
	h.t.Helper()
	h.now = h.now.Add(d)
}

func (h *queueHarness) countEvents(eventType string) int {
	h.t.Helper()
	count := 0
	if err := h.q.wal.Replay(func(e event) error {
		if e.Type == eventType {
			count++
		}
		return nil
	}); err != nil {
		h.t.Fatal(err)
	}
	return count
}
