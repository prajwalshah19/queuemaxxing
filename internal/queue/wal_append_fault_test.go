package queue

import (
	"encoding/json"
	"errors"
	"io"
	"testing"
)

func TestWALAppendShortWriteRollsBackAndRemainsWritable(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	h.q.wal.file = &appendFaultFile{walFile: h.q.wal.file, shortWrite: true}

	if _, err := h.q.Enqueue(json.RawMessage(`{"failed":true}`), 0, 0); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short-write error = %v", err)
	}
	if h.q.Stats().Total != 0 || h.q.nextSeq != 0 {
		t.Fatalf("short write mutated queue: stats=%#v seq=%d", h.q.Stats(), h.q.nextSeq)
	}
	kept, err := h.q.Enqueue(json.RawMessage(`{"kept":true}`), 0, 0)
	if err != nil {
		t.Fatalf("append after recovered short write: %v", err)
	}
	h.reopen()
	if h.q.Stats().Total != 1 || h.q.messages[kept.ID] == nil {
		t.Fatalf("restart state after short write: stats=%#v messages=%#v", h.q.Stats(), h.q.messages)
	}
}

func TestWALAppendSyncFailurePoisonsLaterMutations(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	h.q.wal.file = &appendFaultFile{walFile: h.q.wal.file, syncErr: errors.New("injected sync failure")}

	if _, err := h.q.Enqueue(json.RawMessage(`{"ambiguous":true}`), 0, 0); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("sync-failure error = %v", err)
	}
	if h.q.Stats().Total != 0 || h.q.nextSeq != 0 {
		t.Fatalf("sync failure mutated memory: stats=%#v seq=%d", h.q.Stats(), h.q.nextSeq)
	}
	before, err := h.q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.q.Enqueue(json.RawMessage(`{"must_not_append":true}`), 0, 0); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("mutation after poison = %v", err)
	}
	after, err := h.q.wal.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("poisoned mutation changed WAL size: %d -> %d", before.Size(), after.Size())
	}

	h.reopen()
	for _, m := range h.q.messages {
		if string(m.Body) == `{"must_not_append":true}` {
			t.Fatal("mutation after poison appeared on restart")
		}
	}
}

func TestWALAppendRollbackFailurePoisonsLaterMutations(t *testing.T) {
	h := newQueueHarness(t, Config{Discipline: FIFO})
	h.q.wal.file = &appendFaultFile{
		walFile:     h.q.wal.file,
		shortWrite:  true,
		truncateErr: errors.New("injected truncate failure"),
	}
	if _, err := h.q.Enqueue(json.RawMessage(`{"failed":true}`), 0, 0); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("rollback-failure error = %v", err)
	}
	if _, err := h.q.Enqueue(json.RawMessage(`{"later":true}`), 0, 0); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("mutation after rollback failure = %v", err)
	}
}

type appendFaultFile struct {
	walFile
	shortWrite  bool
	syncErr     error
	truncateErr error
}

func (f *appendFaultFile) Write(p []byte) (int, error) {
	if !f.shortWrite {
		return f.walFile.Write(p)
	}
	f.shortWrite = false
	limit := len(p) / 2
	if limit == 0 {
		limit = 1
	}
	return f.walFile.Write(p[:limit])
}

func (f *appendFaultFile) Sync() error {
	if f.syncErr == nil {
		return f.walFile.Sync()
	}
	err := f.syncErr
	f.syncErr = nil
	return err
}

func (f *appendFaultFile) Truncate(size int64) error {
	if f.truncateErr == nil {
		return f.walFile.Truncate(size)
	}
	err := f.truncateErr
	f.truncateErr = nil
	return err
}
