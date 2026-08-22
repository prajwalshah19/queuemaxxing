package main

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/prajwalshah19/queuemaxxing/internal/queue"
)

func TestCompactQueueOnStartIsOptInAndProducesRestartableSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.wal")
	q, err := queue.Open(path, queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(json.RawMessage(`{"job":1}`), 4, 0); err != nil {
		t.Fatal(err)
	}

	if _, err := compactQueueOnStart(q, false); err != nil {
		t.Fatal(err)
	}
	if got := firstRecordType(t, path); got != "config" {
		t.Fatalf("disabled compaction first record = %q, want config", got)
	}

	result, err := compactQueueOnStart(q, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.Messages != 1 || result.OldBytes == 0 || result.NewBytes == 0 {
		t.Fatalf("compaction result = %+v", result)
	}
	if got := firstRecordType(t, path); got != "snapshot_begin" {
		t.Fatalf("enabled compaction first record = %q, want snapshot_begin", got)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := queue.Open(path, queue.FIFO)
	if err != nil {
		t.Fatalf("reopen compacted queue: %v", err)
	}
	defer reopened.Close()
	if stats := reopened.Stats(); stats.Total != 1 || stats.Ready != 1 {
		t.Fatalf("reopened stats = %+v", stats)
	}
}

func firstRecordType(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 8 {
		t.Fatalf("WAL is only %d bytes", len(data))
	}
	length := int(binary.BigEndian.Uint32(data[:4]))
	if length <= 0 || len(data) < 8+length {
		t.Fatalf("invalid first WAL frame length %d in %d bytes", length, len(data))
	}
	var record struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data[8:8+length], &record); err != nil {
		t.Fatal(err)
	}
	return record.Type
}
