package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/prajwalshah19/queuemaxxing/internal/queue"
)

func TestUnifiedEntrypointRunsClientCommands(t *testing.T) {
	var stdout, stderr bytes.Buffer
	wantArgs := []string{"-url", "http://queue.test", "stats"}
	code := runApplicationWithClient(wantArgs, &stdout, &stderr, func(args []string, output, _ io.Writer) int {
		if !reflect.DeepEqual(args, wantArgs) {
			t.Fatalf("client args = %v, want %v", args, wantArgs)
		}
		_, _ = fmt.Fprintln(output, `{"ready": 3}`)
		return 0
	})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %s", code, stderr.String())
	}
	if got := stdout.String(); !bytes.Contains([]byte(got), []byte(`"ready": 3`)) {
		t.Fatalf("stdout = %q", got)
	}
}

func TestUnifiedEntrypointRejectsUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runApplication([]string{"unknown"}, &stdout, &stderr)
	if code != 2 || !bytes.Contains(stderr.Bytes(), []byte("unknown command")) {
		t.Fatalf("exit code = %d, stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

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
