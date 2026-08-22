package queue

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSnapshotRequiresCommitWithoutTruncating(t *testing.T) {
	t.Parallel()

	path := writeSnapshotFixture(t, func(writer *replacementWriter) error {
		begin := validSnapshotBegin("missing-commit")
		_, err := writer.Append(event{Type: "snapshot_begin", SnapshotBegin: &begin})
		return err
	})
	before := fileSize(t, path)

	if _, err := Open(path, FIFO); err == nil || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("Open error = %v, want missing snapshot commit", err)
	}
	if after := fileSize(t, path); after != before {
		t.Fatalf("invalid snapshot was truncated: size %d -> %d", before, after)
	}
}

func TestSnapshotRejectsInvalidCommit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*snapshotCommit)
		want   string
	}{
		{name: "generation", mutate: func(c *snapshotCommit) { c.Generation = "wrong" }, want: "generation"},
		{name: "counts", mutate: func(c *snapshotCommit) { c.MessageCount = 1 }, want: "count"},
		{name: "checksum", mutate: func(c *snapshotCommit) { c.Checksum = strings.Repeat("0", 64) }, want: "checksum"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := writeSnapshotFixture(t, func(writer *replacementWriter) error {
				begin := validSnapshotBegin("generation-a")
				hash := sha256.New()
				if err := appendHashedSnapshotEvent(writer, hash, event{Type: "snapshot_begin", SnapshotBegin: &begin}); err != nil {
					return err
				}
				commit := snapshotCommit{
					Generation:       begin.Generation,
					MessageCount:     0,
					DeadLetterCount:  0,
					IdempotencyCount: 0,
					Checksum:         hex.EncodeToString(hash.Sum(nil)),
				}
				tc.mutate(&commit)
				_, err := writer.Append(event{Type: "snapshot_commit", SnapshotCommit: &commit})
				return err
			})

			if _, err := Open(path, FIFO); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Open error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSnapshotReplayRestoresCompleteDurableState(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	ready := &storedMessage{
		Message:  Message{ID: "ready", Body: []byte(`{"kind":"ready"}`), Priority: 9, VisibleAt: now.Add(-time.Minute), EnqueuedAt: now.Add(-time.Hour), Attempts: 2},
		Sequence: 5,
	}
	leased := &storedMessage{
		Message:    Message{ID: "leased", Body: []byte(`{"kind":"leased"}`), Priority: 3, VisibleAt: now.Add(-time.Minute), EnqueuedAt: now.Add(-30 * time.Minute), Attempts: 1},
		Sequence:   7,
		Receipt:    "receipt-secret",
		LeaseUntil: now.Add(time.Hour),
	}
	dead := snapshotDeadLetter{
		Message:        Message{ID: "dead", Body: []byte(`{"kind":"dead"}`), Priority: 1, VisibleAt: now.Add(-time.Hour), EnqueuedAt: now.Add(-2 * time.Hour), Attempts: 5},
		DeadLetteredAt: now.Add(-10 * time.Minute),
		Reason:         "max_attempts",
		Sequence:       6,
	}
	idempotency := snapshotIdempotency{
		Key:         "producer-key",
		RequestHash: strings.Repeat("a", 64),
		Message:     Message{ID: "acked", Body: []byte(`{"kind":"acked"}`), Priority: 4, VisibleAt: now.Add(-time.Hour), EnqueuedAt: now.Add(-time.Hour)},
		ExpiresAt:   now.Add(24 * time.Hour),
	}

	path := writeSnapshotFixture(t, func(writer *replacementWriter) error {
		begin := validSnapshotBegin("complete-state")
		begin.CreatedAt = now
		begin.NextSequence = 10
		begin.MessageCount = 2
		begin.DeadLetterCount = 1
		begin.IdempotencyCount = 1
		hash := sha256.New()
		events := []event{
			{Type: "snapshot_begin", SnapshotBegin: &begin},
			{Type: "snapshot_message", Message: ready},
			{Type: "snapshot_message", Message: leased},
			{Type: "snapshot_dead_letter", SnapshotDeadLetter: &dead},
			{Type: "snapshot_idempotency", SnapshotIdempotency: &idempotency},
		}
		for _, e := range events {
			if err := appendHashedSnapshotEvent(writer, hash, e); err != nil {
				return err
			}
		}
		commit := snapshotCommit{
			Generation:       begin.Generation,
			MessageCount:     begin.MessageCount,
			DeadLetterCount:  begin.DeadLetterCount,
			IdempotencyCount: begin.IdempotencyCount,
			Checksum:         hex.EncodeToString(hash.Sum(nil)),
		}
		_, err := writer.Append(event{Type: "snapshot_commit", SnapshotCommit: &commit})
		return err
	})

	q, err := openWithConfig(path, Config{Discipline: FIFO, RetryPolicy: DefaultRetryPolicy()}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()

	if q.nextSeq != 10 || len(q.messages) != 2 || len(q.deadLetters) != 1 || len(q.idempotency) != 1 {
		t.Fatalf("restored state counts: next=%d messages=%d dead=%d idempotency=%d", q.nextSeq, len(q.messages), len(q.deadLetters), len(q.idempotency))
	}
	if got := q.messages["leased"]; got == nil || got.Receipt != leased.Receipt || !got.LeaseUntil.Equal(leased.LeaseUntil) || got.Attempts != leased.Attempts {
		t.Fatalf("leased message changed: %+v", got)
	}
	if got := q.deadLetters["dead"]; got == nil || got.Sequence != dead.Sequence || got.Reason != dead.Reason {
		t.Fatalf("dead letter changed: %+v", got)
	}
	if got := q.idempotency["producer-key"]; got.RequestHash != idempotency.RequestHash || got.Message.ID != idempotency.Message.ID || !got.ExpiresAt.Equal(idempotency.ExpiresAt) {
		t.Fatalf("idempotency state changed: %+v", got)
	}
	if q.ready.Len() != 1 || q.scheduled.Len() != 1 || q.idempotencyExpiry.Len() != 1 {
		t.Fatalf("derived indexes not rebuilt: ready=%d scheduled=%d idempotency=%d", q.ready.Len(), q.scheduled.Len(), q.idempotencyExpiry.Len())
	}
}

func validSnapshotBegin(generation string) snapshotBegin {
	return snapshotBegin{
		Version:     snapshotVersion,
		Generation:  generation,
		CreatedAt:   time.Date(2026, 8, 22, 1, 0, 0, 0, time.UTC),
		Discipline:  FIFO,
		RetryPolicy: DefaultRetryPolicy(),
	}
}

func appendHashedSnapshotEvent(writer *replacementWriter, hash interface{ Write([]byte) (int, error) }, e event) error {
	payload, err := writer.Append(e)
	if err != nil {
		return err
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	_, _ = hash.Write(length[:])
	_, _ = hash.Write(payload)
	return nil
}

func writeSnapshotFixture(t *testing.T, write func(*replacementWriter) error) string {
	t.Helper()
	w := openTestWAL(t)
	path := w.path
	if _, err := w.Replace(write); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
