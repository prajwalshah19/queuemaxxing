package queue

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"sort"
)

type CompactionResult struct {
	OldBytes        int64 `json:"old_bytes"`
	NewBytes        int64 `json:"new_bytes"`
	SizeDelta       int64 `json:"size_delta"`
	Messages        int   `json:"messages"`
	DeadLetters     int   `json:"dead_letters"`
	IdempotencyKeys int   `json:"idempotency_keys"`
}

// Compact replaces operational history with a committed snapshot of current
// durable state. The queue mutex defines the exact state cut and prevents an
// acknowledged mutation from straddling the descriptor handoff.
func (q *Queue) Compact() (CompactionResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := q.ensureWritableLocked(); err != nil {
		return CompactionResult{}, err
	}

	now := q.clock.Now().UTC()
	q.pruneExpiredIdempotency(now)

	messages := make([]*storedMessage, 0, len(q.messages))
	for _, message := range q.messages {
		messages = append(messages, message)
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Sequence != messages[j].Sequence {
			return messages[i].Sequence < messages[j].Sequence
		}
		return messages[i].ID < messages[j].ID
	})

	deadLetters := make([]*DeadLetter, 0, len(q.deadLetters))
	for _, letter := range q.deadLetters {
		deadLetters = append(deadLetters, letter)
	}
	sort.Slice(deadLetters, func(i, j int) bool {
		if deadLetters[i].Sequence != deadLetters[j].Sequence {
			return deadLetters[i].Sequence < deadLetters[j].Sequence
		}
		return deadLetters[i].ID < deadLetters[j].ID
	})

	idempotencyKeys := make([]string, 0, len(q.idempotency))
	for key := range q.idempotency {
		idempotencyKeys = append(idempotencyKeys, key)
	}
	sort.Strings(idempotencyKeys)

	generation, err := randomID()
	if err != nil {
		return CompactionResult{}, err
	}
	begin := snapshotBegin{
		Version:          snapshotVersion,
		Generation:       generation,
		CreatedAt:        now,
		Discipline:       q.discipline,
		RetryPolicy:      q.retryPolicy,
		NextSequence:     q.nextSeq,
		MessageCount:     uint64(len(messages)),
		DeadLetterCount:  uint64(len(deadLetters)),
		IdempotencyCount: uint64(len(idempotencyKeys)),
	}

	result := CompactionResult{
		Messages:        len(messages),
		DeadLetters:     len(deadLetters),
		IdempotencyKeys: len(idempotencyKeys),
	}
	replaced, replaceErr := q.wal.Replace(func(writer *replacementWriter) error {
		digest := sha256.New()
		if err := appendSnapshotRecord(writer, digest, event{Type: "snapshot_begin", SnapshotBegin: &begin}); err != nil {
			return err
		}
		for _, message := range messages {
			if err := appendSnapshotRecord(writer, digest, event{Type: "snapshot_message", Message: message}); err != nil {
				return err
			}
		}
		for _, letter := range deadLetters {
			record := snapshotDeadLetter{
				Message:        letter.Message,
				DeadLetteredAt: letter.DeadLetteredAt,
				Reason:         letter.Reason,
				Sequence:       letter.Sequence,
			}
			if err := appendSnapshotRecord(writer, digest, event{Type: "snapshot_dead_letter", SnapshotDeadLetter: &record}); err != nil {
				return err
			}
		}
		for _, key := range idempotencyKeys {
			record := q.idempotency[key]
			snapshot := snapshotIdempotency{
				Key:         key,
				RequestHash: record.RequestHash,
				Message:     record.Message,
				ExpiresAt:   record.ExpiresAt,
			}
			if err := appendSnapshotRecord(writer, digest, event{Type: "snapshot_idempotency", SnapshotIdempotency: &snapshot}); err != nil {
				return err
			}
		}
		commit := snapshotCommit{
			Generation:       generation,
			MessageCount:     begin.MessageCount,
			DeadLetterCount:  begin.DeadLetterCount,
			IdempotencyCount: begin.IdempotencyCount,
			Checksum:         hex.EncodeToString(digest.Sum(nil)),
		}
		_, err := writer.Append(event{Type: "snapshot_commit", SnapshotCommit: &commit})
		return err
	})
	result.OldBytes = replaced.OldBytes
	result.NewBytes = replaced.NewBytes
	result.SizeDelta = result.OldBytes - result.NewBytes
	if replaceErr != nil {
		if errors.Is(replaceErr, ErrStorageUnavailable) {
			q.notifyLocked()
		}
		return result, replaceErr
	}
	return result, nil
}

func appendSnapshotRecord(writer *replacementWriter, digest hash.Hash, e event) error {
	payload, err := writer.Append(e)
	if err != nil {
		return err
	}
	hashSnapshotPayload(digest, payload)
	return nil
}

func (q *Queue) ensureWritableLocked() error {
	if q.closed {
		return ErrClosed
	}
	if err := q.wal.Err(); err != nil {
		return err
	}
	return nil
}

// StorageError reports a terminal durability error without exposing mutable WAL
// internals. A nil result means mutations are still permitted.
func (q *Queue) StorageError() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.wal.Err()
}
