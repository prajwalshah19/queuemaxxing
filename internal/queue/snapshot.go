package queue

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"strings"
	"time"
)

const snapshotVersion uint32 = 1

type snapshotBegin struct {
	Version          uint32      `json:"version"`
	Generation       string      `json:"generation"`
	CreatedAt        time.Time   `json:"created_at"`
	Discipline       Discipline  `json:"discipline"`
	RetryPolicy      RetryPolicy `json:"retry_policy"`
	NextSequence     uint64      `json:"next_sequence"`
	MessageCount     uint64      `json:"message_count"`
	DeadLetterCount  uint64      `json:"dead_letter_count"`
	IdempotencyCount uint64      `json:"idempotency_count"`
}

type snapshotDeadLetter struct {
	Message        Message   `json:"message"`
	DeadLetteredAt time.Time `json:"dead_lettered_at"`
	Reason         string    `json:"reason"`
	Sequence       uint64    `json:"sequence"`
}

type snapshotIdempotency struct {
	Key         string    `json:"key"`
	RequestHash string    `json:"request_hash"`
	Message     Message   `json:"message"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type snapshotCommit struct {
	Generation       string `json:"generation"`
	MessageCount     uint64 `json:"message_count"`
	DeadLetterCount  uint64 `json:"dead_letter_count"`
	IdempotencyCount uint64 `json:"idempotency_count"`
	Checksum         string `json:"checksum"`
}

type queueReplayResult struct {
	configured            bool
	retryPolicyConfigured bool
	snapshotWasCommitted  bool
}

type snapshotReplay struct {
	begin       snapshotBegin
	digest      hash.Hash
	messages    map[string]*storedMessage
	deadLetters map[string]*DeadLetter
	idempotency map[string]idempotencyRecord
	maxSequence uint64
	phase       uint8
}

func replayQueueWAL(w *wal, q *Queue) (queueReplayResult, error) {
	var result queueReplayResult
	var snapshot *snapshotReplay

	replayResult, err := w.ReplayRecords(func(record replayRecord) error {
		if snapshot != nil {
			committed, err := snapshot.consume(record)
			if err != nil {
				return err
			}
			if committed {
				snapshot.install(q)
				result.configured = true
				result.retryPolicyConfigured = true
				result.snapshotWasCommitted = true
				snapshot = nil
			}
			return nil
		}

		if !result.configured && record.Event.Type == "snapshot_begin" {
			var err error
			snapshot, err = newSnapshotReplay(record)
			return err
		}
		return applyOperationalReplay(q, record.Event, &result)
	})
	if err != nil {
		return queueReplayResult{}, err
	}
	if snapshot != nil {
		return queueReplayResult{}, errors.New("snapshot is missing commit")
	}
	if replayResult.TornTailOffset != nil {
		if err := w.truncateTail(*replayResult.TornTailOffset); err != nil {
			return queueReplayResult{}, err
		}
	}
	return result, nil
}

func applyOperationalReplay(q *Queue, e event, result *queueReplayResult) error {
	if e.Type == "config" {
		if result.configured {
			return errors.New("duplicate queue configuration")
		}
		if e.Discipline != FIFO && e.Discipline != LIFO {
			return fmt.Errorf("invalid persisted discipline %q", e.Discipline)
		}
		result.configured = true
		q.discipline = e.Discipline
		return nil
	}
	if !result.configured {
		return errors.New("event encountered before queue configuration")
	}
	if e.Type == "retry_policy" {
		if result.retryPolicyConfigured || e.RetryPolicy == nil {
			return errors.New("invalid or duplicate retry policy")
		}
		if err := validateRetryPolicy(*e.RetryPolicy); err != nil {
			return fmt.Errorf("invalid persisted retry policy: %w", err)
		}
		result.retryPolicyConfigured = true
		q.retryPolicy = *e.RetryPolicy
		return nil
	}
	if requiresRetryPolicy(e.Type) && !result.retryPolicyConfigured {
		return errors.New("retry event encountered before retry policy")
	}
	return q.apply(e)
}

func newSnapshotReplay(record replayRecord) (*snapshotReplay, error) {
	begin := record.Event.SnapshotBegin
	if begin == nil {
		return nil, errors.New("snapshot_begin record is missing state")
	}
	if begin.Version != snapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", begin.Version)
	}
	if begin.Generation == "" {
		return nil, errors.New("snapshot generation is empty")
	}
	if begin.CreatedAt.IsZero() {
		return nil, errors.New("snapshot creation timestamp is empty")
	}
	if begin.Discipline != FIFO && begin.Discipline != LIFO {
		return nil, fmt.Errorf("invalid snapshot discipline %q", begin.Discipline)
	}
	if err := validateRetryPolicy(begin.RetryPolicy); err != nil {
		return nil, fmt.Errorf("invalid snapshot retry policy: %w", err)
	}

	s := &snapshotReplay{
		begin:       *begin,
		digest:      sha256.New(),
		messages:    make(map[string]*storedMessage),
		deadLetters: make(map[string]*DeadLetter),
		idempotency: make(map[string]idempotencyRecord),
	}
	hashSnapshotPayload(s.digest, record.Payload)
	return s, nil
}

func (s *snapshotReplay) consume(record replayRecord) (bool, error) {
	switch record.Event.Type {
	case "snapshot_begin":
		return false, errors.New("nested snapshot_begin record")
	case "snapshot_message":
		if s.phase > 0 {
			return false, errors.New("snapshot_message record is out of order")
		}
		if err := s.addMessage(record.Event.Message); err != nil {
			return false, err
		}
		hashSnapshotPayload(s.digest, record.Payload)
	case "snapshot_dead_letter":
		if s.phase > 1 {
			return false, errors.New("snapshot_dead_letter record is out of order")
		}
		s.phase = 1
		if err := s.addDeadLetter(record.Event.SnapshotDeadLetter); err != nil {
			return false, err
		}
		hashSnapshotPayload(s.digest, record.Payload)
	case "snapshot_idempotency":
		s.phase = 2
		if err := s.addIdempotency(record.Event.SnapshotIdempotency); err != nil {
			return false, err
		}
		hashSnapshotPayload(s.digest, record.Payload)
	case "snapshot_commit":
		if err := s.validateCommit(record.Event.SnapshotCommit); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, fmt.Errorf("ordinary event %q encountered before snapshot commit", record.Event.Type)
	}
	return false, nil
}

func (s *snapshotReplay) addMessage(message *storedMessage) error {
	if message == nil || !validSnapshotMessage(message.Message) || message.Sequence == 0 {
		return errors.New("invalid snapshot message")
	}
	if (message.Receipt == "") != message.LeaseUntil.IsZero() {
		return errors.New("snapshot message has inconsistent lease state")
	}
	if _, exists := s.messages[message.ID]; exists {
		return fmt.Errorf("duplicate snapshot message %q", message.ID)
	}
	if _, exists := s.deadLetters[message.ID]; exists {
		return fmt.Errorf("snapshot message %q also exists as a dead letter", message.ID)
	}
	copy := *message
	copy.Body = append(json.RawMessage(nil), message.Body...)
	s.messages[copy.ID] = &copy
	if copy.Sequence > s.maxSequence {
		s.maxSequence = copy.Sequence
	}
	return nil
}

func (s *snapshotReplay) addDeadLetter(snapshot *snapshotDeadLetter) error {
	if snapshot == nil || !validSnapshotMessage(snapshot.Message) || snapshot.Sequence == 0 || snapshot.DeadLetteredAt.IsZero() || snapshot.Reason == "" {
		return errors.New("invalid snapshot dead letter")
	}
	if _, exists := s.deadLetters[snapshot.Message.ID]; exists {
		return fmt.Errorf("duplicate snapshot dead letter %q", snapshot.Message.ID)
	}
	if _, exists := s.messages[snapshot.Message.ID]; exists {
		return fmt.Errorf("snapshot dead letter %q also exists as a live message", snapshot.Message.ID)
	}
	letter := &DeadLetter{
		Message:        cloneMessage(snapshot.Message),
		DeadLetteredAt: snapshot.DeadLetteredAt,
		Reason:         snapshot.Reason,
		Sequence:       snapshot.Sequence,
	}
	s.deadLetters[letter.ID] = letter
	if letter.Sequence > s.maxSequence {
		s.maxSequence = letter.Sequence
	}
	return nil
}

func (s *snapshotReplay) addIdempotency(snapshot *snapshotIdempotency) error {
	if snapshot == nil || snapshot.Key == "" || len(snapshot.Key) > MaxIdempotencyKeyBytes || snapshot.RequestHash == "" || !validSnapshotMessage(snapshot.Message) || snapshot.ExpiresAt.IsZero() {
		return errors.New("invalid snapshot idempotency record")
	}
	if _, exists := s.idempotency[snapshot.Key]; exists {
		return fmt.Errorf("duplicate snapshot idempotency key %q", snapshot.Key)
	}
	s.idempotency[snapshot.Key] = idempotencyRecord{
		RequestHash: snapshot.RequestHash,
		Message:     cloneMessage(snapshot.Message),
		ExpiresAt:   snapshot.ExpiresAt,
	}
	return nil
}

func (s *snapshotReplay) validateCommit(commit *snapshotCommit) error {
	if commit == nil {
		return errors.New("snapshot_commit record is missing state")
	}
	if commit.Generation != s.begin.Generation {
		return errors.New("snapshot commit generation mismatch")
	}
	if commit.MessageCount != s.begin.MessageCount || commit.MessageCount != uint64(len(s.messages)) ||
		commit.DeadLetterCount != s.begin.DeadLetterCount || commit.DeadLetterCount != uint64(len(s.deadLetters)) ||
		commit.IdempotencyCount != s.begin.IdempotencyCount || commit.IdempotencyCount != uint64(len(s.idempotency)) {
		return errors.New("snapshot record count mismatch")
	}
	if s.begin.NextSequence < s.maxSequence {
		return errors.New("snapshot next sequence is below stored sequence")
	}
	wantChecksum := hex.EncodeToString(s.digest.Sum(nil))
	if commit.Checksum != wantChecksum || strings.ToLower(commit.Checksum) != commit.Checksum {
		return errors.New("snapshot checksum mismatch")
	}
	return nil
}

func (s *snapshotReplay) install(q *Queue) {
	q.discipline = s.begin.Discipline
	q.retryPolicy = s.begin.RetryPolicy
	q.nextSeq = s.begin.NextSequence
	q.messages = s.messages
	q.deadLetters = s.deadLetters
	q.idempotency = make(map[string]idempotencyRecord, len(s.idempotency))
	q.idempotencyExpiry = nil
	for key, record := range s.idempotency {
		q.trackIdempotency(key, record)
	}
}

func validSnapshotMessage(message Message) bool {
	return message.ID != "" && len(message.Body) > 0 && json.Valid(message.Body) && !message.VisibleAt.IsZero() && !message.EnqueuedAt.IsZero()
}

func hashSnapshotPayload(digest hash.Hash, payload []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(payload)
}
