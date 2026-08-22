package queue

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEncodeEventFramePreservesLegacyFormat(t *testing.T) {
	t.Parallel()

	e := event{Type: "config", Discipline: FIFO}
	payload, frame, err := encodeEventFrame(e)
	if err != nil {
		t.Fatal(err)
	}

	wantPayload, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	wantFrame := make([]byte, frameHeaderSize+len(wantPayload))
	binary.BigEndian.PutUint32(wantFrame[:4], uint32(len(wantPayload)))
	binary.BigEndian.PutUint32(wantFrame[4:8], crc32.ChecksumIEEE(wantPayload))
	copy(wantFrame[frameHeaderSize:], wantPayload)

	if !bytes.Equal(payload, wantPayload) {
		t.Fatalf("payload changed: got %q, want %q", payload, wantPayload)
	}
	if !bytes.Equal(frame, wantFrame) {
		t.Fatalf("frame changed: got %x, want %x", frame, wantFrame)
	}
}

func TestReplayRecordsReturnsRawPayloadAndDefersTornTailTruncation(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "queue.wal")
	w, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	e := event{Type: "config", Discipline: FIFO}
	wantPayload, frame, err := encodeEventFrame(e)
	if err != nil {
		t.Fatal(err)
	}
	torn := []byte{0, 0, 0, 20, 0, 0}
	if _, err := w.file.Write(append(frame, torn...)); err != nil {
		t.Fatal(err)
	}
	if err := w.file.Sync(); err != nil {
		t.Fatal(err)
	}

	var got replayRecord
	result, err := w.ReplayRecords(func(record replayRecord) error {
		got = record
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Payload, wantPayload) {
		t.Fatalf("raw payload changed: got %q, want %q", got.Payload, wantPayload)
	}
	if got.Offset != 0 || got.Event.Type != "config" {
		t.Fatalf("unexpected replay record: %+v", got)
	}
	if result.TornTailOffset == nil || *result.TornTailOffset != int64(len(frame)) {
		t.Fatalf("torn tail offset = %v, want %d", result.TornTailOffset, len(frame))
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(frame)+len(torn)) {
		t.Fatalf("ReplayRecords truncated WAL: size = %d, want %d", info.Size(), len(frame)+len(torn))
	}
}

func TestReplayCompatibilityWrapperTruncatesTornTail(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "queue.wal")
	w, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	_, frame, err := encodeEventFrame(event{Type: "config", Discipline: FIFO})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.file.Write(append(frame, 1, 2, 3)); err != nil {
		t.Fatal(err)
	}

	if err := w.Replay(func(event) error { return nil }); err != nil {
		t.Fatal(err)
	}
	info, err := w.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != int64(len(frame)) {
		t.Fatalf("size = %d, want %d", info.Size(), len(frame))
	}
}

func TestWALReplaceCommitsCompleteGenerationAndKeepsDescriptorOpen(t *testing.T) {
	t.Parallel()

	w := openTestWAL(t)
	old := w.file
	if err := w.Append(event{Type: "config", Discipline: FIFO}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(event{Type: "retry_policy", RetryPolicy: ptr(DefaultRetryPolicy())}); err != nil {
		t.Fatal(err)
	}

	result, err := w.Replace(func(writer *replacementWriter) error {
		_, err := writer.Append(event{Type: "config", Discipline: LIFO})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OldBytes <= result.NewBytes || result.NewBytes == 0 {
		t.Fatalf("unexpected replacement sizes: %+v", result)
	}
	if w.file == old {
		t.Fatal("replacement did not install a new descriptor")
	}
	if _, err := old.Stat(); err == nil {
		t.Fatal("obsolete descriptor was not closed")
	}

	var got []event
	if err := w.Replay(func(e event) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Discipline != LIFO {
		t.Fatalf("replacement replay = %+v", got)
	}
	if err := w.Append(event{Type: "retry_policy", RetryPolicy: ptr(DefaultRetryPolicy())}); err != nil {
		t.Fatalf("append through replacement descriptor: %v", err)
	}
}

func TestWALReplaceFailureBeforeRenamePreservesWritableOldGeneration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*wal)
		write  func(*replacementWriter) error
	}{
		{
			name: "directory open",
			mutate: func(w *wal) {
				w.ops.openDir = func(string) (syncCloser, error) {
					return nil, errors.New("injected directory open failure")
				}
			},
		},
		{
			name: "directory preflight sync",
			mutate: func(w *wal) {
				w.ops.openDir = func(string) (syncCloser, error) {
					return &failNthSyncCloser{failAt: 1}, nil
				}
			},
		},
		{
			name: "temp create",
			mutate: func(w *wal) {
				w.ops.createTemp = func(string, string) (walFile, error) {
					return nil, errors.New("injected temp create failure")
				}
			},
		},
		{
			name: "writer",
			write: func(*replacementWriter) error {
				return errors.New("injected writer failure")
			},
		},
		{
			name: "replacement write",
			mutate: func(w *wal) {
				createTemp := w.ops.createTemp
				w.ops.createTemp = func(dir, pattern string) (walFile, error) {
					f, err := createTemp(dir, pattern)
					if err != nil {
						return nil, err
					}
					return &failWriteWALFile{walFile: f}, nil
				}
			},
		},
		{
			name: "replacement sync",
			mutate: func(w *wal) {
				createTemp := w.ops.createTemp
				w.ops.createTemp = func(dir, pattern string) (walFile, error) {
					f, err := createTemp(dir, pattern)
					if err != nil {
						return nil, err
					}
					return &failSyncWALFile{walFile: f}, nil
				}
			},
		},
		{
			name: "rename",
			mutate: func(w *wal) {
				w.ops.rename = func(string, string) error { return errors.New("injected rename failure") }
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := openTestWAL(t)
			old := w.file
			if err := w.Append(event{Type: "config", Discipline: FIFO}); err != nil {
				t.Fatal(err)
			}
			if tc.mutate != nil {
				tc.mutate(w)
			}
			write := tc.write
			if write == nil {
				write = func(writer *replacementWriter) error {
					_, err := writer.Append(event{Type: "config", Discipline: LIFO})
					return err
				}
			}

			if _, err := w.Replace(write); err == nil {
				t.Fatal("Replace succeeded")
			}
			if w.file != old {
				t.Fatal("failed replacement changed active descriptor")
			}
			if err := w.Append(event{Type: "retry_policy", RetryPolicy: ptr(DefaultRetryPolicy())}); err != nil {
				t.Fatalf("old generation is not writable: %v", err)
			}
			matches, err := filepath.Glob(filepath.Join(filepath.Dir(w.path), ".queue.wal.compact-*.tmp"))
			if err != nil {
				t.Fatal(err)
			}
			if len(matches) != 0 {
				t.Fatalf("stale temp after definite failure: %v", matches)
			}
		})
	}
}

func TestWALReplacePreservesPrivateFileMode(t *testing.T) {
	t.Parallel()

	w := openTestWAL(t)
	if _, err := w.Replace(func(writer *replacementWriter) error {
		_, err := writer.Append(event{Type: "config", Discipline: FIFO})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(w.path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("replacement mode = %04o, want 0600", got)
	}
}

func TestWALReplaceDirectorySyncFailureInstallsGenerationAndPoisonsWAL(t *testing.T) {
	t.Parallel()

	w := openTestWAL(t)
	old := w.file
	w.ops.openDir = func(string) (syncCloser, error) {
		return &failNthSyncCloser{failAt: 2}, nil
	}

	_, err := w.Replace(func(writer *replacementWriter) error {
		_, err := writer.Append(event{Type: "config", Discipline: LIFO})
		return err
	})
	if !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Replace error = %v, want ErrStorageUnavailable", err)
	}
	if !errors.Is(w.Err(), ErrStorageUnavailable) {
		t.Fatalf("wal.Err = %v, want ErrStorageUnavailable", w.Err())
	}
	if w.file == old {
		t.Fatal("renamed replacement descriptor was not installed")
	}
	if err := w.Append(event{Type: "config", Discipline: FIFO}); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("Append error = %v, want ErrStorageUnavailable", err)
	}
	if _, err := w.Replace(func(*replacementWriter) error { return nil }); !errors.Is(err, ErrStorageUnavailable) {
		t.Fatalf("second Replace error = %v, want ErrStorageUnavailable", err)
	}

	var got event
	if err := w.Replay(func(e event) error { got = e; return nil }); err != nil {
		t.Fatal(err)
	}
	if got.Discipline != LIFO {
		t.Fatalf("active generation = %+v", got)
	}
}

func TestWALReplaceRejectsSymlinkPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "target.wal")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "queue.wal")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	w, err := openWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	_, err = w.Replace(func(*replacementWriter) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Replace error = %v, want symlink rejection", err)
	}
	if err := w.Append(event{Type: "config", Discipline: FIFO}); err != nil {
		t.Fatalf("symlink rejection poisoned old WAL: %v", err)
	}
}

func openTestWAL(t *testing.T) *wal {
	t.Helper()
	w, err := openWAL(filepath.Join(t.TempDir(), "queue.wal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

type failSyncWALFile struct {
	walFile
}

func (f *failSyncWALFile) Sync() error { return errors.New("injected sync failure") }

type failWriteWALFile struct {
	walFile
}

func (f *failWriteWALFile) Write([]byte) (int, error) {
	return 0, errors.New("injected write failure")
}

type failNthSyncCloser struct {
	calls  int
	failAt int
}

func (f *failNthSyncCloser) Sync() error {
	f.calls++
	if f.calls == f.failAt {
		return errors.New("injected directory sync failure")
	}
	return nil
}
func (*failNthSyncCloser) Close() error { return nil }

func ptr[T any](value T) *T { return &value }
