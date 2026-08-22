package queue

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

const (
	frameHeaderSize = 8
	maxFrameSize    = 16 << 20
)

var (
	ErrStorageUnavailable    = errors.New("queue storage is unavailable")
	ErrWALReplacementSupport = errors.New("crash-safe WAL replacement is unsupported on this platform")
)

type wal struct {
	path        string
	file        walFile
	ops         walOps
	terminalErr error
}

type walFile interface {
	io.ReaderAt
	io.Writer
	io.Seeker
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
	Name() string
}

type replayRecord struct {
	Event   event
	Payload []byte
	Offset  int64
}

type replayResult struct {
	TornTailOffset *int64
}

type syncCloser interface {
	Sync() error
	Close() error
}

type walOps struct {
	createTemp func(dir, pattern string) (walFile, error)
	openDir    func(path string) (syncCloser, error)
	rename     func(oldPath, newPath string) error
	remove     func(path string) error
	lstat      func(path string) (os.FileInfo, error)
}

type replacementWriter struct {
	file  walFile
	bytes int64
}

type replaceResult struct {
	OldBytes int64
	NewBytes int64
}

func openWAL(path string) (*wal, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	return &wal{path: path, file: f, ops: defaultWALOps()}, nil
}

func defaultWALOps() walOps {
	return walOps{
		createTemp: func(dir, pattern string) (walFile, error) {
			return os.CreateTemp(dir, pattern)
		},
		openDir: func(path string) (syncCloser, error) {
			return os.Open(path)
		},
		rename: os.Rename,
		remove: os.Remove,
		lstat:  os.Lstat,
	}
}

func (w *wal) Replay(apply func(event) error) error {
	result, err := w.ReplayRecords(func(record replayRecord) error {
		return apply(record.Event)
	})
	if err != nil {
		return err
	}
	if result.TornTailOffset != nil {
		return w.truncateTail(*result.TornTailOffset)
	}
	return nil
}

// ReplayRecords validates and decodes complete records, but deliberately leaves
// an incomplete final frame untouched. Callers decide whether the logical stream
// is complete enough to make truncating that tail safe.
func (w *wal) ReplayRecords(apply func(replayRecord) error) (replayResult, error) {
	info, err := w.file.Stat()
	if err != nil {
		return replayResult{}, fmt.Errorf("stat WAL: %w", err)
	}

	size := info.Size()
	var offset int64
	header := make([]byte, frameHeaderSize)
	for offset < size {
		if size-offset < frameHeaderSize {
			tornAt := offset
			return replayResult{TornTailOffset: &tornAt}, nil
		}
		if _, err := w.file.ReadAt(header, offset); err != nil {
			return replayResult{}, fmt.Errorf("read WAL header at %d: %w", offset, err)
		}

		length := binary.BigEndian.Uint32(header[:4])
		expectedCRC := binary.BigEndian.Uint32(header[4:])
		if length == 0 || length > maxFrameSize {
			return replayResult{}, fmt.Errorf("invalid WAL frame length %d at %d", length, offset)
		}

		frameEnd := offset + frameHeaderSize + int64(length)
		if frameEnd > size {
			tornAt := offset
			return replayResult{TornTailOffset: &tornAt}, nil
		}

		payload := make([]byte, length)
		if _, err := w.file.ReadAt(payload, offset+frameHeaderSize); err != nil {
			return replayResult{}, fmt.Errorf("read WAL payload at %d: %w", offset, err)
		}
		if actual := crc32.ChecksumIEEE(payload); actual != expectedCRC {
			return replayResult{}, fmt.Errorf("WAL checksum mismatch at %d", offset)
		}

		var e event
		if err := json.Unmarshal(payload, &e); err != nil {
			return replayResult{}, fmt.Errorf("decode WAL event at %d: %w", offset, err)
		}
		if err := apply(replayRecord{Event: e, Payload: payload, Offset: offset}); err != nil {
			return replayResult{}, fmt.Errorf("apply WAL event at %d: %w", offset, err)
		}
		offset = frameEnd
	}
	return replayResult{}, nil
}

func (w *wal) truncateTail(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate partial WAL frame at %d: %w", offset, err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync truncated WAL: %w", err)
	}
	return nil
}

func (w *wal) Append(e event) error {
	if err := w.Err(); err != nil {
		return err
	}
	_, frame, err := encodeEventFrame(e)
	if err != nil {
		return err
	}

	start, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}
	n, err := w.file.Write(frame)
	if err != nil || n != len(frame) {
		if truncateErr := w.file.Truncate(start); truncateErr != nil {
			w.terminalErr = fmt.Errorf("%w: append failed (%v) and rollback failed: %v", ErrStorageUnavailable, err, truncateErr)
			return w.terminalErr
		}
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("append WAL: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		w.terminalErr = fmt.Errorf("%w: sync WAL after complete write: %v", ErrStorageUnavailable, err)
		return w.terminalErr
	}
	return nil
}

func (w *wal) Err() error {
	if w == nil {
		return ErrStorageUnavailable
	}
	return w.terminalErr
}

// Replace atomically installs a fully written sibling file as the active WAL.
// Its caller must serialize it with Append and replay operations.
func (w *wal) Replace(write func(*replacementWriter) error) (replaceResult, error) {
	if err := w.Err(); err != nil {
		return replaceResult{}, err
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		return replaceResult{}, fmt.Errorf("%w: %s", ErrWALReplacementSupport, runtime.GOOS)
	}
	info, err := w.ops.lstat(w.path)
	if err != nil {
		return replaceResult{}, fmt.Errorf("inspect WAL path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return replaceResult{}, errors.New("refusing to replace WAL through a symlink path")
	}
	oldInfo, err := w.file.Stat()
	if err != nil {
		return replaceResult{}, fmt.Errorf("stat active WAL: %w", err)
	}

	dir := filepath.Dir(w.path)
	dirFile, err := w.ops.openDir(dir)
	if err != nil {
		return replaceResult{}, fmt.Errorf("open WAL directory: %w", err)
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return replaceResult{}, fmt.Errorf("preflight sync WAL directory: %w", err)
	}

	temp, err := w.ops.createTemp(dir, "."+filepath.Base(w.path)+".compact-*.tmp")
	if err != nil {
		return replaceResult{}, fmt.Errorf("create replacement WAL: %w", err)
	}
	tempPath := temp.Name()
	renamed := false
	defer func() {
		if renamed {
			return
		}
		_ = temp.Close()
		_ = w.ops.remove(tempPath)
	}()

	writer := &replacementWriter{file: temp}
	if err := write(writer); err != nil {
		return replaceResult{}, fmt.Errorf("write replacement WAL: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return replaceResult{}, fmt.Errorf("sync replacement WAL: %w", err)
	}
	newInfo, err := temp.Stat()
	if err != nil {
		return replaceResult{}, fmt.Errorf("stat replacement WAL: %w", err)
	}
	result := replaceResult{OldBytes: oldInfo.Size(), NewBytes: newInfo.Size()}
	if err := w.ops.rename(tempPath, w.path); err != nil {
		return replaceResult{}, fmt.Errorf("rename replacement WAL: %w", err)
	}
	renamed = true

	obsolete := w.file
	w.file = temp
	if err := dirFile.Sync(); err != nil {
		w.terminalErr = fmt.Errorf("%w: sync WAL directory after replacement: %v", ErrStorageUnavailable, err)
		_ = obsolete.Close()
		return result, w.terminalErr
	}

	// Closing the unlinked descriptor does not affect the committed generation.
	// There is no safe rollback at this point, so an obsolete-close failure is
	// intentionally non-fatal.
	_ = obsolete.Close()
	return result, nil
}

func (w *replacementWriter) Append(e event) ([]byte, error) {
	payload, frame, err := encodeEventFrame(e)
	if err != nil {
		return nil, err
	}
	n, err := w.file.Write(frame)
	if err != nil || n != len(frame) {
		if err == nil {
			err = io.ErrShortWrite
		}
		return nil, fmt.Errorf("write replacement WAL: %w", err)
	}
	w.bytes += int64(n)
	return payload, nil
}

func encodeEventFrame(e event) ([]byte, []byte, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return nil, nil, fmt.Errorf("encode WAL event: %w", err)
	}
	if len(payload) > maxFrameSize {
		return nil, nil, fmt.Errorf("WAL event is too large: %d bytes", len(payload))
	}

	frame := make([]byte, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderSize:], payload)
	return payload, frame, nil
}

func (w *wal) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}
