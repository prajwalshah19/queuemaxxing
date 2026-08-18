package queue

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	frameHeaderSize = 8
	maxFrameSize    = 16 << 20
)

type wal struct {
	file *os.File
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
	return &wal{file: f}, nil
}

func (w *wal) Replay(apply func(event) error) error {
	info, err := w.file.Stat()
	if err != nil {
		return fmt.Errorf("stat WAL: %w", err)
	}

	size := info.Size()
	var offset int64
	header := make([]byte, frameHeaderSize)
	for offset < size {
		if size-offset < frameHeaderSize {
			return w.truncateTail(offset)
		}
		if _, err := w.file.ReadAt(header, offset); err != nil {
			return fmt.Errorf("read WAL header at %d: %w", offset, err)
		}

		length := binary.BigEndian.Uint32(header[:4])
		expectedCRC := binary.BigEndian.Uint32(header[4:])
		if length == 0 || length > maxFrameSize {
			return fmt.Errorf("invalid WAL frame length %d at %d", length, offset)
		}

		frameEnd := offset + frameHeaderSize + int64(length)
		if frameEnd > size {
			return w.truncateTail(offset)
		}

		payload := make([]byte, length)
		if _, err := w.file.ReadAt(payload, offset+frameHeaderSize); err != nil {
			return fmt.Errorf("read WAL payload at %d: %w", offset, err)
		}
		if actual := crc32.ChecksumIEEE(payload); actual != expectedCRC {
			return fmt.Errorf("WAL checksum mismatch at %d", offset)
		}

		var e event
		if err := json.Unmarshal(payload, &e); err != nil {
			return fmt.Errorf("decode WAL event at %d: %w", offset, err)
		}
		if err := apply(e); err != nil {
			return fmt.Errorf("apply WAL event at %d: %w", offset, err)
		}
		offset = frameEnd
	}
	return nil
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
	payload, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode WAL event: %w", err)
	}
	if len(payload) > maxFrameSize {
		return fmt.Errorf("WAL event is too large: %d bytes", len(payload))
	}

	frame := make([]byte, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[frameHeaderSize:], payload)

	start, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}
	n, err := w.file.Write(frame)
	if err != nil || n != len(frame) {
		_ = w.file.Truncate(start)
		if err == nil {
			err = io.ErrShortWrite
		}
		return fmt.Errorf("append WAL: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}
	return nil
}

func (w *wal) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}
