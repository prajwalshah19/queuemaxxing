package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	compactionCrashPathEnv  = "QUEUEMAXXING_TEST_COMPACTION_CRASH_PATH"
	compactionCrashPhaseEnv = "QUEUEMAXXING_TEST_COMPACTION_CRASH_PHASE"
)

func TestCompactionProcessCrashRecovery(t *testing.T) {
	tests := []struct {
		phase    string
		exitCode int
		tempFile bool
	}{
		{phase: "before_rename", exitCode: 91, tempFile: true},
		{phase: "after_rename", exitCode: 92},
		{phase: "after_directory_sync", exitCode: 93},
	}

	for _, tc := range tests {
		t.Run(tc.phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "queue.wal")
			command := exec.Command(os.Args[0], "-test.run=^TestCompactionProcessCrashHelper$")
			command.Env = append(os.Environ(),
				compactionCrashPathEnv+"="+path,
				compactionCrashPhaseEnv+"="+tc.phase,
			)
			output, err := command.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != tc.exitCode {
				t.Fatalf("helper phase %s: err=%v output=%s", tc.phase, err, output)
			}

			q, err := Open(path, FIFO)
			if err != nil {
				t.Fatalf("reopen after %s: %v\n%s", tc.phase, err, output)
			}
			if stats := q.Stats(); stats.Total != 1 || stats.Ready != 1 {
				_ = q.Close()
				t.Fatalf("state after %s = %+v", tc.phase, stats)
			}
			if err := q.Close(); err != nil {
				t.Fatal(err)
			}

			temps, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".queue.wal.compact-*.tmp"))
			if err != nil {
				t.Fatal(err)
			}
			if got := len(temps) > 0; got != tc.tempFile {
				t.Fatalf("stale temp after %s = %v, want present=%v", tc.phase, temps, tc.tempFile)
			}
		})
	}
}

func TestCompactionProcessCrashHelper(t *testing.T) {
	phase := os.Getenv(compactionCrashPhaseEnv)
	if phase == "" {
		return
	}
	path := os.Getenv(compactionCrashPathEnv)
	q, err := Open(path, FIFO)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(70)
	}
	if _, err := q.Enqueue(json.RawMessage(`{"survives":true}`), 10, 0); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(71)
	}

	switch phase {
	case "before_rename":
		q.wal.ops.rename = func(string, string) error {
			os.Exit(91)
			return nil
		}
	case "after_rename":
		rename := q.wal.ops.rename
		q.wal.ops.rename = func(oldPath, newPath string) error {
			if err := rename(oldPath, newPath); err != nil {
				return err
			}
			os.Exit(92)
			return nil
		}
	case "after_directory_sync":
		openDir := q.wal.ops.openDir
		q.wal.ops.openDir = func(path string) (syncCloser, error) {
			dir, err := openDir(path)
			if err != nil {
				return nil, err
			}
			return &exitAfterDirectorySync{syncCloser: dir, exitCode: 93}, nil
		}
	default:
		fmt.Fprintln(os.Stderr, "unknown phase", phase)
		os.Exit(72)
	}

	if _, err := q.Compact(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(73)
	}
	os.Exit(74)
}

type exitAfterDirectorySync struct {
	syncCloser
	calls    int
	exitCode int
}

func (d *exitAfterDirectorySync) Sync() error {
	if err := d.syncCloser.Sync(); err != nil {
		return err
	}
	d.calls++
	if d.calls == 2 {
		os.Exit(d.exitCode)
	}
	return nil
}
