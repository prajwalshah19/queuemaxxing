package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prajwalshah19/queuemaxxing/internal/clientcmd"
	"github.com/prajwalshah19/queuemaxxing/internal/httpapi"
	"github.com/prajwalshah19/queuemaxxing/internal/queue"
)

func main() {
	os.Exit(runApplication(os.Args[1:], os.Stdout, os.Stderr))
}

func runApplication(args []string, stdout, stderr io.Writer) int {
	return runApplicationWithClient(args, stdout, stderr, clientcmd.Execute)
}

func runApplicationWithClient(args []string, stdout, stderr io.Writer, executeClient func([]string, io.Writer, io.Writer) int) int {
	if len(args) == 0 {
		applicationUsage(stderr)
		return 2
	}
	if args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		applicationUsage(stdout)
		return 0
	}
	if args[0] == "serve" {
		if err := runServer(args[1:], stdout, stderr); err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
		return 0
	}
	if isLegacyServerInvocation(args[0]) {
		if err := runServer(args, stdout, stderr); err != nil {
			fmt.Fprintln(stderr, "error:", err)
			return 1
		}
		return 0
	}
	if args[0] == "-url" || strings.HasPrefix(args[0], "-url=") || clientcmd.IsCommand(args[0]) {
		return executeClient(args, stdout, stderr)
	}

	fmt.Fprintf(stderr, "unknown command %q\n", args[0])
	applicationUsage(stderr)
	return 2
}

func runServer(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	addr := flags.String("addr", ":8080", "HTTP listen address")
	data := flags.String("data", "data/queue.wal", "path to the queue WAL")
	discipline := flags.String("discipline", "fifo", "tie-break order: fifo or lifo")
	idempotencyRetention := flags.Duration("idempotency-retention", queue.DefaultIdempotencyRetention, "producer idempotency-key retention")
	maxAttempts := flags.Int("max-attempts", queue.DefaultMaxAttempts, "maximum delivery attempts before dead-lettering")
	retryBaseDelay := flags.Duration("retry-base-delay", queue.DefaultRetryBaseDelay, "automatic retry backoff base")
	retryMaxDelay := flags.Duration("retry-max-delay", queue.DefaultRetryMaxDelay, "automatic retry backoff cap")
	compactOnStart := flags.Bool("compact-on-start", false, "replace WAL history with a current-state snapshot before listening")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("serve takes no positional arguments: %q", flags.Args())
	}

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	q, err := queue.OpenWithConfig(*data, queue.Config{
		Discipline:           queue.Discipline(*discipline),
		IdempotencyRetention: *idempotencyRetention,
		RetryPolicy: queue.RetryPolicy{
			MaxAttempts: *maxAttempts,
			BaseDelay:   *retryBaseDelay,
			MaxDelay:    *retryMaxDelay,
		},
	})
	if err != nil {
		return fmt.Errorf("open queue: %w", err)
	}

	if *compactOnStart {
		result, err := compactQueueOnStart(q, true)
		if err != nil {
			_ = q.Close()
			return fmt.Errorf("compact queue on start: %w", err)
		}
		logger.Info("compacted queue on start",
			"old_bytes", result.OldBytes,
			"new_bytes", result.NewBytes,
			"size_delta", result.SizeDelta,
			"messages", result.Messages,
			"dead_letters", result.DeadLetters,
			"idempotency_keys", result.IdempotencyKeys,
		)
	}

	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(q),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("queuemaxxing listening", "addr", *addr, "discipline", *discipline, "wal", *data)
		errCh <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)

	var serveErr error
	select {
	case sig := <-signals:
		logger.Info("shutting down", "signal", sig)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			serveErr = fmt.Errorf("serve: %w", err)
		}
	}

	if serveErr == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			serveErr = fmt.Errorf("shutdown HTTP server: %w", err)
		}
	}
	if err := q.Close(); err != nil && serveErr == nil {
		serveErr = fmt.Errorf("close queue: %w", err)
	}
	if serveErr != nil {
		return serveErr
	}
	fmt.Fprintln(stdout, "stopped")
	return nil
}

func compactQueueOnStart(q *queue.Queue, enabled bool) (queue.CompactionResult, error) {
	if !enabled {
		return queue.CompactionResult{}, nil
	}
	return q.Compact()
}

func isLegacyServerInvocation(first string) bool {
	for _, name := range []string{
		"addr", "data", "discipline", "idempotency-retention", "max-attempts",
		"retry-base-delay", "retry-max-delay", "compact-on-start",
	} {
		if first == "-"+name || strings.HasPrefix(first, "-"+name+"=") {
			return true
		}
	}
	return false
}

func applicationUsage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  queuemaxxing serve [SERVER_FLAGS]
  queuemaxxing [-url URL] COMMAND [COMMAND_FLAGS]

Server flags:
  -addr :8080
  -data data/queue.wal
  -discipline fifo|lifo
  -compact-on-start

Legacy server invocation without "serve" remains supported.`)
	clientcmd.Usage(w)
}
