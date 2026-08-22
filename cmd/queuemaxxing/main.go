package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prajwalshah19/queuemaxxing/internal/httpapi"
	"github.com/prajwalshah19/queuemaxxing/internal/queue"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	data := flag.String("data", "data/queue.wal", "path to the queue WAL")
	discipline := flag.String("discipline", "fifo", "tie-break order: fifo or lifo")
	idempotencyRetention := flag.Duration("idempotency-retention", queue.DefaultIdempotencyRetention, "producer idempotency-key retention")
	maxAttempts := flag.Int("max-attempts", queue.DefaultMaxAttempts, "maximum delivery attempts before dead-lettering")
	retryBaseDelay := flag.Duration("retry-base-delay", queue.DefaultRetryBaseDelay, "automatic retry backoff base")
	retryMaxDelay := flag.Duration("retry-max-delay", queue.DefaultRetryMaxDelay, "automatic retry backoff cap")
	compactOnStart := flag.Bool("compact-on-start", false, "replace WAL history with a current-state snapshot before listening")
	flag.Parse()

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
		slog.Error("open queue", "error", err)
		os.Exit(1)
	}
	if *compactOnStart {
		result, err := compactQueueOnStart(q, true)
		if err != nil {
			_ = q.Close()
			slog.Error("compact queue on start", "error", err)
			os.Exit(1)
		}
		slog.Info("compacted queue on start",
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
		slog.Info("queuemaxxing listening", "addr", *addr, "discipline", *discipline, "wal", *data)
		errCh <- server.ListenAndServe()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		slog.Info("shutting down", "signal", sig)
	case err := <-errCh:
		if err != nil && err != http.ErrServerClosed {
			slog.Error("serve", "error", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("shutdown HTTP server", "error", err)
	}
	if err := q.Close(); err != nil {
		slog.Error("close queue", "error", err)
	}
	fmt.Println("stopped")
}

func compactQueueOnStart(q *queue.Queue, enabled bool) (queue.CompactionResult, error) {
	if !enabled {
		return queue.CompactionResult{}, nil
	}
	return q.Compact()
}
