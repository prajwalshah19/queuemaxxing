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
	flag.Parse()

	q, err := queue.Open(*data, queue.Discipline(*discipline))
	if err != nil {
		slog.Error("open queue", "error", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:              *addr,
		Handler:           httpapi.New(q),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("shutdown HTTP server", "error", err)
	}
	if err := q.Close(); err != nil {
		slog.Error("close queue", "error", err)
	}
	fmt.Println("stopped")
}
