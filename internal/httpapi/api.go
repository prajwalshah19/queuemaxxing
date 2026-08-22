package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/prajwalshah19/queuemaxxing/internal/queue"
)

const (
	maxRequestBody  = 1 << 20
	maxLongPollWait = 20 * time.Second
)

type API struct {
	queue        *queue.Queue
	storageError func() error
}

func New(q *queue.Queue) http.Handler {
	a := &API{queue: q, storageError: q.StorageError}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.health)
	mux.HandleFunc("GET /v1/stats", a.stats)
	mux.HandleFunc("POST /v1/messages", a.enqueue)
	mux.HandleFunc("POST /v1/messages/reserve", a.reserve)
	mux.HandleFunc("POST /v1/messages/{id}/ack", a.ack)
	mux.HandleFunc("POST /v1/messages/{id}/nack", a.nack)
	mux.HandleFunc("POST /v1/messages/{id}/lease", a.extendLease)
	mux.HandleFunc("GET /v1/dead-letters", a.deadLetters)
	mux.HandleFunc("POST /v1/dead-letters/{id}/replay", a.replayDeadLetter)
	return mux
}

func (a *API) health(w http.ResponseWriter, _ *http.Request) {
	if err := a.storageError(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "storage_unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.queue.Stats())
}

func (a *API) enqueue(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Body         json.RawMessage `json:"body"`
		Priority     int             `json:"priority"`
		DelaySeconds float64         `json:"delay_seconds"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if len(input.Body) == 0 {
		writeError(w, http.StatusBadRequest, errors.New("body is required"))
		return
	}
	delay, err := seconds(input.DelaySeconds, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	result, err := a.queue.EnqueueIdempotent(input.Body, input.Priority, delay, r.Header.Get("Idempotency-Key"))
	if err != nil {
		switch {
		case errors.Is(err, queue.ErrStorageUnavailable):
			writeError(w, http.StatusServiceUnavailable, err)
		case errors.Is(err, queue.ErrIdempotencyConflict):
			writeError(w, http.StatusConflict, err)
		case errors.Is(err, queue.ErrInvalidIdempotencyKey):
			writeError(w, http.StatusBadRequest, err)
		default:
			writeError(w, http.StatusInternalServerError, err)
		}
		return
	}
	if result.Replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	writeJSON(w, http.StatusCreated, result.Message)
}

func (a *API) reserve(w http.ResponseWriter, r *http.Request) {
	var input struct {
		VisibilityTimeoutSeconds float64 `json:"visibility_timeout_seconds"`
		WaitTimeoutSeconds       float64 `json:"wait_timeout_seconds"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if input.VisibilityTimeoutSeconds == 0 {
		input.VisibilityTimeoutSeconds = 30
	}
	visibility, err := seconds(input.VisibilityTimeoutSeconds, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	wait, err := seconds(input.WaitTimeoutSeconds, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if wait > maxLongPollWait {
		writeError(w, http.StatusBadRequest, fmt.Errorf("wait timeout must not exceed %s", maxLongPollWait))
		return
	}
	delivery, err := a.queue.ReserveWait(r.Context(), visibility, wait)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		writeQueueError(w, err)
		return
	}
	if delivery == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, delivery)
}

func (a *API) extendLease(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Receipt                  string  `json:"receipt"`
		VisibilityTimeoutSeconds float64 `json:"visibility_timeout_seconds"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	visibility, err := seconds(input.VisibilityTimeoutSeconds, false)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	extension, err := a.queue.ExtendLease(r.PathValue("id"), input.Receipt, visibility)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Status     string    `json:"status"`
		LeaseUntil time.Time `json:"lease_until"`
	}{Status: "lease_extended", LeaseUntil: extension.LeaseUntil})
}

func (a *API) ack(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Receipt string `json:"receipt"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.queue.Ack(r.PathValue("id"), input.Receipt); err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
}

func (a *API) nack(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Receipt      string   `json:"receipt"`
		DelaySeconds *float64 `json:"delay_seconds"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	var delayOverride *time.Duration
	if input.DelaySeconds != nil {
		delay, err := seconds(*input.DelaySeconds, true)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		delayOverride = &delay
	}
	result, err := a.queue.NackWithOptions(r.PathValue("id"), input.Receipt, delayOverride)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) deadLetters(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("limit must be an integer"))
			return
		}
		limit = parsed
	}
	letters, err := a.queue.ListDeadLetters(limit)
	if err != nil {
		if limit < 1 || limit > queue.MaxDeadLetterListLimit {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, letters)
}

func (a *API) replayDeadLetter(w http.ResponseWriter, r *http.Request) {
	var input struct {
		DelaySeconds float64 `json:"delay_seconds"`
	}
	if err := decodeJSON(w, r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	delay, err := seconds(input.DelaySeconds, true)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	m, err := a.queue.ReplayDeadLetter(r.PathValue("id"), delay)
	if err != nil {
		writeQueueError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

func seconds(value float64, allowZero bool) (time.Duration, error) {
	maxSeconds := float64(math.MaxInt64) / float64(time.Second)
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > maxSeconds || (!allowZero && value == 0) {
		return 0, errors.New("duration must be positive")
	}
	d := time.Duration(value * float64(time.Second))
	if value > 0 && d <= 0 {
		return 0, errors.New("duration is too small")
	}
	return d, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request must contain one JSON object")
	}
	return nil
}

func writeQueueError(w http.ResponseWriter, err error) {
	if errors.Is(err, queue.ErrStorageUnavailable) {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	if errors.Is(err, queue.ErrInvalidLeaseExtension) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if errors.Is(err, queue.ErrDeadLetterNotFound) {
		writeError(w, http.StatusNotFound, err)
		return
	}
	if errors.Is(err, queue.ErrInvalidReceipt) {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeError(w, http.StatusInternalServerError, err)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
