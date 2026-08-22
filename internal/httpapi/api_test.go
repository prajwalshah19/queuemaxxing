package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prajwalshah19/queuemaxxing/internal/queue"
)

func TestLongPollWakesWhenMessageArrives(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	req := httptest.NewRequest(http.MethodPost, "/v1/messages/reserve", bytes.NewBufferString(`{"wait_timeout_seconds":1}`))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, req)
		close(done)
	}()

	created := request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"job":1}}`, "")
	if created.Code != http.StatusCreated {
		t.Fatalf("enqueue status = %d, body = %s", created.Code, created.Body.String())
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("long poll did not wake after enqueue")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("long-poll status = %d, body = %s", response.Code, response.Body.String())
	}
	var delivery queue.Delivery
	if err := json.Unmarshal(response.Body.Bytes(), &delivery); err != nil {
		t.Fatal(err)
	}
	if delivery.ID == "" || string(delivery.Body) != `{"job":1}` {
		t.Fatalf("delivery = %#v", delivery)
	}
}

func TestLongPollTimeoutAndValidation(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	response := request(t, handler, http.MethodPost, "/v1/messages/reserve", `{"wait_timeout_seconds":0.01}`, "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("timeout status = %d, body = %s", response.Code, response.Body.String())
	}
	for _, body := range []string{
		`{"wait_timeout_seconds":-1}`,
		`{"wait_timeout_seconds":21}`,
	} {
		response = request(t, handler, http.MethodPost, "/v1/messages/reserve", body, "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, body = %s", body, response.Code, response.Body.String())
		}
	}
}

func TestLongPollHonorsCancelledRequest(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages/reserve", bytes.NewBufferString(`{"wait_timeout_seconds":1}`)).WithContext(ctx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if got := q.Stats(); got.Total != 0 || got.InFlight != 0 {
		t.Fatalf("cancelled request mutated queue: %#v", got)
	}
}

func TestLeaseExtensionHTTPContract(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"job":1}}`, "")
	reserved := request(t, handler, http.MethodPost, "/v1/messages/reserve", `{}`, "")
	var delivery queue.Delivery
	if err := json.Unmarshal(reserved.Body.Bytes(), &delivery); err != nil {
		t.Fatal(err)
	}

	extended := request(t, handler, http.MethodPost, "/v1/messages/"+delivery.ID+"/lease", fmtJSON(t, map[string]any{
		"receipt":                    delivery.Receipt,
		"visibility_timeout_seconds": 60,
	}), "")
	if extended.Code != http.StatusOK || !strings.Contains(extended.Body.String(), `"status":"lease_extended"`) {
		t.Fatalf("extension response = %d %s", extended.Code, extended.Body.String())
	}
	var extension struct {
		LeaseUntil time.Time `json:"lease_until"`
	}
	if err := json.Unmarshal(extended.Body.Bytes(), &extension); err != nil {
		t.Fatal(err)
	}
	if !extension.LeaseUntil.After(delivery.LeaseUntil) {
		t.Fatalf("extended deadline = %s, original = %s", extension.LeaseUntil, delivery.LeaseUntil)
	}

	wrongReceipt := request(t, handler, http.MethodPost, "/v1/messages/"+delivery.ID+"/lease", `{"receipt":"wrong","visibility_timeout_seconds":60}`, "")
	if wrongReceipt.Code != http.StatusConflict {
		t.Fatalf("wrong-receipt status = %d, body = %s", wrongReceipt.Code, wrongReceipt.Body.String())
	}
	nonExtending := request(t, handler, http.MethodPost, "/v1/messages/"+delivery.ID+"/lease", fmtJSON(t, map[string]any{
		"receipt":                    delivery.Receipt,
		"visibility_timeout_seconds": 1,
	}), "")
	if nonExtending.Code != http.StatusBadRequest {
		t.Fatalf("non-extending status = %d, body = %s", nonExtending.Code, nonExtending.Body.String())
	}
}

func TestStorageUnavailableMapsToServiceUnavailable(t *testing.T) {
	response := httptest.NewRecorder()
	writeQueueError(response, queue.ErrStorageUnavailable)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("storage-unavailable status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestHealthReportsTerminalStorageFailureWithoutLeakingCause(t *testing.T) {
	a := &API{storageError: func() error { return errors.New("secret storage detail") }}
	response := httptest.NewRecorder()
	a.health(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("health status = %d, body = %s", response.Code, response.Body.String())
	}
	if got := response.Body.String(); !strings.Contains(got, `"status":"storage_unavailable"`) || strings.Contains(got, "secret") {
		t.Fatalf("health body = %s", got)
	}
}

func TestNackAppliesAutomaticRetryAndReturnsStatus(t *testing.T) {
	q, err := queue.OpenWithConfig(filepath.Join(t.TempDir(), "queue.wal"), queue.Config{
		Discipline: queue.FIFO,
		RetryPolicy: queue.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Second,
			MaxDelay:    time.Minute,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	response := request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"task":"test"}}`, "")
	if response.Code != http.StatusCreated {
		t.Fatalf("enqueue status = %d", response.Code)
	}
	response = request(t, handler, http.MethodPost, "/v1/messages/reserve", `{}`, "")
	var delivery queue.Delivery
	if err := json.Unmarshal(response.Body.Bytes(), &delivery); err != nil {
		t.Fatal(err)
	}

	response = request(t, handler, http.MethodPost, "/v1/messages/"+delivery.ID+"/nack", fmtJSON(t, map[string]string{"receipt": delivery.Receipt}), "")
	if response.Code != http.StatusOK {
		t.Fatalf("nack status = %d, body = %s", response.Code, response.Body.String())
	}
	var result queue.RetryResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != queue.RetryScheduled || result.Attempts != 1 || result.VisibleAt == nil || result.VisibleAt.IsZero() {
		t.Fatalf("nack result = %#v", result)
	}
}

func TestDeadLetterInspectionAndReplay(t *testing.T) {
	q, err := queue.OpenWithConfig(filepath.Join(t.TempDir(), "queue.wal"), queue.Config{
		Discipline: queue.FIFO,
		RetryPolicy: queue.RetryPolicy{
			MaxAttempts: 1,
			BaseDelay:   time.Second,
			MaxDelay:    time.Minute,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	createdResponse := request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"task":"poison"},"priority":8}`, "")
	var created queue.Message
	if err := json.Unmarshal(createdResponse.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	reservedResponse := request(t, handler, http.MethodPost, "/v1/messages/reserve", `{}`, "")
	var delivery queue.Delivery
	if err := json.Unmarshal(reservedResponse.Body.Bytes(), &delivery); err != nil {
		t.Fatal(err)
	}

	nackResponse := request(t, handler, http.MethodPost, "/v1/messages/"+delivery.ID+"/nack", fmtJSON(t, map[string]string{"receipt": delivery.Receipt}), "")
	if nackResponse.Code != http.StatusOK || !strings.Contains(nackResponse.Body.String(), `"status":"dead_lettered"`) || strings.Contains(nackResponse.Body.String(), `"visible_at"`) {
		t.Fatalf("nack response = %d %s", nackResponse.Code, nackResponse.Body.String())
	}

	listResponse := request(t, handler, http.MethodGet, "/v1/dead-letters?limit=10", "", "")
	if listResponse.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listResponse.Code, listResponse.Body.String())
	}
	var letters []queue.DeadLetter
	if err := json.Unmarshal(listResponse.Body.Bytes(), &letters); err != nil {
		t.Fatal(err)
	}
	if len(letters) != 1 || letters[0].ID != created.ID || string(letters[0].Body) != `{"task":"poison"}` {
		t.Fatalf("dead letters = %#v", letters)
	}

	replayResponse := request(t, handler, http.MethodPost, "/v1/dead-letters/"+created.ID+"/replay", `{"delay_seconds":0}`, "")
	if replayResponse.Code != http.StatusCreated {
		t.Fatalf("replay status = %d, body = %s", replayResponse.Code, replayResponse.Body.String())
	}
	var replayed queue.Message
	if err := json.Unmarshal(replayResponse.Body.Bytes(), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ID == created.ID || replayed.OriginalMessageID != created.ID || replayed.Priority != created.Priority {
		t.Fatalf("replayed = %#v", replayed)
	}

	missing := request(t, handler, http.MethodPost, "/v1/dead-letters/"+created.ID+"/replay", `{"delay_seconds":0}`, "")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing replay status = %d, body = %s", missing.Code, missing.Body.String())
	}
}

func TestDeadLetterListValidatesLimit(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)
	for _, path := range []string{"/v1/dead-letters?limit=0", "/v1/dead-letters?limit=nope", "/v1/dead-letters?limit=1001"} {
		response := request(t, handler, http.MethodGet, path, "", "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, body = %s", path, response.Code, response.Body.String())
		}
	}
}

func TestMessageLifecycle(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	response := request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"task":"test"},"priority":7}`, "")
	if response.Code != http.StatusCreated {
		t.Fatalf("enqueue status = %d", response.Code)
	}

	response = request(t, handler, http.MethodPost, "/v1/messages/reserve", `{}`, "")
	if response.Code != http.StatusOK {
		t.Fatalf("reserve status = %d", response.Code)
	}
	var delivery struct {
		ID      string          `json:"id"`
		Body    json.RawMessage `json:"body"`
		Receipt string          `json:"receipt"`
	}
	if err := json.NewDecoder(response.Body).Decode(&delivery); err != nil {
		t.Fatal(err)
	}
	if string(delivery.Body) != `{"task":"test"}` {
		t.Fatalf("body = %s", delivery.Body)
	}

	payload := fmtJSON(t, map[string]string{"receipt": delivery.Receipt})
	response = request(t, handler, http.MethodPost, "/v1/messages/"+delivery.ID+"/ack", payload, "")
	if response.Code != http.StatusOK {
		t.Fatalf("ack status = %d", response.Code)
	}

	response = request(t, handler, http.MethodPost, "/v1/messages/reserve", `{}`, "")
	if response.Code != http.StatusNoContent {
		t.Fatalf("empty reserve status = %d", response.Code)
	}
}

func TestEnqueueRequiresBody(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	response := request(t, handler, http.MethodPost, "/v1/messages", `{"priority":7}`, "")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("enqueue status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestEnqueueIdempotencyContract(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	handler := New(q)

	first := request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"job":1},"priority":7}`, "request-123")
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
	}
	var created queue.Message
	if err := json.Unmarshal(first.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}

	replay := request(t, handler, http.MethodPost, "/v1/messages", `{"priority":7,"body":{"job":1}}`, "request-123")
	if replay.Code != http.StatusCreated || replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf("replay status=%d header=%q body=%s", replay.Code, replay.Header().Get("Idempotency-Replayed"), replay.Body.String())
	}
	var replayed queue.Message
	if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.ID != created.ID {
		t.Fatalf("replay ID = %s, want %s", replayed.ID, created.ID)
	}

	conflict := request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"job":2},"priority":7}`, "request-123")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, body = %s", conflict.Code, conflict.Body.String())
	}

	tooLong := strings.Repeat("k", queue.MaxIdempotencyKeyBytes+1)
	invalid := request(t, handler, http.MethodPost, "/v1/messages", `{"body":{"job":1}}`, tooLong)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, body = %s", invalid.Code, invalid.Body.String())
	}
}

func request(t *testing.T, handler http.Handler, method, path, body, idempotencyKey string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}

func fmtJSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
