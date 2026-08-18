package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/prajwalshah19/queuemaxxing/internal/queue"
)

func TestMessageLifecycle(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(New(q))
	defer server.Close()

	response := post(t, server.URL+"/v1/messages", `{"body":{"task":"test"},"priority":7}`)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("enqueue status = %d", response.StatusCode)
	}
	response.Body.Close()

	response = post(t, server.URL+"/v1/messages/reserve", `{}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("reserve status = %d", response.StatusCode)
	}
	var delivery struct {
		ID      string          `json:"id"`
		Body    json.RawMessage `json:"body"`
		Receipt string          `json:"receipt"`
	}
	if err := json.NewDecoder(response.Body).Decode(&delivery); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if string(delivery.Body) != `{"task":"test"}` {
		t.Fatalf("body = %s", delivery.Body)
	}

	payload := fmtJSON(t, map[string]string{"receipt": delivery.Receipt})
	response = post(t, server.URL+"/v1/messages/"+delivery.ID+"/ack", payload)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ack status = %d", response.StatusCode)
	}
	response.Body.Close()

	response = post(t, server.URL+"/v1/messages/reserve", `{}`)
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("empty reserve status = %d", response.StatusCode)
	}
	response.Body.Close()
}

func TestEnqueueRequiresBody(t *testing.T) {
	q, err := queue.Open(filepath.Join(t.TempDir(), "queue.wal"), queue.FIFO)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	server := httptest.NewServer(New(q))
	defer server.Close()

	response := post(t, server.URL+"/v1/messages", `{"priority":7}`)
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("enqueue status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	response, err := http.Post(url, "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
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
