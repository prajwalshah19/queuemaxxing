package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestPutSendsIdempotencyKey(t *testing.T) {
	transport := &recordingDoer{}
	c := &client{baseURL: "http://queue.test", http: transport}
	if err := run(c, "put", []string{"-idempotency-key", "job-123", `{"job":1}`}); err != nil {
		t.Fatal(err)
	}
	if transport.request == nil {
		t.Fatal("client did not send a request")
	}
	if got := transport.request.Header.Get("Idempotency-Key"); got != "job-123" {
		t.Fatalf("Idempotency-Key = %q, want %q", got, "job-123")
	}
}

func TestNackOmitsDelayForAutomaticBackoff(t *testing.T) {
	transport := &recordingDoer{}
	c := &client{baseURL: "http://queue.test", http: transport}
	if err := run(c, "nack", []string{"message-1", "receipt-1"}); err != nil {
		t.Fatal(err)
	}
	if transport.request.URL.Path != "/v1/messages/message-1/nack" {
		t.Fatalf("path = %s", transport.request.URL.Path)
	}
	var body map[string]any
	if err := json.NewDecoder(transport.request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if _, exists := body["delay_seconds"]; exists {
		t.Fatalf("automatic nack sent delay_seconds: %#v", body)
	}
}

func TestNackExplicitZeroOverridesBackoff(t *testing.T) {
	transport := &recordingDoer{}
	c := &client{baseURL: "http://queue.test", http: transport}
	if err := run(c, "nack", []string{"-delay", "0s", "message-1", "receipt-1"}); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.NewDecoder(transport.request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if value, exists := body["delay_seconds"]; !exists || value != float64(0) {
		t.Fatalf("explicit nack body = %#v", body)
	}
}

func TestGetSendsLongPollWait(t *testing.T) {
	transport := &recordingDoer{}
	c := &client{baseURL: "http://queue.test", http: transport}
	if err := run(c, "get", []string{"-visibility", "45s", "-wait", "20s"}); err != nil {
		t.Fatal(err)
	}
	if transport.request.URL.Path != "/v1/messages/reserve" {
		t.Fatalf("path = %s", transport.request.URL.Path)
	}
	var body map[string]any
	if err := json.NewDecoder(transport.request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["visibility_timeout_seconds"] != float64(45) || body["wait_timeout_seconds"] != float64(20) {
		t.Fatalf("get body = %#v", body)
	}
}

func TestExtendSendsReceiptAndVisibility(t *testing.T) {
	transport := &recordingDoer{}
	c := &client{baseURL: "http://queue.test", http: transport}
	if err := run(c, "extend", []string{"-visibility", "60s", "message-1", "receipt-1"}); err != nil {
		t.Fatal(err)
	}
	if transport.request.URL.Path != "/v1/messages/message-1/lease" {
		t.Fatalf("path = %s", transport.request.URL.Path)
	}
	var body map[string]any
	if err := json.NewDecoder(transport.request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["receipt"] != "receipt-1" || body["visibility_timeout_seconds"] != float64(60) {
		t.Fatalf("extend body = %#v", body)
	}
}

func TestDeadLetterCommands(t *testing.T) {
	transport := &recordingDoer{}
	c := &client{baseURL: "http://queue.test", http: transport}
	if err := run(c, "dead", []string{"list", "-limit", "25"}); err != nil {
		t.Fatal(err)
	}
	if transport.request.Method != http.MethodGet || transport.request.URL.RequestURI() != "/v1/dead-letters?limit=25" {
		t.Fatalf("list request = %s %s", transport.request.Method, transport.request.URL.RequestURI())
	}
	if err := run(c, "dead", []string{"replay", "-delay", "3s", "dead-1"}); err != nil {
		t.Fatal(err)
	}
	if transport.request.Method != http.MethodPost || transport.request.URL.Path != "/v1/dead-letters/dead-1/replay" {
		t.Fatalf("replay request = %s %s", transport.request.Method, transport.request.URL.Path)
	}
	var body map[string]any
	if err := json.NewDecoder(transport.request.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["delay_seconds"] != float64(3) {
		t.Fatalf("replay body = %#v", body)
	}
}

type recordingDoer struct {
	request *http.Request
}

func (d *recordingDoer) Do(request *http.Request) (*http.Response, error) {
	d.request = request
	return &http.Response{
		StatusCode: http.StatusCreated,
		Status:     "201 Created",
		Body:       io.NopCloser(strings.NewReader(`{"id":"message-1"}`)),
		Header:     make(http.Header),
	}, nil
}
