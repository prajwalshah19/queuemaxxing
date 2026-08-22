//go:build fidelity

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCompiledServerSurvivesSIGKILLWithLeaseAndIdempotencyState(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL fidelity scenario requires Unix process semantics")
	}
	binary := os.Getenv("QUEUEMAXXING_BIN")
	if binary == "" {
		t.Fatal("QUEUEMAXXING_BIN is required; run make fidelity")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("queue binary: %v", err)
	}

	walPath := filepath.Join(t.TempDir(), "queue.wal")
	addr := freeAddress(t)
	server := startQueueServer(t, binary, addr, walPath)
	client := &http.Client{Timeout: 7 * time.Second}
	baseURL := "http://" + addr

	created := postJSON(t, client, baseURL+"/v1/messages", `{"body":{"job":"slow"},"priority":7}`, nil)
	if created.status != http.StatusCreated {
		t.Fatalf("enqueue = %d %s\nserver logs:\n%s", created.status, created.body, server.logs.String())
	}
	reserved := postJSON(t, client, baseURL+"/v1/messages/reserve", `{"visibility_timeout_seconds":30}`, nil)
	if reserved.status != http.StatusOK {
		t.Fatalf("reserve = %d %s", reserved.status, reserved.body)
	}
	var delivery struct {
		ID      string `json:"id"`
		Receipt string `json:"receipt"`
	}
	if err := json.Unmarshal(reserved.body, &delivery); err != nil {
		t.Fatal(err)
	}
	extended := postJSON(t, client, baseURL+"/v1/messages/"+delivery.ID+"/lease", fmt.Sprintf(`{"receipt":%q,"visibility_timeout_seconds":120}`, delivery.Receipt), nil)
	if extended.status != http.StatusOK {
		t.Fatalf("extend = %d %s", extended.status, extended.body)
	}

	server.kill(t)
	client.CloseIdleConnections()
	server = startQueueServer(t, binary, addr, walPath)
	hidden := postJSON(t, client, baseURL+"/v1/messages/reserve", `{}`, nil)
	if hidden.status != http.StatusNoContent {
		t.Fatalf("extended lease was visible after SIGKILL restart: %d %s\nserver logs:\n%s", hidden.status, hidden.body, server.logs.String())
	}
	acked := postJSON(t, client, baseURL+"/v1/messages/"+delivery.ID+"/ack", fmt.Sprintf(`{"receipt":%q}`, delivery.Receipt), nil)
	if acked.status != http.StatusOK {
		t.Fatalf("persisted receipt could not ack after restart: %d %s", acked.status, acked.body)
	}

	longPoll := make(chan httpResult, 1)
	go func() {
		longPoll <- postJSONResult(client, baseURL+"/v1/messages/reserve", `{"visibility_timeout_seconds":30,"wait_timeout_seconds":5}`, nil)
	}()
	select {
	case result := <-longPoll:
		t.Fatalf("empty long poll returned before enqueue: %d %s, %v", result.status, result.body, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	wakeup := postJSON(t, client, baseURL+"/v1/messages", `{"body":{"job":"wake"}}`, nil)
	if wakeup.status != http.StatusCreated {
		t.Fatalf("wakeup enqueue = %d %s", wakeup.status, wakeup.body)
	}
	select {
	case result := <-longPoll:
		if result.err != nil || result.status != http.StatusOK {
			t.Fatalf("long poll result = %d %s, %v", result.status, result.body, result.err)
		}
	case <-time.After(7 * time.Second):
		t.Fatal("real TCP long poll did not wake")
	}

	requestHeaders := http.Header{"Idempotency-Key": []string{"crash-safe-request"}}
	first := postJSON(t, client, baseURL+"/v1/messages", `{"body":{"job":"dedupe"}}`, requestHeaders)
	if first.status != http.StatusCreated {
		t.Fatalf("idempotent enqueue = %d %s", first.status, first.body)
	}
	var firstMessage struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(first.body, &firstMessage); err != nil {
		t.Fatal(err)
	}

	server.kill(t)
	client.CloseIdleConnections()
	server = startQueueServer(t, binary, addr, walPath)
	replayed := postJSON(t, client, baseURL+"/v1/messages", `{"body":{"job":"dedupe"}}`, requestHeaders)
	if replayed.status != http.StatusCreated || replayed.header.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("idempotent replay after SIGKILL = %d header=%q body=%s", replayed.status, replayed.header.Get("Idempotency-Replayed"), replayed.body)
	}
	var replayedMessage struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(replayed.body, &replayedMessage); err != nil {
		t.Fatal(err)
	}
	if replayedMessage.ID != firstMessage.ID {
		t.Fatalf("idempotent replay ID = %s, want %s", replayedMessage.ID, firstMessage.ID)
	}
}

func TestSingleBinaryClientAndStartupCompactionEndToEnd(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL fidelity scenario requires Unix process semantics")
	}
	binary := queueBinary(t)
	walPath := filepath.Join(t.TempDir(), "queue.wal")
	addr := freeAddress(t)
	server := startQueueServer(t, binary, addr, walPath, "-max-attempts", "1")
	baseURL := "http://" + addr

	put := runQueueCommand(t, binary, "-url", baseURL, "put", "-priority", "10", "-idempotency-key", "single-entrypoint-key", `{"job":"single-entrypoint-secret"}`)
	var created struct {
		ID string `json:"id"`
	}
	decodeCommandJSON(t, put, &created)
	if created.ID == "" {
		t.Fatalf("put output = %s", put)
	}

	reserve := runQueueCommand(t, binary, "-url", baseURL, "reserve", "-visibility", "30s")
	var delivery struct {
		ID      string `json:"id"`
		Receipt string `json:"receipt"`
	}
	decodeCommandJSON(t, reserve, &delivery)
	if delivery.ID != created.ID || delivery.Receipt == "" {
		t.Fatalf("reserve output = %s", reserve)
	}
	runQueueCommand(t, binary, "-url", baseURL, "extend", "-visibility", "60s", delivery.ID, delivery.Receipt)
	runQueueCommand(t, binary, "-url", baseURL, "nack", delivery.ID, delivery.Receipt)

	deadOutput := runQueueCommand(t, binary, "-url", baseURL, "dead", "list")
	var deadLetters []struct {
		ID string `json:"id"`
	}
	decodeCommandJSON(t, deadOutput, &deadLetters)
	if len(deadLetters) != 1 || deadLetters[0].ID != created.ID {
		t.Fatalf("dead list output = %s", deadOutput)
	}
	replayOutput := runQueueCommand(t, binary, "-url", baseURL, "dead", "replay", created.ID)
	var replayed struct {
		ID                string `json:"id"`
		OriginalMessageID string `json:"original_message_id"`
	}
	decodeCommandJSON(t, replayOutput, &replayed)
	if replayed.ID == "" || replayed.ID == created.ID || replayed.OriginalMessageID != created.ID {
		t.Fatalf("dead replay output = %s", replayOutput)
	}

	replayedDeliveryOutput := runQueueCommand(t, binary, "-url", baseURL, "reserve")
	var replayedDelivery struct {
		ID      string `json:"id"`
		Receipt string `json:"receipt"`
	}
	decodeCommandJSON(t, replayedDeliveryOutput, &replayedDelivery)
	if replayedDelivery.ID != replayed.ID {
		t.Fatalf("replayed reserve output = %s", replayedDeliveryOutput)
	}
	runQueueCommand(t, binary, "-url", baseURL, "ack", replayedDelivery.ID, replayedDelivery.Receipt)

	server.kill(t)
	clientBefore, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	server = startQueueServer(t, binary, addr, walPath, "-max-attempts", "1", "-compact-on-start")
	clientAfter, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if clientAfter.Size() >= clientBefore.Size() {
		t.Fatalf("startup compaction did not reduce WAL: %d -> %d\nlogs:\n%s", clientBefore.Size(), clientAfter.Size(), server.logs.String())
	}
	if logs := server.logs.String(); !strings.Contains(logs, "compacted queue on start") || strings.Contains(logs, "single-entrypoint-secret") || strings.Contains(logs, "single-entrypoint-key") {
		t.Fatalf("startup compaction logs are missing metrics or leak payload/key:\n%s", logs)
	}

	replayedPut := runQueueCommand(t, binary, "-url", baseURL, "put", "-priority", "10", "-idempotency-key", "single-entrypoint-key", `{"job":"single-entrypoint-secret"}`)
	var deduplicated struct {
		ID string `json:"id"`
	}
	decodeCommandJSON(t, replayedPut, &deduplicated)
	if deduplicated.ID != created.ID {
		t.Fatalf("idempotency changed through startup compaction: got %s, want %s", deduplicated.ID, created.ID)
	}

	statsOutput := runQueueCommand(t, binary, "-url", baseURL, "stats")
	var stats struct {
		Total       int `json:"total"`
		DeadLetters int `json:"dead_letters"`
	}
	decodeCommandJSON(t, statsOutput, &stats)
	if stats.Total != 0 || stats.DeadLetters != 0 {
		t.Fatalf("stats after complete lifecycle = %s", statsOutput)
	}
}

type queueServer struct {
	cmd     *exec.Cmd
	done    chan error
	logs    *lockedBuffer
	running bool
}

func startQueueServer(t *testing.T, binary, addr, walPath string, extraArgs ...string) *queueServer {
	t.Helper()
	logs := &lockedBuffer{}
	args := []string{"serve",
		"-addr", addr,
		"-data", walPath,
		"-discipline", "fifo",
		"-retry-base-delay", "100ms",
		"-retry-max-delay", "100ms",
	}
	args = append(args, extraArgs...)
	cmd := exec.Command(binary, args...)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	server := &queueServer{cmd: cmd, done: make(chan error, 1), logs: logs, running: true}
	go func() { server.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if server.running {
			_ = server.cmd.Process.Kill()
			<-server.done
			server.running = false
		}
	})

	client := &http.Client{Timeout: 250 * time.Millisecond}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + addr + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return server
			}
		}
		select {
		case err := <-server.done:
			server.running = false
			t.Fatalf("queue server exited before healthy: %v\n%s", err, logs.String())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	server.kill(t)
	t.Fatalf("queue server did not become healthy\n%s", logs.String())
	return nil
}

func queueBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("QUEUEMAXXING_BIN")
	if binary == "" {
		t.Fatal("QUEUEMAXXING_BIN is required; run make fidelity")
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("queue binary: %v", err)
	}
	return binary
}

func runQueueCommand(t *testing.T, binary string, args ...string) []byte {
	t.Helper()
	command := exec.Command(binary, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("%s %v: %v\nstdout:\n%s\nstderr:\n%s", binary, args, err, stdout.String(), stderr.String())
	}
	return stdout.Bytes()
}

func decodeCommandJSON(t *testing.T, output []byte, value any) {
	t.Helper()
	if err := json.Unmarshal(output, value); err != nil {
		t.Fatalf("decode command output %q: %v", output, err)
	}
}

func (s *queueServer) kill(t *testing.T) {
	t.Helper()
	if !s.running {
		return
	}
	if err := s.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill queue server: %v", err)
	}
	if err := <-s.done; err == nil {
		t.Fatal("SIGKILL server unexpectedly exited successfully")
	}
	s.running = false
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type httpResult struct {
	status int
	header http.Header
	body   []byte
	err    error
}

func postJSON(t *testing.T, client *http.Client, url, body string, headers http.Header) httpResult {
	t.Helper()
	result := postJSONResult(client, url, body, headers)
	if result.err != nil {
		t.Fatal(result.err)
	}
	return result
}

func postJSONResult(client *http.Client, url, body string, headers http.Header) httpResult {
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		return httpResult{err: err}
	}
	request.Header.Set("Content-Type", "application/json")
	for name, values := range headers {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return httpResult{err: err}
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	return httpResult{status: response.StatusCode, header: response.Header.Clone(), body: payload, err: err}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}
