package clientcmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Runner struct {
	BaseURL string
	HTTP    HTTPDoer
	Output  io.Writer
}

func Execute(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("queuemaxxing", flag.ContinueOnError)
	fs.SetOutput(stderr)
	baseURL := fs.String("url", "http://localhost:8080", "queue server URL")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 {
		fmt.Fprintln(stderr, "client command is required")
		Usage(stderr)
		return 2
	}
	runner := &Runner{
		BaseURL: strings.TrimRight(*baseURL, "/"),
		HTTP:    &http.Client{Timeout: 30 * time.Second},
		Output:  stdout,
	}
	if err := runner.Run(fs.Arg(0), fs.Args()[1:]); err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func IsCommand(command string) bool {
	switch command {
	case "put", "reserve", "get", "ack", "nack", "extend", "dead", "dead-letter", "stats":
		return true
	default:
		return false
	}
}

func (c *Runner) Run(command string, args []string) error {
	switch command {
	case "put":
		fs := c.flagSet("put")
		priority := fs.Int("priority", 0, "message priority (higher is first)")
		delay := fs.Duration("delay", 0, "initial delivery delay")
		idempotencyKey := fs.String("idempotency-key", "", "deduplicate producer retries using this key")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 || !json.Valid([]byte(fs.Arg(0))) {
			return errors.New("put requires one valid JSON value")
		}
		return c.postWithIdempotencyKey("/v1/messages", map[string]any{
			"body": json.RawMessage(fs.Arg(0)), "priority": *priority, "delay_seconds": delay.Seconds(),
		}, *idempotencyKey)
	case "reserve", "get":
		fs := c.flagSet(command)
		visibility := fs.Duration("visibility", 30*time.Second, "acknowledgement deadline")
		wait := fs.Duration("wait", 0, "wait for a message when the queue is empty")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return fmt.Errorf("%s takes no positional arguments", command)
		}
		return c.post("/v1/messages/reserve", map[string]any{
			"visibility_timeout_seconds": visibility.Seconds(),
			"wait_timeout_seconds":       wait.Seconds(),
		})
	case "ack":
		if len(args) != 2 {
			return errors.New("ack requires MESSAGE_ID RECEIPT")
		}
		return c.post("/v1/messages/"+args[0]+"/ack", map[string]string{"receipt": args[1]})
	case "nack":
		fs := c.flagSet("nack")
		delay := fs.Duration("delay", 0, "redelivery delay")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 2 {
			return errors.New("nack requires MESSAGE_ID RECEIPT")
		}
		payload := map[string]any{"receipt": fs.Arg(1)}
		delaySet := false
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "delay" {
				delaySet = true
			}
		})
		if delaySet {
			payload["delay_seconds"] = delay.Seconds()
		}
		return c.post("/v1/messages/"+fs.Arg(0)+"/nack", payload)
	case "extend":
		fs := c.flagSet("extend")
		visibility := fs.Duration("visibility", 30*time.Second, "new acknowledgement deadline from server time")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 2 {
			return errors.New("extend requires MESSAGE_ID RECEIPT")
		}
		return c.post("/v1/messages/"+fs.Arg(0)+"/lease", map[string]any{
			"receipt":                    fs.Arg(1),
			"visibility_timeout_seconds": visibility.Seconds(),
		})
	case "dead", "dead-letter":
		return c.runDeadLetter(args)
	case "stats":
		if len(args) != 0 {
			return errors.New("stats takes no arguments")
		}
		return c.get("/v1/stats")
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

func (c *Runner) runDeadLetter(args []string) error {
	if len(args) == 0 {
		return errors.New("dead requires list or replay")
	}
	switch args[0] {
	case "list":
		fs := c.flagSet("dead list")
		limit := fs.Int("limit", 100, "maximum dead letters to return")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("dead list takes no positional arguments")
		}
		return c.get(fmt.Sprintf("/v1/dead-letters?limit=%d", *limit))
	case "replay":
		fs := c.flagSet("dead replay")
		delay := fs.Duration("delay", 0, "initial delay for the replayed message")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 1 {
			return errors.New("dead replay requires MESSAGE_ID")
		}
		return c.post("/v1/dead-letters/"+fs.Arg(0)+"/replay", map[string]any{"delay_seconds": delay.Seconds()})
	default:
		return fmt.Errorf("unknown dead command %q", args[0])
	}
}

func (c *Runner) flagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func (c *Runner) get(path string) error {
	request, err := http.NewRequest(http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	return c.do(request)
}

func (c *Runner) post(path string, value any) error {
	return c.postWithIdempotencyKey(path, value, "")
}

func (c *Runner) postWithIdempotencyKey(path string, value any, idempotencyKey string) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return c.do(request)
}

func (c *Runner) do(request *http.Request) error {
	response, err := c.HTTP.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	output := c.Output
	if output == nil {
		output = io.Discard
	}
	if response.StatusCode == http.StatusNoContent {
		fmt.Fprintln(output, "no messages ready")
		return nil
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, body, "", "  ") == nil {
		fmt.Fprintln(output, pretty.String())
	} else {
		_, _ = output.Write(body)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", response.Status)
	}
	return nil
}

func Usage(w io.Writer) {
	fmt.Fprintln(w, `Client commands:
  put [-priority N] [-delay 5s] [-idempotency-key KEY] '{"task":"email"}'
  reserve [-visibility 30s] [-wait 20s]
  ack MESSAGE_ID RECEIPT
  nack [-delay 5s] MESSAGE_ID RECEIPT
  extend [-visibility 30s] MESSAGE_ID RECEIPT
  dead list [-limit 100]
  dead replay [-delay 5s] MESSAGE_ID
  stats`)
}
