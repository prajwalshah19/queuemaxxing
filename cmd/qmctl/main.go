package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	baseURL := flag.String("url", "http://localhost:8080", "queue server URL")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() == 0 {
		usage()
		os.Exit(2)
	}

	client := &client{baseURL: strings.TrimRight(*baseURL, "/"), http: &http.Client{Timeout: 30 * time.Second}}
	if err := run(client, flag.Arg(0), flag.Args()[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(c *client, command string, args []string) error {
	switch command {
	case "put":
		fs := flag.NewFlagSet("put", flag.ContinueOnError)
		priority := fs.Int("priority", 0, "message priority (higher is first)")
		delay := fs.Duration("delay", 0, "initial delivery delay")
		idempotencyKey := fs.String("idempotency-key", "", "deduplicate producer retries using this key")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 1 || !json.Valid([]byte(fs.Arg(0))) {
			return errors.New("put requires one valid JSON value")
		}
		body := json.RawMessage(fs.Arg(0))
		return c.postWithIdempotencyKey("/v1/messages", map[string]any{
			"body": body, "priority": *priority, "delay_seconds": delay.Seconds(),
		}, *idempotencyKey)
	case "get":
		fs := flag.NewFlagSet("get", flag.ContinueOnError)
		visibility := fs.Duration("visibility", 30*time.Second, "acknowledgement deadline")
		wait := fs.Duration("wait", 0, "wait for a message when the queue is empty")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() != 0 {
			return errors.New("get takes no positional arguments")
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
		fs := flag.NewFlagSet("nack", flag.ContinueOnError)
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
		fs := flag.NewFlagSet("extend", flag.ContinueOnError)
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
	case "dead":
		if len(args) == 0 {
			return errors.New("dead requires list or replay")
		}
		switch args[0] {
		case "list":
			fs := flag.NewFlagSet("dead list", flag.ContinueOnError)
			limit := fs.Int("limit", 100, "maximum dead letters to return")
			if err := fs.Parse(args[1:]); err != nil {
				return err
			}
			if fs.NArg() != 0 {
				return errors.New("dead list takes no positional arguments")
			}
			return c.get(fmt.Sprintf("/v1/dead-letters?limit=%d", *limit))
		case "replay":
			fs := flag.NewFlagSet("dead replay", flag.ContinueOnError)
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
	case "stats":
		if len(args) != 0 {
			return errors.New("stats takes no arguments")
		}
		return c.get("/v1/stats")
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

type client struct {
	baseURL string
	http    httpDoer
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func (c *client) get(path string) error {
	request, err := http.NewRequest(http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	return c.do(request)
}

func (c *client) post(path string, value any) error {
	return c.postWithIdempotencyKey(path, value, "")
}

func (c *client) postWithIdempotencyKey(path string, value any, idempotencyKey string) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	return c.do(request)
}

func (c *client) do(request *http.Request) error {
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode == http.StatusNoContent {
		fmt.Println("no messages ready")
		return nil
	}
	var pretty bytes.Buffer
	if json.Indent(&pretty, body, "", "  ") == nil {
		fmt.Println(pretty.String())
	} else {
		fmt.Print(string(body))
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", response.Status)
	}
	return nil
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage: qmctl [-url URL] COMMAND

Commands:
  put [-priority N] [-delay 5s] [-idempotency-key KEY] '{"task":"email"}'
  get [-visibility 30s] [-wait 20s]
  ack MESSAGE_ID RECEIPT
  nack [-delay 5s] MESSAGE_ID RECEIPT
  extend [-visibility 30s] MESSAGE_ID RECEIPT
  dead list [-limit 100]
  dead replay [-delay 5s] MESSAGE_ID
  stats`)
}
