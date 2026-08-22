package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prajwalshah19/queuemaxxing/internal/clientcmd"
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
	return (&clientcmd.Runner{BaseURL: c.baseURL, HTTP: c.http, Output: os.Stdout}).Run(command, args)
}

type client struct {
	baseURL string
	http    httpDoer
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage: qmctl [-url URL] COMMAND

Commands:
  put [-priority N] [-delay 5s] [-idempotency-key KEY] '{"task":"email"}'
  reserve [-visibility 30s] [-wait 20s] (alias: get)
  ack MESSAGE_ID RECEIPT
  nack [-delay 5s] MESSAGE_ID RECEIPT
  extend [-visibility 30s] MESSAGE_ID RECEIPT
  dead list [-limit 100]
  dead replay [-delay 5s] MESSAGE_ID
  stats`)
}
