package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

type inboxRequest struct {
	Command string        `json:"command"`
	Lane    string        `json:"lane,omitempty"`
	ID      int64         `json:"id,omitempty"`
	Unread  bool          `json:"unread,omitempty"`
	All     bool          `json:"all,omitempty"`
	JSON    bool          `json:"json,omitempty"`
	Timeout time.Duration `json:"timeout"`
}

type inboxResult struct {
	Code   int    `json:"code"`
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

func runInboxLocal(req inboxRequest, stdout, stderr io.Writer, socket string) int {
	if socket == "" {
		socket = socketPathFromEnv()
	}
	conn, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		fmt.Fprintln(stderr, "inbox: daemon unavailable")
		return ExitDaemonUnavailable
	}
	defer conn.Close()
	timeout := 15 * time.Second
	if req.Command == "wait" {
		timeout = req.Timeout
		if timeout < time.Duration(1<<63-1)-15*time.Second {
			timeout += 15 * time.Second
		}
	}
	timer := time.AfterFunc(timeout, func() { _ = conn.Close() })
	defer timer.Stop()
	ms := timeout.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	if err := json.NewEncoder(conn).Encode(localRequest{Op: "inbox", Inbox: &req, TimeoutMS: ms}); err != nil {
		fmt.Fprintln(stderr, "inbox: daemon unavailable")
		return ExitDaemonUnavailable
	}
	scan := bufio.NewScanner(conn)
	scan.Buffer(make([]byte, 4096), 64<<20)
	if !scan.Scan() {
		fmt.Fprintln(stderr, "inbox: daemon unavailable")
		return ExitDaemonUnavailable
	}
	var resp struct {
		OK     bool        `json:"ok"`
		Code   int         `json:"code"`
		Error  string      `json:"error"`
		Result inboxResult `json:"result"`
	}
	if json.Unmarshal(scan.Bytes(), &resp) != nil {
		fmt.Fprintln(stderr, "inbox: invalid daemon response")
		return ExitInternal
	}
	if !resp.OK {
		fmt.Fprintln(stderr, resp.Error)
		return resp.Code
	}
	_, _ = io.WriteString(stdout, resp.Result.Stdout)
	_, _ = io.WriteString(stderr, resp.Result.Stderr)
	return resp.Result.Code
}

func (d *Daemon) handleInbox(ctx context.Context, req *inboxRequest) (any, error) {
	if d.cfg.Hub.Client == nil {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("inbox: daemon has no hub client")}
	}
	if req == nil || req.Timeout <= 0 || (req.Lane != "" && !validReportRelayLaneName(req.Lane)) ||
		(req.Command != "list" && req.Command != "read" && req.Command != "ack" && req.Command != "count" && req.Command != "wait") ||
		(req.Unread && req.Command != "list") || (req.All && req.Command != "ack") ||
		((req.Command == "read" || req.Command == "ack") && !req.All && req.ID <= 0) ||
		((req.Command == "list" || req.Command == "count" || req.Command == "wait" || req.All) && req.ID != 0) ||
		(req.Command == "wait" && req.Lane == "") {
		return nil, &codedError{ExitUsage, errors.New("inbox: invalid request")}
	}
	var out, errOut bytes.Buffer
	client := inboxNodeClient{ctx: ctx, client: d.cfg.Hub.Client}
	code := executeInbox(client, &out, &errOut, *req, false)
	return inboxResult{Code: code, Stdout: out.String(), Stderr: errOut.String()}, nil
}

type inboxNodeClient struct {
	ctx    context.Context
	client *HubClient
}

func (c inboxNodeClient) do(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	var payload []byte
	if body != nil {
		payload, _ = io.ReadAll(body)
	}
	var last error
	for _, endpoint := range c.client.endpoints {
		base, err := url.Parse(endpoint)
		if err != nil {
			last = err
			continue
		}
		if base.Scheme == "wss" {
			base.Scheme = "https"
		} else {
			base.Scheme = "http"
		}
		base.Path = ""
		hc := &hubOperatorClient{base: base, client: &http.Client{Transport: hubGuardedTransport{base: http.DefaultTransport}, CheckRedirect: func(*http.Request, []*http.Request) error { return errHubOperatorRedirect }}, token: c.client.token, machineID: c.client.machineID, cf: hubCFAccessEnv{ClientID: c.client.cfAccessClientID, ClientSecret: c.client.cfAccessSecret}}
		callCtx, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(c.ctx, cancel)
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(payload)
		}
		response, err := hc.do(callCtx, method, path, query, reader)
		if err == nil {
			response.Body = &inboxResponseBody{ReadCloser: response.Body, cancel: cancel, stop: stop}
			return response, nil
		}
		stop()
		cancel()
		last = err
		if c.ctx.Err() != nil || ctx.Err() != nil {
			break
		}
	}
	if last == nil {
		last = errors.New("hub unavailable")
	}
	return nil, last
}

type inboxResponseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	stop   func() bool
}

func (b *inboxResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.stop()
	b.cancel()
	return err
}
