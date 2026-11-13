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
	"time"

	"github.com/mgh3326/panewire/internal/hubretry"
)

type inboxRequest struct {
	Command string        `json:"command"`
	Lane    string        `json:"lane,omitempty"`
	ID      int64         `json:"id,omitempty"`
	Unread  bool          `json:"unread,omitempty"`
	All     bool          `json:"all,omitempty"`
	JSON    bool          `json:"json,omitempty"`
	Timeout time.Duration `json:"timeout"`
	// EventIDs are the ids a status request asks about; no other command
	// carries them.
	EventIDs []string `json:"event_ids,omitempty"`
}

// localTextResult is a daemon op's rendered answer: the same text, streams and
// exit status the CLI would have printed itself.
type localTextResult struct {
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
		OK     bool            `json:"ok"`
		Code   int             `json:"code"`
		Error  string          `json:"error"`
		Result localTextResult `json:"result"`
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

// validInboxStatusEventIDs reports whether the event ids fit the command:
// status needs 1..hubInboxAckStateMaxIDs valid ids, every other command none.
func validInboxStatusEventIDs(command string, eventIDs []string) bool {
	if command != "status" {
		return len(eventIDs) == 0
	}
	if len(eventIDs) == 0 || len(eventIDs) > hubInboxAckStateMaxIDs {
		return false
	}
	for _, eventID := range eventIDs {
		if !validLaneEventID(eventID) {
			return false
		}
	}
	return true
}

func (d *Daemon) handleInbox(ctx context.Context, req *inboxRequest) (any, error) {
	if d.cfg.Hub.Client == nil {
		return nil, &codedError{ExitDaemonUnavailable, errors.New("inbox: daemon has no hub client")}
	}
	if req == nil || req.Timeout <= 0 || (req.Lane != "" && !validHistoricalLaneResource(req.Lane)) ||
		(req.Command != "list" && req.Command != "read" && req.Command != "ack" && req.Command != "count" && req.Command != "wait" && req.Command != "status") ||
		!validInboxStatusEventIDs(req.Command, req.EventIDs) ||
		(req.Unread && req.Command != "list") || (req.All && req.Command != "ack") ||
		((req.Command == "read" || req.Command == "ack") && !req.All && req.ID <= 0) ||
		((req.Command == "list" || req.Command == "count" || req.Command == "wait" || req.All) && req.ID != 0) ||
		(req.Command == "wait" && req.Lane == "") ||
		(req.Command == "status" && req.Lane != "") {
		return nil, &codedError{ExitUsage, errors.New("inbox: invalid request")}
	}
	var out, errOut bytes.Buffer
	// executeInbox owns this command's retries, so the stderr line it prints
	// reaches the CLI that asked; the node client sends each call once.
	client := nodeHubClient{ctx: ctx, client: d.cfg.Hub.Client, retry: hubretry.Policy{Budget: -1}}
	code := executeInbox(client, &out, &errOut, *req, false, hubretry.Policy{})
	return localTextResult{Code: code, Stdout: out.String(), Stderr: errOut.String()}, nil
}
