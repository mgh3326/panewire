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

	"github.com/mgh3326/panewire/internal/daemonop"
	"github.com/mgh3326/panewire/internal/hubretry"
)

// inboxRequest is the inbox op's payload, shared with the fleet spawn engine
// so its wait reads the lane inbox with the same request the CLI sends.
type inboxRequest = daemonop.Inbox

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
	// budget is the daemon's own request budget, sent as TimeoutMS; the CLI
	// holds the socket for longer so the daemon's answer still arrives when
	// that budget runs out (hk 4344).
	budget := inboxNodeRequestTimeout
	if req.Command == "wait" {
		// The daemon re-issues the long poll inside the CLI deadline, so it
		// needs a whole server bound beyond it.
		budget = req.Timeout
		if budget < time.Duration(1<<63-1)-15*time.Second {
			budget += 15 * time.Second
		}
	}
	// Clamp the sum so a huge --timeout cannot overflow the socket deadline
	// into a negative duration.
	socketDeadline := budget
	if socketDeadline <= time.Duration(1<<63-1)-inboxDaemonReplySlack {
		socketDeadline += inboxDaemonReplySlack
	}
	timer := time.AfterFunc(socketDeadline, func() { _ = conn.Close() })
	defer timer.Stop()
	ms := budget.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	if err := json.NewEncoder(conn).Encode(localRequest{Op: daemonop.InboxOp, Inbox: &req, TimeoutMS: ms}); err != nil {
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
	if req == nil || req.Timeout <= 0 || (req.Lane != "" && !validLaneReference(req.Lane)) ||
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
	code := executeInbox(client, &out, &errOut, *req, false, hubretry.Policy{}, inboxNodeRequestTimeout)
	return localTextResult{Code: code, Stdout: out.String(), Stderr: errOut.String()}, nil
}
