package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/mgh3326/panewire/internal/herdrpanes"
)

// A trial (hk 4258, docs/claude-session-message.md): for the lanes named in
// PANEWIRE_CLAUDE_SESSION_MESSAGE_LANES, the node delivers a lane's inbox wake
// line to the lane's Claude Code session as a session message instead of typing
// it into the pane. The message arrives marked as coming from another session,
// is read between tool calls, starts a turn in an idle session, and never
// touches the input box. The hub inbox stays the record. A missing or stale
// registration, or a failed post, falls back to the typed line.
//
// The hub's wire is unchanged: the wake line is the plain relay.inject the hub
// already sends, recognised on the node by its transport id and exact text.

const claudeSessionMessageLanesEnv = "PANEWIRE_CLAUDE_SESSION_MESSAGE_LANES"

// claudeSessionPostTimeout bounds connect, write and the wait for the session
// to close its end.
const claudeSessionPostTimeout = 5 * time.Second

// postClaudeWake posts the wake line to the lane's registered Claude Code
// session. The hook registers under the same default root as panewire emit, so
// a daemon started without --inbox-root or --hub-jobs-root reads it there.
func (client *HubClient) postClaudeWake(ctx context.Context, lane string, message hubOutboundMessage) error {
	registration, err := readClaudeSocketRegistrationRecord(emitInboxRoot(client.jobsInboxRoot), lane, message.Pane)
	if err != nil {
		return err
	}
	if err := client.verifyClaudeSession(ctx, message.Pane, registration); err != nil {
		return err
	}
	return postClaudeSessionMessage(ctx, registration.Socket, "panewire-hub: "+message.Text)
}

// verifyClaudeSession proves that the registered session is the live session
// running in pane. If the registration has no session id, if herdr cannot tell
// the live session, or if the live session does not match, it returns errSessionStale.
func (client *HubClient) verifyClaudeSession(ctx context.Context, pane string, registration sessionRegistration) error {
	if registration.Session == "" {
		return errSessionStale
	}
	liveSession, err := client.paneLiveAgentSession(ctx, pane)
	if err != nil || liveSession == "" || liveSession != registration.Session {
		return errSessionStale
	}
	return nil
}

// paneLiveAgentSession queries herdr for the live agent_session value in pane.
// It returns ("", nil) when herdr reports no agent_session for the pane.
func (client *HubClient) paneLiveAgentSession(ctx context.Context, pane string) (string, error) {
	if client == nil {
		return "", errors.New("nil client")
	}
	runner := client.nodeHerdrRunnerFor("", pane)
	if runner == nil {
		return "", errors.New("no herdr runner")
	}
	raw, err := herdrpanes.List(ctx, runner, func(out []byte) error {
		var dummy json.RawMessage
		return json.Unmarshal(out, &dummy)
	})
	if err != nil {
		return "", err
	}
	return parsePaneAgentSession(raw, pane)
}

type herdrPaneEntry struct {
	PaneID       string          `json:"pane_id"`
	AgentSession json.RawMessage `json:"agent_session"`
}

func parsePaneAgentSession(raw []byte, pane string) (string, error) {
	var envelope struct {
		Result struct {
			Panes  []herdrPaneEntry `json:"panes"`
			Agents []herdrPaneEntry `json:"agents"`
		} `json:"result"`
		Panes  []herdrPaneEntry `json:"panes"`
		Agents []herdrPaneEntry `json:"agents"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", err
	}
	panes := envelope.Result.Panes
	if len(panes) == 0 {
		panes = envelope.Panes
	}
	if len(panes) == 0 {
		panes = envelope.Result.Agents
	}
	if len(panes) == 0 {
		panes = envelope.Agents
	}
	for _, p := range panes {
		if p.PaneID == pane {
			return parseAgentSessionValue(p.AgentSession), nil
		}
	}
	return "", nil
}

func parseAgentSessionValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil && s != "" {
		return s
	}
	var obj struct {
		Value string `json:"value"`
	}
	if json.Unmarshal(raw, &obj) == nil {
		return obj.Value
	}
	return ""
}

// claudeSessionLine is one line of a Claude Code inbox socket: the user-turn
// form its own `--messaging-socket-path` banner documents. No auth line is
// sent; see claude_session_socket.go.
type claudeSessionLine struct {
	Type    string               `json:"type"`
	Message claudeSessionContent `json:"message"`
}

type claudeSessionContent struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// postClaudeSessionMessage writes one message to the socket, half-closes, and
// waits briefly for the session to close its end (a live session does once it
// has read the line). Success means the line was completely written to the
// socket: neither that the session read it nor that it was delivered to Claude,
// which the session decides by its own inbound rules. A session that holds the
// connection open past the wait still counts as written.
func postClaudeSessionMessage(ctx context.Context, socket, text string) error {
	if err := validSessionSocketPath(socket); err != nil {
		return err
	}
	body, err := json.Marshal(claudeSessionLine{Type: "user", Message: claudeSessionContent{Role: "user", Content: text}})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, claudeSessionPostTimeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if _, err := conn.Write(append(body, '\n')); err != nil {
		return err
	}
	if unix, ok := conn.(*net.UnixConn); ok {
		if err := unix.CloseWrite(); err != nil {
			return err
		}
	}
	// A reply is not part of the protocol; EOF is the session finishing with the
	// line. A timeout here still means the line was written.
	if _, err := io.Copy(io.Discard, conn); err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("session closed the connection: %w", err)
	}
	return nil
}
