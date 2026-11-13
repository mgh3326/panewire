package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"regexp"
	"strings"
	"time"
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

// claudeWakeCounts is the variable part of the wake line between a lane's name
// and its list command (inboxWakeText).
var claudeWakeCounts = regexp.MustCompile(`^\d+ unread \(most urgent: [a-z-]+\)$`)

const claudeSessionMessageLanesEnv = "PANEWIRE_CLAUDE_SESSION_MESSAGE_LANES"

// claudeSessionPostTimeout bounds connect, write and the wait for the session
// to close its end.
const claudeSessionPostTimeout = 5 * time.Second

// claudeSessionMessageLanesFromEnv is the trial's switch. Unset or empty is
// off; an entry that is not a lane label is ignored.
func claudeSessionMessageLanesFromEnv() []string {
	var lanes []string
	for _, lane := range strings.Split(os.Getenv(claudeSessionMessageLanesEnv), ",") {
		if lane = strings.TrimSpace(lane); LaneNamePattern.MatchString(lane) {
			lanes = append(lanes, lane)
		}
	}
	return lanes
}

// The delivery stages the node reports for a session-message wake. Only the
// first is claimed: a post that wrote the line to the session's socket. Claude
// Code gives the poster no acknowledgement, and a message its receiving session
// holds for approval, drops (rate limit, duplicate, full queue) or never reads
// looks the same to the node (docs/claude-session-message.md).
const (
	claudeSessionStageReachedSocket = "reached_socket"
	claudeSessionStageFellBack      = "fell_back_to_typed"
)

// claudeWakeLane returns the configured lane a plain inject is the inbox wake
// line of, or "". The line is matched twice: the transport id the hub derives
// from the lane, and the exact line text for that lane. The desk's "no pane"
// line for a paneless design lane shares the lane's transport id but not its
// text, so it is never posted to the lane's own session.
func (client *HubClient) claudeWakeLane(message hubOutboundMessage) string {
	if message.Lane != "" || message.EventID != 0 {
		return ""
	}
	for _, lane := range client.relayControl.sessionMessageLanes {
		if message.JobID != laneEventTransportID(lane, "inbox-wake") {
			continue
		}
		head, tail := "[inbox] "+lane+": ", " | panewire inbox list --lane "+lane
		if middle, ok := strings.CutSuffix(strings.TrimPrefix(message.Text, head), tail); ok && strings.HasPrefix(message.Text, head) && claudeWakeCounts.MatchString(middle) {
			return lane
		}
	}
	return ""
}

// deliverWakeAsSessionMessage posts the lane's wake line to its registered
// Claude Code session. It reports true when the line reached the socket; false
// leaves the caller to type the line as before.
func (client *HubClient) deliverWakeAsSessionMessage(ctx context.Context, message hubOutboundMessage) bool {
	lane := client.claudeWakeLane(message)
	if lane == "" {
		return false
	}
	// The hook registers under the same default root as panewire emit, so a
	// daemon started without --inbox-root or --hub-jobs-root reads it there.
	socket, err := readClaudeSocketRegistration(emitInboxRoot(client.jobsInboxRoot), lane, message.Pane)
	if err == nil {
		err = postClaudeSessionMessage(ctx, socket, "panewire-hub: "+message.Text)
	}
	if err != nil {
		// The socket path and the text stay out of the log.
		slog.Warn("inbox wake session message failed; typing the line", "lane", lane, "stage", claudeSessionStageFellBack, "reason", claudeSessionFailureReason(err))
		return false
	}
	slog.Info("inbox wake posted as a session message", "lane", lane, "stage", claudeSessionStageReachedSocket)
	return true
}

func claudeSessionFailureReason(err error) string {
	switch {
	case errors.Is(err, errClaudeSocketMissing):
		return "not_registered"
	case errors.Is(err, errClaudeSocketOtherPane):
		return "other_pane"
	case errors.Is(err, errClaudeSocketInvalid):
		return "registration_invalid"
	case errors.Is(err, os.ErrNotExist):
		return "socket_gone"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "post_failed"
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
	if err := validClaudeSocketPath(socket); err != nil {
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
