package panewire

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"regexp"
	"strings"
)

// Session-message receivers (hk 4258 Claude, hk 4261 codex and omp;
// docs/claude-session-message.md, docs/session-message-receivers.md). For the
// lanes a harness's trial switch names, the node delivers the lane's inbox wake
// line to the lane's own session instead of typing it into the pane. The hub's
// wire is unchanged: the wake line is the plain relay.inject the hub already
// sends, recognised on the node by its transport id and exact text. A lane is
// served by the first harness whose list names it (claude, codex, omp), so one
// line is never delivered twice. A missing or stale registration, or a failed
// post, falls back to the typed line.

const (
	sessionHarnessClaude = "claude"
	sessionHarnessCodex  = "codex"
	sessionHarnessOmp    = "omp"
)

// sessionMessageLanes are the trial switches: the lanes whose wake line goes to
// the lane's session as a message, per harness. Empty is off.
type sessionMessageLanes struct {
	claude, codex, omp []string
}

func sessionMessageLanesFromEnv() sessionMessageLanes {
	return sessionMessageLanes{
		claude: laneListFromEnv(claudeSessionMessageLanesEnv),
		codex:  laneListFromEnv(codexSessionMessageLanesEnv),
		omp:    laneListFromEnv(ompSessionMessageLanesEnv),
	}
}

// laneListFromEnv reads a comma separated lane list. Unset or empty is off; an
// entry that is not a lane label is ignored.
func laneListFromEnv(name string) []string {
	var lanes []string
	for _, lane := range strings.Split(os.Getenv(name), ",") {
		if lane = strings.TrimSpace(lane); validLaneReference(lane) {
			lanes = append(lanes, lane)
		}
	}
	return lanes
}

// The delivery stages the node reports for a session-message wake, per
// harness. Each names how far the line is known to have got, no further:
//   - claude: reached_socket. The line was completely written to the session's
//     socket; Claude Code gives the poster no acknowledgement.
//   - codex: queued. `codex queue` accepted the line into the thread's queue
//     and exited 0.
//   - omp: acked. The panewire extension in the session answered that it passed
//     the line to pi.sendMessage.
//
// None says the model read the line. Each is logged by the node, not written to
// the hub inbox.
const (
	sessionStageReachedSocket = "reached_socket"
	sessionStageQueued        = "queued"
	sessionStageAcked         = "acked"
	sessionStageFellBack      = "fell_back_to_typed"
)

// wakeLineCounts is the variable part of the wake line between a lane's name
// and its list command (inboxWakeText).
var wakeLineCounts = regexp.MustCompile(`^\d+ unread \(most urgent: [a-z-]+\)$`)

// wakeLaneIn returns the lane in lanes a plain inject is the inbox wake line
// of, or "". The line is matched twice: the transport id the hub derives from
// the lane, and the exact line text for that lane. The desk's "no pane" line
// for a paneless design lane shares the lane's transport id but not its text,
// so it is never posted to the lane's own session.
func wakeLaneIn(message hubOutboundMessage, lanes []string) string {
	if message.Lane != "" || message.EventID != 0 {
		return ""
	}
	for _, lane := range lanes {
		if message.JobID != laneEventTransportID(lane, "inbox-wake") {
			continue
		}
		head, tail := "[inbox] "+lane+": ", " | panewire inbox list --lane "+lane
		if middle, ok := strings.CutSuffix(strings.TrimPrefix(message.Text, head), tail); ok && strings.HasPrefix(message.Text, head) && wakeLineCounts.MatchString(middle) {
			return lane
		}
	}
	return ""
}

// sessionWakeReceiver returns the harness and lane a plain inject is the wake
// line of, or "" when no receiver is configured for it.
func (client *HubClient) sessionWakeReceiver(message hubOutboundMessage) (harness, lane string) {
	lanes := client.relayControl.sessionMessageLanes
	for _, candidate := range []struct {
		harness string
		lanes   []string
	}{{sessionHarnessClaude, lanes.claude}, {sessionHarnessCodex, lanes.codex}, {sessionHarnessOmp, lanes.omp}} {
		if lane := wakeLaneIn(message, candidate.lanes); lane != "" {
			return candidate.harness, lane
		}
	}
	return "", ""
}

// deliverWakeAsSessionMessage posts the lane's wake line to its registered
// session. It reports true when the line reached the session as far as the
// harness's stage says; false leaves the caller to type the line as before.
func (client *HubClient) deliverWakeAsSessionMessage(ctx context.Context, message hubOutboundMessage) bool {
	harness, lane := client.sessionWakeReceiver(message)
	if harness == "" {
		return false
	}
	var (
		stage string
		err   error
	)
	switch harness {
	case sessionHarnessClaude:
		stage, err = sessionStageReachedSocket, client.postClaudeWake(ctx, lane, message)
	case sessionHarnessCodex:
		stage, err = sessionStageQueued, client.postCodexWake(ctx, lane, message)
	default:
		stage, err = sessionStageAcked, client.postOmpWake(ctx, lane, message)
	}
	if err != nil {
		// The socket path, the thread id and the text stay out of the log.
		slog.Warn("inbox wake session message failed; typing the line", "harness", harness, "lane", lane, "stage", sessionStageFellBack, "reason", sessionFailureReason(err))
		return false
	}
	slog.Info("inbox wake delivered as a session message", "harness", harness, "lane", lane, "stage", stage)
	return true
}

func sessionFailureReason(err error) string {
	switch {
	case errors.Is(err, errSessionNotRegistered):
		return "not_registered"
	case errors.Is(err, errSessionOtherPane):
		return "other_pane"
	case errors.Is(err, errSessionRegistrationInvalid):
		return "registration_invalid"
	case errors.Is(err, errSessionNoAck):
		return "no_ack"
	case errors.Is(err, errCodexQueueRejected):
		return "queue_failed"
	case errors.Is(err, os.ErrNotExist):
		return "socket_gone"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "post_failed"
}
