package panewire

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Inbox-only lanes (hk 3602). A design or desk lane -- and a worker lane in
// the relay.worker_inbox_trial -- never has a notification typed into its
// pane: receiveInbox files it as an unread inbox item, and this file types one
// line instead, coalesced per lane:
//
//	[inbox] <lane>: N unread (most urgent: <kind>) | panewire inbox list --lane <lane>
//
// The first wake-worthy arrival is announced at once and opens a window
// (relay.inbox_wake_window, default 5m). Later arrivals inside the window
// produce at most one more line, at the window's end, and only while the lane
// still has unread items; that line opens the next window, and a window end
// that finds nothing unread closes it. A line owed to a worker lane whose
// trial has ended is dropped. The hub owns the
// decision because it owns the inbox store and sees each arrival; the line
// travels to the lane's node like any relay and the node daemon types it (for a
// lane in PANEWIRE_CLAUDE_SESSION_MESSAGE_LANES it posts the line to the lane's
// Claude Code session first: docs/claude-session-message.md).
// The line is a plain, unidentified inject: it writes no handoffkeep row, so
// nothing replays it after a hub restart, and the window state is in memory:
// a hub start owes each inbox-only lane with unread items above info one line
// instead (seedStartupInboxWakes). See docs/quiet-delivery.md#inbox-only-lanes.

// inboxWakeUrgency is the closed urgency order of the line's "most urgent"
// field, least urgent first. info counts toward N but never wakes by itself.
var inboxWakeUrgency = []string{"info", "ready", "findings", "automation-failed", "blocked", "decision"}

// inboxWakeTypeKinds maps a declared notification type to its urgency kind.
// Every type not named here (merged, closed, progress, report, ack, info,
// idle-wake) is info.
var inboxWakeTypeKinds = map[string]string{
	"decision": "decision", "order": "decision", "deploy-approval": "decision",
	"blocked": "blocked", "watch-hold": "blocked", "culprit-hold": "blocked",
	"automation-failed": "automation-failed",
	"findings":          "findings",
	"ready":             "ready",
}

var (
	inboxWakeDecisionTags = regexp.MustCompile(`(?i)\b(?:ESC|URGENT|needs_decision)\b`)
	inboxWakeBlockedTags  = regexp.MustCompile(`(?i)\b(?:BLOCKER|BLOCKED|BOUNCE)\b`)
	inboxWakeFailedTags   = regexp.MustCompile(`\bSPAWN_FAILED\b`)
	inboxWakeReadyTags    = regexp.MustCompile(`(?i)\bREADY\b`)
)

// inboxWakeKind is an item's urgency kind: its declared type, else its event
// kind, else the action tags of untyped text. A SPAWN_FAILED notice counts as
// automation-failed whether or not it declares the type.
func inboxWakeKind(kind string, e hubJobEventPayload) string {
	if k, ok := inboxWakeTypeKinds[e.NotifyType]; ok {
		return k
	}
	if e.NotifyType != "" {
		return "info"
	}
	switch kind {
	case "job.escalate":
		return "decision"
	case "job.lost", "job.revoked":
		return "blocked"
	case "job.completed", "job.joined":
		return "ready"
	}
	if strings.HasPrefix(e.Text, consultDoneMarker) {
		return "ready"
	}
	text := deliveryEventText(e)
	switch {
	case inboxWakeDecisionTags.MatchString(text):
		return "decision"
	case inboxWakeBlockedTags.MatchString(text):
		return "blocked"
	case inboxWakeFailedTags.MatchString(text):
		return "automation-failed"
	case inboxWakeReadyTags.MatchString(text):
		return "ready"
	}
	return "info"
}

func inboxWakeRank(kind string) int {
	for rank, k := range inboxWakeUrgency {
		if k == kind {
			return rank
		}
	}
	return 0
}

// inboxOnlyExempt names the only traffic the inbox-only rule never touches:
// an operator chat turn the hub's own chat created (not a lane notification)
// and an operator HTTP ingress event (R24, the operator's own path into a
// pane). Both are private flags the hub sets where it creates the event; an
// event id that merely looks like chat-N earns nothing. The hub's digest and
// hold batches are not exempt: flushInbox files them (fileInboxOnlyBatch).
func inboxOnlyExempt(e hubJobEventPayload) bool {
	return e.operatorIngress
}

// inboxWakeText is the wake line. N comes from a successful count read only.
func inboxWakeText(lane string, unread int, mostUrgent string) string {
	return fmt.Sprintf("[inbox] %s: %d unread (most urgent: %s) | panewire inbox list --lane %s", lane, unread, mostUrgent, lane)
}

type inboxWakeLane struct {
	// windowEnd closes the lane's current coalescing window; zero or past
	// means the next wake-worthy arrival is announced at once.
	windowEnd time.Time
	// pending is set by a wake-worthy arrival inside the window, by a wake
	// that could not be sent, or by a hub start, and asks for one line at
	// windowEnd.
	pending bool
	class   string
	// startup marks the wake a hub start owes the lane (reason startup).
	startup bool
	// unrouted is set when the last attempt found the lane's pane out of
	// reach; a node hello then retries at once (retryUnroutedInboxWakes).
	unrouted bool
}

type inboxWakes struct {
	// mu serializes the window state and every send, so a lane never gets
	// two lines for one window.
	mu    sync.Mutex
	lanes map[string]*inboxWakeLane
}

// inboxWakeOutcome is what one attempt at a lane's line came to.
type inboxWakeOutcome int

const (
	inboxWakeSent inboxWakeOutcome = iota
	// inboxWakeNothing: the lane has nothing unread, so no line is owed.
	inboxWakeNothing
	// inboxWakeCountFailed and inboxWakeUnrouted leave the line owed.
	inboxWakeCountFailed
	inboxWakeUnrouted
)

// noteInboxWakeArrival records a fresh wake-worthy item for an inbox-only
// lane: the first one sends the line now and opens the window; one inside an
// open window is left for the window's end.
func (h *HubServer) noteInboxWakeArrival(lane, class string) {
	w := &h.inbox.wakes
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.lanes == nil {
		w.lanes = map[string]*inboxWakeLane{}
	}
	now := h.now()
	state := w.lanes[lane]
	if state == nil {
		state = &inboxWakeLane{}
		w.lanes[lane] = state
	}
	state.class, state.startup = class, false
	if now.Before(state.windowEnd) {
		state.pending = true
		return
	}
	h.attemptInboxWake(lane, state, now)
}

// attemptInboxWake sends the lane's line now. A sent line opens the window;
// a lane with nothing unread owes nothing, so its window closes and the next
// arrival is announced at once; a line that could not be sent stays owed at
// the next window's end. The caller holds the wake mutex.
func (h *HubServer) attemptInboxWake(lane string, state *inboxWakeLane, now time.Time) {
	outcome := h.sendInboxWake(lane, state.class, state.startup)
	if outcome == inboxWakeNothing {
		delete(h.inbox.wakes.lanes, lane)
		return
	}
	state.windowEnd = now.Add(h.inbox.policy.Relay.wakeWindow)
	state.pending = outcome != inboxWakeSent
	state.unrouted = outcome == inboxWakeUnrouted
	if outcome == inboxWakeSent {
		state.startup = false
	}
}

// flushInboxWakes sends the window-end line of every lane whose window has
// closed with arrivals still owed one. The lane's class is taken again at
// the flush: a worker lane whose inbox trial has ended since is a worker lane
// again, and its owed line is dropped. runInbox calls it on every tick.
func (h *HubServer) flushInboxWakes() {
	if h.inbox == nil {
		return
	}
	w := &h.inbox.wakes
	w.mu.Lock()
	defer w.mu.Unlock()
	now := h.now()
	for lane, state := range w.lanes {
		if now.Before(state.windowEnd) {
			continue
		}
		class := h.inbox.policy.inboxOnlyClass(lane, now)
		if !state.pending || class == "" {
			delete(w.lanes, lane)
			continue
		}
		state.class = class
		h.attemptInboxWake(lane, state, now)
	}
}

// seedStartupInboxWakes owes one line to every inbox-only lane that has unread
// items above info when the hub starts: the window state is in memory, so
// without it an item filed before a restart would never be announced. The
// line is sent on the first flush, through the normal window, so repeated
// restarts give one line per start at most. A lane whose unread list cannot
// be read is skipped, never announced as empty.
func (h *HubServer) seedStartupInboxWakes() {
	lanes, err := h.inbox.store.laneNames()
	if err != nil {
		h.logger.Warn("startup inbox wakes skipped: lanes unreadable", "error", h.inbox.store.loggable("startup inbox wake", err))
		return
	}
	now := h.now()
	w := &h.inbox.wakes
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, lane := range lanes {
		class := h.inbox.policy.inboxOnlyClass(lane, now)
		if class == "" {
			continue
		}
		items, err := h.inbox.store.list(lane, true)
		if err != nil {
			h.logger.Warn("startup inbox wake skipped: unread items unreadable", "lane", lane, "error", h.inbox.store.loggable("startup inbox wake", err))
			continue
		}
		if inboxWakeMostUrgent(items) == "info" {
			continue
		}
		if w.lanes == nil {
			w.lanes = map[string]*inboxWakeLane{}
		}
		w.lanes[lane] = &inboxWakeLane{
			windowEnd: now.Add(h.inbox.policy.Relay.wakeWindow),
			pending:   true,
			class:     class,
			startup:   true,
		}
	}
}

// retryUnroutedInboxWakes makes every line owed only because its pane was out
// of reach due at the next flush. A node hello calls it.
func (h *HubServer) retryUnroutedInboxWakes() {
	if h.inbox == nil {
		return
	}
	w := &h.inbox.wakes
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, state := range w.lanes {
		if state.pending && state.unrouted {
			state.windowEnd = time.Time{}
		}
	}
}

func inboxWakeMostUrgent(items []laneInboxItem) string {
	most := "info"
	for _, item := range items {
		if k := inboxWakeKind(item.Kind, item.Event); inboxWakeRank(k) > inboxWakeRank(most) {
			most = k
		}
	}
	return most
}

// sendInboxWake reads the lane's unread count and its most urgent unread
// item and types the line into the lane's pane. A failed read never becomes
// "0 unread": the line is skipped and stays owed. A lane with nothing unread
// owes nothing. The caller holds the wake mutex.
func (h *HubServer) sendInboxWake(lane, class string, startup bool) inboxWakeOutcome {
	counts, err := h.inbox.store.counts(lane)
	var items []laneInboxItem
	if err == nil {
		items, err = h.inbox.store.list(lane, true)
	}
	if err != nil {
		h.logger.Warn("inbox wake skipped: unread count unavailable; retrying at the window's end", "lane", lane, "error", h.inbox.store.loggable("inbox wake count", err))
		h.emitFleet(fleetEvent{Kind: "inbox_wake", Job: lane, Outcome: "skipped", Reason: "count_failed", Detail: "lane=" + lane + " class=" + class})
		return inboxWakeCountFailed
	}
	unread := counts[lane]
	if unread == 0 {
		return inboxWakeNothing
	}
	most := inboxWakeMostUrgent(items)
	route, agent, routed := h.resolveRelayRoute("lane.event", hubJobEventPayload{OwnerLane: lane})
	route, agent, routed, text, deskFallback := h.inboxWakeDestination(lane, class, route, agent, routed, unread, most)
	detail := fmt.Sprintf("lane=%s class=%s unread=%d most_urgent=%s machine=%s pane=%s", lane, class, unread, most, route.Machine, route.Pane)
	if deskFallback {
		detail = fmt.Sprintf("lane=%s class=%s unread=%d most_urgent=%s desk=%s machine=%s pane=%s", lane, class, unread, most, deskPolicyLane, route.Machine, route.Pane)
		if startup {
			detail += " startup=true"
		}
	}
	if !routed || route.Sink || route.Pane == "" || !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: laneEventTransportID(lane, "inbox-wake"), Pane: route.Pane, Text: text}) {
		h.logger.Info("inbox wake not sent: lane pane unreachable; retrying at the window's end or the next node hello", "lane", lane)
		h.emitFleet(fleetEvent{Kind: "inbox_wake", Job: lane, Outcome: "skipped", Reason: "unrouted", Detail: detail})
		return inboxWakeUnrouted
	}
	reason := class
	if startup {
		reason = "startup"
	}
	if deskFallback {
		reason = "desk_fallback"
	}
	h.emitFleet(fleetEvent{Kind: "inbox_wake", Job: lane, Outcome: "sent", Reason: reason, Detail: detail})
	return inboxWakeSent
}

// inboxWakeWorthy reports whether an item filed in an inbox-only lane asks for
// a wake: before hk 3602 it would have been typed into the pane (its class
// was interrupt and designQuiet had not kept it), and it is not a declared
// info record (merged, closed, progress, report, ack, info, idle-wake: SPAWNED
// and MERGED notices). Untyped text that would have been typed still wakes,
// ranked by its tags: the hub cannot tell it is routine.
func inboxWakeWorthy(class string, quiet bool, kind string, e hubJobEventPayload) bool {
	return class == "interrupt" && !quiet && (e.NotifyType == "" || inboxWakeKind(kind, e) != "info")
}
