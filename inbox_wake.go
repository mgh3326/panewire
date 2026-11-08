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
// still has unread items; that line opens the next window. The hub owns the
// decision because it owns the inbox store and sees each arrival; the line
// travels to the lane's node like any relay and the node daemon types it.
// The line is a plain, unidentified inject: it writes no handoffkeep row, so
// nothing replays it after a hub restart, and the window state is in memory.
// See docs/quiet-delivery.md#inbox-only-lanes.

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

// inboxOnlyExempt names the traffic the inbox-only rule never touches: an
// operator chat turn (not a lane notification), an operator HTTP ingress
// event (R24, the operator's own path into a pane), and the hub's own digest
// and hold batches.
func inboxOnlyExempt(e hubJobEventPayload) bool {
	return e.operatorIngress || e.noDigest || chatRelayEventPattern.MatchString(e.EventID)
}

// inboxWakeText is the wake line. N comes from a successful count read only.
func inboxWakeText(lane string, unread int, mostUrgent string) string {
	return fmt.Sprintf("[inbox] %s: %d unread (most urgent: %s) | panewire inbox list --lane %s", lane, unread, mostUrgent, lane)
}

type inboxWakeLane struct {
	// windowEnd closes the lane's current coalescing window; zero or past
	// means the next wake-worthy arrival is announced at once.
	windowEnd time.Time
	// pending is set by a wake-worthy arrival inside the window, or by a wake
	// that could not be sent, and asks for one line at windowEnd.
	pending bool
	class   string
}

type inboxWakes struct {
	// mu serializes the window state and every send, so a lane never gets
	// two lines for one window.
	mu    sync.Mutex
	lanes map[string]*inboxWakeLane
}

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
	state.class = class
	if now.Before(state.windowEnd) {
		state.pending = true
		return
	}
	state.windowEnd = now.Add(h.inbox.policy.Relay.wakeWindow)
	state.pending = !h.sendInboxWake(lane, class)
}

// flushInboxWakes sends the window-end line of every lane whose window has
// closed with arrivals still owed one. runInbox calls it on every tick.
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
		if !state.pending {
			delete(w.lanes, lane)
			continue
		}
		state.windowEnd = now.Add(h.inbox.policy.Relay.wakeWindow)
		state.pending = !h.sendInboxWake(lane, state.class)
	}
}

// sendInboxWake reads the lane's unread count and its most urgent unread
// item and types the line into the lane's pane. It reports false when the
// wake is still owed: the count could not be read, or the lane's pane cannot
// be reached. A failed read never becomes "0 unread": the line is skipped and
// retried at the next window's end. A lane with nothing unread owes nothing.
// The caller holds the wake mutex.
func (h *HubServer) sendInboxWake(lane, class string) bool {
	counts, err := h.inbox.store.counts(lane)
	var items []laneInboxItem
	if err == nil {
		items, err = h.inbox.store.list(lane, true)
	}
	if err != nil {
		h.logger.Warn("inbox wake skipped: unread count unavailable; retrying at the window's end", "lane", lane, "error", h.inbox.store.loggable("inbox wake count", err))
		h.emitFleet(fleetEvent{Kind: "inbox_wake", Job: lane, Outcome: "skipped", Reason: "count_failed", Detail: "lane=" + lane + " class=" + class})
		return false
	}
	unread := counts[lane]
	if unread == 0 {
		return true
	}
	most := "info"
	for _, item := range items {
		if k := inboxWakeKind(item.Kind, item.Event); inboxWakeRank(k) > inboxWakeRank(most) {
			most = k
		}
	}
	route, agent, routed := h.resolveRelayRoute("lane.event", hubJobEventPayload{OwnerLane: lane})
	text := inboxWakeText(lane, unread, most)
	detail := fmt.Sprintf("lane=%s class=%s unread=%d most_urgent=%s machine=%s pane=%s", lane, class, unread, most, route.Machine, route.Pane)
	if !routed || route.Sink || route.Pane == "" || !agent.queueRelay(hubRelayInjectEvent{Type: "relay.inject", JobID: laneEventTransportID(lane, "inbox-wake"), Pane: route.Pane, Text: text}) {
		h.logger.Info("inbox wake not sent: lane pane unreachable; retrying at the window's end", "lane", lane)
		h.emitFleet(fleetEvent{Kind: "inbox_wake", Job: lane, Outcome: "skipped", Reason: "unrouted", Detail: detail})
		return false
	}
	h.emitFleet(fleetEvent{Kind: "inbox_wake", Job: lane, Outcome: "sent", Reason: class, Detail: detail})
	return true
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
