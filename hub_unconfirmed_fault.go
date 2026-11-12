package panewire

// Contract: docs/relay-harness-delivery.md, "Escalation" (hk 4270, decision 152).
//
// A delivery the node could not confirm used to reach the worker's design lane
// at once, one notice per drop. Thirteen of the design lanes' 173 interrupting
// items on 2026-11-12 were such notices, and six of them were one fault (a
// host whose panes could not be read). Now:
//
//   - A row where nothing was typed (pane_read_failed, working_wait_expired)
//     is delivered again after designQuietTimings.unconfirmedRetryAfter,
//     through the row's one labelled replay (hub_unconfirmed_once.go). A row
//     that may already be in the pane is not retyped.
//   - The host fault lane (panewire-design) is told once per (machine, reason)
//     window; the drops that follow are folded, and the window's end sends one
//     summary that names every folded row.
//   - The repository's design lane is told, as before, but only when the row
//     is still unconfirmed after designQuietTimings.unconfirmedUnreadAfter.
//
// The durable row stays in hk undelivered until the pane confirms it, so
// nothing depends on a notice. The memory below is lost on a restart; the
// startup replay then gives every marked row its one labelled replay.

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// relayReplaySourceRetry names the timed retry in replay logs and
	// broadcasts, beside the startup, hello and idle sources.
	relayReplaySourceRetry = "retry"
	// unconfirmedRowsMax bounds the rows the hub remembers as unconfirmed.
	unconfirmedRowsMax = 4096
	// hostFaultListMax bounds the rows one summary names.
	hostFaultListMax = 40
	hostFaultPrefix  = "host-fault-"
)

// unconfirmedRow is one delivery the node dropped as unconfirmed that no one
// has confirmed since.
type unconfirmedRow struct {
	lane    string
	retried bool
}

// retriableUnconfirmed names the drops that typed nothing: the pane read
// failed before the send, or the row was held and never typed.
func retriableUnconfirmed(reason string) bool {
	return reason == relayDroppedPaneReadFailed || reason == relayDroppedWorkingExpired
}

// noteUnconfirmedRow records the drop and reports whether it is the row's
// first. A row whose retry also dropped is not first.
func (h *HubServer) noteUnconfirmedRow(machine string, dropped relayDroppedPayload) bool {
	t := h.quiet.timings
	if t.unconfirmedUnreadAfter <= 0 && t.unconfirmedRetryAfter <= 0 {
		return true
	}
	h.quiet.mu.Lock()
	defer h.quiet.mu.Unlock()
	if _, seen := h.quiet.unconfirmed[dropped.OriginalEventID]; seen {
		return false
	}
	if h.quiet.unconfirmed == nil {
		h.quiet.unconfirmed = make(map[int64]unconfirmedRow)
	}
	if len(h.quiet.unconfirmed) < unconfirmedRowsMax {
		h.quiet.unconfirmed[dropped.OriginalEventID] = unconfirmedRow{lane: dropped.Lane}
	}
	return true
}

// confirmUnconfirmedRow clears a row the pane confirmed after all: a late
// relay.delivered, or the retry's own delivery.
func (h *HubServer) confirmUnconfirmedRow(id int64) {
	if id < 1 {
		return
	}
	h.quiet.mu.Lock()
	delete(h.quiet.unconfirmed, id)
	h.quiet.mu.Unlock()
}

func (h *HubServer) markUnconfirmedRetried(id int64) {
	h.quiet.mu.Lock()
	defer h.quiet.mu.Unlock()
	if row, ok := h.quiet.unconfirmed[id]; ok {
		row.retried = true
		h.quiet.unconfirmed[id] = row
	}
}

func (h *HubServer) scheduleUnconfirmedRetry(machine string, dropped relayDroppedPayload) {
	after := h.quiet.timings.unconfirmedRetryAfter
	if after <= 0 || !retriableUnconfirmed(dropped.Reason) {
		return
	}
	time.AfterFunc(after, func() { h.retryUnconfirmedRow(dropped) })
}

// retryUnconfirmedRow gives the row its one labelled replay. Every guard of
// replayMarkedUnconfirmed applies; the retry adds that the row is still
// undelivered and is a producer's lane.event.
func (h *HubServer) retryUnconfirmedRow(dropped relayDroppedPayload) {
	if h.closeCtx.Err() != nil {
		return
	}
	ctx := h.closeCtx
	record, found, err := h.handoffkeep.relayEvent(ctx, dropped.OriginalEventID)
	if err != nil || !found || record.DeliveredAt != "" || !escalatesWhenUnconfirmed(record) {
		return
	}
	// The original send's injection claim outlives the node's drop; that send
	// is over, so its claim is released and the replay takes it again.
	key := relayEventDedupeKey(record.Kind, relayEventFromRecord(record))
	h.mu.Lock()
	if h.relayDedupe[key] == record.ID {
		delete(h.relayDedupe, key)
	}
	h.mu.Unlock()
	h.markUnconfirmedRetried(record.ID)
	h.replayMarkedUnconfirmed(ctx, record, relayReplaySourceRetry, false)
}

// relayRetryLabel is the one-line prefix a timed retry carries.
func relayRetryLabel(record handoffkeepRelayEvent) string {
	stamp := "unknown-time"
	if when, ok := unconfirmedMarkFirstSent(record); ok {
		stamp = when.In(relayUnconfirmedLabelZone).Format("2006-01-02 15:04:05 MST")
	}
	return "[retry - first sent " + stamp + " - the first delivery could not be confirmed] "
}

func (h *HubServer) scheduleUnreadEscalation(machine string, dropped relayDroppedPayload) {
	time.AfterFunc(h.quiet.timings.unconfirmedUnreadAfter, func() { h.escalateUnreadRow(machine, dropped) })
}

// escalateUnreadRow tells the worker's design lane about a row that is still
// unconfirmed when its time is up.
func (h *HubServer) escalateUnreadRow(machine string, dropped relayDroppedPayload) {
	if h.closeCtx.Err() != nil {
		return
	}
	h.quiet.mu.Lock()
	row, unconfirmed := h.quiet.unconfirmed[dropped.OriginalEventID]
	delete(h.quiet.unconfirmed, dropped.OriginalEventID)
	h.quiet.mu.Unlock()
	if !unconfirmed {
		return // confirmed since
	}
	h.escalateUnconfirmedRelayAttempt(machine, dropped, 0, row.retried)
}

// noteHostFault tells the host fault lane about a drop: at once for the first
// drop of a (machine, reason) window, folded into the window's summary for the
// rest.
func (h *HubServer) noteHostFault(machine string, dropped relayDroppedPayload) {
	lane := h.quiet.timings.hostFaultLane
	window := h.quiet.timings.hostFaultWindow
	// A pane that stayed busy for the whole hold is no fault of its host.
	if lane == "" || window <= 0 || dropped.Reason == relayDroppedWorkingExpired {
		return
	}
	now := h.now()
	note, _, send := h.quiet.hostFaults.admitWithin(machine, dropped.Reason, dropped.OriginalEventID, now, window, "on this host")
	if !send {
		return
	}
	eventID := hostFaultPrefix + hostFaultToken(machine, dropped.Reason) + "-" + strconv.FormatInt(now.Unix(), 10)
	text := fmt.Sprintf("[host fault] machine=%s reason=%s: delivery to a worker pane was not confirmed (hk row %d, lane %s, pane %s). Rows stay undelivered in hk and are retried where nothing was typed. Further drops for this host and reason are summarized once at %s UTC, naming each row.%s",
		truncateRelayText(machine, 64), dropped.Reason, dropped.OriginalEventID, truncateRelayText(dropped.Lane, 64), truncateRelayText(dropped.Pane, 64), now.Add(window).UTC().Format("15:04"), note.text)
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, NotifyType: "automation-failed", Host: "hub", Label: "host-fault", Reason: dropped.Reason, Text: utf8PrefixWithin(text, laneEventTextLimit)}
	result := h.relayLaneEventContext(h.closeCtx, event, nil)
	if result.ID == 0 {
		// Nobody was told: no window is recorded, so the next drop is sent
		// and its notice names this one.
		h.quiet.hostFaults.fold(machine, dropped.Reason, dropped.OriginalEventID, now)
		return
	}
	h.quiet.hostFaults.record(machine, dropped.Reason, dropped.OriginalEventID, note, now)
	time.AfterFunc(window, func() { h.closeHostFault(machine, dropped.Reason) })
}

// closeHostFault ends a (machine, reason) window. When drops were folded into
// it, one summary names every one of them.
func (h *HubServer) closeHostFault(machine, reason string) {
	lane := h.quiet.timings.hostFaultLane
	if lane == "" || h.closeCtx.Err() != nil {
		return
	}
	rows, over, since := h.quiet.hostFaults.closeWindow(machine, reason)
	if len(rows) == 0 && over == 0 {
		return
	}
	var listed []string
	for i, id := range rows {
		if i == hostFaultListMax {
			over += len(rows) - i
			break
		}
		listed = append(listed, strconv.FormatInt(id, 10))
	}
	now := h.now()
	eventID := hostFaultPrefix + hostFaultToken(machine, reason) + "-" + strconv.FormatInt(now.Unix(), 10) + "-end"
	text := fmt.Sprintf("[host fault] machine=%s reason=%s: %d more deliveries were not confirmed since %s UTC. hk rows: %s", truncateRelayText(machine, 64), reason, len(rows)+over, since.UTC().Format("15:04"), strings.Join(listed, ", "))
	if over > 0 {
		text += fmt.Sprintf(" (+%d not listed; list the undelivered rows in hk)", over)
	}
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, NotifyType: "automation-failed", Host: "hub", Label: "host-fault", Reason: reason, Text: utf8PrefixWithin(text, laneEventTextLimit)}
	if h.relayLaneEventContext(h.closeCtx, event, nil).ID == 0 {
		// Nobody was told: the rows are counted again for the next notice.
		h.logger.Error("host fault summary was not stored", "machine", machine, "reason", reason, "rows", len(rows)+over)
		for _, id := range rows {
			h.quiet.hostFaults.fold(machine, reason, id, since)
		}
	}
}

// hostFaultToken makes the machine and reason part of a producer event id.
func hostFaultToken(machine, reason string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '_'
	}, machine+"-"+reason)
}
