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
	"context"
	"encoding/json"
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
	// relayUnconfirmedWatchLane is the owner_lane of the durable rows that keep
	// an unconfirmed row's unread deadline. Like relayUnconfirmedMarkLane the
	// slash keeps it unroutable and unnameable by a producer.
	relayUnconfirmedWatchLane = "hub/unconfirmed-watch"
	unconfirmedWatchPrefix    = "unconfirmed-watch-"
)

// unconfirmedRow is one delivery the node dropped as unconfirmed that no one
// has confirmed since. watchID is the hk row (relayUnconfirmedWatchLane) that
// keeps its deadline across a hub restart; zero when none was written.
type unconfirmedRow struct {
	lane    string
	retried bool
	watchID int64
	// confirmed: a relay.delivered proved the pane has the row. The proof is
	// kept until the durable write that closes the hk row succeeds, so a
	// failed write cannot make the row eligible for retyping.
	confirmed bool
	// escalating: the unread timer fired and the design-lane notice is in
	// flight; the entry stays so each retry of that notice can see a late
	// confirmation.
	escalating bool
}

// unconfirmedWatch is the payload of a watch row: what a restarted hub needs to
// tell the worker's design lane about a row still unconfirmed at its deadline.
type unconfirmedWatch struct {
	Machine string `json:"machine"`
	Lane    string `json:"lane"`
	Pane    string `json:"pane"`
	Reason  string `json:"reason"`
	Row     int64  `json:"row"`
	AtMS    int64  `json:"at_ms"`
}

// retriableUnconfirmed names the drops that typed nothing: the pane read
// failed before the send, or the row was held and never typed.
func retriableUnconfirmed(reason string) bool {
	return reason == relayDroppedPaneReadFailed || reason == relayDroppedWorkingExpired
}

func isUnconfirmedWatchRecord(record handoffkeepRelayEvent) bool {
	return record.Kind == "lane.event" && record.OwnerLane == relayUnconfirmedWatchLane
}

// noteUnconfirmedRow records the drop. first is false for a row whose retry
// also dropped. tracked is false when the hub keeps no record of the row (the
// timers are off, or the memory is full): its design lane is then told at once
// rather than trusting a missing entry to mean "confirmed".
func (h *HubServer) noteUnconfirmedRow(machine string, dropped relayDroppedPayload) (first, tracked bool) {
	t := h.quiet.timings
	if t.unconfirmedUnreadAfter <= 0 && t.unconfirmedRetryAfter <= 0 {
		return true, false
	}
	h.quiet.mu.Lock()
	defer h.quiet.mu.Unlock()
	if _, seen := h.quiet.unconfirmed[dropped.OriginalEventID]; seen {
		return false, true
	}
	if h.quiet.unconfirmed == nil {
		h.quiet.unconfirmed = make(map[int64]unconfirmedRow)
	}
	if len(h.quiet.unconfirmed) >= unconfirmedRowsMax {
		return true, false
	}
	h.quiet.unconfirmed[dropped.OriginalEventID] = unconfirmedRow{lane: dropped.Lane}
	return true, true
}

func (h *HubServer) forgetUnconfirmedRow(id int64) (unconfirmedRow, bool) {
	h.quiet.mu.Lock()
	defer h.quiet.mu.Unlock()
	row, ok := h.quiet.unconfirmed[id]
	delete(h.quiet.unconfirmed, id)
	return row, ok
}

// confirmUnconfirmedRow records that a relay.delivered proved the pane has the
// row. The entry is kept (settleConfirmedRow removes it once hk has the
// delivery) so the retry and the unread notice both see the proof.
func (h *HubServer) confirmUnconfirmedRow(id int64) {
	if id < 1 {
		return
	}
	h.quiet.mu.Lock()
	if row, ok := h.quiet.unconfirmed[id]; ok {
		row.confirmed = true
		h.quiet.unconfirmed[id] = row
	}
	h.quiet.mu.Unlock()
}

// settleConfirmedRow ends a confirmed row's tracking after hk recorded the
// delivery, and closes its watch row.
func (h *HubServer) settleConfirmedRow(id int64) {
	if id < 1 {
		return
	}
	row, ok := h.forgetUnconfirmedRow(id)
	if ok && row.confirmed && row.watchID > 0 {
		go h.retireUnconfirmedWatch(row.watchID, "confirmed")
	}
}

// unconfirmedRowConfirmed reports whether a delivery proof is held for id.
func (h *HubServer) unconfirmedRowConfirmed(id int64) bool {
	h.quiet.mu.Lock()
	defer h.quiet.mu.Unlock()
	return h.quiet.unconfirmed[id].confirmed
}

// persistUnconfirmedWatch writes the row's deadline to hk and records the watch
// row's id. It reports false when hk refused it.
func (h *HubServer) persistUnconfirmedWatch(machine string, dropped relayDroppedPayload) bool {
	payload, err := json.Marshal(unconfirmedWatch{Machine: machine, Lane: dropped.Lane, Pane: dropped.Pane, Reason: dropped.Reason, Row: dropped.OriginalEventID, AtMS: h.now().UnixMilli()})
	if err != nil {
		return false
	}
	eventID := unconfirmedWatchPrefix + strconv.FormatInt(dropped.OriginalEventID, 10)
	event := hubJobEventPayload{JobID: laneEventTransportID(relayUnconfirmedWatchLane, eventID), Epoch: 1, OwnerLane: relayUnconfirmedWatchLane, EventID: eventID, Text: string(payload)}
	stored, _, ok := h.persistRelayEventRecord(h.closeCtx, "lane.event", event, reportRelayRoute{})
	if !ok || stored.ID < 1 {
		return false
	}
	h.quiet.mu.Lock()
	if row, tracked := h.quiet.unconfirmed[dropped.OriginalEventID]; tracked {
		row.watchID = stored.ID
		h.quiet.unconfirmed[dropped.OriginalEventID] = row
	}
	h.quiet.mu.Unlock()
	return true
}

// retireUnconfirmedWatch closes a watch row. A failure leaves it open: a
// restarted hub then re-reads the row it names and closes it again.
func (h *HubServer) retireUnconfirmedWatch(id int64, why string) {
	if h.handoffkeep == nil || id < 1 {
		return
	}
	if err := h.handoffkeep.markDelivered(context.Background(), id, relayFilteredMachine, "watch-closed:"+why); err != nil {
		h.logger.Warn("unconfirmed watch row was not closed", "event_id", id, "why", why)
	}
}

// resumeUnconfirmedWatch re-arms the deadline of a watch row the walk found
// open after a hub restart. A row the pane confirmed is closed; any other is
// told to the design lane when its deadline ends, at once when that has passed.
func (h *HubServer) resumeUnconfirmedWatch(ctx context.Context, record handoffkeepRelayEvent) {
	var watch unconfirmedWatch
	if json.Unmarshal([]byte(record.Text), &watch) != nil || watch.Row < 1 {
		h.retireUnconfirmedWatch(record.ID, "unreadable")
		return
	}
	h.quiet.mu.Lock()
	_, live := h.quiet.unconfirmed[watch.Row]
	h.quiet.mu.Unlock()
	if live {
		return
	}
	origin, found, err := h.handoffkeep.relayEvent(ctx, watch.Row)
	switch {
	case err != nil:
		return // the next walk tries again
	case !found:
		h.retireUnconfirmedWatch(record.ID, "row-gone")
		return
	case origin.DeliveredAt != "" && !strings.HasPrefix(origin.DeliveredTo, relayReplayRetiredMarker):
		h.retireUnconfirmedWatch(record.ID, "confirmed")
		return
	}
	h.quiet.mu.Lock()
	if h.quiet.unconfirmed == nil {
		h.quiet.unconfirmed = make(map[int64]unconfirmedRow)
	}
	h.quiet.unconfirmed[watch.Row] = unconfirmedRow{lane: watch.Lane, retried: origin.DeliveredAt != "", watchID: record.ID}
	h.quiet.mu.Unlock()
	dropped := relayDroppedPayload{Pane: watch.Pane, Lane: watch.Lane, OriginalEventID: watch.Row, Reason: watch.Reason}
	wait := time.UnixMilli(watch.AtMS).Add(h.quiet.timings.unconfirmedUnreadAfter).Sub(h.now())
	time.AfterFunc(max(wait, 0), func() { h.escalateUnreadRow(watch.Machine, dropped) })
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
	time.AfterFunc(after, func() { h.retryUnconfirmedRow(dropped, 0) })
}

// retryUnconfirmedRow gives the row its one labelled replay. Every guard of
// replayMarkedUnconfirmed applies; the retry adds that the row is still
// undelivered and is a producer's lane.event.
//
// A row hk cannot be read for is read again on hkRetryBackoff, and the row is
// read once more right before it is typed. A row a relay.delivered proved
// confirmed is never typed, even if hk has not recorded the delivery yet.
func (h *HubServer) retryUnconfirmedRow(dropped relayDroppedPayload, attempt int) {
	if h.closeCtx.Err() != nil || h.unconfirmedRowConfirmed(dropped.OriginalEventID) {
		return
	}
	ctx := h.closeCtx
	record, found, err := h.handoffkeep.relayEvent(ctx, dropped.OriginalEventID)
	if err != nil {
		backoff := h.quiet.timings.hkRetryBackoff
		if attempt >= len(backoff) {
			h.logger.Error("unconfirmed retry gave up reading its row", "event_id", dropped.OriginalEventID, "lane", dropped.Lane)
			return
		}
		h.logger.Warn("unconfirmed retry could not read its row; will retry", "event_id", dropped.OriginalEventID, "attempt", attempt+1)
		time.AfterFunc(backoff[attempt], func() { h.retryUnconfirmedRow(dropped, attempt+1) })
		return
	}
	if !found || record.DeliveredAt != "" || !escalatesWhenUnconfirmed(record) || h.unconfirmedRowConfirmed(record.ID) {
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
// unconfirmed when its time is up, then closes the row's watch.
func (h *HubServer) escalateUnreadRow(machine string, dropped relayDroppedPayload) {
	if h.closeCtx.Err() != nil {
		return
	}
	h.quiet.mu.Lock()
	row, tracked := h.quiet.unconfirmed[dropped.OriginalEventID]
	switch {
	case !tracked:
		h.quiet.mu.Unlock()
		return // settled since
	case row.confirmed:
		delete(h.quiet.unconfirmed, dropped.OriginalEventID)
		h.quiet.mu.Unlock()
		if row.watchID > 0 {
			h.retireUnconfirmedWatch(row.watchID, "confirmed")
		}
		return
	}
	// The entry stays while the notice is in flight, so each of its retries
	// sees a confirmation that arrives meanwhile; it is dropped afterwards.
	row.escalating = true
	h.quiet.unconfirmed[dropped.OriginalEventID] = row
	h.quiet.mu.Unlock()
	h.escalateUnconfirmedRelayAttempt(machine, dropped, 0, row.retried)
	if row.watchID > 0 {
		h.retireUnconfirmedWatch(row.watchID, "told")
	}
	var settle time.Duration = time.Minute
	for _, d := range unconfirmedEscalationBackoff {
		settle += d
	}
	time.AfterFunc(settle, func() { h.forgetUnconfirmedRow(dropped.OriginalEventID) })
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
// it, one summary names every one of them. The window is settled only once the
// summary is durable; a failed write is retried on hkRetryBackoff,
// independently of further drops.
func (h *HubServer) closeHostFault(machine, reason string) {
	h.closeHostFaultAttempt(machine, reason, 0)
}

func (h *HubServer) closeHostFaultAttempt(machine, reason string, attempt int) {
	lane := h.quiet.timings.hostFaultLane
	if lane == "" || h.closeCtx.Err() != nil {
		return
	}
	for range 3 {
		rows, over, since := h.quiet.hostFaults.snapshot(machine, reason)
		if len(rows) == 0 && over == 0 {
			h.quiet.hostFaults.settle(machine, reason, nil, 0)
			return
		}
		var listed []string
		unlisted := over
		for i, id := range rows {
			if i == hostFaultListMax {
				unlisted += len(rows) - i
				break
			}
			listed = append(listed, strconv.FormatInt(id, 10))
		}
		now := h.now()
		eventID := hostFaultPrefix + hostFaultToken(machine, reason) + "-" + strconv.FormatInt(now.Unix(), 10) + "-end"
		text := fmt.Sprintf("[host fault] machine=%s reason=%s: %d more deliveries were not confirmed since %s UTC. hk rows: %s", truncateRelayText(machine, 64), reason, len(rows)+over, since.UTC().Format("15:04"), strings.Join(listed, ", "))
		if unlisted > 0 {
			text += fmt.Sprintf(" (+%d not listed; list the undelivered rows in hk)", unlisted)
		}
		event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, NotifyType: "automation-failed", Host: "hub", Label: "host-fault", Reason: reason, Text: utf8PrefixWithin(text, laneEventTextLimit)}
		if h.relayLaneEventContext(h.closeCtx, event, nil).ID == 0 {
			if attempt >= len(h.quiet.timings.hkRetryBackoff) {
				h.logger.Error("host fault summary gave up; its rows stay undelivered in hk", "machine", machine, "reason", reason, "rows", len(rows)+over)
				return
			}
			h.logger.Warn("host fault summary was not stored; will retry", "machine", machine, "reason", reason, "attempt", attempt+1)
			time.AfterFunc(h.quiet.timings.hkRetryBackoff[attempt], func() { h.closeHostFaultAttempt(machine, reason, attempt+1) })
			return
		}
		if h.quiet.hostFaults.settle(machine, reason, rows, over) == 0 {
			return
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
