package panewire

// #1002: a relay row whose pane submission could not be proven
// (relay.unconfirmed) gets at most one labelled replay after a hub restart
// and is then retired whatever that replay's outcome. The durable mark is a
// separate marker row because the contract offers no column to set on an
// existing row: the duplicate POST updates only attempts, and /delivered
// would remove the row from the undelivered listing before its one replay.
//
// The marker is a lane.event row on the unroutable relayUnconfirmedMarkLane
// with event_id "unconfirmed-mark-<row id>". While the marker is undelivered
// the row it names is pending one labelled replay; the marked row itself
// stays an ordinary undelivered row, so a late relay.delivered still closes
// it under the real machine/pane and a node resend still re-injects it. The
// replay retires the marked row with hub/replay-retired:unconfirmed and the
// marker with hub/replay-retired:unconfirmed-mark.

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var relayUnconfirmedLabelZone = time.FixedZone("KST", 9*60*60)

// isUnconfirmedMarkRecord reports whether the durable row is a marker, never
// a relayable event of its own.
func isUnconfirmedMarkRecord(record handoffkeepRelayEvent) bool {
	return record.Kind == "lane.event" && record.OwnerLane == relayUnconfirmedMarkLane
}

func unconfirmedMarkEventID(rowID int64) string {
	return relayUnconfirmedMarkEventPrefix + strconv.FormatInt(rowID, 10)
}

// unconfirmedDigestMarkEventID names the marker of a row that was an inbox
// digest or escalation batch when it was marked. The fact travels on the
// marker so a walk with no inbox store open can still tell the row must not
// be replayed as an ordinary event.
func unconfirmedDigestMarkEventID(rowID int64) string {
	return unconfirmedMarkEventID(rowID) + relayUnconfirmedDigestMarkSuffix
}

func unconfirmedMarkIsDigest(eventID string) bool {
	return strings.HasPrefix(eventID, relayUnconfirmedMarkEventPrefix) && strings.HasSuffix(eventID, relayUnconfirmedDigestMarkSuffix)
}

func unconfirmedMarkOriginID(eventID string) int64 {
	if !strings.HasPrefix(eventID, relayUnconfirmedMarkEventPrefix) {
		return 0
	}
	id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(eventID, relayUnconfirmedMarkEventPrefix), relayUnconfirmedDigestMarkSuffix), 10, 64)
	if err != nil || id < 1 {
		return 0
	}
	return id
}

// unconfirmedMarkFirstSent names the original send: the request's own
// event_time, falling back to handoffkeep's received_at for older rows.
func unconfirmedMarkFirstSent(record handoffkeepRelayEvent) (time.Time, bool) {
	if when, err := time.Parse(time.RFC3339Nano, record.EventTime); err == nil {
		return when, true
	}
	if when, err := time.Parse(time.RFC3339Nano, record.ReceivedAt); err == nil {
		return when, true
	}
	return time.Time{}, false
}

// relayUnconfirmedLabel is the one-line pane-visible duplicate warning the
// replayed text carries. It names the original send in KST and tells the pane
// the content may already have run once.
func relayUnconfirmedLabel(record handoffkeepRelayEvent) string {
	stamp := "unknown-time"
	if when, ok := unconfirmedMarkFirstSent(record); ok {
		stamp = when.In(relayUnconfirmedLabelZone).Format("2006-01-02 15:04:05 MST")
	}
	return "[replayed after hub restart - first sent " + stamp + " - may be a duplicate, do not re-run] "
}

// markRelayEventUnconfirmed writes the durable marker for the row a live
// ack window names, before the window itself is retired: a hub crash between
// the two steps loses nothing — the mark is already durable, and the still-
// live window simply marks idempotently on a repeated report. It reports
// whether the window may be retired: true once the mark is durable and also
// when there is nothing to mark (no durable row, an unknown row, an already
// delivered row, or no store at all — the window's old outcome). Only a mark
// that was needed and could not be written returns false, and the handler
// then keeps the window so a repeated report — or the ack timeout — decides,
// like any other live window (#1034).
func (h *HubServer) markRelayEventUnconfirmed(pending relayPending) bool {
	return h.markRelayEventUnconfirmedContext(context.Background(), pending)
}

func (h *HubServer) markRelayEventUnconfirmedContext(ctx context.Context, pending relayPending) bool {
	if h.handoffkeep == nil {
		return true
	}
	eventID := pending.eventID
	if eventID == 0 && pending.kind != "" {
		key := relayEventDedupeKey(pending.kind, pending.event)
		h.mu.Lock()
		eventID = h.lanePersisted[key]
		if eventID == 0 {
			eventID = h.relayDedupe[key]
		}
		h.mu.Unlock()
	}
	if eventID == 0 && pending.kind == "lane.event" {
		// An id-less lane window still names its row through the transport id
		// the injection carried, exactly like the late-delivery path. A lookup
		// error is not "no row": the mark cannot be proven unneeded, so the
		// window is kept like a failed marker POST and only a clean found=false
		// falls through to the nothing-to-mark outcome (#1034).
		record, found, err := h.relayEventByTransportID(ctx, pending.event.JobID)
		if err != nil {
			h.logger.Warn("relay unconfirmed transport lookup failed", "job", pending.event.JobID, "error_class", hubHKReadErrorClass(err))
			return false
		}
		if found {
			eventID = record.ID
		}
	}
	if eventID == 0 {
		h.logger.Warn("relay unconfirmed named no durable row", "job", pending.event.JobID, "kind", pending.kind, "machine", pending.machine, "pane", pending.pane)
		return true
	}
	record, found, err := h.handoffkeep.relayEvent(ctx, eventID)
	if err != nil {
		// The read cannot tell whether a mark is needed, so nothing durable
		// was written: the window is kept for a repeated report, like a
		// failed marker POST.
		h.logger.Warn("relay unconfirmed mark could not be read", "event_id", eventID, "error_class", hubHKReadErrorClass(err))
		return false
	}
	if !found {
		h.logger.Warn("relay unconfirmed named an unknown durable row", "event_id", eventID)
		return true
	}
	if record.DeliveredAt != "" {
		// The row already closed — typically the second unconfirmed on a row
		// the one labelled replay just retired. There is nothing to mark.
		return true
	}
	markID := unconfirmedMarkEventID(record.ID)
	if h.inbox != nil && record.Kind == "lane.event" {
		// An unreadable batch table cannot prove the mark unneeded or ordinary,
		// so the window is kept like a failed marker POST.
		isBatch, err := h.inbox.store.isDigestBatch(record.OwnerLane, record.EventID)
		if err != nil {
			h.logger.Warn("relay unconfirmed batch lookup failed", "event_id", record.ID)
			return false
		}
		if isBatch {
			markID = unconfirmedDigestMarkEventID(record.ID)
		}
	}
	mark := hubJobEventPayload{
		JobID:     laneEventTransportID(relayUnconfirmedMarkLane, markID),
		Epoch:     1,
		OwnerLane: relayUnconfirmedMarkLane,
		EventID:   markID,
		Text:      fmt.Sprintf("relay.unconfirmed mark: durable row %d keeps one labelled replay", record.ID),
	}
	if _, _, persisted := h.persistRelayEventRecord(ctx, "lane.event", mark, reportRelayRoute{}); !persisted {
		h.logger.Warn("relay unconfirmed mark was not persisted", "event_id", record.ID)
		return false
	}
	h.logger.Info("relay row marked unconfirmed for one labelled replay", "event_id", record.ID, "kind", record.Kind, "lane", record.OwnerLane, "job", record.JobID)
	return true
}

// unconfirmedMarkOrigins walks the undelivered listing once and returns the
// set of durable row ids a live marker still names, and the subset whose
// marker says the row was a digest or escalation batch. Marker rows are always
// lane.event, so the scan shares that listing.
func (h *HubServer) unconfirmedMarkOrigins(ctx context.Context) (origins, digests map[int64]bool, err error) {
	origins = make(map[int64]bool)
	digests = make(map[int64]bool)
	var afterID int64
	for {
		pageStart := afterID
		records, err := h.handoffkeep.listUndelivered(ctx, "", "lane.event", afterID, handoffkeepReplayLimit)
		if err != nil {
			return nil, nil, err
		}
		for _, record := range records {
			if isUnconfirmedMarkRecord(record) {
				if origin := unconfirmedMarkOriginID(record.EventID); origin > 0 {
					origins[origin] = true
					if unconfirmedMarkIsDigest(record.EventID) {
						digests[origin] = true
					}
				}
			}
			if record.ID > afterID {
				afterID = record.ID
			}
		}
		if len(records) < handoffkeepReplayLimit || afterID <= pageStart {
			return origins, digests, nil
		}
	}
}

// replayMarkedUnconfirmed gives a marked row its one labelled replay. The
// gates ahead of the queue match replayRelayEvent exactly; the differences
// are the label on the directive text and the write order at the end: the
// durable retire is spent BEFORE any copy is queued, so a failed retire
// defers the replay to the next trigger instead of repeating it every
// restart, and a crash or queue failure between the retire and the queue
// can never leave the row open for another labelled inject (#1034). The
// rule is: no labelled copy is ever queued unless the durable retire for
// that row has already succeeded.
func (h *HubServer) replayMarkedUnconfirmed(ctx context.Context, record handoffkeepRelayEvent, source string, digestMark bool) {
	if h.retireLegacyChatReplay(record) || h.retireUnconfirmedSettledInInbox(record) {
		return
	}
	if h.retireUnconfirmedDigest(record, digestMark) {
		return
	}
	// An idle re-delivery is bounded by the time of its drop, not its receipt
	// (hk 3411, relayReplayRetireReasonAged).
	receiptAge := source != relayReplaySourceIdle
	if h.replayInboxAged(record, receiptAge) {
		return
	}
	if h.handoffkeep == nil {
		return
	}
	event := relayEventFromRecord(record)
	if event.OwnerLane == "" || event.JobID == "" {
		return
	}
	if record.DeliveredAt != "" || strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) {
		return
	}
	if reason := h.relayReplayRetireReasonAged(record, receiptAge); reason != "" {
		h.retireRelayReplay(record, reason)
		return
	}
	if record.Attempts >= relayReplayMaxAttempts {
		h.broadcastRelayReplayExhausted(record)
		return
	}
	key := relayEventDedupeKey(record.Kind, event)
	if record.Kind == "lane.event" {
		h.rememberLanePersisted(key, record.ID)
		h.rememberLaneEventSHA(key, record.Text)
	}
	h.mu.Lock()
	if _, exists := h.relayDedupe[key]; exists {
		h.mu.Unlock()
		return
	}
	h.relayDedupe[key] = record.ID
	h.mu.Unlock()
	route, agent, routed := h.resolveRelayRoute(record.Kind, event)
	if !routed || (record.Kind == "lane.event" && route.Sink) {
		h.forgetRelayEvent(key)
		h.broadcastRelayUnrouted(event)
		return
	}
	// The listing page is stale the moment it is read: another walk — or a
	// late delivery — may have closed this row since, and an ack timeout may
	// have released the dedupe claim it held. The row is re-read before its
	// one replay is spent; delivered, retired, or gone all end the replay
	// here instead of in a pane (#1034).
	latest, found, err := h.handoffkeep.relayEvent(ctx, record.ID)
	if err != nil {
		h.forgetRelayEvent(key)
		h.logger.Warn("unconfirmed replay could not re-read its row", "event_id", record.ID)
		return
	}
	if !found || latest.DeliveredAt != "" || strings.HasPrefix(latest.DeliveredTo, relayReplayRetiredMarker) {
		h.forgetRelayEvent(key)
		return
	}
	if h.refuseInboxOnlyBody(record.Kind, event, record.ID, key) {
		return
	}
	// Spend before inject: the retire write is what bounds the row to one
	// labelled replay, so it must be durable before any copy is queued. A
	// failed retire spends nothing — the claim and any ack registration are
	// released exactly as the queue-failure path releases them, the mark
	// stays pending, and the next trigger retries the whole sequence.
	if err := h.handoffkeep.markDelivered(ctx, record.ID, relayReplayRetiredMachine, relayReplayRetiredPane+relayUnconfirmedRetireReason); err != nil {
		h.forgetRelayEvent(key)
		h.forgetRelayAckEvent(record.ID, event.JobID)
		h.logger.Warn("unconfirmed replay could not retire its row", "event_id", record.ID)
		return
	}
	h.registerRelayAckEvent(record.Kind, event, route.Machine, route.Pane, record.ID)
	directive := h.inboxRelayDirective(record.Kind, event, route, record.ID)
	label := relayUnconfirmedLabel(record)
	if source == relayReplaySourceIdle {
		label = relayIdleRedeliveryLabel(record)
	}
	directive.Text = label + directive.Text
	if !agent.queueRelay(directive) {
		h.forgetRelayEvent(key)
		h.forgetRelayAckEvent(record.ID, event.JobID)
		// The durable retire already spent the one replay, so nothing may be
		// queued now and no later trigger may inject it.
		h.logger.Warn("unconfirmed replay was retired but never queued", "event_id", record.ID, "machine", route.Machine, "pane", route.Pane)
		h.broadcastRelayUnrouted(event)
		return
	}
	h.armRelayAckEvent(record.ID, event.JobID)
	firstSent := "unknown"
	if stamp, ok := unconfirmedMarkFirstSent(record); ok {
		firstSent = stamp.Format(time.RFC3339)
	}
	h.logger.Info("relay unconfirmed replay injected", "event_id", record.ID, "kind", record.Kind, "lane", record.OwnerLane, "machine", route.Machine, "pane", route.Pane, "first_sent", firstSent, "source", source)
	h.broadcastRelayReplayInjected(record, route, record.Attempts, source)
}

// retireUnconfirmedDigest spends no replay on an inbox digest or escalation
// batch: its count was true only when it was made. It runs after the settled
// check, which already retires a batch whose own inbox item the receiver has.
// A row is a batch only when lane_inbox_batches names it for its lane; the
// table is never pruned, and an event id alone proves nothing because a
// producer may choose any id. With no inbox store the table cannot be read, so
// the marker decides: a marker written for a batch (while a store was open)
// defers the row without a copy, anything else is an ordinary row. The batch's
// still-unread items are released before the row retires, so a crash between the two leaves at worst a fresh
// escalation plus a retire the next restart repeats, never an item that no
// batch will ever escalate. The next flush then escalates them through the
// normal path with the current count. A failed lookup, release or retire
// replays nothing and the next trigger tries again; chat rows are never
// batches.
func (h *HubServer) retireUnconfirmedDigest(record handoffkeepRelayEvent, digestMark bool) bool {
	if h.inbox == nil {
		// The marker, written while a store was open, says the row was a batch;
		// without a store its items cannot be released, so nothing is spent and
		// no copy is injected. A later run with the store retires and releases.
		return digestMark && h.handoffkeep != nil && record.DeliveredAt == "" && !strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker)
	}
	if h.handoffkeep == nil || record.Kind != "lane.event" || record.DeliveredAt != "" || strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) {
		return false
	}
	isBatch, err := h.inbox.store.isDigestBatch(record.OwnerLane, record.EventID)
	if err != nil {
		h.logger.Warn("inbox batch lookup unavailable; unconfirmed replay deferred", "event_id", record.ID)
		return true
	}
	if !isBatch {
		return false
	}
	if err := h.inbox.store.releaseBatchClaims(record.EventID); err != nil {
		h.logger.Warn("inbox batch claims were not released; unconfirmed replay deferred", "event_id", record.ID)
		return true
	}
	h.retireRelayReplay(record, relayDigestNotReplayedRetireReason)
	return true
}

// retireUnconfirmedSettledInInbox spends no labelled replay on a row whose
// lane inbox item the receiver already has (delivered, read, acked or
// superseded): the row is retired as inbox-delivered and the marker retires
// with it on the same walk, because settleUnconfirmedMarker sees the origin
// closed. An absent or still undelivered, unread item leaves the row to its
// one labelled replay. A failed lookup or retire replays nothing; the next
// trigger tries again. Chat rows keep the chat store as their authority.
func (h *HubServer) retireUnconfirmedSettledInInbox(record handoffkeepRelayEvent) bool {
	if h.inbox == nil || h.handoffkeep == nil || record.DeliveredAt != "" || strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) {
		return false
	}
	settled, err := h.inbox.store.settled(relayEventDedupeKey(record.Kind, relayEventFromRecord(record)))
	if err != nil {
		h.logger.Warn("inbox evidence unavailable; unconfirmed replay deferred", "event_id", record.ID)
		return true
	}
	if !settled {
		return false
	}
	h.retireRelayReplay(record, relayUnconfirmedInboxRetireReason)
	return true
}

// settleUnconfirmedMarker keeps a marker alive only while the row it names
// is still undelivered and replayable. Once the original row is delivered,
// exhausted, or gone, the mark has no work left and retires with the other
// replay-retired rows.
func (h *HubServer) settleUnconfirmedMarker(ctx context.Context, marker handoffkeepRelayEvent) {
	origin := unconfirmedMarkOriginID(marker.EventID)
	if origin > 0 {
		record, found, err := h.handoffkeep.relayEvent(ctx, origin)
		switch {
		case err != nil:
			return // a transient read failure keeps the mark for the next pass
		case found && record.DeliveredAt == "" && record.Attempts < relayReplayMaxAttempts:
			return // still pending its one replay
		}
	}
	h.retireRelayReplay(marker, relayUnconfirmedMarkRetireReason)
}
