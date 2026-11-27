package panewire

// Legacy handoffkeep marker decoding and the no-store compatibility replay.
// Store-backed hubs schedule retries on delivery_obligations.

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

func (h *HubServer) markRelayEventUnconfirmed(pending relayPending) bool {
	return h.scheduleRelayRetry(context.Background(), pending, obligationFaultUnconfirmed) != relayFaultFailed
}

// scheduleRelayRetry records the fault before the live window can be removed.
// A lookup or persistence failure keeps a node-reported window eligible for
// a repeated report, acknowledgement or timeout.
type relayFaultWrite uint8

const (
	relayFaultFailed relayFaultWrite = iota
	relayFaultUnneeded
	relayFaultScheduled
)

func (h *HubServer) scheduleRelayRetry(ctx context.Context, pending relayPending, fault string) relayFaultWrite {
	if h.handoffkeep == nil {
		return relayFaultUnneeded
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
			return relayFaultFailed
		}
		if found {
			eventID = record.ID
		}
	}
	if eventID == 0 {
		h.logger.Warn("relay unconfirmed named no durable row", "job", pending.event.JobID, "kind", pending.kind, "machine", pending.machine, "pane", pending.pane)
		return relayFaultUnneeded
	}
	record, found, err := h.handoffkeep.relayEvent(ctx, eventID)
	if err != nil {
		// The read cannot tell whether a mark is needed, so nothing durable
		// was written: the window is kept for a repeated report, like a
		// failed marker POST.
		h.logger.Warn("relay unconfirmed mark could not be read", "event_id", eventID, "error_class", hubHKReadErrorClass(err))
		return relayFaultFailed
	}
	if !found {
		h.logger.Warn("relay unconfirmed named an unknown durable row", "event_id", eventID)
		return relayFaultUnneeded
	}
	if record.DeliveredAt != "" || (h.inbox != nil && pending.kind != "lane.event" && record.EventID != pending.event.EventID) {
		return relayFaultUnneeded
	}
	if h.inbox != nil {
		o := newDeliveryObligation{
			key: relayEventDedupeKey(pending.kind, pending.event), relayID: record.ID,
			kind: pending.kind, lane: pending.event.OwnerLane, machine: pending.machine, pane: pending.pane,
			attempts: record.Attempts,
		}
		store := h.inbox.store
		if err := store.flagObligationRetry(ctx, o, fault, "", h.now().UTC()); err != nil {
			h.logger.Warn("relay retry could not be scheduled", "event_id", record.ID, "error", store.loggable("schedule retry", err))
			return relayFaultFailed
		}
		if record.Kind == "lane.event" {
			// Establish rollback protection before accepting a maybe-typed
			// report. Seed spent first, while attempts still counts sends.
			batch, err := store.isDigestBatch(record.OwnerLane, record.EventID)
			if err != nil {
				h.logger.Warn("relay unconfirmed batch lookup failed", "event_id", record.ID)
				return relayFaultFailed
			}
			needsProtection := fault != obligationAckTimeout || batch
			if !needsProtection {
				tracked, err := store.retryObligationForKey(ctx, o.key)
				if err != nil {
					h.logger.Warn("relay retry fault could not be read", "event_id", record.ID, "error", store.loggable("retry fault", err))
					return relayFaultFailed
				}
				// An earlier explicit report can have persisted its fault
				// while handoffkeep refused the rollback protection.
				needsProtection = tracked.state == obligationOwed && tracked.fault != obligationAckTimeout
			}
			if needsProtection {
				if _, ok := h.bumpObligationAttempts(ctx, record, reportRelayRoute{Machine: pending.machine, Pane: pending.pane}, true); !ok {
					return relayFaultFailed
				}
			}
		}
		return relayFaultScheduled
	}
	markID := unconfirmedMarkEventID(record.ID)
	mark := hubJobEventPayload{
		JobID:     laneEventTransportID(relayUnconfirmedMarkLane, markID),
		Epoch:     1,
		OwnerLane: relayUnconfirmedMarkLane,
		EventID:   markID,
		Text:      fmt.Sprintf("relay.unconfirmed mark: durable row %d keeps one labelled replay", record.ID),
	}
	if _, _, persisted := h.persistRelayEventRecord(ctx, "lane.event", mark, reportRelayRoute{}); !persisted {
		h.logger.Warn("relay unconfirmed mark was not persisted", "event_id", record.ID)
		return relayFaultFailed
	}
	h.logger.Info("relay row marked unconfirmed for one labelled replay", "event_id", record.ID, "kind", record.Kind, "lane", record.OwnerLane, "job", record.JobID)
	return relayFaultUnneeded
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
					if h.inbox != nil {
						h.convertUnconfirmedMarker(ctx, record)
					}
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

// relayRoomFree reports whether queueRelay would take a directive now. It is a
// check, not a reservation: another path can fill the last place before the
// caller queues, and then the replay is spent with nothing queued (the warning
// replayLegacyUnconfirmed logs).
func (agent *hubAgent) relayRoomFree() bool {
	return agent != nil && agent.relays != nil && len(agent.relays) < cap(agent.relays)
}

// replayLegacyUnconfirmed gives a marked row its one labelled replay. The
// gates ahead of the queue match replayRelayEvent exactly; the differences
// are the label on the directive text and the write order at the end: the
// durable retire is spent BEFORE any copy is queued, so a failed retire
// defers the replay to the next trigger instead of repeating it every
// restart, and a crash or queue failure between the retire and the queue
// can never leave the row open for another labelled inject (#1034). The
// rule is: no labelled copy is ever queued unless the durable retire for
// that row has already succeeded.
func (h *HubServer) replayLegacyUnconfirmed(ctx context.Context, record handoffkeepRelayEvent, source string, digestMark bool) {
	if h.retireLegacyChatReplay(record) || digestMark {
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
		h.announceUnroutedReplay(record.ID, event)
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
	// The retire below spends the one replay, and nothing is queued after it. A
	// full queue is seen before the spend, so the row stays unspent for the next
	// trigger instead of being retired with no copy in any pane (hk 6893).
	if !agent.relayRoomFree() {
		h.forgetRelayEvent(key)
		h.logger.Warn("unconfirmed replay deferred: the pane's injection queue is full", "event_id", record.ID, "machine", route.Machine, "pane", route.Pane)
		h.broadcastRelayUnrouted(event)
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

// convertUnconfirmedMarker records the origin's retry without closing an owed
// marker. An older binary can still recognise the marker after a rollback.
func (h *HubServer) convertUnconfirmedMarker(ctx context.Context, marker handoffkeepRelayEvent) {
	origin := unconfirmedMarkOriginID(marker.EventID)
	if origin == 0 {
		return
	}
	record, found, err := h.handoffkeep.relayEvent(ctx, origin)
	if err != nil || !found {
		return
	}
	event := relayEventFromRecord(record)
	if event.OwnerLane == "" || event.JobID == "" || isUnconfirmedMarkRecord(record) {
		return
	}
	o := newDeliveryObligation{key: relayEventDedupeKey(record.Kind, event), relayID: record.ID, kind: record.Kind, lane: record.OwnerLane, attempts: record.Attempts}
	store := h.inbox.store
	now := h.now().UTC()
	if record.DeliveredAt == "" {
		err = store.flagObligationRetry(ctx, o, obligationFaultUnconfirmed, marker.EventID, now)
	} else {
		err = h.convertDeliveredMarker(ctx, o, record, marker, now)
	}
	if err != nil {
		h.logger.Warn("legacy unconfirmed marker could not be converted", "event_id", origin, "error", store.loggable("convert marker", err))
	}
}

func (h *HubServer) convertDeliveredMarker(ctx context.Context, newObligation newDeliveryObligation, record, marker handoffkeepRelayEvent, now time.Time) error {
	store := h.inbox.store
	if record.DeliveredTo == relayReplayRetiredMarker+relayUnconfirmedRetireReason {
		newObligation.attempts = max(newObligation.attempts, 1)
	}
	if err := store.createObligation(ctx, newObligation, now, now); err != nil {
		return err
	}
	o, err := store.retryObligationForKey(ctx, newObligation.key)
	if err != nil {
		return err
	}
	if err := h.classifyObligation(ctx, o, record, now); err != nil {
		return err
	}
	h.retireRelayReplay(marker, relayUnconfirmedMarkRetireReason)
	return nil
}
