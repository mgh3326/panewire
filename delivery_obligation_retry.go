package panewire

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"
)

const obligationRetrySource = "obligation"

var obligationRetryBackoff = [...]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

// A report records the fault and, the first time, schedules the next copy.
// Repeated reports for the same window leave the schedule alone. Only claiming
// a copy advances the retry budget. The fault changes as
// obligationFaultReplaces says, on an exhausted row too: a working expiry that
// ends the last budget copy still earns the revive. A fault that replaces the
// stored one also records where it happened, and a maybe-typed report makes the
// row due a notice (noticedAtReport).
func (s *laneInboxStore) flagObligationRetry(parent context.Context, o newDeliveryObligation, fault, marker string, now time.Time) error {
	if err := s.createObligation(parent, o, now, now); err != nil {
		return err
	}
	ctx, done := s.op(parent)
	defer done()
	where, keys := s.eventKeyPredicate(o.key)
	replace, replaced := obligationFaultReplaces(fault)
	at := now.UnixMilli()
	// A fault that names a destination records it: the incident is keyed on the
	// machine the fault happened on, which a later route may not be.
	var destination string
	var destined []any
	if o.machine != "" {
		destination = `machine=CASE WHEN ` + replace + ` THEN ? ELSE machine END,
		    pane=CASE WHEN ` + replace + ` THEN ? ELSE pane END,`
		destined = slices.Concat(replaced, []any{utf8PrefixWithin(o.machine, 255)}, replaced, []any{utf8PrefixWithin(o.pane, 255)})
	}
	// The due mark is set with the fault: a pass that settles the row between two
	// writes would leave a notice that is never due.
	due, dued := obligationReportDue(fault, at)
	// fault is assigned after every CASE that reads it: MySQL reads an assigned
	// column's new value.
	args := slices.Concat([]any{o.attempts, o.attempts, o.attempts, o.attempts, at}, replaced, []any{at, at},
		destined, dued, replaced, []any{fault, marker}, keys)
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations
		SET spent=CASE WHEN fault='' AND spent<? THEN ? ELSE spent END,
		    attempts_seen=CASE WHEN fault='' AND attempts_seen<? THEN ? ELSE attempts_seen END,
		    next_at=CASE WHEN fault='' THEN ? ELSE next_at END,
		    last_fault_at=CASE WHEN `+replace+` THEN ? ELSE last_fault_at END,
		    updated_at=CASE WHEN fault='' THEN ? ELSE updated_at END,
		    `+destination+`
		    `+due+`
		    fault=CASE WHEN `+replace+` THEN ? ELSE fault END,
		    marker_event_id=CASE WHEN marker_event_id='' THEN ? ELSE marker_event_id END
		WHERE `+obligationOpen+` AND `+where), args...)
	return err
}

// retryOwnerOf returns the fault scheduled on the key's obligation, if one
// is, and when it was last recorded.
func (s *laneInboxStore) retryOwnerOf(parent context.Context, key string) (fault string, faultAt int64, err error) {
	ctx, done := s.op(parent)
	defer done()
	where, keys := s.eventKeyPredicate(key)
	err = s.db.QueryRowContext(ctx, s.q(`SELECT fault,last_fault_at FROM delivery_obligations WHERE fault<>'' AND `+where), keys...).Scan(&fault, &faultAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, nil
	}
	return fault, faultAt, err
}

// Ordinary replay and source resends hand scheduled copies to the reconciler.
func (h *HubServer) relayRetryIsManaged(key string, relayID int64) bool {
	managed, _ := h.relayRetryOwner(key, relayID)
	return managed
}

// relayRetryOwner reports whether a scheduled retry owns the row, and whether
// its receipt age must not retire it: a working-expired row inside its drop
// window may still get the revived copy, and the node held it up to
// relay.cmd_hold_max since its receipt. A failed check proves neither, so the
// row stays owned and open.
func (h *HubServer) relayRetryOwner(key string, relayID int64) (managed, heldForIdle bool) {
	if h.inbox == nil {
		return false, false
	}
	store := h.inbox.store
	fault, faultAt, err := store.retryOwnerOf(store.life, key)
	if err != nil {
		h.logger.Warn("relay replay ownership could not be checked", "event_id", relayID, "error", store.loggable("retry ownership", err))
		return true, true
	}
	return fault != "", fault == relayDroppedWorkingExpired && dropWindowOpen(h.now().UTC(), faultAt)
}

// An aged unconfirmed batch stays with the reconciler so its claim release
// can complete before any replay retirement.
func (h *HubServer) replayDefersUnconfirmedBatch(record handoffkeepRelayEvent) bool {
	if record.Kind != "lane.event" {
		return false
	}
	batch, err := h.inbox.store.isDigestBatch(record.OwnerLane, record.EventID)
	if err != nil {
		h.logger.Warn("unconfirmed replay batch lookup failed", "event_id", record.ID)
	}
	return batch || err != nil
}

// spendObligationRetry claims one scheduled attempt with a compare-and-swap.
// The write commits before any handoffkeep call or queue operation.
func (s *laneInboxStore) spendObligationRetry(parent context.Context, o openObligation, labelled int, now, next time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations
		SET spent=spent+1,labelled=labelled+?,next_at=?,updated_at=?
		WHERE id=? AND state='owed' AND spent=? AND next_at=? AND fault=? AND labelled=?`),
		labelled, next.UnixMilli(), now.UnixMilli(), o.id, o.spent, o.nextAt, o.fault, o.labelled)
	if err != nil {
		return false, err
	}
	count, err := s.dialect.rowsMatched(result)
	return count == 1, err
}

// retryObligation applies the fault policy to an owed row, or to a row whose
// revive was requested: it ends the retries, waits, or queues one copy. A
// revived copy comes first and is not gated on the budget.
func (h *HubServer) retryObligation(ctx context.Context, o openObligation, record handoffkeepRelayEvent, now time.Time) error {
	store := h.inbox.store
	event := relayEventFromRecord(record)
	lane := record.Kind == "lane.event"
	revive := lane && o.revivable(now)
	// What blocks a revive is brief: the node's hold, an open send, a
	// registration.
	blocked := obligationRecheckMin
	if revive {
		blocked = obligationReviveRecheck
	}
	wait := func(delay time.Duration) error {
		return store.deferObservedObligation(ctx, o, now, now.Add(delay))
	}
	if record.DeliveredAt != "" || strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) || event.OwnerLane == "" || event.JobID == "" {
		return wait(obligationRecheckMin)
	}
	if h.replayAwaitsAppliedReport(record) {
		return wait(blocked)
	}
	if h.retireLegacyChatReplay(record) {
		return nil
	}
	if settled, err := store.settled(relayEventDedupeKey(record.Kind, event)); err != nil {
		return err
	} else if settled {
		return store.closeObligation(ctx, o.id, obligationSettled, obligationSettledInbox, now)
	}
	if lane {
		batch, err := store.isDigestBatch(record.OwnerLane, record.EventID)
		if err != nil {
			return err
		}
		if batch {
			if err := store.releaseBatchClaims(record.EventID); err != nil {
				return err
			}
			return store.closeObligation(ctx, o.id, obligationSettled, obligationSettledDigest, now)
		}
	}
	exhaust := func() error {
		return store.exhaustObservedObligation(ctx, o, now, now.Add(obligationParkRecheck))
	}
	// A working-expired row has no budget retry. It ends here, ahead of the
	// inbox policy, which judges a body about to be sent.
	if lane && !revive && classifyObligationFault(o.fault) == obligationWorkingExpired {
		return exhaust()
	}

	// A digest must release its claims even when its original body aged out.
	// A revive is bounded by its drop instead of its receipt (revivable).
	if h.relayReplayRetireReasonAged(record, !revive) != "" {
		// No further copy of an aged row is queued: its retries end here.
		if err := store.endUnprovenNotice(ctx, o.id, now); err != nil {
			return err
		}
		return wait(obligationRecheckMin)
	}

	// Re-evaluate the current inbox policy before routing a saved body.
	if h.replayInboxAfterAgeGate(record) {
		return wait(obligationRecheckMin)
	}

	if !revive && o.retriesDone(record.Kind) {
		// The node still holds a row whose send was rejected. Its drop says
		// whether anything was typed, so the row is not ended ahead of it.
		if o.fault == obligationFaultPromptUnproven && h.relayHeldByNode(record.ID) {
			return wait(blocked)
		}
		return exhaust()
	}

	route, agent, routed := h.resolveRelayRoute(record.Kind, event)
	if !routed || route.Sink {
		// Nothing is queued and nothing is spent. A safe fault is renamed
		// unrouted; any other keeps its name, which still decides its copies.
		h.announceUnroutedReplay(record.ID, event)
		if !revive && classifyObligationFault(o.fault) == obligationSafe {
			return store.markObligationUnrouted(ctx, o, now, now.Add(obligationRecheckMin))
		}
		return wait(obligationRecheckMin)
	}
	key := relayEventDedupeKey(record.Kind, event)
	h.mu.Lock()
	_, claimed := h.relayDedupe[key]
	_, held := h.relayHeld[record.ID]
	claimed = claimed || held
	available := !lane || h.retryLaneAgentReadyLocked(route.Machine, agent)
	if !claimed && available {
		h.relayDedupe[key] = record.ID
	}
	h.mu.Unlock()
	if claimed || !available {
		return wait(blocked)
	}
	releaseClaim := true
	defer func() {
		if releaseClaim {
			h.mu.Lock()
			if h.relayDedupe[key] == record.ID {
				delete(h.relayDedupe, key)
			}
			h.mu.Unlock()
		}
	}()

	if h.refuseInboxOnlyBody(record.Kind, event, record.ID, key) {
		releaseClaim = false // The inbox-only path already released it.
		return nil
	}
	labelled := 0
	if lane {
		labelled = 1
		h.rememberLanePersisted(key, record.ID)
		h.rememberLaneEventSHA(key, record.Text)
	}
	next := now.Add(obligationRetryBackoff[min(max(o.spent-o.attemptsSeen, 0), len(obligationRetryBackoff)-1)])
	claim := store.spendObligationRetry
	if revive {
		next = now.Add(obligationReviveBackoff)
		claim = func(ctx context.Context, o openObligation, _ int, now, next time.Time) (bool, error) {
			return store.spendObligationRevive(ctx, o, now, next)
		}
	}
	spent, err := claim(ctx, o, labelled, now, next)
	if err != nil || !spent {
		return err
	}

	releaseLabel := func(delay time.Duration, cause error) error {
		release := context.WithoutCancel(ctx)
		if revive {
			return errors.Join(cause, store.releaseObligationRevive(release, o, next, now.Add(delay)))
		}
		return errors.Join(cause, store.deferUnqueuedLabel(release, o.id, o.spent+1, next, now.Add(delay)))
	}
	attempts, bumped := h.bumpObligationAttempts(ctx, record, route, labelled != 0)
	if labelled != 0 && !bumped {
		return releaseLabel(5*time.Minute, nil)
	}
	// A late ack may have landed while the attempt was being committed.
	latest, found, err := h.handoffkeep.relayEvent(ctx, record.ID)
	if (err != nil || !found) && labelled != 0 {
		return releaseLabel(obligationRecheckMin, err)
	}
	if err != nil || !found {
		return errors.Join(err, store.releaseUnqueuedJobRetry(context.WithoutCancel(ctx), o, next, h.now().UTC().Add(obligationRecheckMin)))
	}
	if latest.DeliveredAt != "" || strings.HasPrefix(latest.DeliveredTo, relayReplayRetiredMarker) {
		return h.classifyObligation(ctx, o, latest, now)
	}
	source := obligationRetrySource
	var queued bool
	if labelled == 0 {
		releaseClaim = false // injectRelayEvent owns the claim from here.
		queued = h.injectRelayEvent(record.Kind, event, route, agent, nil, record.ID, key)
	} else {
		directive := h.inboxRelayDirective(record.Kind, event, route, record.ID)
		label := relayUnconfirmedLabel(record)
		if revive {
			source = relayReplaySourceIdle
			// Nothing was typed for a working expiry, so its copy warns of no
			// duplicate.
			if o.fault == relayDroppedWorkingExpired {
				label = relayIdleRedeliveryLabel(record)
			}
		}
		directive.Text = label + directive.Text
		var current bool
		queued, current = h.queueObligationLaneRetry(ctx, record.ID, event, route, agent, directive)
		if !current {
			return releaseLabel(obligationRecheckMin, nil)
		}
		if queued {
			releaseClaim = false
			h.armRelayAckEvent(record.ID, event.JobID)
		} else {
			h.broadcastRelayUnrouted(event)
			return releaseLabel(obligationRecheckMin, nil)
		}
	}
	if queued {
		h.broadcastRelayReplayInjected(record, route, attempts, source)
	}
	return nil
}

// relayHeldByNode reports whether a node says it holds the row.
func (h *HubServer) relayHeldByNode(id int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, held := h.relayHeld[id]
	return held
}

// The replay lock covers only validation and the nonblocking queue write.
func (h *HubServer) queueObligationLaneRetry(ctx context.Context, id int64, event hubJobEventPayload, route reportRelayRoute, agent *hubAgent, directive hubRelayInjectEvent) (queued, current bool) {
	h.replayMu.RLock()
	defer h.replayMu.RUnlock()
	h.mu.Lock()
	defer h.mu.Unlock()
	if ctx.Err() != nil || !h.retryLaneAgentReadyLocked(route.Machine, agent) {
		return false, false
	}
	if !agent.queueRelay(directive) {
		return false, true
	}
	h.registerRelayAckEventLocked("lane.event", event, route.Machine, route.Pane, id)
	return true, true
}

func (h *HubServer) retryLaneAgentReadyLocked(machine string, agent *hubAgent) bool {
	node := h.nodes[machine]
	return node != nil && node.agent == agent && !node.appliedPending
}

func (h *HubServer) bumpObligationAttempts(ctx context.Context, record handoffkeepRelayEvent, route reportRelayRoute, labelled bool) (int, bool) {
	if labelled && record.Attempts >= relayReplayMaxAttempts {
		return record.Attempts, true
	}
	limit := 1
	if labelled {
		limit = 4
	}
	attempts := record.Attempts
	request := h.relayEventRequest(record.Kind, relayEventFromRecord(record), route)
	for i := 0; i < limit && ctx.Err() == nil; i++ {
		stored, status, err := h.handoffkeep.appendEvent(ctx, request)
		if err == nil && status == http.StatusOK && stored.ID == record.ID {
			attempts = stored.Attempts
			if !labelled || attempts >= relayReplayMaxAttempts {
				return attempts, true
			}
		}
	}
	h.logger.Warn("obligation retry attempt was not recorded in handoffkeep", "event_id", record.ID, "labelled", labelled)
	return attempts, false
}

func (s *laneInboxStore) retryObligationForKey(parent context.Context, key string) (openObligation, error) {
	ctx, done := s.op(parent)
	defer done()
	where, keys := s.eventKeyPredicate(key)
	o, err := scanOpenObligation(s.db.QueryRowContext(ctx, s.q(`SELECT `+openObligationColumns+` FROM delivery_obligations WHERE `+where), keys...))
	if errors.Is(err, sql.ErrNoRows) {
		return o, nil
	}
	return o, err
}

// A failed ceiling write proves no directive was queued. Keep the spent
// attempt and release only the reservation for the pane-visible label.
func (s *laneInboxStore) deferUnqueuedLabel(parent context.Context, id int64, spent int, next, due time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations
		SET labelled=labelled-1,next_at=? WHERE id=? AND state='owed' AND spent=? AND next_at=?`),
		due.UnixMilli(), id, spent, next.UnixMilli())
	return err
}

// releaseUnqueuedJobRetry returns the one-copy allowance when the final proof
// read failed before queueing. The handoffkeep attempt counter stays a mirror.
func (s *laneInboxStore) releaseUnqueuedJobRetry(parent context.Context, o openObligation, next, due time.Time) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET spent=spent-1,next_at=?
		WHERE id=? AND state='owed' AND spent=? AND labelled=? AND next_at=?`),
		due.UnixMilli(), o.id, o.spent+1, o.labelled, next.UnixMilli())
	return err
}
