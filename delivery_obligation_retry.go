package panewire

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"
)

const (
	obligationAckTimeout  = "ack_timeout"
	obligationRetrySource = "obligation"
)

var obligationRetryBackoff = [...]time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}

// A report schedules the next copy. Repeated reports for the same window
// leave its schedule alone. Only claiming a copy advances the retry budget.
func (s *laneInboxStore) flagObligationRetry(parent context.Context, o newDeliveryObligation, fault, marker string, now time.Time) error {
	if err := s.createObligation(parent, o, now, now); err != nil {
		return err
	}
	ctx, done := s.op(parent)
	defer done()
	where, keys := s.eventKeyPredicate(o.key)
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations
		SET spent=CASE WHEN fault='' AND spent<? THEN ? ELSE spent END,
		    attempts_seen=CASE WHEN fault='' AND attempts_seen<? THEN ? ELSE attempts_seen END,
		    next_at=CASE WHEN fault='' THEN ? ELSE next_at END,
		    last_fault_at=CASE WHEN fault='' OR fault='ack_timeout' THEN ? ELSE last_fault_at END,
		    updated_at=CASE WHEN fault='' THEN ? ELSE updated_at END,
		    fault=CASE WHEN fault='' OR fault='ack_timeout' THEN ? ELSE fault END,
		    marker_event_id=CASE WHEN marker_event_id='' THEN ? ELSE marker_event_id END
		WHERE state='owed' AND `+where), append([]any{o.attempts, o.attempts, o.attempts, o.attempts, now.UnixMilli(), now.UnixMilli(), now.UnixMilli(), fault, marker}, keys...)...)
	return err
}

func (s *laneInboxStore) obligationOwnsRetry(parent context.Context, key string) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	where, keys := s.eventKeyPredicate(key)
	var count int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM delivery_obligations WHERE fault<>'' AND `+where), keys...).Scan(&count)
	return count != 0, err
}

// Ordinary replay and source resends hand scheduled copies to the reconciler.
func (h *HubServer) relayRetryIsManaged(key string, relayID int64) bool {
	if h.inbox == nil {
		return false
	}
	store := h.inbox.store
	owned, err := store.obligationOwnsRetry(store.life, key)
	if err != nil {
		h.logger.Warn("relay replay ownership could not be checked", "event_id", relayID, "error", store.loggable("retry ownership", err))
	}
	return owned || err != nil
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

func (h *HubServer) retryObligation(ctx context.Context, o openObligation, record handoffkeepRelayEvent, now time.Time, source string) (bool, error) {
	store := h.inbox.store
	event := relayEventFromRecord(record)
	if record.DeliveredAt != "" || strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) || event.OwnerLane == "" || event.JobID == "" ||
		h.replayAwaitsAppliedReport(record) {
		return false, store.deferObservedObligation(ctx, o, now, now.Add(obligationRecheckMin))
	}
	if h.retireLegacyChatReplay(record) {
		return false, nil
	}
	if settled, err := store.settled(relayEventDedupeKey(record.Kind, event)); err != nil {
		return false, err
	} else if settled {
		return true, store.closeObligation(ctx, o.id, obligationSettled, obligationSettledInbox, now)
	}
	if record.Kind == "lane.event" {
		batch, err := store.isDigestBatch(record.OwnerLane, record.EventID)
		if err != nil {
			return false, err
		}
		if batch {
			if err := store.releaseBatchClaims(record.EventID); err != nil {
				return false, err
			}
			return true, store.closeObligation(ctx, o.id, obligationSettled, obligationSettledDigest, now)
		}
	}

	// A digest must release its claims even when its original body aged out.
	if h.relayReplayRetireReasonAged(record, source != relayReplaySourceIdle) != "" {
		return false, store.deferObservedObligation(ctx, o, now, now.Add(obligationRecheckMin))
	}

	// Re-evaluate the current inbox policy before routing a saved body.
	if h.replayInboxAged(record, false) {
		return false, store.deferObservedObligation(ctx, o, now, now.Add(obligationRecheckMin))
	}

	if o.spent >= relayReplayMaxAttempts || (o.fault != obligationAckTimeout && (o.labelled > 0 || (record.Kind != "lane.event" && o.spent > o.attemptsSeen))) {
		return true, store.exhaustObligation(ctx, o.id, o.fault, now, now.Add(obligationParkRecheck))
	}

	route, agent, routed := h.resolveRelayRoute(record.Kind, event)
	if !routed || route.Sink {
		h.announceUnroutedReplay(record.ID, event)
		return false, store.deferObservedObligation(ctx, o, now, now.Add(obligationRecheckMin))
	}
	key := relayEventDedupeKey(record.Kind, event)
	h.mu.Lock()
	_, claimed := h.relayDedupe[key]
	_, held := h.relayHeld[record.ID]
	claimed = claimed || held
	available := record.Kind != "lane.event" || h.retryLaneAgentReadyLocked(route.Machine, agent)
	if !claimed && available {
		h.relayDedupe[key] = record.ID
	}
	h.mu.Unlock()
	if claimed || !available {
		return false, store.deferObservedObligation(ctx, o, now, now.Add(obligationRecheckMin))
	}
	queued := false
	defer func() {
		if !queued {
			h.forgetRelayEvent(key)
		}
	}()
	if h.refuseInboxOnlyBody(record.Kind, event, record.ID, key) {
		return false, nil
	}
	labelled := 0
	if record.Kind == "lane.event" {
		labelled = 1
		h.rememberLanePersisted(key, record.ID)
		h.rememberLaneEventSHA(key, record.Text)
	}
	next := now.Add(obligationRetryBackoff[min(max(o.spent-o.attemptsSeen, 0), len(obligationRetryBackoff)-1)])
	spent, err := store.spendObligationRetry(ctx, o, labelled, now, next)
	if err != nil || !spent {
		return spent, err
	}

	attempts, bumped := h.bumpObligationAttempts(ctx, record, route, labelled != 0)
	if labelled != 0 && !bumped {
		return false, store.deferUnqueuedLabel(ctx, o.id, o.spent+1, next, now.Add(5*time.Minute))
	}
	// A late ack may have landed while the attempt was being committed.
	latest, found, err := h.handoffkeep.relayEvent(ctx, record.ID)
	if (err != nil || !found) && labelled != 0 {
		return false, errors.Join(err, store.deferUnqueuedLabel(context.WithoutCancel(ctx), o.id, o.spent+1, next, now.Add(obligationRecheckMin)))
	}
	if err != nil || !found || latest.DeliveredAt != "" || strings.HasPrefix(latest.DeliveredTo, relayReplayRetiredMarker) {
		return true, err
	}
	if labelled == 0 {
		queued = h.injectRelayEvent(record.Kind, event, route, agent, nil, record.ID, key)
	} else {
		directive := h.inboxRelayDirective(record.Kind, event, route, record.ID)
		label := relayUnconfirmedLabel(record)
		if source == relayReplaySourceIdle {
			label = relayIdleRedeliveryLabel(record)
		}
		directive.Text = label + directive.Text
		var current bool
		queued, current = h.queueObligationLaneRetry(ctx, record.ID, event, route, agent, directive)
		if !current {
			return false, store.deferUnqueuedLabel(context.WithoutCancel(ctx), o.id, o.spent+1, next, now.Add(obligationRecheckMin))
		}
		if queued {
			h.armRelayAckEvent(record.ID, event.JobID)
		} else {
			h.forgetRelayAckEvent(record.ID, event.JobID)
			h.broadcastRelayUnrouted(event)
		}
	}
	if queued {
		h.broadcastRelayReplayInjected(record, route, attempts, source)
	}
	return true, nil
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
	h.registerRelayAckEventLocked("lane.event", event, route.Machine, route.Pane, id)
	return agent.queueRelay(directive), true
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
	var o openObligation
	err := s.db.QueryRowContext(ctx, s.q(`SELECT id,relay_id,event_key,state,created_at,next_at,fault,spent,labelled,attempts_seen
		FROM delivery_obligations WHERE `+where), keys...).
		Scan(&o.id, &o.relayID, &o.storedKey, &o.state, &o.createdAt, &o.nextAt, &o.fault, &o.spent, &o.labelled, &o.attemptsSeen)
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
