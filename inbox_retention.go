package panewire

import (
	"database/sql"
	"time"
)

// laneInboxTerminal is shared by retention and replay. Must-ack work completes
// only on ack; automatic interrupt read-on-insert never completes an item.
const laneInboxTerminal = `(superseded_at>0 OR acked_at>0 OR (must_ack=0 AND (delivered_at>0 OR (class='inbox' AND read_at>0))))`

// laneInboxSettled is the evidence that the receiver already has an item:
// superseded, acked, confirmed delivered, or read by the receiver. A read
// stamped at insert (a non-must-ack interrupt) is not the receiver's read, so
// it never counts.
const laneInboxSettled = `(superseded_at>0 OR acked_at>0 OR delivered_at>0 OR (read_at>0 AND (must_ack=1 OR class='inbox')))`

// settled reports whether the inbox already holds the event key as an item
// the receiver has been given. An absent item is not settled.
func (s *laneInboxStore) settled(key string) (bool, error) {
	ctx, done := s.op(s.life)
	defer done()
	var settled bool
	err := s.db.QueryRowContext(ctx, s.q(`SELECT `+laneInboxSettled+` FROM lane_inbox WHERE event_key=?`), s.storedKey(key)).Scan(&settled)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return settled, err
}

// prune keeps all unread work, pending deliveries, and evidence touched within
// the replay window. Ranking includes every item, so K means newest K per lane,
// not K completed items in addition to an arbitrarily large unread backlog.
func (s *laneInboxStore) prune(now time.Time, policy inboxRetentionPolicy) (int64, error) {
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if s.dialect == laneInboxSQLite {
		if _, err := tx.ExecContext(ctx, `UPDATE lane_inbox_id_floor SET floor=max(floor,COALESCE((SELECT max(id) FROM lane_inbox),0)) WHERE singleton=1`); err != nil {
			return 0, err
		}
	}
	result, err := tx.ExecContext(ctx, s.q(`WITH ranked AS (
 SELECT id, ROW_NUMBER() OVER (PARTITION BY lane ORDER BY created_at DESC,id DESC) AS position FROM lane_inbox
 ) DELETE FROM lane_inbox WHERE (created_at<? OR id IN (SELECT id FROM ranked WHERE position>?))
 AND `+laneInboxTerminal+`
 AND NOT `+laneInboxUnread+`
 AND created_at<? AND read_at<? AND acked_at<? AND delivered_at<? AND superseded_at<?
 AND NOT (acked_at=0 AND superseded_at=0 AND ((hold_until>0 AND digest_batch='') OR (digest_at>0 AND digest_batch='')))
 AND NOT EXISTS (SELECT 1 FROM lane_inbox_batches b WHERE b.sent=0 AND (b.id=lane_inbox.digest_batch OR b.id=lane_inbox.escalation_batch))`), now.Add(-time.Duration(policy.Days)*24*time.Hour).UnixMilli(), policy.PerLane, now.Add(-relayReplayMaxAge).UnixMilli(), now.Add(-relayReplayMaxAge).UnixMilli(), now.Add(-relayReplayMaxAge).UnixMilli(), now.Add(-relayReplayMaxAge).UnixMilli(), now.Add(-relayReplayMaxAge).UnixMilli())
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return count, tx.Commit()
}

// pruneInbox owns its schedule under flushMu, including when digest delivery
// is disabled or backing off. A failed prune retries at the next hourly pass.
func (h *HubServer) pruneInbox() {
	if h.inbox == nil || !h.inbox.flushMu.TryLock() {
		return
	}
	defer h.inbox.flushMu.Unlock()
	now := h.now().UTC()
	if h.closeCtx.Err() != nil || now.Before(h.inbox.nextPrune) {
		return
	}
	h.inbox.nextPrune = now.Add(time.Hour)
	if _, err := h.inbox.store.prune(now, h.inbox.policy.Retention); err != nil {
		h.logger.Warn("inbox retention prune failed", "error", h.inbox.store.loggable("retention", err))
	}
}

// recordDelivery preserves receiver confirmation independently of handoffkeep.
// The confirmation is also the row's final delivery stage, so a sender reading
// ack-state sees that its event reached the pane.
func (s *laneInboxStore) recordDelivery(key, machine, pane string, now time.Time) error {
	ctx, done := s.op(s.life)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET delivered_at=CASE WHEN delivered_at=0 THEN ? ELSE delivered_at END,delivery_stage=?,delivery_reason='',delivery_machine=?,delivery_pane=?,delivery_at=? WHERE event_key=?`), now.UnixMilli(), laneInboxStageDelivered, machine, pane, now.UnixMilli(), s.storedKey(key))
	return err
}

// recordStage moves a row to a stage the hub learned after it queued the
// event: held, typed, unconfirmed or dropped. A delivered row is final, so no
// later report can move it backwards.
func (s *laneInboxStore) recordStage(key, stage, reason, machine, pane string, now time.Time) error {
	if stage == "" || stage == laneInboxStageDelivered {
		return nil
	}
	ctx, done := s.op(s.life)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET delivery_stage=?,delivery_reason=?,delivery_machine=?,delivery_pane=?,delivery_at=? WHERE event_key=? AND delivery_stage<>?`), stage, reason, machine, pane, now.UnixMilli(), s.storedKey(key), laneInboxStageDelivered)
	return err
}

// recordStageByRelayID is recordStage for a node report that names the
// durable handoffkeep row (relay.held, relay.released, relay.dropped).
func (s *laneInboxStore) recordStageByRelayID(relayID int64, stage, reason, machine, pane string, now time.Time) error {
	if relayID <= 0 || stage == "" || stage == laneInboxStageDelivered {
		return nil
	}
	ctx, done := s.op(s.life)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET delivery_stage=?,delivery_reason=?,delivery_machine=?,delivery_pane=?,delivery_at=? WHERE relay_id=? AND delivery_stage<>?`), stage, reason, machine, pane, now.UnixMilli(), relayID, laneInboxStageDelivered)
	return err
}

// linkRelayID ties a row to the durable handoffkeep row the hub persisted for
// it, so the node reports that carry only that id can update the stage.
func (s *laneInboxStore) linkRelayID(key string, relayID int64) error {
	if relayID <= 0 {
		return nil
	}
	ctx, done := s.op(s.life)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET relay_id=? WHERE event_key=? AND relay_id=0`), relayID, s.storedKey(key))
	return err
}

// holds reports whether the inbox stores an item under the event key, whatever
// its state.
func (s *laneInboxStore) holds(key string) (bool, error) {
	ctx, done := s.op(s.life)
	defer done()
	var n int
	if err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM lane_inbox WHERE event_key=?`), s.storedKey(key)).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}
