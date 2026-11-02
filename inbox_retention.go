package panewire

import (
	"database/sql"
	"time"
)

// replayTerminal uses explicit receiver evidence. Interrupt read_at alone is
// not proof: put sets it before the node has confirmed the injection.
func (s *laneInboxStore) replayTerminal(key string) (bool, error) {
	ctx, done := s.op(s.life)
	defer done()
	var terminal bool
	err := s.db.QueryRowContext(ctx, s.q(`SELECT (acked_at>0 OR delivered_at>0 OR (class='inbox' AND read_at>0 AND must_ack=0)) FROM lane_inbox WHERE event_key=?`), s.storedKey(key)).Scan(&terminal)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return terminal, err
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
 AND (acked_at>0 OR delivered_at>0 OR (class='inbox' AND read_at>0))
 AND NOT `+laneInboxUnread+`
 AND created_at<? AND read_at<? AND acked_at<? AND delivered_at<?
 AND NOT (acked_at=0 AND ((hold_until>0 AND digest_batch='') OR (digest_at>0 AND digest_batch='')))
 AND NOT EXISTS (SELECT 1 FROM lane_inbox_batches b WHERE b.sent=0 AND (b.id=lane_inbox.digest_batch OR b.id=lane_inbox.escalation_batch))`), now.Add(-time.Duration(policy.Days)*24*time.Hour).UnixMilli(), policy.PerLane, now.Add(-relayReplayMaxAge).UnixMilli(), now.Add(-relayReplayMaxAge).UnixMilli(), now.Add(-relayReplayMaxAge).UnixMilli(), now.Add(-relayReplayMaxAge).UnixMilli())
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
func (s *laneInboxStore) recordDelivery(key string, now time.Time) error {
	ctx, done := s.op(s.life)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE lane_inbox SET delivered_at=CASE WHEN delivered_at=0 THEN ? ELSE delivered_at END WHERE event_key=?`), now.UnixMilli(), s.storedKey(key))
	return err
}
