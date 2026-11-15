package panewire

import (
	"context"
	"time"
)

// Inbox-only lanes are never typed a body (hk 3602). receiveInbox files what
// arrives for one, but a body can still reach the injection path without it:
// a record an older hub stored as an interrupt keeps its durable class on a
// producer retry or a replay, and a hub batch row can be replayed. So the
// rule is held again at the last point before typing: every injection of a
// relay body asks refuseInboxOnlyBody first, whatever the item's stored class.
// See docs/quiet-delivery.md#inbox-only-lanes.

// refuseInboxOnlyBody is the delivery-time inbox-only check. It reports true
// when the body of event (kind) must not be typed: its receiving lane is
// inbox-only now and the event is not operator chat or ingress. The event is
// then filed unread in the lane's inbox instead -- a hub batch row files the
// items it names -- its durable handoffkeep row is closed as delivered to the
// inbox, and the lane gets its wake. The dedupe claim is released either way:
// a resend finds the filed item, and a failed filing leaves the row to replay.
func (h *HubServer) refuseInboxOnlyBody(kind string, event hubJobEventPayload, eventID int64, key string) bool {
	if h.inbox == nil || inboxOnlyExempt(event) {
		return false
	}
	lane := h.receivingLane(kind, event)
	class := h.inbox.policy.inboxOnlyClass(lane, h.now())
	if class == "" {
		return false
	}
	defer h.forgetRelayEvent(key)
	now := h.now().UTC()
	var items []laneInboxItem
	var batch bool
	var err error
	if kind == "lane.event" {
		batch, err = h.inbox.store.hasBatch(lane, event.EventID)
	}
	if err == nil && batch {
		items, err = h.inbox.store.fileUnread(now, `lane=? AND (digest_batch=? OR escalation_batch=?)`, lane, event.EventID, event.EventID)
	} else if err == nil {
		items, err = h.inbox.store.fileEventUnread(relayEventDedupeKey(kind, event), lane, kind, event, now)
	}
	if err != nil {
		h.logger.Error("inbox-only lane: body not typed and not filed; replay retries", "lane", lane, "kind", kind, "error", h.inbox.store.loggable("inbox-only filing", err))
		return true
	}
	h.logger.Warn("inbox-only lane: body filed in the inbox instead of typed", "lane", lane, "kind", kind, "event_id", eventID, "items", len(items))
	if h.handoffkeep != nil && eventID > 0 {
		if err := h.handoffkeep.markDelivered(context.Background(), eventID, "hub", "inbox"); err != nil {
			h.logger.Warn("inbox-only filing not recorded on its relay row; replay files it again", "event_id", eventID)
		}
	}
	if inboxWakeFiledWorthy(items) {
		h.noteInboxWakeArrival(lane, class)
	}
	return true
}

// fileInboxOnlyBatch files a pending hub digest or hold batch for an
// inbox-only lane instead of typing it: the items it claims become unread
// inbox items, the batch is retired untyped, and the lane gets its wake.
func (h *HubServer) fileInboxOnlyBatch(lane, class, id string) error {
	items, err := h.inbox.store.fileUnread(h.now().UTC(), `lane=? AND (digest_batch=? OR escalation_batch=?)`, lane, id, id)
	if err != nil {
		return err
	}
	if inboxWakeFiledWorthy(items) {
		h.noteInboxWakeArrival(lane, class)
	}
	return h.inbox.store.quarantineBatch(id, inboxOnlyBatchReason)
}

// inboxOnlyBatchReason is the quarantine reason of a batch filed untyped.
const inboxOnlyBatchReason = "inbox-only lane: items filed unread"

// inboxWakeFiledWorthy reports whether items filed in place of a typed body
// ask for a wake: each would have been typed, so any that is not a declared
// info record does (inboxWakeWorthy).
func inboxWakeFiledWorthy(items []laneInboxItem) bool {
	for _, item := range items {
		if inboxWakeWorthy("interrupt", false, item.Kind, item.Event) {
			return true
		}
	}
	return false
}

// hasBatch reports whether id names a hub digest, escalation or hold batch
// for lane.
func (s *laneInboxStore) hasBatch(lane, id string) (bool, error) {
	ctx, done := s.op(s.life)
	defer done()
	var n int
	if err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM lane_inbox_batches WHERE id=? AND lane=?`), id, lane).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// fileEventUnread files one event as an unread inbox item: a new row is
// inserted as one, and a row stored earlier under another class is turned
// into one (fileUnread). It returns the items it filed.
func (s *laneInboxStore) fileEventUnread(key, lane, kind string, event hubJobEventPayload, now time.Time) ([]laneInboxItem, error) {
	item, err := s.putFrom("", key, lane, kind, "inbox", event, now, 0, 0)
	if err != nil {
		return nil, err
	}
	if item.Fresh {
		return []laneInboxItem{item}, nil
	}
	return s.fileUnread(now, `event_key=?`, s.storedKey(key))
}

// fileUnread turns the rows matching where -- those the receiver does not
// already have (laneInboxSettled) and that are not plain inbox items yet --
// into unread inbox items with no deadline, digest or hold. An interrupt's
// insert-time read is cleared, so the item counts as unread. It returns the
// rows it changed, as they were.
func (s *laneInboxStore) fileUnread(now time.Time, where string, args ...any) ([]laneInboxItem, error) {
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, s.q(`SELECT `+laneInboxColumns+` FROM lane_inbox WHERE `+where+` AND NOT `+laneInboxSettled+` AND (class<>'inbox' OR due_at>0 OR digest_at>0 OR hold_until>0)`), args...)
	if err != nil {
		return nil, err
	}
	var items []laneInboxItem
	var ids []int64
	for rows.Next() {
		item, err := scanLaneInbox(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		items = append(items, item)
		ids = append(ids, item.ID)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err := s.updateIDs(ctx, tx, `UPDATE lane_inbox SET class='inbox',read_at=CASE WHEN must_ack=0 AND class<>'inbox' THEN 0 ELSE read_at END,due_at=0,digest_at=0,hold_until=0,delivery_stage=?,delivery_at=?`, []any{laneInboxStageInboxOnly, now.UnixMilli()}, ids); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if len(ids) > 0 {
		close(s.notify)
		s.notify = make(chan struct{})
	}
	return items, nil
}
