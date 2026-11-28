package panewire

import (
	"context"
)

// hk 7125: the node's queue is counted once, at an attempt's first scheduling
// pass, and stored on the operation (spawn_operations.open_dispatches and
// queued_spawns). A landing reads the record, so the tags survive a dispatch that
// has already ended, and a hub restart. A bind resets the record, so a rebound
// attempt never carries another binding's counts. Counting at the first pass, not
// at the send, is what lets a spawn that waits behind others carry the queue it
// joined.

// recordSpawnQueue stores the queue an attempt's first pass counted. It runs
// outside the scheduler's lock and before the attempt's send starts, so the
// landing reads the record. A failed write is logged; the record stays unset and
// the trace has no queue tags.
func (h *HubServer) recordSpawnQueue(ctx context.Context, task hubSpawnTask, open, queued int) {
	if err := h.inbox.store.recordSpawnDispatchQueue(ctx, task.Request.RequestID, task.Node.RequestID, task.Attempt, open, queued); err != nil {
		h.logger.Warn("hub spawn queue unrecorded; the trace will not carry it", "request_id", task.Request.RequestID, "error", err)
	}
}

// countSpawnQueueAhead counts the other operations bound to task's machine that
// are in flight or waiting and were created before it: the queue it joined. A
// busy one is counted as waiting, the same as one a node slot would take.
func (s *laneInboxStore) countSpawnQueueAhead(parent context.Context, task hubSpawnTask) (int, error) {
	ctx, done := s.op(parent)
	defer done()
	created := task.Created.UnixMilli()
	var queued int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT COUNT(*) FROM spawn_operations WHERE bound_machine=? AND status IN (`+spawnBoundInFlightStatuses+`) AND (created_ms<? OR (created_ms=? AND request_id<?))`), task.Node.Machine, created, created, task.Request.RequestID).Scan(&queued)
	return queued, err
}

// recordSpawnDispatchQueue stores the queue of an attempt's first pass. Only an
// unrecorded attempt of the named node binding is written, so a later pass never
// moves the count, and a write for an earlier binding matches no row once the
// attempt is rebound (node_id is unique per binding).
func (s *laneInboxStore) recordSpawnDispatchQueue(parent context.Context, requestID, nodeID string, attempt, open, queued int) error {
	ctx, done := s.op(parent)
	defer done()
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET open_dispatches=?,queued_spawns=? WHERE request_id=? AND attempt=? AND node_id=? AND open_dispatches<0`), open, queued, requestID, attempt, nodeID)
	return err
}

// spawnDispatchQueue reads what a first pass recorded for an attempt. recorded
// is false when none did, so the trace carries no queue tags.
func (s *laneInboxStore) spawnDispatchQueue(parent context.Context, requestID string, attempt int) (open, queued int, recorded bool, err error) {
	ctx, done := s.op(parent)
	defer done()
	err = s.db.QueryRowContext(ctx, s.q(`SELECT open_dispatches,queued_spawns FROM spawn_operations WHERE request_id=? AND attempt=?`), requestID, attempt).Scan(&open, &queued)
	if err != nil {
		return 0, 0, false, err
	}
	return open, queued, open >= 0, nil
}
