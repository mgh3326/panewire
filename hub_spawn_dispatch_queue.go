package panewire

import (
	"context"
)

// hk 7125: the node's queue is counted once, at an attempt's first scheduling
// pass, and stored on the operation (spawn_operations.open_dispatches and
// queued_spawns). A landing reads the record, so the tags survive a dispatch that
// has already ended, and a hub restart. Counting at the first pass, not at the
// send, is what lets a spawn that waits behind others carry the queue it joined.

// recordSpawnQueueAtFirstPass counts and stores the queue of task's attempt. It
// runs for a bound attempt whose record is still unset, and it runs before the
// scheduler decides whether the attempt is busy, so a waiting attempt records the
// queue it joined. inFlight is the node's dispatches in flight now; dispatching
// is true when this pass sends the attempt, which then counts itself. A failed
// read or write is logged and leaves the record unset, so scheduling continues
// and the trace has no queue tags.
func (h *HubServer) recordSpawnQueueAtFirstPass(ctx context.Context, task hubSpawnTask, inFlight int, dispatching bool) {
	queued, err := h.inbox.store.countSpawnQueueAhead(ctx, task)
	if err != nil {
		h.logger.Warn("hub spawn queue unreadable; the trace will not carry it", "request_id", task.Request.RequestID, "error", err)
		return
	}
	open := inFlight
	if dispatching {
		open++
	}
	if err := h.inbox.store.recordSpawnDispatchQueue(ctx, task.Request.RequestID, task.Attempt, open, queued); err != nil {
		h.logger.Warn("hub spawn queue unrecorded; the trace will not carry it", "request_id", task.Request.RequestID, "error", err)
	}
}

// countSpawnQueueAhead counts the other operations bound to task's machine that
// are in flight or waiting and were created before it: the queue it joined. A
// busy one is counted as waiting, the same as one a node slot would take.
func (s *laneInboxStore) countSpawnQueueAhead(ctx context.Context, task hubSpawnTask) (int, error) {
	created := task.Created.UnixMilli()
	var queued int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT COUNT(*) FROM spawn_operations WHERE bound_machine=? AND status IN (`+spawnBoundInFlightStatuses+`) AND (created_ms<? OR (created_ms=? AND request_id<?))`), task.Node.Machine, created, created, task.Request.RequestID).Scan(&queued)
	return queued, err
}

// recordSpawnDispatchQueue stores the queue of an attempt's first pass. Only an
// unrecorded attempt is written, so a later pass never moves the count.
func (s *laneInboxStore) recordSpawnDispatchQueue(ctx context.Context, requestID string, attempt, open, queued int) error {
	_, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET open_dispatches=?,queued_spawns=? WHERE request_id=? AND attempt=? AND open_dispatches<0`), open, queued, requestID, attempt)
	return err
}

// spawnDispatchQueue reads what a first pass recorded for an attempt. recorded
// is false when none did, so the trace carries no queue tags.
func (s *laneInboxStore) spawnDispatchQueue(ctx context.Context, requestID string, attempt int) (open, queued int, recorded bool, err error) {
	err = s.db.QueryRowContext(ctx, s.q(`SELECT open_dispatches,queued_spawns FROM spawn_operations WHERE request_id=? AND attempt=?`), requestID, attempt).Scan(&open, &queued)
	if err != nil {
		return 0, 0, false, err
	}
	return open, queued, open >= 0, nil
}
