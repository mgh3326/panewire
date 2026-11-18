package panewire

import (
	"context"
	"time"
)

// The hub store's retention pass (hk 5297; design pw-4898-hub-store-mysql §1,
// operator answer dr-4898-1). pruneInbox runs it hourly, with the inbox prune.
// Each table deletes at most hubStoreRetentionBatch rows a pass, in its own
// short transaction. A row goes only past its window, and only when no live
// row still points at it. A non-terminal row is never deleted.

// hubStoreRetentionBatch bounds the rows each table deletes in one pass.
const hubStoreRetentionBatch = 1000

const (
	// A grant or start-by row is needed only while its attempt can still be
	// granted or fenced, which ends with the operation's terminal state. The day
	// is margin past that.
	spawnAttemptFenceRetention = 24 * time.Hour
	spawnOperationRetention    = 30 * 24 * time.Hour
	consultRetention           = 30 * 24 * time.Hour
	poolActionRetention        = 30 * 24 * time.Hour
)

// spawnTerminalStatuses is the SQL list of spawn statuses nothing moves again.
// 'retryable' is not in it: the expiry sweep returns it to accepted or gives
// it up. spawnPending is the complement for the live statuses.
const spawnTerminalStatuses = `'succeeded','failed','refused','given_up','cancelled'`

// spawnTerminalStatus reports whether status is one of spawnTerminalStatuses.
func spawnTerminalStatus(status string) bool {
	switch status {
	case "succeeded", "failed", "refused", "given_up", "cancelled":
		return true
	}
	return false
}

// poolFinalMS is the final_ms a pool action in phase carries: the time it
// became final, or 0 while it is live.
func poolFinalMS(phase string, now time.Time) int64 {
	if phase == poolActionDone || phase == poolActionRefused {
		return now.UnixMilli()
	}
	return 0
}

// hubStoreRetentionCounts is what one pass deleted, per table.
type hubStoreRetentionCounts struct {
	SentBatches, Quarantine, AttemptGrants, AttemptStartBy, SpawnOperations, Consults, PoolActions int64
}

func (c hubStoreRetentionCounts) total() int64 {
	return c.SentBatches + c.Quarantine + c.AttemptGrants + c.AttemptStartBy + c.SpawnOperations + c.Consults + c.PoolActions
}

func (c hubStoreRetentionCounts) attrs() []any {
	return []any{
		"sent_batches", c.SentBatches, "batch_quarantine", c.Quarantine,
		"attempt_grants", c.AttemptGrants, "attempt_start_by", c.AttemptStartBy,
		"spawn_operations", c.SpawnOperations, "consults", c.Consults, "pool_actions", c.PoolActions,
	}
}

// sentBatchesPrunable are sent batches no inbox item still names. A batch's
// items age out under the lane_inbox retention, so the batch follows them.
const sentBatchesPrunable = `SELECT b.id FROM lane_inbox_batches b WHERE b.sent=1 AND NOT EXISTS (SELECT 1 FROM lane_inbox i WHERE i.digest_batch=b.id) AND NOT EXISTS (SELECT 1 FROM lane_inbox i WHERE i.escalation_batch=b.id) ORDER BY b.id LIMIT ?`

// retiredSpawnNodes are the node request ids of operations terminal past the
// given cut. An attempt's grant and start_by rows are keyed by its node id.
const retiredSpawnNodes = `SELECT node_id FROM spawn_operations WHERE status IN (` + spawnTerminalStatuses + `) AND terminal_ms>0 AND terminal_ms<? AND node_id IS NOT NULL ORDER BY terminal_ms,request_id LIMIT ?`

// spawnOperationsRetirable are operations terminal past the cut that no other
// row still needs. Each NOT EXISTS is one table that points at the operation
// while its row is live: an undelivered or unrecorded notification, an
// unrecorded placement snapshot, an unfinished supersede landing, a queued or
// unnotified consult, a live pool action of the job, and a reservation. A
// failed or refused operation whose hk task-failure comment is unposted stays,
// since the sweep still owes that comment.
const spawnOperationsRetirable = `SELECT so.request_id FROM spawn_operations so WHERE so.status IN (` + spawnTerminalStatuses + `) AND so.terminal_ms>0 AND so.terminal_ms<?
 AND NOT (so.status IN ('failed','refused') AND so.task_fail_notice_ms=0)
 AND NOT EXISTS (SELECT 1 FROM spawn_notifications n WHERE n.request_id=so.request_id AND (n.delivered=0 OR n.telemetry_recorded=0))
 AND NOT EXISTS (SELECT 1 FROM spawn_placement_snapshots p WHERE p.request_id=so.request_id AND p.recorded=0)
 AND NOT EXISTS (SELECT 1 FROM supersede_landings l WHERE (l.request_id=so.request_id OR l.parent=so.request_id OR l.successor=so.request_id) AND l.state<>'done')
 AND NOT EXISTS (SELECT 1 FROM consult_requests c WHERE c.request_id=so.request_id AND (c.state IN ('queued','running') OR (c.state='answered' AND c.notified=0)))
 AND NOT EXISTS (SELECT 1 FROM pool_actions a WHERE a.job=so.job AND a.phase IN ('pending','guarded','settled'))
 AND NOT EXISTS (SELECT 1 FROM spawn_reservations r WHERE r.request_id=so.request_id)
 ORDER BY so.terminal_ms,so.request_id LIMIT ?`

// pruneHubStore runs one bounded retention pass at now. limit caps the rows
// each table deletes. Each step is one statement in its own transaction, and a
// step that fails returns the counts it already made. A repeated pass is safe.
func (s *laneInboxStore) pruneHubStore(now time.Time, limit int) (hubStoreRetentionCounts, error) {
	var c hubStoreRetentionCounts
	ctx, done := s.op(s.life)
	defer done()
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if c.SentBatches, c.Quarantine, err = s.pruneSentBatches(ctx, limit); err != nil {
		return c, err
	}
	fenceCut := now.Add(-spawnAttemptFenceRetention).UnixMilli()
	if c.AttemptGrants, err = s.retentionExec(ctx, `DELETE FROM spawn_attempt_grants WHERE node_id IN (`+retiredSpawnNodes+`)`, fenceCut, limit); err != nil {
		return c, err
	}
	if c.AttemptStartBy, err = s.retentionExec(ctx, `DELETE FROM spawn_attempt_start_by WHERE node_id IN (`+retiredSpawnNodes+`)`, fenceCut, limit); err != nil {
		return c, err
	}
	if c.SpawnOperations, err = s.retentionExec(ctx, `DELETE FROM spawn_operations WHERE request_id IN (`+spawnOperationsRetirable+`)`, now.Add(-spawnOperationRetention).UnixMilli(), limit); err != nil {
		return c, err
	}
	if c.Consults, err = s.retentionExec(ctx, `DELETE FROM consult_requests WHERE id IN (SELECT id FROM consult_requests WHERE state NOT IN ('queued','running') AND NOT (state='answered' AND notified=0) AND updated_ms<? ORDER BY updated_ms,id LIMIT ?)`, now.Add(-consultRetention).UnixMilli(), limit); err != nil {
		return c, err
	}
	// A final pool action is the dedupe fence for its job (hasPoolAction,
	// supersedeActionsOf). It goes only once its window has passed and the job
	// has no spawn row still in progress.
	if c.PoolActions, err = s.retentionExec(ctx, `DELETE FROM pool_actions WHERE id IN (SELECT p.id FROM pool_actions p WHERE p.phase IN ('done','refused') AND p.final_ms>0 AND p.final_ms<? AND NOT EXISTS (SELECT 1 FROM spawn_operations so WHERE so.job=p.job AND so.status NOT IN (`+spawnTerminalStatuses+`)) ORDER BY p.final_ms,p.id LIMIT ?)`, now.Add(-poolActionRetention).UnixMilli(), limit); err != nil {
		return c, err
	}
	return c, nil
}

// pruneSentBatches deletes sent batches no inbox item names, with their
// quarantine rows, in one transaction. The quarantine delete runs first, on
// the same candidate set the batch delete then uses.
func (s *laneInboxStore) pruneSentBatches(ctx context.Context, limit int) (batches, quarantine int64, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	q, err := tx.ExecContext(ctx, s.q(`DELETE FROM lane_inbox_batch_quarantine WHERE batch_id IN (`+sentBatchesPrunable+`)`), limit)
	if err != nil {
		return 0, 0, err
	}
	if quarantine, err = q.RowsAffected(); err != nil {
		return 0, 0, err
	}
	b, err := tx.ExecContext(ctx, s.q(`DELETE FROM lane_inbox_batches WHERE id IN (`+sentBatchesPrunable+`)`), limit)
	if err != nil {
		return 0, 0, err
	}
	if batches, err = b.RowsAffected(); err != nil {
		return 0, 0, err
	}
	return batches, quarantine, tx.Commit()
}

// retentionExec runs one bounded delete in its own transaction and reports the
// rows it removed.
func (s *laneInboxStore) retentionExec(ctx context.Context, query string, args ...any) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(query), args...)
	if err != nil {
		return 0, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, tx.Commit()
}
