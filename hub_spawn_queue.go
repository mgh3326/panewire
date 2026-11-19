// The spawn queue and the task-failure notice outbox (hk 5299, phase 0 of
// hk 4898; design hk:doc design/2026-11-18/pw-4898-hub-store-mysql §2).
//
// spawn_operations keeps every operation, so a scan that asks "what is still
// to do" by its status column reads a table that only grows. Two small tables
// carry the hot sets instead:
//
//   - spawn_queue holds one row per non-terminal operation. The row is
//     inserted in the transaction that creates the operation and deleted in
//     the transaction that ends it, so the table is as large as the work in
//     flight and the coordinator's scans read it, not the history.
//     next_check_ms mirrors the operation's own next_check_ms (the parked
//     wake time); every writer of that column copies it across in the same
//     transaction (syncSpawnQueueNextCheck).
//   - task_fail_notice_outbox holds one row per failed or refused operation
//     whose hk task comment is still owed (hk 5137). The row is inserted in
//     the transaction that ends the operation as failed or refused, and
//     deleted once the sweep recorded the notice. retry_ms is the row's next
//     attempt after hk did not take its comment.
//
// spawn_operations keeps every row and column. task_fail_notice_ms stays on
// the operation as the record that the sweep judged it. The queue is
// authoritative for "is this operation still to do": a status write that
// leaves an operation terminal must go through endSpawnQueue in the same
// transaction (docs/hub-spawn-operations.md lists the transitions).
package panewire

import (
	"context"
	"database/sql"
	"time"
)

// spawnQueueTable is the SQLite form; the Postgres form is migration 0030.
const spawnQueueTable = `CREATE TABLE IF NOT EXISTS spawn_queue (request_id TEXT PRIMARY KEY,next_check_ms BIGINT NOT NULL DEFAULT 0)`

// spawnQueueNextCheckIndex serves the coordinator's due-time read.
const spawnQueueNextCheckIndex = `CREATE INDEX IF NOT EXISTS spawn_queue_next_check ON spawn_queue(next_check_ms,request_id)`

// taskFailNoticeOutboxTable is the SQLite form; the Postgres form is
// migration 0030.
const taskFailNoticeOutboxTable = `CREATE TABLE IF NOT EXISTS task_fail_notice_outbox (request_id TEXT PRIMARY KEY,retry_ms BIGINT NOT NULL DEFAULT 0)`

// taskFailNoticeOutboxIndex serves the sweep's due read.
const taskFailNoticeOutboxIndex = `CREATE INDEX IF NOT EXISTS task_fail_notice_outbox_retry ON task_fail_notice_outbox(retry_ms,request_id)`

// The terminal statuses are spawnTerminalStatuses (hub_store_retention.go):
// retryable is not one of them (a replay or the expiry gives it a new attempt
// or ends it), nor is deadline_exceeded (a late node result can still land
// it), nor any *_grant refinement.

// spawnStatusOwesTaskFailNotice reports whether a request that ended as
// status owes the hk task comment of hk 5137.
func spawnStatusOwesTaskFailNotice(status string) bool {
	return status == "failed" || status == "refused"
}

// enqueueSpawn adds a new non-terminal operation to the queue, in the
// transaction that inserts it. Repeating it is a no-op.
func (s *laneInboxStore) enqueueSpawn(ctx context.Context, tx *sql.Tx, requestID string) error {
	_, err := tx.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0)`, "")), requestID)
	return err
}

// dequeueSpawn drops an operation's queue row, for an operation the
// transaction deletes.
func (s *laneInboxStore) dequeueSpawn(ctx context.Context, tx *sql.Tx, requestID string) error {
	_, err := tx.ExecContext(ctx, s.q(`DELETE FROM spawn_queue WHERE request_id=?`), requestID)
	return err
}

// endSpawnQueue is the one place an operation leaves the queue: call it in
// the transaction that writes a terminal status. An operation that ends as
// failed or refused owes its hk task comment, so its outbox row is written in
// the same transaction.
func (s *laneInboxStore) endSpawnQueue(ctx context.Context, tx *sql.Tx, requestID, status string) error {
	if err := s.dequeueSpawn(ctx, tx, requestID); err != nil {
		return err
	}
	if !spawnStatusOwesTaskFailNotice(status) {
		return nil
	}
	_, err := tx.ExecContext(ctx, s.q(s.dialect.insertIgnore(`INSERT INTO task_fail_notice_outbox(request_id,retry_ms) VALUES(?,0)`, "")), requestID)
	return err
}

// spawnQueueExecer is the part of *sql.DB and *sql.Tx the queue sync needs.
type spawnQueueExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// syncSpawnQueueNextCheck copies the operation's next_check_ms to its queue
// row. A writer of spawn_operations.next_check_ms calls it in the same
// transaction as that write, so the queue's due time never lags the row's.
func (s *laneInboxStore) syncSpawnQueueNextCheck(ctx context.Context, ex spawnQueueExecer, requestID string) error {
	_, err := ex.ExecContext(ctx, s.q(`UPDATE spawn_queue SET next_check_ms=(SELECT next_check_ms FROM spawn_operations WHERE spawn_operations.request_id=spawn_queue.request_id) WHERE request_id=?`), requestID)
	return err
}

// reconcileSpawnQueue makes the queue and the notice outbox equal what the
// operations say is still owed, in one transaction:
//
//   - every non-terminal operation has a queue row carrying its next_check_ms,
//     and a queue row whose operation is terminal (or gone) is dropped;
//   - every failed or refused operation whose task comment is unjudged
//     (task_fail_notice_ms=0) has an outbox row, and an outbox row whose
//     operation is judged (or gone) is dropped.
//
// It runs at every start, on both dialects, not once at the upgrade: a hub
// binary from before these tables (a rollback) accepts, moves and ends
// operations without touching them, so the first start after returning to
// this release must queue what that binary left running, refresh the wake
// times it changed, drop what it ended, and drop the outbox rows it already
// posted and marked. The same pass is the copy of operations that predate the
// tables (Postgres also does it once in migration 0030). It must follow
// backfillSpawnTaskFailNotices on SQLite: a failure that stamp judged has its
// mark set and is not owed again. The caller gives it the startup deadline.
func (s *laneInboxStore) reconcileSpawnQueue(ctx context.Context) error {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{
		s.dialect.insertIgnore(`INSERT INTO spawn_queue(request_id,next_check_ms) SELECT request_id,next_check_ms FROM spawn_operations WHERE status NOT IN (`+spawnTerminalStatuses+`)`, "request_id"),
		`UPDATE spawn_queue SET next_check_ms=(SELECT next_check_ms FROM spawn_operations WHERE spawn_operations.request_id=spawn_queue.request_id) WHERE next_check_ms<>(SELECT next_check_ms FROM spawn_operations WHERE spawn_operations.request_id=spawn_queue.request_id)`,
		`DELETE FROM spawn_queue WHERE NOT EXISTS (SELECT 1 FROM spawn_operations o WHERE o.request_id=spawn_queue.request_id AND o.status NOT IN (` + spawnTerminalStatuses + `))`,
		s.dialect.insertIgnore(`INSERT INTO task_fail_notice_outbox(request_id,retry_ms) SELECT request_id,task_fail_retry_ms FROM spawn_operations WHERE status IN ('failed','refused') AND task_fail_notice_ms=0`, "request_id"),
		`DELETE FROM task_fail_notice_outbox WHERE NOT EXISTS (SELECT 1 FROM spawn_operations o WHERE o.request_id=task_fail_notice_outbox.request_id AND o.status IN ('failed','refused') AND o.task_fail_notice_ms=0)`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// syncSpawnQueueNextChecks copies the operations' next_check_ms to every
// queue row that differs, for a writer that changes many operations in one
// statement. It reads only the queue and the rows it names.
func (s *laneInboxStore) syncSpawnQueueNextChecks(ctx context.Context, ex spawnQueueExecer) error {
	_, err := ex.ExecContext(ctx, `UPDATE spawn_queue SET next_check_ms=(SELECT next_check_ms FROM spawn_operations WHERE spawn_operations.request_id=spawn_queue.request_id) WHERE next_check_ms<>(SELECT next_check_ms FROM spawn_operations WHERE spawn_operations.request_id=spawn_queue.request_id)`)
	return err
}

// reconcileSpawnQueueWithin runs reconcileSpawnQueue under the startup
// deadline.
func (s *laneInboxStore) reconcileSpawnQueueWithin(parent context.Context) error {
	ctx, done := s.opWithin(parent, laneInboxStartupTimeout)
	defer done()
	return s.reconcileSpawnQueue(ctx)
}

// spawnQueueWake is what the coordinator's gate needs from the queue once a
// scan found nothing to run: when the queue next needs a look, and whether
// waiting or parked rows keep a poll of their own alive (hk 5302).
type spawnQueueWake struct {
	// next is the earliest unix ms a parked, unbound accepted row comes due;
	// 0 means none.
	next int64
	// stale are the deadline_exceeded rows' probe inputs (deadline, last run).
	stale []hubSpawnTask
	// waiting counts rows held by start conditions; parked counts accepted
	// rows that carry a park reason.
	waiting, parked int
}

// spawnQueueWake reads the queue, joined to the operations for the status test
// the queue leaves to its owner. The queue is the work still to do, so the read
// is as large as that work. now splits the accepted rows into due ones (the
// scan just ran, so none is expected) and later ones.
func (s *laneInboxStore) spawnQueueWake(parent context.Context, now time.Time) (spawnQueueWake, error) {
	ctx, done := s.op(parent)
	defer done()
	var wake spawnQueueWake
	var next sql.NullInt64
	var stale int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT min(CASE WHEN o.status='accepted' AND q.next_check_ms>? THEN q.next_check_ms END),count(CASE WHEN o.status='waiting' THEN 1 END),count(CASE WHEN o.status='accepted' AND o.park_reason<>'' THEN 1 END),count(CASE WHEN o.status IN ('deadline_exceeded','deadline_exceeded_grant') THEN 1 END) FROM spawn_queue q JOIN spawn_operations o ON o.request_id=q.request_id`), now.UnixMilli()).Scan(&next, &wake.waiting, &wake.parked, &stale)
	if err != nil {
		return wake, err
	}
	if next.Valid {
		wake.next = next.Int64
	}
	if stale == 0 {
		return wake, nil
	}
	// The probe interval is a function of each row's age, so the earliest probe
	// is worked out from the rows the coordinator itself reads.
	wake.stale, err = s.queryPendingSpawns(ctx, `o.status IN ('deadline_exceeded','deadline_exceeded_grant') ORDER BY o.last_run_ms,o.created_ms LIMIT 256`)
	return wake, err
}
