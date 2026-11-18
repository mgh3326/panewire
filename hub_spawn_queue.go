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
)

// spawnQueueTable is the SQLite form; the Postgres form is migration 0028.
const spawnQueueTable = `CREATE TABLE IF NOT EXISTS spawn_queue (request_id TEXT PRIMARY KEY,next_check_ms BIGINT NOT NULL DEFAULT 0)`

// spawnQueueNextCheckIndex serves the coordinator's due-time read.
const spawnQueueNextCheckIndex = `CREATE INDEX IF NOT EXISTS spawn_queue_next_check ON spawn_queue(next_check_ms,request_id)`

// taskFailNoticeOutboxTable is the SQLite form; the Postgres form is
// migration 0028.
const taskFailNoticeOutboxTable = `CREATE TABLE IF NOT EXISTS task_fail_notice_outbox (request_id TEXT PRIMARY KEY,retry_ms BIGINT NOT NULL DEFAULT 0)`

// taskFailNoticeOutboxIndex serves the sweep's due read.
const taskFailNoticeOutboxIndex = `CREATE INDEX IF NOT EXISTS task_fail_notice_outbox_retry ON task_fail_notice_outbox(retry_ms,request_id)`

// spawnTerminalStatuses are the stored statuses an operation never leaves.
// retryable is not one of them (a replay or the expiry gives it a new attempt
// or ends it), nor is deadline_exceeded (a late node result can still land
// it), nor any *_grant refinement.
var spawnTerminalStatuses = []string{"succeeded", "failed", "refused", "given_up", "cancelled"}

// spawnTerminalStatusList is spawnTerminalStatuses as an SQL list, for the
// backfills and the tests.
const spawnTerminalStatusList = `'succeeded','failed','refused','given_up','cancelled'`

// spawnStatusTerminal reports whether status ends an operation.
func spawnStatusTerminal(status string) bool {
	for _, terminal := range spawnTerminalStatuses {
		if status == terminal {
			return true
		}
	}
	return false
}

// spawnStatusOwesTaskFailNotice reports whether a request that ended as
// status owes the hk task comment of hk 5137.
func spawnStatusOwesTaskFailNotice(status string) bool {
	return status == "failed" || status == "refused"
}

// enqueueSpawn adds a new non-terminal operation to the queue, in the
// transaction that inserts it. Repeating it is a no-op.
func (s *laneInboxStore) enqueueSpawn(ctx context.Context, tx *sql.Tx, requestID string) error {
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO spawn_queue(request_id,next_check_ms) VALUES(?,0) ON CONFLICT DO NOTHING`), requestID)
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
	_, err := tx.ExecContext(ctx, s.q(`INSERT INTO task_fail_notice_outbox(request_id,retry_ms) VALUES(?,0) ON CONFLICT DO NOTHING`), requestID)
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

// backfillSpawnQueue copies the operations that predate the two tables, once
// per SQLite store (Postgres does it in migration 0028): every non-terminal
// operation into the queue, and every failed or refused operation whose
// comment is still owed into the outbox. It runs after
// backfillSpawnTaskFailNotices, so a failure that backfill stamped judged
// (its mark is set) is never put back in the outbox. The guard row and the
// copies commit together, so a hub that stops half way redoes it and a
// finished one never rescans; each copy is also idempotent.
func (s *laneInboxStore) backfillSpawnQueue(parent context.Context) error {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS spawn_queue_backfill (singleton INTEGER PRIMARY KEY CHECK(singleton=1))`); err != nil {
		return err
	}
	marked, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO spawn_queue_backfill(singleton) VALUES(1)`)
	if err != nil {
		return err
	}
	if n, _ := marked.RowsAffected(); n == 1 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO spawn_queue(request_id,next_check_ms) SELECT request_id,next_check_ms FROM spawn_operations WHERE status NOT IN (`+spawnTerminalStatusList+`) ON CONFLICT DO NOTHING`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO task_fail_notice_outbox(request_id,retry_ms) SELECT request_id,task_fail_retry_ms FROM spawn_operations WHERE status IN ('failed','refused') AND task_fail_notice_ms=0 ON CONFLICT DO NOTHING`); err != nil {
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
