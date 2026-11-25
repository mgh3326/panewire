package panewire

// Contract: docs/hub-spawn-operations.md, "Drain for a spawn without a
// checkout" (hk 6433).
//
// A node admits a spawn without a checkout alone. While one waits, newer
// checkout spawns for its machine would keep the node busy for good, so the
// coordinator holds them back. Which machines have such a spawn waiting is
// read from the store once per scan, from two columns the bind writes
// (bound_machine, exclusive). Nothing is carried from one scan to the next.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const spawnBoundExclusiveIndex = `CREATE INDEX IF NOT EXISTS spawn_operations_bound_exclusive ON spawn_operations(bound_machine,exclusive,status,created_ms,request_id)`

// spawnExclusiveMark is the exclusive column of an operation bound as node: 1
// when node has no overlap key.
func spawnExclusiveMark(node *hubSpawnRequest) int {
	if hubSpawnOverlapKey(node) == "" {
		return 1
	}
	return 0
}

// spawnDrainMark is the queue position of a machine's waiting exclusive.
type spawnDrainMark struct {
	createdMS int64
	requestID string
}

// spawnDrainMarks is one scan's drain, by machine.
type spawnDrainMarks map[string]spawnDrainMark

// holds reports whether task waits for the exclusive marked on its machine: it
// is a checkout spawn bound there, behind the exclusive in (created, request
// id) order.
func (m spawnDrainMarks) holds(task hubSpawnTask) bool {
	if task.Node == nil || hubSpawnOverlapKey(task.Node) == "" {
		return false
	}
	mark, marked := m[task.Node.Machine]
	if !marked {
		return false
	}
	created := task.Created.UnixMilli()
	return mark.createdMS < created || mark.createdMS == created && mark.requestID < task.Request.RequestID
}

// readSpawnDrainMarks is the prepass of one scan. A machine is marked only by
// an exclusive that is dispatchable now. One that is not is unbound or expired
// by this scan (handleQueuedBinding), so it holds nothing back, and the next
// scan reads the queue again.
func (h *HubServer) readSpawnDrainMarks(ctx context.Context) (spawnDrainMarks, error) {
	waiting, err := h.inbox.store.boundExclusiveSpawns(ctx, h.now().UTC())
	if err != nil {
		return nil, err
	}
	marks := spawnDrainMarks{}
	for _, row := range waiting {
		task, err := h.inbox.store.spawnTask(ctx, row.requestID)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		// The row may have moved since the query.
		if task.Status != "bound" || task.Node == nil || task.Node.Machine != row.machine || !h.boundSpawnDispatchable(task) {
			continue
		}
		marks[row.machine] = spawnDrainMark{createdMS: row.createdMS, requestID: row.requestID}
	}
	return marks, nil
}

type boundExclusiveSpawn struct {
	requestID, machine string
	createdMS          int64
}

// boundExclusiveSpawns returns each machine's oldest unsent exclusive whose
// deadline is after now: at most one row per machine, however long the
// backlog is.
func (s *laneInboxStore) boundExclusiveSpawns(parent context.Context, now time.Time) ([]boundExclusiveSpawn, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT request_id,bound_machine,created_ms FROM (SELECT request_id,bound_machine,created_ms,ROW_NUMBER() OVER (PARTITION BY bound_machine ORDER BY created_ms,request_id) AS rn FROM spawn_operations WHERE exclusive=1 AND bound_machine<>'' AND node_id IS NOT NULL AND status IN ('bound','bound_grant') AND deadline_ms>?) t WHERE rn=1`), now.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var found []boundExclusiveSpawn
	for rows.Next() {
		var row boundExclusiveSpawn
		if err := rows.Scan(&row.requestID, &row.machine, &row.createdMS); err != nil {
			return nil, err
		}
		found = append(found, row)
	}
	return found, rows.Err()
}

// spawnBoundBackfillBatch is the rows one batch of the backfill reads and
// writes.
const spawnBoundBackfillBatch = 500

// spawnBoundBackfillVersion is the row the backfill adds to the migration
// ledger on Postgres and MySQL. No migration file has this name.
const spawnBoundBackfillVersion = "go_spawn_operations_bound_exclusive_backfill"

// sqliteSpawnBoundVersion is the PRAGMA user_version a SQLite store holds once
// the backfill has run. It follows sqliteSucceededNoticeVersion, whose
// backfill runs first at every start.
const sqliteSpawnBoundVersion = 2

// backfillSpawnBoundMachines fills bound_machine and exclusive on the bound
// operations written before those columns, once: batches by request id, each
// its own transaction, then the completion record. A start cut off part way
// keeps the finished batches, and the next start does the rest.
func (s *laneInboxStore) backfillSpawnBoundMachines(parent context.Context) error {
	ctx, done := s.opWithin(parent, laneInboxStartupTimeout)
	defer done()
	recorded, err := s.spawnBoundBackfillRecorded(ctx)
	if err != nil || recorded {
		return err
	}
	var batches []int
	for after := ""; ; {
		read, last, err := s.backfillSpawnBoundBatch(ctx, after)
		if err != nil {
			return err
		}
		if read > 0 {
			batches = append(batches, read)
		}
		if read < spawnBoundBackfillBatch {
			break
		}
		after = last
	}
	record := s.dialect.insertIgnore(`INSERT INTO panewire_schema_migrations(version) VALUES(?)`, "version")
	args := []any{spawnBoundBackfillVersion}
	if s.dialect.ownsSchema() {
		// PRAGMA takes no bound argument; the 2 is sqliteSpawnBoundVersion.
		record, args = `PRAGMA user_version=2`, nil
	}
	if _, err := s.db.ExecContext(ctx, s.q(record), args...); err != nil {
		return err
	}
	s.spawnBoundBackfilled = batches
	return nil
}

func (s *laneInboxStore) spawnBoundBackfillRecorded(ctx context.Context) (bool, error) {
	if s.dialect.ownsSchema() {
		var version int
		err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
		return version >= sqliteSpawnBoundVersion, err
	}
	var recorded int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT COUNT(*) FROM panewire_schema_migrations WHERE version=?`), spawnBoundBackfillVersion).Scan(&recorded)
	return recorded > 0, err
}

// backfillSpawnBoundBatch fills the next batch of bound rows after the request
// id after. It returns how many rows it read and the last one's id.
func (s *laneInboxStore) backfillSpawnBoundBatch(ctx context.Context, after string) (read int, last string, err error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, "", err
	}
	defer tx.Rollback()
	type bound struct{ requestID, nodeID, payload string }
	var batch []bound
	if err := func() error {
		rows, err := tx.QueryContext(ctx, s.q(`SELECT request_id,node_id,node_payload FROM spawn_operations WHERE request_id>? AND node_id IS NOT NULL AND bound_machine='' AND status IN ('bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant') ORDER BY request_id LIMIT ?`), after, spawnBoundBackfillBatch)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row bound
			if err := rows.Scan(&row.requestID, &row.nodeID, &row.payload); err != nil {
				return err
			}
			batch = append(batch, row)
		}
		return rows.Err()
	}(); err != nil {
		return 0, "", err
	}
	for _, row := range batch {
		// A binding with no node request names no machine to mark.
		if row.payload == "" {
			continue
		}
		var node hubSpawnRequest
		if err := json.Unmarshal([]byte(row.payload), &node); err != nil {
			return 0, "", fmt.Errorf("spawn operation %s: node request: %w", row.requestID, err)
		}
		// node_id guards the write against a row bound again since the read.
		if _, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET bound_machine=?,exclusive=? WHERE request_id=? AND node_id=?`), node.Machine, spawnExclusiveMark(&node), row.requestID, row.nodeID); err != nil {
			return 0, "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, "", err
	}
	if len(batch) > 0 {
		last = batch[len(batch)-1].requestID
	}
	return len(batch), last, nil
}
