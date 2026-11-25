package panewire

// Contract: docs/hub-spawn-operations.md, "Drain for a spawn without a
// checkout" (hk 6433).
//
// A node admits a spawn without a checkout alone. While one waits, newer
// checkout spawns for its machine would keep the node busy for good, so the
// coordinator holds them back. Which machines have such a spawn waiting is
// read from the store once per scan, from two columns the bind writes
// (bound_machine, exclusive). Nothing is carried from one scan to the next.
// A start fills them once for rows older than the columns, and every start
// repairs them for the attempts in flight.

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

// spawnBoundBatch is the rows one batch of the backfill or of the repair reads.
const spawnBoundBatch = 500

// The stored statuses each walk covers: the backfill's, and for the repair
// every status of an attempt in flight on a node. A finished operation, and an
// ended attempt waiting for a replay ('retryable'), are in neither.
const (
	spawnBoundBackfillStatuses = `'bound','dispatching','reconciling','bound_grant','dispatching_grant','reconciling_grant'`
	spawnBoundInFlightStatuses = `'bound','dispatching','reconciling','deadline_exceeded','bound_grant','dispatching_grant','reconciling_grant','deadline_exceeded_grant'`
)

// spawnBoundWalk names the rows one walk of the columns covers: query reads the
// next batch after a request id, and statuses guards each write.
type spawnBoundWalk struct{ query, statuses string }

var (
	// The backfill reads the operations that have no columns yet.
	spawnBoundBackfillWalk = spawnBoundWalk{
		query:    `SELECT request_id,node_id,node_payload,bound_machine,exclusive FROM spawn_operations WHERE request_id>? AND node_id IS NOT NULL AND bound_machine='' AND status IN (` + spawnBoundBackfillStatuses + `) ORDER BY request_id LIMIT ?`,
		statuses: spawnBoundBackfillStatuses,
	}
	// The repair reads through spawn_queue, which holds the operations still
	// owed work, so it is as large as that work and not the history.
	spawnBoundRepairWalk = spawnBoundWalk{
		query:    `SELECT o.request_id,o.node_id,o.node_payload,o.bound_machine,o.exclusive FROM spawn_queue q JOIN spawn_operations o ON o.request_id=q.request_id WHERE q.request_id>? AND o.node_id IS NOT NULL AND o.status IN (` + spawnBoundInFlightStatuses + `) ORDER BY q.request_id LIMIT ?`,
		statuses: spawnBoundInFlightStatuses,
	}
)

// spawnBoundBackfillVersion is the row the backfill adds to the migration
// ledger on Postgres and MySQL. No migration file has this name.
const spawnBoundBackfillVersion = "go_spawn_operations_bound_exclusive_backfill"

// sqliteSpawnBoundVersion is the PRAGMA user_version a SQLite store holds once
// the backfill has run. It follows sqliteSucceededNoticeVersion, whose
// backfill runs first at every start.
const sqliteSpawnBoundVersion = 2

// backfillSpawnBoundMachines fills bound_machine and exclusive on the bound
// operations written before those columns, once, then records that it ran. A
// start cut off part way keeps the finished batches, and the next start does
// the rest.
func (s *laneInboxStore) backfillSpawnBoundMachines(parent context.Context) error {
	ctx, done := s.opWithin(parent, laneInboxStartupTimeout)
	defer done()
	recorded, err := s.spawnBoundBackfillRecorded(ctx)
	if err != nil || recorded {
		return err
	}
	batches, _, err := s.walkSpawnBoundColumns(ctx, spawnBoundBackfillWalk)
	if err != nil {
		return err
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

// repairSpawnBoundMachines rewrites, at every start, the columns of the
// attempts in flight whose stored values differ from their node request. A hub
// from before the columns, run again after a rollback, neither writes nor
// clears them, and the backfill does not run twice. It runs after the queue is
// reconciled, so the queue names every such attempt. A start that finds nothing
// to repair writes nothing.
func (s *laneInboxStore) repairSpawnBoundMachines(parent context.Context) error {
	ctx, done := s.opWithin(parent, laneInboxStartupTimeout)
	defer done()
	batches, written, err := s.walkSpawnBoundColumns(ctx, spawnBoundRepairWalk)
	s.spawnBoundRepairBatches, s.spawnBoundRepaired = batches, written
	return err
}

// walkSpawnBoundColumns reads walk's rows in batches by request id, each batch
// its own transaction, and writes the columns of each row that differs from its
// node request. It returns the rows each batch read and the rows written.
func (s *laneInboxStore) walkSpawnBoundColumns(ctx context.Context, walk spawnBoundWalk) (batches []int, written int, err error) {
	batches = []int{}
	for after := ""; ; {
		read, wrote, last, err := s.writeSpawnBoundBatch(ctx, walk, after)
		if err != nil {
			return batches, written, err
		}
		written += wrote
		if read > 0 {
			batches = append(batches, read)
		}
		if read < spawnBoundBatch {
			return batches, written, nil
		}
		after = last
	}
}

// writeSpawnBoundBatch handles walk's next batch after the request id after. It
// returns how many rows it read and wrote, and the last one's id.
func (s *laneInboxStore) writeSpawnBoundBatch(ctx context.Context, walk spawnBoundWalk, after string) (read, written int, last string, err error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, 0, "", err
	}
	defer tx.Rollback()
	type bound struct {
		requestID, nodeID, payload, machine string
		exclusive                           int
	}
	var batch []bound
	if err := func() error {
		rows, err := tx.QueryContext(ctx, s.q(walk.query), after, spawnBoundBatch)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row bound
			if err := rows.Scan(&row.requestID, &row.nodeID, &row.payload, &row.machine, &row.exclusive); err != nil {
				return err
			}
			batch = append(batch, row)
		}
		return rows.Err()
	}(); err != nil {
		return 0, 0, "", err
	}
	for _, row := range batch {
		// A binding with no node request names no machine to mark.
		if row.payload == "" {
			continue
		}
		var node hubSpawnRequest
		if err := json.Unmarshal([]byte(row.payload), &node); err != nil {
			return 0, 0, "", fmt.Errorf("spawn operation %s: node request: %w", row.requestID, err)
		}
		exclusive := spawnExclusiveMark(&node)
		if node.Machine == row.machine && exclusive == row.exclusive {
			continue
		}
		// node_id and the status guard the write against a row bound again, or
		// finished, since the read.
		result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET bound_machine=?,exclusive=? WHERE request_id=? AND node_id=? AND status IN (`+walk.statuses+`)`), node.Machine, exclusive, row.requestID, row.nodeID)
		if err != nil {
			return 0, 0, "", err
		}
		matched, err := s.dialect.rowsMatched(result)
		if err != nil {
			return 0, 0, "", err
		}
		written += int(matched)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, "", err
	}
	if len(batch) > 0 {
		last = batch[len(batch)-1].requestID
	}
	return len(batch), written, last, nil
}
