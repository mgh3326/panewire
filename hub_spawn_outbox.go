package panewire

// Contract: docs/hub-spawn-operations.md, "Outboxes" (hk 5298, design
// hk:doc design/2026-11-18/pw-4898-hub-store-mysql §2-§3).
//
// Two small tables hold the work the hub still owes, so an idle tick reads an
// empty table instead of scanning the ever-growing history it used to scan:
//
//   - placement_record_outbox(request_id, attempt): one row for every
//     spawn_placement_snapshots row whose recorded mark is not 1. The row is
//     written in the transaction that writes the snapshot and deleted in the
//     transaction that seals it, so the outbox always equals the set
//     `recorded<>1`.
//   - spawn_notice_outbox(event_id): one row for every spawn_notifications row
//     whose delivered mark is not 1 (not routed, held, or being published).
//     The row is written with its notification and deleted in the transaction
//     that marks the notification delivered.
//
// The history tables keep their rows (hk 5297 prunes them by age) and stay the
// record of state: `recorded` and `delivered` are still written, and the
// telemetry_recorded lookup by event_id is unchanged. An outbox row is only
// the "still owed" membership.
//
// A crash between the work and the delete leaves the row, and the next pass
// redoes the work under the idempotency key it always had: the hk record is
// keyed by (job, attempt), a lane event by its event id.

import (
	"context"
	"time"
)

// enqueuePlacementRecord adds an attempt's snapshot to the outbox when its
// record is still owed. Reading the snapshot row keeps a sealed one out, and
// the key makes a repeat a no-op.
func (s *laneInboxStore) enqueuePlacementRecord(ctx context.Context, x sqlExecer, requestID string, attempt int) error {
	_, err := x.ExecContext(ctx, s.q(`INSERT INTO placement_record_outbox(request_id,attempt) SELECT request_id,attempt FROM spawn_placement_snapshots WHERE request_id=? AND attempt=? AND recorded<>1 ON CONFLICT(request_id,attempt) DO NOTHING`), requestID, attempt)
	return err
}

// dequeuePlacementRecord removes an attempt from the outbox. It belongs in the
// transaction that marks the snapshot recorded.
func (s *laneInboxStore) dequeuePlacementRecord(ctx context.Context, x sqlExecer, requestID string, attempt int) error {
	_, err := x.ExecContext(ctx, s.q(`DELETE FROM placement_record_outbox WHERE request_id=? AND attempt=?`), requestID, attempt)
	return err
}

// enqueueSpawnNotice adds a notification to the outbox while it is not
// delivered. A notification already delivered stays out, and a repeat is a
// no-op.
func (s *laneInboxStore) enqueueSpawnNotice(ctx context.Context, x sqlExecer, eventID string) error {
	_, err := x.ExecContext(ctx, s.q(`INSERT INTO spawn_notice_outbox(event_id) SELECT event_id FROM spawn_notifications WHERE event_id=? AND delivered<>1 ON CONFLICT(event_id) DO NOTHING`), eventID)
	return err
}

// saveSpawnNotice writes a notification and its outbox row in one transaction,
// for a producer that has no transaction of its own.
func (s *laneInboxStore) saveSpawnNotice(ctx context.Context, eventID string, task hubSpawnTask, raw string, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, s.q(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms) VALUES(?,?,?,?,?,?) ON CONFLICT DO NOTHING`), eventID, task.Request.RequestID, task.Job, task.Owner, raw, now.UnixMilli()); err != nil {
		return err
	}
	if err := s.enqueueSpawnNotice(ctx, tx, eventID); err != nil {
		return err
	}
	return s.commitNoting(tx, readSpawnNotice)
}

// dequeueDeliveredSpawnNotice removes a notification from the outbox once its
// row says delivered, in the transaction that wrote the mark.
func (s *laneInboxStore) dequeueDeliveredSpawnNotice(ctx context.Context, x sqlExecer, eventID string) error {
	_, err := x.ExecContext(ctx, s.q(`DELETE FROM spawn_notice_outbox WHERE event_id=? AND EXISTS (SELECT 1 FROM spawn_notifications WHERE event_id=? AND delivered=1)`), eventID, eventID)
	return err
}

// The owed rows of the two history tables, as a partial index each: the
// reconcile below reads only these, so its cost follows the rows still owed
// (a handful), not the retained history. The predicates match the reconcile's
// word for word, which is what lets either planner use them. Postgres makes
// the same two in migration 0028.
const (
	spawnPlacementUnsealedIndex        = `CREATE INDEX IF NOT EXISTS spawn_placement_snapshots_unsealed ON spawn_placement_snapshots(request_id,attempt) WHERE recorded<>1`
	spawnNotificationsUndeliveredIndex = `CREATE INDEX IF NOT EXISTS spawn_notifications_undelivered ON spawn_notifications(event_id) WHERE delivered<>1`
)

// ensureSpawnOutboxes creates the SQLite outbox tables and reconciles both
// outboxes with the history tables (reconcileSpawnOutboxes). Postgres gets its
// tables from migration 0028.
func (s *laneInboxStore) ensureSpawnOutboxes(parent context.Context) error {
	ctx, done := s.opWithin(parent, laneInboxStartupTimeout)
	defer done()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS placement_record_outbox (request_id TEXT NOT NULL,attempt INTEGER NOT NULL,PRIMARY KEY(request_id,attempt))`,
		`CREATE TABLE IF NOT EXISTS spawn_notice_outbox (event_id TEXT PRIMARY KEY)`,
		ownerNoticeDecisionsTable,
		ownerNoticeDecisionsWindowIndex,
		spawnPlacementUnsealedIndex,
		spawnNotificationsUndeliveredIndex,
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return s.reconcileSpawnOutboxes(ctx)
}

// reconcileSpawnOutboxes makes each outbox equal the set of history rows still
// owed: every snapshot not yet recorded and every notification not yet
// delivered gets a row, and a row whose work is already done goes. It runs at
// every start, on both dialects, not once at the upgrade: a hub binary from
// before the outboxes (a rollback) writes snapshots and notifications and
// seals them without touching the outboxes, so the first start after returning
// to this release must carry over what that binary left pending and drop what
// it finished. All four statements commit together. The caller gives it the
// startup deadline, not the ordinary per-operation one.
func (s *laneInboxStore) reconcileSpawnOutboxes(ctx context.Context) error {
	if s.outboxReconcileCtx != nil {
		s.outboxReconcileCtx(ctx)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{
		`INSERT INTO placement_record_outbox(request_id,attempt) SELECT request_id,attempt FROM spawn_placement_snapshots WHERE recorded<>1 ON CONFLICT(request_id,attempt) DO NOTHING`,
		`INSERT INTO spawn_notice_outbox(event_id) SELECT event_id FROM spawn_notifications WHERE delivered<>1 ON CONFLICT(event_id) DO NOTHING`,
		`DELETE FROM placement_record_outbox WHERE NOT EXISTS (SELECT 1 FROM spawn_placement_snapshots p WHERE p.request_id=placement_record_outbox.request_id AND p.attempt=placement_record_outbox.attempt AND p.recorded<>1)`,
		`DELETE FROM spawn_notice_outbox WHERE NOT EXISTS (SELECT 1 FROM spawn_notifications n WHERE n.event_id=spawn_notice_outbox.event_id AND n.delivered<>1)`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}
