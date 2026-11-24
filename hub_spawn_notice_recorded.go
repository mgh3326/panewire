package panewire

// Contract: docs/hub-spawn-operations.md, "Notifications and telemetry" (hk
// 6283).
//
// telemetry_recorded=0 says a spawn notice may still owe an error event or a
// warning, and retention keeps its operation while one does. A succeeded
// outcome owes neither, so its notice is written 1. Rows written before that
// are marked once: on Postgres and MySQL by the migration file whose name ends
// succeededNoticeMigration, on SQLite by backfillSucceededSpawnNotices.

import (
	"context"
	"strings"
)

// succeededNoticeMigration ends the file name of the backfill in each
// backend's migration series.
const succeededNoticeMigration = "_spawn_notifications_succeeded_recorded"

// spawnNoticeRecordedAtInsert is the telemetry_recorded a notice is inserted
// with. eventID is the outcome's notice id, before a submitter copy appends
// ":<lane>": its last segment is the outcome's status (spawnOutcomeStatus). A
// copy's own id must not be passed, because a lane may be named "succeeded".
func spawnNoticeRecordedAtInsert(eventID string) int {
	if strings.HasSuffix(eventID, ":succeeded") {
		return 1
	}
	return 0
}

// sqliteSucceededNoticeVersion is the PRAGMA user_version a SQLite store holds
// once the backfill has run. SQLite has no migration ledger.
const sqliteSucceededNoticeVersion = 1

// backfillSucceededSpawnNotices marks the unrecorded notices of succeeded
// outcomes on a SQLite store, once: the UPDATE and the version commit together.
// The predicate is the migration files': a row's own status is the segment of
// its event id after <request_id>:<attempt>:, so an owner row ends there and a
// submitter copy ends in :<owner>.
func (s *laneInboxStore) backfillSucceededSpawnNotices(parent context.Context) error {
	ctx, done := s.opWithin(parent, laneInboxStartupTimeout)
	defer done()
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version >= sqliteSucceededNoticeVersion {
		return nil
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	owner := s.dialect.concat("request_id", "':%:succeeded'")
	deeper := s.dialect.concat("request_id", "':%:%:succeeded'")
	submitter := s.dialect.concat("request_id", "':%:succeeded:'", "owner")
	result, err := tx.ExecContext(ctx, `UPDATE spawn_notifications SET telemetry_recorded=1 WHERE telemetry_recorded=0 AND ((event_id LIKE `+owner+` AND event_id NOT LIKE `+deeper+`) OR event_id LIKE `+submitter+`)`)
	if err != nil {
		return err
	}
	marked, err := s.dialect.rowsMatched(result)
	if err != nil {
		return err
	}
	// PRAGMA takes no bound argument; the 1 is sqliteSucceededNoticeVersion.
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version=1`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.succeededNoticesMarked = &marked
	return nil
}
