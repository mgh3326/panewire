package panewire

// The one-time marking of the copies an earlier build made of a
// working_wait_expired obligation (hk 6820, hk 4289 T3;
// docs/r20-relay-persistence.md, "Faults and copies"). That build counted such
// a copy in labelled alone, and kept the fault when the copy then timed out or
// was reported maybe typed. Read by this build, the row would still be owed a
// revive, so it is marked as having had it. It runs once per store: this build
// labels rows too (a budget copy that ends in a working expiry), and those are
// still owed their revive.

import "context"

// obligationEarlierCopiesVersion is the row the marking adds to the migration
// ledger on Postgres and MySQL. No migration file has this name.
const obligationEarlierCopiesVersion = "go_delivery_obligations_earlier_copies_revived"

// sqliteObligationEarlierCopiesVersion is the PRAGMA user_version a SQLite
// store holds once the marking has run. It follows sqliteSpawnBoundVersion,
// whose backfill runs first at every start.
const sqliteObligationEarlierCopiesVersion = 3

// markEarlierObligationCopies marks the labelled working_wait_expired rows as
// revived, once: the UPDATE and its record commit together.
func (s *laneInboxStore) markEarlierObligationCopies(parent context.Context) error {
	ctx, done := s.opWithin(parent, laneInboxStartupTimeout)
	defer done()
	recorded, err := s.earlierObligationCopiesRecorded(ctx)
	if err != nil || recorded {
		return err
	}
	tx, err := s.beginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET revived=1 WHERE fault=? AND labelled>0 AND revived=0`), relayDroppedWorkingExpired)
	if err != nil {
		return err
	}
	marked, err := s.dialect.rowsMatched(result)
	if err != nil {
		return err
	}
	record := s.dialect.insertIgnore(`INSERT INTO panewire_schema_migrations(version) VALUES(?)`, "version")
	args := []any{obligationEarlierCopiesVersion}
	if s.dialect.ownsSchema() {
		// PRAGMA takes no bound argument; the 3 is
		// sqliteObligationEarlierCopiesVersion.
		record, args = `PRAGMA user_version=3`, nil
	}
	if _, err := tx.ExecContext(ctx, s.q(record), args...); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.earlierCopiesMarked = marked
	return nil
}

func (s *laneInboxStore) earlierObligationCopiesRecorded(ctx context.Context) (bool, error) {
	if s.dialect.ownsSchema() {
		var version int
		err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version)
		return version >= sqliteObligationEarlierCopiesVersion, err
	}
	var recorded int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT COUNT(*) FROM panewire_schema_migrations WHERE version=?`), obligationEarlierCopiesVersion).Scan(&recorded)
	return recorded > 0, err
}
