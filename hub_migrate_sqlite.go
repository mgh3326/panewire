package panewire

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/jackc/pgx/v5"
)

const hubMigrateSQLiteUsage = `Usage: panewire hub migrate-sqlite --from PATH [--apply]

Copies a hub inbox SQLite file (--inbox-store) into the Postgres database named
by ` + hubDatabaseURLEnv + `. Without --apply it only reads: it prints the
source row counts and, when the URL is set, the target's state. --apply creates
the schema and copies every row in one transaction. A target that already holds
exactly this source is left unchanged, so a rerun is safe; a target holding
other rows is refused. Stop the hub before applying. See docs/hub-postgres.md.`

// hubInboxSnapshot is every row of the three inbox tables, batches in send
// order, plus the highest inbox ID that must never be reused.
type hubInboxSnapshot struct {
	idFloor    int64
	items      []hubInboxSnapshotItem
	batches    []hubInboxSnapshotBatch
	quarantine [][2]string
}

type hubInboxSnapshotItem struct {
	key  string
	item laneInboxItem
	raw  string
	// sender and eventID are the ack-state lookup's columns; they are not
	// part of laneInboxItem, whose JSON is the public item shape.
	sender  string
	eventID string
}

type hubInboxSnapshotBatch struct {
	batch laneInboxBatch
	sent  int64
}

func (s hubInboxSnapshot) empty() bool {
	return len(s.items)+len(s.batches)+len(s.quarantine) == 0 && s.idFloor == 0
}

func (s hubInboxSnapshot) summary() string {
	unread, mustAck, unacked, pending := 0, 0, 0, 0
	for _, i := range s.items {
		// laneInboxUnread, evaluated in Go.
		switch {
		case i.item.MustAck:
			mustAck++
			if i.item.AckedAt == 0 {
				unacked++
				unread++
			}
		case i.item.Class == "inbox" && i.item.ReadAt == 0:
			unread++
		}
	}
	quarantined := make(map[string]bool, len(s.quarantine))
	for _, q := range s.quarantine {
		quarantined[q[0]] = true
	}
	for _, b := range s.batches {
		if b.sent == 0 && !quarantined[b.batch.ID] {
			pending++
		}
	}
	return fmt.Sprintf("lane_inbox=%d (unread=%d must_ack=%d unacked=%d) lane_inbox_batches=%d (pending=%d) lane_inbox_batch_quarantine=%d",
		len(s.items), unread, mustAck, unacked, len(s.batches), pending, len(s.quarantine))
}

func runHubMigrateSQLite(ctx context.Context, args []string, databaseURL string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("panewire hub migrate-sqlite", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	from := flags.String("from", "", "hub inbox SQLite file to copy")
	apply := flags.Bool("apply", false, "write to the Postgres database; without it the command only reads")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *from == "" {
		return writeHubCLIUsage(stderr, "invalid migrate-sqlite flags", hubMigrateSQLiteUsage)
	}
	snapshot, err := readHubInboxSQLiteSource(ctx, *from)
	if err != nil {
		fmt.Fprintln(stderr, "migrate-sqlite:", err)
		return ExitConditionInvalid
	}
	fmt.Fprintln(stdout, "source:", snapshot.summary())
	if databaseURL == "" {
		if *apply {
			fmt.Fprintln(stderr, "migrate-sqlite: --apply needs "+hubDatabaseURLEnv)
			return ExitConditionInvalid
		}
		fmt.Fprintln(stdout, "target: "+hubDatabaseURLEnv+" unset; not checked")
		fmt.Fprintln(stdout, "dry-run: nothing written")
		return ExitOK
	}
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		fmt.Fprintln(stderr, "migrate-sqlite:", errHubDatabaseURL)
		return ExitConditionInvalid
	}
	return migrateHubInboxToPostgres(ctx, snapshot, config, *apply, stdout, stderr)
}

func migrateHubInboxToPostgres(ctx context.Context, snapshot hubInboxSnapshot, config *pgx.ConnConfig, apply bool, stdout, stderr io.Writer) int {
	db, err := openHubPostgres(ctx, config)
	if err != nil {
		fmt.Fprintln(stderr, "migrate-sqlite:", err)
		return ExitInternal
	}
	defer db.Close()
	if !apply {
		state, err := hubPostgresTargetState(ctx, db, snapshot)
		if err != nil {
			fmt.Fprintln(stderr, "migrate-sqlite:", hubDatabaseError("inspect", err))
			return ExitInternal
		}
		fmt.Fprintln(stdout, "target:", state)
		fmt.Fprintln(stdout, "dry-run: nothing written; rerun with --apply to copy")
		return ExitOK
	}
	copied, err := copyHubInboxSnapshot(ctx, db, snapshot)
	switch {
	case errors.Is(err, errHubInboxTargetNotEmpty) || errors.Is(err, errHubInboxCopyMismatch):
		fmt.Fprintln(stderr, "migrate-sqlite:", err)
		return ExitConditionInvalid
	case err != nil:
		fmt.Fprintln(stderr, "migrate-sqlite:", hubDatabaseError("copy", err))
		return ExitInternal
	case !copied:
		fmt.Fprintln(stdout, "unchanged: the target already holds this source")
		return ExitOK
	}
	fmt.Fprintln(stdout, "applied:", snapshot.summary())
	return ExitOK
}

var (
	errHubInboxSourceUnreadable = errors.New("source is not a readable SQLite file")
	errHubInboxSourceNotInbox   = errors.New("source is not a hub inbox store")
	errHubInboxTargetNotEmpty   = errors.New("target already holds inbox rows that differ from the source; migrate-sqlite copies only into an empty hub database")
	errHubInboxCopyMismatch     = errors.New("copied rows differ from the source; nothing was committed")
)

// readHubInboxSQLiteSource reads the inbox at path through a private copy of
// the database file and its -wal. SQLite applies the copied WAL when it opens
// the copy, so rows a hub committed but never checkpointed are read, and the
// source files are never opened for writing. The hub must be stopped.
func readHubInboxSQLiteSource(ctx context.Context, path string) (hubInboxSnapshot, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return hubInboxSnapshot{}, errHubInboxSourceUnreadable
	}
	dir, err := os.MkdirTemp("", "panewire-migrate-sqlite-")
	if err != nil {
		return hubInboxSnapshot{}, err
	}
	defer os.RemoveAll(dir)
	// The copy holds inbox payloads: private, like the source.
	if err := os.Chmod(dir, 0700); err != nil {
		return hubInboxSnapshot{}, err
	}
	copyPath := filepath.Join(dir, "inbox.sqlite3")
	if err := copyHubInboxFile(path, copyPath); err != nil {
		return hubInboxSnapshot{}, errHubInboxSourceUnreadable
	}
	// No -shm: SQLite rebuilds the WAL index from the -wal itself.
	if err := copyHubInboxFile(path+"-wal", copyPath+"-wal"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return hubInboxSnapshot{}, errHubInboxSourceUnreadable
	}
	if err := checkHubInboxTables(ctx, copyPath); err != nil {
		return hubInboxSnapshot{}, err
	}
	// The usual open on the copy adds must_ack to a store from before it.
	store, err := openLaneInboxStore(copyPath)
	if err != nil {
		return hubInboxSnapshot{}, errHubInboxSourceNotInbox
	}
	defer store.close()
	snapshot, err := readHubInboxSnapshot(ctx, store.db, laneInboxSQLite)
	if err != nil {
		return hubInboxSnapshot{}, errHubInboxSourceNotInbox
	}
	return snapshot, nil
}

func copyHubInboxFile(from, to string) error {
	src, err := os.Open(from)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		return err
	}
	return dst.Close()
}

// checkHubInboxTables refuses a SQLite file without the inbox tables before
// openLaneInboxStore would create them.
func checkHubInboxTables(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return errHubInboxSourceUnreadable
	}
	defer db.Close()
	var tables int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('lane_inbox','lane_inbox_batches','lane_inbox_batch_quarantine')`).Scan(&tables); err != nil {
		return errHubInboxSourceUnreadable
	}
	if tables != 3 {
		return errHubInboxSourceNotInbox
	}
	return nil
}

// readHubInboxSnapshot reads all three tables in one transaction, batches in
// the order pendingBatches sends them.
func readHubInboxSnapshot(ctx context.Context, db *sql.DB, dialect laneInboxDialect) (hubInboxSnapshot, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return hubInboxSnapshot{}, err
	}
	defer tx.Rollback()
	return readHubInboxSnapshotTx(ctx, tx, dialect)
}

func readHubInboxSnapshotTx(ctx context.Context, tx *sql.Tx, dialect laneInboxDialect) (hubInboxSnapshot, error) {
	var snapshot hubInboxSnapshot
	var spawnTable bool
	if dialect == laneInboxSQLite {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='spawn_operations'`).Scan(&count); err != nil {
			return snapshot, err
		}
		spawnTable = count != 0
	} else {
		var name sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass('spawn_operations')::text`).Scan(&name); err != nil {
			return snapshot, err
		}
		spawnTable = name.Valid
	}
	if spawnTable {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM spawn_operations`).Scan(&count); err != nil {
			return snapshot, err
		}
		if count != 0 {
			return snapshot, errors.New("spawn ownership exists: keep the current backend; the inbox-only migrator cannot transfer spawn reservations, receipts or tombstones")
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT event_key,`+laneInboxColumns+`,sender,event_id FROM lane_inbox ORDER BY id`)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var i hubInboxSnapshotItem
		var stored string
		err := rows.Scan(&stored, &i.item.ID, &i.item.Lane, &i.item.Kind, &i.item.Class, &i.raw, &i.item.CreatedAt, &i.item.ReadAt, &i.item.AckedAt, &i.item.DueAt, &i.item.DigestAt, &i.item.DigestBatch, &i.item.EscalationBatch, &i.item.MustAck, &i.item.HoldUntil, &i.item.DeliveredAt, &i.sender, &i.eventID)
		if !validLaneEventID(i.eventID) {
			// A backfilled SQLite id may hold what Postgres text cannot, and
			// the lookup would never accept it.
			i.eventID = ""
		}
		if err == nil {
			// Snapshots hold the raw key, so the two backends compare.
			i.key, err = laneInboxKeyFromStored(dialect, stored)
		}
		if err != nil {
			rows.Close()
			return snapshot, err
		}
		snapshot.items = append(snapshot.items, i)
	}
	if err := closeRows(rows); err != nil {
		return snapshot, err
	}
	order := "rowid"
	if dialect == laneInboxPostgres {
		order = "seq"
	}
	rows, err = tx.QueryContext(ctx, `SELECT id,lane,text,sent FROM lane_inbox_batches ORDER BY `+order)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var b hubInboxSnapshotBatch
		if err := rows.Scan(&b.batch.ID, &b.batch.Lane, &b.batch.Text, &b.sent); err != nil {
			rows.Close()
			return snapshot, err
		}
		snapshot.batches = append(snapshot.batches, b)
	}
	if err := closeRows(rows); err != nil {
		return snapshot, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT batch_id,reason FROM lane_inbox_batch_quarantine`)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var q [2]string
		if err := rows.Scan(&q[0], &q[1]); err != nil {
			rows.Close()
			return snapshot, err
		}
		snapshot.quarantine = append(snapshot.quarantine, q)
	}
	if err := closeRows(rows); err != nil {
		return snapshot, err
	}

	for _, i := range snapshot.items {
		snapshot.idFloor = max(snapshot.idFloor, i.item.ID)
	}
	if dialect == laneInboxSQLite {
		var floor int64
		if err := tx.QueryRowContext(ctx, `SELECT floor FROM lane_inbox_id_floor WHERE singleton=1`).Scan(&floor); err != nil {
			return snapshot, err
		}
		snapshot.idFloor = max(snapshot.idFloor, floor)
	} else {
		// pg_get_serial_sequence returns a SQL-quoted relation name. Read
		// is_called too: setval(next, false) has not allocated next yet.
		var sequence string
		if err := tx.QueryRowContext(ctx, `SELECT pg_get_serial_sequence('lane_inbox','id')`).Scan(&sequence); err != nil {
			return snapshot, err
		}
		var last int64
		var called bool
		if err := tx.QueryRowContext(ctx, `SELECT last_value,is_called FROM `+sequence).Scan(&last, &called); err != nil {
			return snapshot, err
		}
		if !called {
			last--
		}
		snapshot.idFloor = max(snapshot.idFloor, last)
	}
	// Sorted here, not by ORDER BY: Postgres collates text by locale and
	// SQLite by bytes, and the copy check compares the two snapshots.
	sort.Slice(snapshot.quarantine, func(i, j int) bool { return snapshot.quarantine[i][0] < snapshot.quarantine[j][0] })
	return snapshot, nil
}

func closeRows(rows *sql.Rows) error {
	err := rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	return err
}

func hubPostgresTargetState(ctx context.Context, db *sql.DB, source hubInboxSnapshot) (string, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('lane_inbox') IS NOT NULL`).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		return "schema not created yet (--apply creates it)", nil
	}
	target, err := readHubInboxSnapshot(ctx, db, laneInboxPostgres)
	switch {
	case err != nil:
		return "", err
	case target.empty():
		return "empty", nil
	case reflect.DeepEqual(target, source):
		return "already holds this source (--apply changes nothing)", nil
	}
	return target.summary() + " (differs from the source: --apply will refuse)", nil
}

// copyHubInboxSnapshot creates the schema, writes and verifies the snapshot
// in one transaction, so a failure leaves the target as it was. A target that
// already equals the snapshot is left unchanged and reports copied=false.
// The table locks keep a hub that was left running from inserting between
// the check and the commit; item ids are kept so inbox read/ack ids a lane
// already saw stay valid, and the identity sequence moves past them.
func copyHubInboxSnapshot(ctx context.Context, db *sql.DB, snapshot hubInboxSnapshot) (copied bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	// A hub left running holds locks the copy needs; fail instead of
	// waiting on it without end. This covers the migration lock too.
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '10s'`); err != nil {
		return false, err
	}
	if err := migrateHubPostgresTx(ctx, tx); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `LOCK TABLE lane_inbox, lane_inbox_batches, lane_inbox_batch_quarantine IN EXCLUSIVE MODE`); err != nil {
		return false, err
	}
	target, err := readHubInboxSnapshotTx(ctx, tx, laneInboxPostgres)
	if err != nil {
		return false, err
	}
	if !target.empty() {
		if !reflect.DeepEqual(target, snapshot) {
			return false, errHubInboxTargetNotEmpty
		}
		// A rerun: the rows are already here. The commit can only record
		// a migration this binary added since the first run.
		return false, tx.Commit()
	}
	for _, i := range snapshot.items {
		mustAck := 0
		if i.item.MustAck {
			mustAck = 1
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO lane_inbox(id,event_key,lane,kind,class,payload,created_at,read_at,acked_at,due_at,digest_at,digest_batch,escalation_batch,must_ack,hold_until,delivered_at,sender,event_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
			i.item.ID, laneInboxStoredKey(laneInboxPostgres, i.key), i.item.Lane, i.item.Kind, i.item.Class, i.raw, i.item.CreatedAt, i.item.ReadAt, i.item.AckedAt, i.item.DueAt, i.item.DigestAt, i.item.DigestBatch, i.item.EscalationBatch, mustAck, i.item.HoldUntil, i.item.DeliveredAt, i.sender, i.eventID); err != nil {
			return false, err
		}
	}
	if _, err := tx.ExecContext(ctx, `SELECT setval(pg_get_serial_sequence('lane_inbox','id'), (SELECT GREATEST(COALESCE(max(id),0),$1::bigint)+1 FROM lane_inbox), false)`, snapshot.idFloor); err != nil {
		return false, err
	}
	for _, b := range snapshot.batches {
		if _, err := tx.ExecContext(ctx, `INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES($1,$2,$3,$4)`, b.batch.ID, b.batch.Lane, b.batch.Text, b.sent); err != nil {
			return false, err
		}
	}
	for _, q := range snapshot.quarantine {
		if _, err := tx.ExecContext(ctx, `INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES($1,$2)`, q[0], q[1]); err != nil {
			return false, err
		}
	}
	// Row-for-row before commit, not only counts: ids, keys, payload bytes,
	// read/ack state and batch order. A mismatch rolls the whole copy back.
	written, err := readHubInboxSnapshotTx(ctx, tx, laneInboxPostgres)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(written, snapshot) {
		return false, errHubInboxCopyMismatch
	}
	return !snapshot.empty(), tx.Commit()
}
