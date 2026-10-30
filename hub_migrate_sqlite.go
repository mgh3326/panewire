package panewire

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/jackc/pgx/v5"
)

const hubMigrateSQLiteUsage = `Usage: panewire hub migrate-sqlite --from PATH [--apply]

Copies a hub inbox SQLite file (--inbox-store) into the Postgres database named
by ` + hubDatabaseURLEnv + `. Without --apply it only reads: it prints the
source row counts and, when the URL is set, the target's state. --apply copies
every row in one transaction and refuses a target that already holds inbox
rows. Stop the hub before applying. See docs/hub-postgres.md.`

// hubInboxSnapshot is every row of the three inbox tables, batches in send
// order.
type hubInboxSnapshot struct {
	items      []hubInboxSnapshotItem
	batches    []hubInboxSnapshotBatch
	quarantine [][2]string
}

type hubInboxSnapshotItem struct {
	key  string
	item laneInboxItem
	raw  string
}

type hubInboxSnapshotBatch struct {
	batch laneInboxBatch
	sent  int64
}

func (s hubInboxSnapshot) summary() string {
	unread, pending := 0, 0
	for _, i := range s.items {
		if i.item.Class == "inbox" && i.item.ReadAt == 0 {
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
	return fmt.Sprintf("lane_inbox=%d (unread=%d) lane_inbox_batches=%d (pending=%d) lane_inbox_batch_quarantine=%d",
		len(s.items), unread, len(s.batches), pending, len(s.quarantine))
}

func runHubMigrateSQLite(ctx context.Context, args []string, databaseURL string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("panewire hub migrate-sqlite", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	from := flags.String("from", "", "hub inbox SQLite file to copy")
	apply := flags.Bool("apply", false, "write to the Postgres database; without it the command only reads")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *from == "" {
		return writeHubCLIUsage(stderr, "invalid migrate-sqlite flags", hubMigrateSQLiteUsage)
	}
	src, err := openHubInboxSQLiteReadOnly(*from)
	if err != nil {
		fmt.Fprintln(stderr, "migrate-sqlite: source is not a readable SQLite file")
		return ExitConditionInvalid
	}
	defer src.Close()
	snapshot, err := readHubInboxSnapshot(ctx, src, laneInboxSQLite)
	if err != nil {
		fmt.Fprintln(stderr, "migrate-sqlite: source is not a hub inbox store")
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
	if !apply {
		db, err := openHubPostgres(ctx, config)
		if err != nil {
			fmt.Fprintln(stderr, "migrate-sqlite:", err)
			return ExitInternal
		}
		defer db.Close()
		state, err := hubPostgresTargetState(ctx, db)
		if err != nil {
			fmt.Fprintln(stderr, "migrate-sqlite:", hubDatabaseError("inspect", err))
			return ExitInternal
		}
		fmt.Fprintln(stdout, "target:", state)
		fmt.Fprintln(stdout, "dry-run: nothing written; rerun with --apply to copy")
		return ExitOK
	}
	store, err := openLaneInboxStorePostgresConfig(ctx, config)
	if err != nil {
		fmt.Fprintln(stderr, "migrate-sqlite:", err)
		return ExitInternal
	}
	defer store.close()
	if err := copyHubInboxSnapshot(ctx, store.db, snapshot); err != nil {
		if errors.Is(err, errHubInboxTargetNotEmpty) || errors.Is(err, errHubInboxCopyMismatch) {
			fmt.Fprintln(stderr, "migrate-sqlite:", err)
			return ExitConditionInvalid
		}
		fmt.Fprintln(stderr, "migrate-sqlite:", hubDatabaseError("copy", err))
		return ExitInternal
	}
	fmt.Fprintln(stdout, "applied:", snapshot.summary())
	return ExitOK
}

// openHubInboxSQLiteReadOnly opens an existing inbox file without creating,
// migrating, or writing it.
func openHubInboxSQLiteReadOnly(path string) (*sql.DB, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// Built as a URL so '?', '#' or '%' in a file name stay part of the path
	// instead of truncating it or adding parameters.
	uri := url.URL{Scheme: "file", Path: abs, RawQuery: "mode=ro&_pragma=busy_timeout(5000)"}
	db, err := sql.Open("sqlite", uri.String())
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
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
	rows, err := tx.QueryContext(ctx, `SELECT event_key,`+laneInboxColumns+` FROM lane_inbox ORDER BY id`)
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		var i hubInboxSnapshotItem
		err := rows.Scan(&i.key, &i.item.ID, &i.item.Lane, &i.item.Kind, &i.item.Class, &i.raw, &i.item.CreatedAt, &i.item.ReadAt, &i.item.AckedAt, &i.item.DueAt, &i.item.DigestAt, &i.item.DigestBatch, &i.item.EscalationBatch)
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

var (
	errHubInboxTargetNotEmpty = errors.New("target already holds inbox rows; migrate-sqlite copies only into an empty hub database")
	errHubInboxCopyMismatch   = errors.New("copied rows differ from the source; nothing was committed")
)

func hubPostgresTargetState(ctx context.Context, db *sql.DB) (string, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass('lane_inbox') IS NOT NULL`).Scan(&exists); err != nil {
		return "", err
	}
	if !exists {
		return "schema not created yet (--apply creates it)", nil
	}
	snapshot, err := readHubInboxSnapshot(ctx, db, laneInboxPostgres)
	if err != nil {
		return "", err
	}
	if len(snapshot.items)+len(snapshot.batches)+len(snapshot.quarantine) != 0 {
		return snapshot.summary() + " (not empty: --apply will refuse)", nil
	}
	return "empty", nil
}

// copyHubInboxSnapshot writes and verifies the snapshot in one transaction. The table
// locks keep a hub that was left running from inserting between the
// emptiness check and the commit; item ids are kept so inbox read/ack ids a
// lane already saw stay valid, and the identity sequence moves past them.
func copyHubInboxSnapshot(ctx context.Context, db *sql.DB, snapshot hubInboxSnapshot) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `LOCK TABLE lane_inbox, lane_inbox_batches, lane_inbox_batch_quarantine IN EXCLUSIVE MODE`); err != nil {
		return err
	}
	var rows int64
	if err := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM lane_inbox)+(SELECT count(*) FROM lane_inbox_batches)+(SELECT count(*) FROM lane_inbox_batch_quarantine)`).Scan(&rows); err != nil {
		return err
	}
	if rows != 0 {
		return errHubInboxTargetNotEmpty
	}
	for _, i := range snapshot.items {
		if _, err := tx.ExecContext(ctx, `INSERT INTO lane_inbox(id,event_key,lane,kind,class,payload,created_at,read_at,acked_at,due_at,digest_at,digest_batch,escalation_batch) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			i.item.ID, i.key, i.item.Lane, i.item.Kind, i.item.Class, i.raw, i.item.CreatedAt, i.item.ReadAt, i.item.AckedAt, i.item.DueAt, i.item.DigestAt, i.item.DigestBatch, i.item.EscalationBatch); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `SELECT setval(pg_get_serial_sequence('lane_inbox','id'), (SELECT COALESCE(max(id),0)+1 FROM lane_inbox), false)`); err != nil {
		return err
	}
	for _, b := range snapshot.batches {
		if _, err := tx.ExecContext(ctx, `INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES($1,$2,$3,$4)`, b.batch.ID, b.batch.Lane, b.batch.Text, b.sent); err != nil {
			return err
		}
	}
	for _, q := range snapshot.quarantine {
		if _, err := tx.ExecContext(ctx, `INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES($1,$2)`, q[0], q[1]); err != nil {
			return err
		}
	}
	// Row-for-row before commit, not only counts: ids, keys, payload bytes
	// and batch order. A mismatch rolls the whole copy back.
	copied, err := readHubInboxSnapshotTx(ctx, tx, laneInboxPostgres)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(copied, snapshot) {
		return errHubInboxCopyMismatch
	}
	return tx.Commit()
}
