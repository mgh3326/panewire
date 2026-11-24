package panewire

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// hk 6283: the backfill's migration files and the MySQL runner's batched form.
// The rows they must change are asserted by
// TestLaneInboxStoreSpawnNoticeBackfillMarksOnlySucceeded_t6283.

// countingExecer answers each Exec with the next matched-row count.
type countingExecer struct {
	matched []int64
	calls   int
	failAt  int
}

func (e *countingExecer) ExecContext(context.Context, string, ...any) (sql.Result, error) {
	e.calls++
	if e.calls == e.failAt {
		return nil, errors.New("injected")
	}
	return driver.RowsAffected(e.matched[e.calls-1]), nil
}

func (*countingExecer) QueryRowContext(context.Context, string, ...any) *sql.Row { return nil }

func TestHubMySQLBatchedMigrationRunsUntilAShortBatch_t6283(t *testing.T) {
	for _, test := range []struct {
		name    string
		batch   int64
		matched []int64
		total   int64
		calls   int
	}{
		{"short last batch", 1000, []int64{1000, 1000, 37}, 2037, 3},
		{"exact multiple", 1000, []int64{1000, 0}, 1000, 2},
		{"nothing to do", 1000, []int64{0}, 0, 1},
		{"unbatched runs once", 0, []int64{5000}, 5000, 1},
	} {
		x := &countingExecer{matched: test.matched}
		total, err := hubMySQLMigration{statement: "UPDATE t SET a=1", batchRows: test.batch}.run(t.Context(), x)
		if err != nil || total != test.total || x.calls != test.calls {
			t.Errorf("%s: total=%d calls=%d err=%v, want %d in %d calls", test.name, total, x.calls, err, test.total, test.calls)
		}
	}
	// A failed batch fails the file, so no ledger row is written for it.
	x := &countingExecer{matched: []int64{1000, 0}, failAt: 2}
	if _, err := (hubMySQLMigration{statement: "UPDATE t SET a=1", batchRows: 1000}).run(t.Context(), x); err == nil {
		t.Error("a failed second batch reported success")
	}
}

func TestParseHubMySQLMigrationBatchRows_t6283(t *testing.T) {
	const guard = "-- applied-if: SELECT 0\n"
	const update = "UPDATE a SET x=1\nWHERE x=0\nLIMIT 1000;\n"
	good, err := parseHubMySQLMigration(guard + "-- batch-rows: 1000\n" + update)
	if err != nil || good.batchRows != 1000 {
		t.Fatalf("batched file: %+v, %v", good, err)
	}
	for _, c := range []struct{ name, body, want string }{
		{"smaller limit", guard + "-- batch-rows: 1000\nUPDATE a SET x=1 WHERE x=0 LIMIT 100;\n", "ending in LIMIT batch-rows"},
		{"no limit", guard + "-- batch-rows: 1000\nUPDATE a SET x=1 WHERE x=0;\n", "ending in LIMIT batch-rows"},
		{"not an update", guard + "-- batch-rows: 1000\nDELETE FROM a LIMIT 1000;\n", "ending in LIMIT batch-rows"},
		{"zero", guard + "-- batch-rows: 0\n" + update, "not a positive number"},
		{"not a number", guard + "-- batch-rows: many\n" + update, "not a positive number"},
		{"twice", guard + "-- batch-rows: 1000\n-- batch-rows: 1000\n" + update, "more than one batch-rows"},
	} {
		if _, err := parseHubMySQLMigration(c.body); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one naming %q", c.name, err, c.want)
		}
	}
}

// spawnNoticeBackfillFile reads the one migration file of the backfill.
func spawnNoticeBackfillFile(t *testing.T, fsys fs.ReadFileFS, dir string) string {
	t.Helper()
	names, err := fs.Glob(fsys, dir+"/*"+succeededNoticeMigration+".sql")
	if err != nil || len(names) != 1 {
		t.Fatalf("%s backfill files = %v, %v; want exactly one", dir, names, err)
	}
	body, err := fsys.ReadFile(names[0])
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// The Postgres and MySQL files select the rows the SQLite backfill selects.
// SQLite reads both texts (the MySQL one without its LIMIT), so a predicate
// that drifts fails here, without a database server.
func TestSpawnNoticeBackfillFilesSelectTheSameRows_t6283(t *testing.T) {
	mysqlFile, err := parseHubMySQLMigration(spawnNoticeBackfillFile(t, hubMySQLMigrations, "hub_mysql_migrations"))
	if err != nil || mysqlFile.batchRows != 1000 {
		t.Fatalf("MySQL backfill: batch=%d err=%v, want batches of 1000", mysqlFile.batchRows, err)
	}
	for name, statement := range map[string]string{
		"postgres": spawnNoticeBackfillFile(t, hubPostgresMigrations, "hub_postgres_migrations"),
		"mysql":    regexp.MustCompile(`(?i)\s+LIMIT\s+\d+$`).ReplaceAllString(mysqlFile.statement, ""),
	} {
		t.Run(name, func(t *testing.T) {
			s := openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			rows := legacySpawnNotices()
			seedLegacySpawnNotices(t, s, rows)
			if _, err := s.db.Exec(statement); err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				want := row.recorded
				if row.marked {
					want = 1
				}
				wantSpawnNoticeMarks(t, s, want, row.event)
			}
		})
	}
}

// More rows than a batch: the MySQL start marks them all and reports the sum.
// It runs in CI against the MySQL service container.
func TestLaneInboxStoreMySQLSpawnNoticeBackfillRunsInBatches_t6283(t *testing.T) {
	config, _ := hubMySQLTestDatabase(t)
	first, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer first.close()
	const total = 2500
	tx, err := first.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for n := range total {
		request := t14RequestID(n)
		if _, err := tx.Exec(`INSERT INTO spawn_notifications(event_id,request_id,job,owner,result,created_ms,delivered) VALUES(?,?,?,?,?,?,1)`, fmt.Sprintf("%s:1:succeeded", request), request, "job", "parent", `{"rc":0}`, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	forgetSpawnNoticeBackfill(t, first)
	second, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	if second.succeededNoticesMarked == nil || *second.succeededNoticesMarked != total {
		t.Fatalf("marked = %v, want %d", second.succeededNoticesMarked, total)
	}
	if n := countRows(t, second.db, `SELECT count(*) FROM spawn_notifications WHERE telemetry_recorded=0`); n != 0 {
		t.Fatalf("%d rows left unmarked", n)
	}
}
