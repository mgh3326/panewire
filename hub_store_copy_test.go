package panewire

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestHubStoreCopyTableInventory(t *testing.T) {
	want := copyFixtureTables(t)
	if len(want) != len(storeCopyTables) {
		t.Fatalf("portable table count %d, want %d", len(storeCopyTables), len(want))
	}
	for i, name := range want {
		if storeCopyTables[i].name != name {
			t.Fatalf("table %d is %q, want %q", i, storeCopyTables[i].name, name)
		}
	}
}

func TestHubStoreCopyOrderAndColumns(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`CREATE TABLE sample (id TEXT PRIMARY KEY, value TEXT); INSERT INTO sample VALUES ('b','two'),('a','one')`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	backend := &storeCopyDB{db: db, dialect: dialectSQLite, sqlite: true}
	got, err := storeCopyScan(t.Context(), tx, backend, backend, nil, storeCopyTable{"sample", "id"}, []string{"id", "value"}, false)
	if err != nil {
		t.Fatal(err)
	}
	// The encoding is length-delimited values, in primary-key order. This fixed
	// digest catches both a skipped column and an order-by removed from the scan.
	const expected = "591280cc4903beeae1aca3cbdbf66f2d97560bceae6d94588af49e915e2ef330"
	if fmt.Sprintf("%x", got.sum) != expected || got.count != 2 {
		t.Fatalf("ordered checksum %x (%d rows)", got.sum, got.count)
	}
}

func TestHubStoreCopyRoundTrips(t *testing.T) {
	mysqlConfig, mysqlURL := hubMySQLTestDatabase(t)
	pgConfig, pgURL := hubPostgresTestSchema(t)
	_, mysqlTargetURL := hubMySQLTestDatabase(t)
	source, err := openLaneInboxStoreMySQLConfig(t.Context(), mysqlConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.close() })
	names := copyFixtureTables(t)
	seedCopyFixture(t, source.db, names)
	var report bytes.Buffer
	if err := copyHubStore(t.Context(), mysqlURL, pgURL, "", false, false, &report); err != nil {
		t.Fatalf("mysql to postgres: %v\n%s", err, report.String())
	}
	assertCopyReport(t, report.String(), names)
	pgStore, err := openLaneInboxStorePostgresConfig(t.Context(), pgConfig.Copy())
	if err != nil {
		t.Fatal(err)
	}
	longKey := "long" + strings.Repeat("x", mysqlEventKeyMax)
	again, err := pgStore.put(longKey, "lane", "lane.event", "inbox", quietEvent("replay", "test"), time.Now(), 0, 0)
	if err != nil || again.Fresh || again.ID != 2 {
		t.Fatalf("copied-back replay: id=%d fresh=%v err=%v", again.ID, again.Fresh, err)
	}
	_ = pgStore.close()
	report.Reset()
	if err := copyHubStore(t.Context(), pgURL, mysqlTargetURL, "", false, false, &report); err != nil {
		t.Fatalf("postgres to mysql: %v\n%s", err, report.String())
	}
	assertCopyReport(t, report.String(), names)
	report.Reset()
	if err := copyHubStore(t.Context(), pgURL, mysqlTargetURL, "", false, false, &report); err == nil || !strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("non-empty target: %v", err)
	}
	// A changed column must fail verification and identify its table.
	targetConfig, err := hubMySQLConfig(mysqlTargetURL)
	if err != nil {
		t.Fatal(err)
	}
	target, err := openHubMySQL(t.Context(), targetConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if _, err = target.Exec(`UPDATE lane_inbox SET payload='changed' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	report.Reset()
	if err = copyHubStore(t.Context(), pgURL, mysqlTargetURL, "", true, false, &report); err == nil || !strings.Contains(err.Error(), "lane_inbox") {
		t.Fatalf("altered target: %v", err)
	}
	// Build a SQLite fixture independently of the copier's table inventory.
	sqlitePath := filepath.Join(t.TempDir(), "source.sqlite")
	makeCopySQLiteFixture(t, source.db, sqlitePath, names)
	_, sqliteTargetURL := hubMySQLTestDatabase(t)
	report.Reset()
	if err = copyHubStore(t.Context(), "sqlite://"+sqlitePath, sqliteTargetURL, "", false, false, &report); err != nil {
		t.Fatalf("sqlite to mysql: %v\n%s", err, report.String())
	}
	assertCopyReport(t, report.String(), names)
}

func copyFixtureTables(t *testing.T) []string {
	t.Helper()
	paths, err := fs.Glob(hubMySQLMigrations, "hub_mysql_migrations/*.sql")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`(?i)CREATE TABLE ([a-z_]+)\s*\(`)
	var names []string
	for _, path := range paths {
		b, err := hubMySQLMigrations.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		m := re.FindSubmatch(b)
		if m != nil && string(m[1]) != "supersede_lane_cause" {
			names = append(names, string(m[1]))
		}
	}
	return names
}
func assertCopyReport(t *testing.T, report string, names []string) {
	t.Helper()
	for _, name := range names {
		count := "source=2 "
		if name == "supersede_fence" {
			count = "source=1 "
		}
		if !strings.Contains(report, name+" "+count) {
			t.Errorf("missing seeded table %s in report:\n%s", name, report)
		}
	}
}
func seedCopyFixture(t *testing.T, db *sql.DB, names []string) {
	t.Helper()
	ctx := context.Background()
	for _, name := range names {
		rows, err := db.QueryContext(ctx, `SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=? ORDER BY ORDINAL_POSITION`, name)
		if err != nil {
			t.Fatal(err)
		}
		var cols, types []string
		for rows.Next() {
			var col, typ string
			if err := rows.Scan(&col, &typ); err != nil {
				t.Fatal(err)
			}
			cols = append(cols, col)
			types = append(types, typ)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if name == "supersede_fence" {
			if _, err := db.ExecContext(ctx, "DELETE FROM supersede_fence"); err != nil {
				t.Fatal(err)
			}
		}
		count := 2
		if name == "supersede_fence" {
			count = 1
		}
		for n := 1; n <= count; n++ {
			vals := make([]any, len(cols))
			for i, col := range cols {
				switch {
				case col == "must_ack":
					vals[i] = n % 2
				case col == "payload" && name == "lane_inbox":
					vals[i] = "{}"
				case col == "class":
					vals[i] = "inbox"
				case col == "phase":
					vals[i] = "pending"
				case col == "state" && name == "supersede_landings":
					vals[i] = "armed"
				case col == "event_key" && name == "lane_inbox":
					if n == 2 {
						vals[i] = dialectMySQL.storedKey("long" + strings.Repeat("x", mysqlEventKeyMax))
					} else {
						vals[i] = []byte("key-one")
					}
				case strings.Contains(types[i], "int"):
					vals[i] = n
				default:
					vals[i] = fmt.Sprintf("%s_%d", col, n)
				}
			}
			q := "INSERT INTO " + name + " (" + strings.Join(cols, ",") + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
			if _, err := db.ExecContext(ctx, q, vals...); err != nil {
				t.Fatalf("seed %s: %v", name, err)
			}
		}
	}
}
func makeCopySQLiteFixture(t *testing.T, mysql *sql.DB, path string, names []string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range names {
		rows, err := mysql.Query("SELECT * FROM " + name)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		createCols := append([]string(nil), cols...)
		if name == "lane_inbox_batches" {
			for i, c := range createCols {
				if c == "seq" {
					createCols = append(createCols[:i], createCols[i+1:]...)
					break
				}
			}
		}
		if _, err = db.Exec("CREATE TABLE " + name + " (" + strings.Join(createCols, ",") + ")"); err != nil {
			t.Fatal(err)
		}
		insertCols := append([]string(nil), cols...)
		if name == "lane_inbox_batches" {
			for i, c := range insertCols {
				if c == "seq" {
					insertCols[i] = "rowid"
				}
			}
		}
		q := "INSERT INTO " + name + " (" + strings.Join(insertCols, ",") + ") VALUES (" + strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",") + ")"
		for rows.Next() {
			vals := make([]any, len(cols))
			ptr := make([]any, len(cols))
			for i := range vals {
				ptr[i] = &vals[i]
			}
			if err = rows.Scan(ptr...); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(q, vals...); err != nil {
				t.Fatalf("sqlite seed %s: %v", name, err)
			}
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	if _, err = db.Exec(`CREATE TABLE lane_inbox_id_floor (singleton INTEGER, floor INTEGER); INSERT INTO lane_inbox_id_floor VALUES (1, 50)`); err != nil {
		t.Fatal(err)
	}
}

func TestHubStoreCopySQLiteReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE sample (id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	source, err := openStoreCopyDB(t.Context(), "sqlite://"+path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer source.db.Close()
	tx, err := source.db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT INTO sample VALUES(1)`); err == nil {
		t.Fatal("source allowed a write")
	}
}
