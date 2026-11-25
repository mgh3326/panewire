package panewire

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"strings"
	"testing"
)

// hk 6486: the delivery tables joined the copier without the tolerance
// supersede_lane_cause has. A source written before hk 6384 has none of them,
// and the copy must read each as empty rather than fail after the target was
// written. Killed by: removing a storeCopyOlderSourceTables check from one of
// the three reads of the source (columns, scan, high-water).
func TestHubStoreCopyOlderSourceReadsMissingTablesAsEmpty_t6486(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	// An older store: it has rows of its own, and none of the later tables.
	if _, err = db.Exec(`CREATE TABLE sample (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	backend := &storeCopyDB{db: db, dialect: dialectSQLite, sqlite: true}
	// Named here, not read from storeCopyOlderSourceTables, so dropping a table
	// from that set cannot also drop it from this check.
	for _, name := range []string{"supersede_lane_cause", "delivery_obligations", "delivery_incidents", "delivery_notice_slots", "delivery_reconciler_state"} {
		table, ok := storeCopyTableNamed(name)
		if !ok {
			t.Fatalf("%s is not a portable table", name)
		}
		cols, err := storeCopySourceColumns(t.Context(), tx, backend, table)
		if err != nil || cols != nil {
			t.Fatalf("%s: columns %v, err %v; want none and no error", name, cols, err)
		}
		got, err := storeCopyScan(t.Context(), tx, backend, backend, nil, table, nil, false)
		if err != nil || got.count != 0 || got.sum != sha256.Sum256(nil) {
			t.Fatalf("%s: scan %+v, err %v; want an empty copy", name, got, err)
		}
	}
}

func storeCopyTableNamed(name string) (storeCopyTable, bool) {
	for _, table := range storeCopyTables {
		if table.name == name {
			return table, true
		}
	}
	return storeCopyTable{}, false
}

// The tolerance covers only a table that is missing. One that exists is read
// as before, so a check that always reports "absent" fails here.
func TestHubStoreCopyOlderSourceReadsPresentTable_t6486(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`CREATE TABLE delivery_reconciler_state (singleton INTEGER PRIMARY KEY, adopt_after_id INTEGER NOT NULL); INSERT INTO delivery_reconciler_state VALUES (1, 7)`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	backend := &storeCopyDB{db: db, dialect: dialectSQLite, sqlite: true}
	table := storeCopyTable{"delivery_reconciler_state", "singleton"}
	cols, err := storeCopySourceColumns(t.Context(), tx, backend, table)
	if err != nil || len(cols) != 2 {
		t.Fatalf("columns %v, err %v; want the two present columns", cols, err)
	}
	got, err := storeCopyScan(t.Context(), tx, backend, backend, nil, table, cols, false)
	if err != nil || got.count != 1 {
		t.Fatalf("scan count %d, err %v; want the one present row", got.count, err)
	}
}

// hk 6486, CI only (MySQL and Postgres service containers): a MySQL source
// written before hk 6384 has none of the delivery tables. The copy completes,
// and each of those tables is reported as an empty copy. Before the tolerance
// covered the high-water read, this copy failed after the target was written.
func TestHubStoreCopyFromOlderSourceWithoutDeliveryTables_t6486(t *testing.T) {
	mysqlConfig, mysqlURL := hubMySQLTestDatabase(t)
	_, pgURL := hubPostgresTestSchema(t)
	source, err := openLaneInboxStoreMySQLConfig(t.Context(), mysqlConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.close() })
	delivery := []string{"delivery_obligations", "delivery_incidents", "delivery_notice_slots", "delivery_reconciler_state"}
	if _, err := source.db.Exec(`DROP TABLE ` + strings.Join(delivery, ", ")); err != nil {
		t.Fatal(err)
	}
	var report bytes.Buffer
	if err := copyHubStore(t.Context(), mysqlURL, pgURL, "", false, false, &report); err != nil {
		t.Fatalf("copy from an older source: %v\n%s", err, report.String())
	}
	for _, name := range delivery {
		if !strings.Contains(report.String(), name+" source=0 ") {
			t.Errorf("missing table %s is not reported as an empty copy:\n%s", name, report.String())
		}
	}
}
