package panewire

import (
	"testing"
)

// hk 6507: the high-water read of an older source must skip the sequenced
// delivery tables the source predates instead of failing on their missing
// sequences. Only delivery_obligations and delivery_incidents reach this
// read's query (the other older-source tables have no sequence column, and a
// SQLite source skips the query entirely), so MySQL in CI pins the check
// here. Killed by: removing the storeCopyOlderSourceTables check from
// storeCopyHighWater (the information_schema read then finds no row for a
// missing table).
func TestHubStoreCopyHighWaterSkipsMissingOlderSourceTables_t6507(t *testing.T) {
	mysqlConfig, _ := hubMySQLTestDatabase(t)
	source, err := openLaneInboxStoreMySQLConfig(t.Context(), mysqlConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.close() })
	// A source written before hk 6384 has none of the delivery tables.
	if _, err := source.db.Exec(`DROP TABLE delivery_obligations, delivery_incidents`); err != nil {
		t.Fatal(err)
	}
	backend := &storeCopyDB{db: source.db, dialect: dialectMySQL}
	tx, err := source.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Named here, not read from storeCopyOlderSourceTables, so dropping a
	// table from that set cannot also drop it from this check.
	var tables []storeCopyTable
	for _, name := range []string{"delivery_obligations", "delivery_incidents"} {
		table, ok := storeCopyTableNamed(name)
		if !ok {
			t.Fatalf("%s is not a portable table", name)
		}
		tables = append(tables, table)
	}
	got, err := storeCopyHighWater(t.Context(), tx, backend, tables)
	if err != nil {
		t.Fatalf("high-water of an older source: %v; want the missing tables skipped", err)
	}
	if len(got) != 0 {
		t.Fatalf("high-water %v; want no entries for the missing tables", got)
	}
}
