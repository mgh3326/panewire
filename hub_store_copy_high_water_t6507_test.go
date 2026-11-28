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
// missing table), or by a mutant that always skips (the recreated
// delivery_obligations then reports no high-water).
func TestHubStoreCopyHighWaterSkipsMissingOlderSourceTables_t6507(t *testing.T) {
	mysqlConfig, _ := hubMySQLTestDatabase(t)
	// A copy source reads information_schema without the server's cached
	// statistics (openStoreCopyDB); match that so AUTO_INCREMENT is fresh.
	mysqlConfig.Params["information_schema_stats_expiry"] = "0"
	source, err := openLaneInboxStoreMySQLConfig(t.Context(), mysqlConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.close() })
	// The positive half below recreates the table from its own CREATE.
	var table, createObligations string
	if err := source.db.QueryRow(`SHOW CREATE TABLE delivery_obligations`).Scan(&table, &createObligations); err != nil {
		t.Fatal(err)
	}
	// A source written before hk 6384 has none of the delivery tables.
	if _, err := source.db.Exec(`DROP TABLE delivery_obligations, delivery_incidents`); err != nil {
		t.Fatal(err)
	}
	backend := &storeCopyDB{db: source.db, dialect: dialectMySQL}
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
	highWater := func() (map[string]int64, error) {
		tx, err := source.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		return storeCopyHighWater(t.Context(), tx, backend, tables)
	}
	got, err := highWater()
	if err != nil {
		t.Fatalf("high-water of an older source: %v; want the missing tables skipped", err)
	}
	if len(got) != 0 {
		t.Fatalf("high-water %v; want no entries for the missing tables", got)
	}
	// The positive half: a present table still reports AUTO_INCREMENT-1, so
	// the skip cannot be unconditional. delivery_incidents stays missing.
	if _, err := source.db.Exec(createObligations); err != nil {
		t.Fatal(err)
	}
	if _, err := source.db.Exec(`ALTER TABLE delivery_obligations AUTO_INCREMENT=7`); err != nil {
		t.Fatal(err)
	}
	got, err = highWater()
	if err != nil {
		t.Fatalf("high-water with a present delivery_obligations: %v", err)
	}
	if len(got) != 1 || got["delivery_obligations"] != 6 {
		t.Fatalf("high-water %v; want only delivery_obligations=6", got)
	}
}
