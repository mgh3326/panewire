package arbiter

import (
	"context"
	"os"
	"testing"
)

// TestSchemaDocArbiterState creates the arbiter state.db with the production
// bootstrap path (Store.rw with bootstrap set, the same call Claim and
// AppendEvent make on a fresh ledger) under PANEWIRE_SCHEMA_DOC_ARBITER_DIR,
// for tbls schema doc generation in CI. It is gated by that variable, skips
// everywhere else, and leaves the database in place for tbls (hk 6117).
//
// When PANEWIRE_SCHEMA_DOC_SQLITE_VERSION_FILE is set, the driver's
// sqlite_version() is written there so the tbls description can name it.
func TestSchemaDocArbiterState(t *testing.T) {
	dir := os.Getenv("PANEWIRE_SCHEMA_DOC_ARBITER_DIR")
	if dir == "" {
		t.Skip("PANEWIRE_SCHEMA_DOC_ARBITER_DIR is unset; this test runs only in the schema-doc workflow")
	}
	ctx := context.Background()
	s := &Store{DB: StatePath(dir)}
	c, err := s.rw(ctx, true)
	if err != nil {
		t.Fatalf("create state db: %v", err)
	}
	defer c.Close()

	var version string
	if err := c.row(`select sqlite_version()`).Scan(&version); err != nil {
		t.Fatalf("query sqlite_version(): %v", err)
	}
	t.Logf("SQLite version: %s", version)
	if path := os.Getenv("PANEWIRE_SCHEMA_DOC_SQLITE_VERSION_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(version), 0644); err != nil {
			t.Fatalf("write version file: %v", err)
		}
	}
}
