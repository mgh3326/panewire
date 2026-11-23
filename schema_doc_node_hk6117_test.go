package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSchemaDocNodeStores builds the node's two SQLite stores that live in the
// root package, the event store (OpenStore) and the spawn journal
// (openNodeSpawnJournal), under PANEWIRE_SCHEMA_DOC_NODE_DIR for tbls schema
// doc generation in CI. It is gated by that variable, skips everywhere else,
// and leaves both files in place for tbls (hk 6117). The arbiter state.db is
// built by internal/arbiter's TestSchemaDocArbiterState.
//
// When PANEWIRE_SCHEMA_DOC_SQLITE_VERSION_FILE is set, the driver's
// sqlite_version() is written there so the tbls description can name it.
func TestSchemaDocNodeStores(t *testing.T) {
	dir := os.Getenv("PANEWIRE_SCHEMA_DOC_NODE_DIR")
	if dir == "" {
		t.Skip("PANEWIRE_SCHEMA_DOC_NODE_DIR is unset; this test runs only in the schema-doc workflow")
	}
	store, err := OpenStore(filepath.Join(dir, "panewire.sqlite3"))
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("store.Close: %v", err)
		}
	}()
	journal, err := openNodeSpawnJournal(filepath.Join(dir, "spawn_inbox"))
	if err != nil {
		t.Fatalf("openNodeSpawnJournal failed: %v", err)
	}
	defer func() {
		if err := journal.db.Close(); err != nil {
			t.Errorf("journal.db.Close: %v", err)
		}
	}()

	var version string
	if err := store.db.QueryRow(`select sqlite_version()`).Scan(&version); err != nil {
		t.Fatalf("query sqlite_version(): %v", err)
	}
	t.Logf("SQLite version: %s", version)
	if path := os.Getenv("PANEWIRE_SCHEMA_DOC_SQLITE_VERSION_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(version), 0644); err != nil {
			t.Fatalf("write version file: %v", err)
		}
	}
}
