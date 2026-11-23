package panewire

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// nodeEventStoreLazyTables are created on the event store by constructors
// that run at daemon start, outside OpenStore: quota-stop manager
// (quota_stop_runtime.go newQuotaStopManager) and stall-safety state
// (job_inactivity_state.go newStallSafetyState). The builder calls the same
// constructors so tbls documents them too (hk 6117).
var nodeEventStoreLazyTables = []string{
	"quota_stop_decisions",
	"quota_blocked_wakes",
	"quota_stall_wakes",
	"stall_safety_episodes",
	"stall_safety_examinations",
}

// TestSchemaDocNodeStores builds the node's two SQLite stores that live in the
// root package, the event store (OpenStore, plus the daemon-start constructors
// that create nodeEventStoreLazyTables) and the spawn journal
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
	// These constructors only create tables and in-memory state; they start
	// no goroutines and need no running daemon.
	if _, err := newStallSafetyState(store); err != nil {
		t.Fatalf("newStallSafetyState failed: %v", err)
	}
	if _, err := newQuotaStopManager(store, dir, false, slog.Default()); err != nil {
		t.Fatalf("newQuotaStopManager failed: %v", err)
	}
	for _, table := range nodeEventStoreLazyTables {
		var n int
		if err := store.db.QueryRow(`select count(*) from sqlite_master where type='table' and name=?`, table).Scan(&n); err != nil {
			t.Fatalf("query sqlite_master for %s: %v", table, err)
		}
		if n != 1 {
			t.Errorf("table %s missing from the event store after its constructor ran", table)
		}
	}
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
