package panewire

import (
	"context"
	"os"
	"testing"
)

// TestHubMySQLMigrateForSchemaDoc drives the production MySQL migration path
// (openLaneInboxStoreMySQL) against a throwaway database for tbls schema doc
// generation in CI. It is gated by PANEWIRE_SCHEMA_DOC_MYSQL_URL, skips everywhere
// else, and leaves the migrated schema intact when done (hk 6104).
func TestHubMySQLMigrateForSchemaDoc(t *testing.T) {
	rawURL := os.Getenv("PANEWIRE_SCHEMA_DOC_MYSQL_URL")
	if rawURL == "" {
		t.Skip("PANEWIRE_SCHEMA_DOC_MYSQL_URL is unset; this test runs only in the schema-doc workflow")
	}
	store, err := openLaneInboxStoreMySQL(context.Background(), rawURL)
	if err != nil {
		t.Fatalf("openLaneInboxStoreMySQL failed: %v", err)
	}
	defer func() {
		if err := store.close(); err != nil {
			t.Errorf("store.close: %v", err)
		}
	}()

	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM panewire_schema_migrations`).Scan(&count); err != nil {
		t.Fatalf("query panewire_schema_migrations: %v", err)
	}
	if want := hubMySQLMigrationCount(t); count != want {
		t.Fatalf("recorded migrations = %d, want %d", count, want)
	}

	var version string
	if err := store.db.QueryRow(`SELECT VERSION()`).Scan(&version); err != nil {
		t.Fatalf("query SELECT VERSION(): %v", err)
	}
	t.Logf("MySQL server version: %s", version)
	if path := os.Getenv("PANEWIRE_SCHEMA_DOC_VERSION_FILE"); path != "" {
		if err := os.WriteFile(path, []byte(version), 0644); err != nil {
			t.Fatalf("write version file: %v", err)
		}
	}
}
