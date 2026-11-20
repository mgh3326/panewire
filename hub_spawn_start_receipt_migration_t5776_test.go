package panewire

import (
	"context"
	"testing"
	"time"
)

// hk 5776 review: start_receipt_ms is read and written by every bind, so a hub on
// Postgres or MySQL, whose schema comes only from the migration files, needs the
// column there. This reads the files themselves (no database), so it runs
// everywhere.
func TestStartReceiptColumnHasAMigrationOnEveryDialect_t5776(t *testing.T) {
	postgres := columnsOf(t, migrationSources(t, hubPostgresMigrations, "hub_postgres_migrations/*.sql"))
	mysql := columnsOf(t, migrationSources(t, hubMySQLMigrations, "hub_mysql_migrations/*.sql"))
	for name, tables := range map[string]map[string]map[string]bool{"postgres": postgres, "mysql": mysql} {
		if !tables["spawn_operations"]["start_receipt_ms"] {
			t.Errorf("%s migrations do not add spawn_operations.start_receipt_ms", name)
		}
	}
}

// The receipt round-trips on every backend the suite opens (Postgres in CI): a bind
// writes the column, the node's receipt sets it once, and a new bind clears it.
func TestStartReceiptRoundTripsOnEveryBackend_t5776(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if err := s.ensureSpawnTasks(); err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		now := time.Now().UTC()
		request := durableSpawnRequest()
		task, _, err := s.claimSpawn(ctx, request, "job-receipt-5776", "parent", now)
		if err != nil {
			t.Fatal(err)
		}
		node := hubSpawnRequest{RequestID: t14RequestID(577601), Machine: "machine-a"}
		bound, err := s.bindSpawn(ctx, task.Request.RequestID, task.Attempt, node, now)
		if err != nil {
			t.Fatalf("bind: %v", err)
		}
		if stored, err := s.markSpawnStarted(ctx, node.RequestID, "machine-a", now); err != nil || stored {
			t.Fatalf("receipt for an unsent attempt: stored=%v err=%v", stored, err)
		}
		sent := sentSpawnFixture(t, s, bound)
		if stored, err := s.markSpawnStarted(ctx, node.RequestID, "machine-a", now); err != nil || !stored {
			t.Fatalf("receipt: stored=%v err=%v", stored, err)
		}
		if ok, err := s.spawnStartReceipt(ctx, sent.Request.RequestID, sent.Attempt, node.RequestID); err != nil || !ok {
			t.Fatalf("receipt read: %v %v", ok, err)
		}
	})
}
