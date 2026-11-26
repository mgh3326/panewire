package panewire

// hk 6820: a store copy takes the source's record of the earlier-copy marking
// with the obligations, so the target's next start marks exactly the rows the
// source had not.

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// t6820CopySource writes a SQLite hub store holding one labelled
// working-expired row, as this build left it (marked) or as an earlier build
// did.
func t6820CopySource(t *testing.T, marked bool) string {
	t.Helper()
	path := freshLaneInboxFile(t)
	s, err := openLaneInboxStoreTimed(t, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ensureSpawnTasks(); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC)
	o := newDeliveryObligation{key: "lane.event/lane-a/copied", relayID: 301, kind: "lane.event", lane: "lane-a", machine: "host-a", pane: "w1:p1", attempts: 1}
	if err := s.createObligation(t.Context(), o, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE delivery_obligations SET state='exhausted',fault=?,labelled=1,last_fault_at=?`, relayDroppedWorkingExpired, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if !marked {
		if _, err := s.db.Exec(`PRAGMA user_version=2`); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// The source's record and the target's own history in all four combinations:
// after the copy and the target's next start, a marked source's row keeps its
// revive and an unmarked source's row is marked. Killed by: leaving the
// target's record as it was.
func TestObligationStoreContractCopyCarriesMarking(t *testing.T) {
	for _, backend := range []struct {
		name   string
		target func(*testing.T) (url string, open func() *laneInboxStore)
	}{
		{"postgres", func(t *testing.T) (string, func() *laneInboxStore) {
			config, url := hubPostgresTestSchema(t)
			return url, func() *laneInboxStore {
				s, err := openLaneInboxStorePostgresConfig(context.Background(), config.Copy())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.close() })
				return s
			}
		}},
		{"mysql", func(t *testing.T) (string, func() *laneInboxStore) {
			config, url := hubMySQLTestDatabase(t)
			return url, func() *laneInboxStore {
				s, err := openLaneInboxStoreMySQLConfig(context.Background(), config.Clone())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.close() })
				return s
			}
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			for _, c := range []struct {
				name                        string
				sourceMarked, targetStarted bool
				revived                     int
			}{
				{"marked source, fresh target", true, false, 0},
				{"marked source, started target", true, true, 0},
				{"unmarked source, fresh target", false, false, 1},
				{"unmarked source, started target", false, true, 1},
			} {
				t.Run(c.name, func(t *testing.T) {
					url, open := backend.target(t)
					if c.targetStarted {
						if err := queueTestStore(t, open).close(); err != nil {
							t.Fatal(err)
						}
					}
					var report bytes.Buffer
					if err := copyHubStore(t.Context(), "sqlite://"+t6820CopySource(t, c.sourceMarked), url, "delivery_obligations", false, true, &report); err != nil {
						t.Fatalf("copy: %v\n%s", err, report.String())
					}
					target := queueTestStore(t, open)
					var revived int
					if err := target.db.QueryRow(`SELECT revived FROM delivery_obligations WHERE relay_id=301`).Scan(&revived); err != nil || revived != c.revived {
						t.Fatalf("revived=%d err=%v, want %d", revived, err, c.revived)
					}
				})
			}
		})
	}
}

// The two halves of that carry, without a database server: the source is read
// from its user_version, and the target's ledger is made to match it. Killed
// by: keeping a record the source lacks; not writing one the source has.
func TestStoreCopyEarlierMarkingRecord_t6820(t *testing.T) {
	for _, marked := range []bool{true, false} {
		src, err := openStoreCopyDB(t.Context(), "sqlite://"+t6820CopySource(t, marked), true)
		if err != nil {
			t.Fatal(err)
		}
		got, err := storeCopySourceEarlierCopiesMarked(t.Context(), src)
		_ = src.db.Close()
		if err != nil || got != marked {
			t.Fatalf("source marked=%v read as %v, err=%v", marked, got, err)
		}
	}
	ledger, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "ledger.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	if _, err := ledger.Exec(`CREATE TABLE panewire_schema_migrations (version TEXT PRIMARY KEY); INSERT INTO panewire_schema_migrations VALUES('0001_lane_inbox')`); err != nil {
		t.Fatal(err)
	}
	for _, recorded := range []bool{false, true, true, false, false, true} {
		tx, err := ledger.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := copyEarlierObligationCopiesRecord(t.Context(), tx, recorded); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		var mine, all int
		if err := ledger.QueryRow(`SELECT count(CASE WHEN version=? THEN 1 END),count(*) FROM panewire_schema_migrations`, obligationEarlierCopiesVersion).Scan(&mine, &all); err != nil {
			t.Fatal(err)
		}
		want := 0
		if recorded {
			want = 1
		}
		if mine != want || all != want+1 {
			t.Fatalf("recorded=%v: ledger holds %d of this record among %d rows", recorded, mine, all)
		}
	}
}
