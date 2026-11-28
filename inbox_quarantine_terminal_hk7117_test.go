package panewire

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 7117: a quarantined batch turns terminal (sent=2) in the transaction that
// records its quarantine, so the pending page's sent=0 predicate never reads it.
// Every reader that used to test sent=0 for a quarantined batch tests
// sent IN (0,2) instead, and must keep returning what it did before.

// batchSent reads one batch's sent column; a missing batch fails the test.
func batchSent(t *testing.T, s *laneInboxStore, id string) int {
	t.Helper()
	var sent int
	err := s.db.QueryRow(s.q(`SELECT sent FROM lane_inbox_batches WHERE id=?`), id).Scan(&sent)
	if err != nil {
		t.Fatalf("read sent of %s: %v", id, err)
	}
	return sent
}

func TestLaneInboxQuarantinedBatchIsTerminalAtOnce_hk7117(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		if _, err := s.db.Exec(s.q(`INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES(?,?,?,0),(?,?,?,1)`),
			"refused", "lane-a", "one line", "handed-off", "lane-a", "one line"); err != nil {
			t.Fatal(err)
		}
		if err := s.quarantineBatch("refused", "test refusal"); err != nil {
			t.Fatal(err)
		}
		if got := batchSent(t, s, "refused"); got != 2 {
			t.Fatalf("quarantined unsent batch has sent=%d, want 2 (terminal)", got)
		}
		// The pending page reads sent=0 rows only: with the only batch refused,
		// its predicate matches no row, so the page scans none and returns none.
		var unsent int
		if err := s.db.QueryRow(s.q(`SELECT count(*) FROM lane_inbox_batches WHERE sent=0`)).Scan(&unsent); err != nil {
			t.Fatal(err)
		}
		if unsent != 0 {
			t.Fatalf("sent=0 rows after quarantine = %d, want 0", unsent)
		}
		if batches, err := s.pendingBatches(); err != nil || len(batches) != 0 {
			t.Fatalf("pending page after quarantine = %+v, %v; want none", batches, err)
		}
		// A batch already handed off keeps sent=1: quarantine never reopens it.
		if err := s.quarantineBatch("handed-off", "late refusal"); err != nil {
			t.Fatal(err)
		}
		if got := batchSent(t, s, "handed-off"); got != 1 {
			t.Fatalf("quarantined sent batch has sent=%d, want 1 (unchanged)", got)
		}
	})
}

// The escalation claim readers (unclaimedUnread, recoverMustAct) treat an item
// claimed by a quarantined, unsent batch as unclaimed. sent IN (0,2) keeps that.
func TestLaneInboxQuarantinedClaimStaysUnclaimed_hk7117(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		now := time.UnixMilli(1_700_000_000_000).UTC()
		if _, err := s.db.Exec(s.q(`INSERT INTO lane_inbox_batches(id,lane,text) VALUES(?,?,?)`), "claimed", "lane-a", "one line"); err != nil {
			t.Fatal(err)
		}
		item, err := s.put("claimed-item", "lane-a", "lane.event", "inbox", quietEvent("claimed-item", "FYI"), now, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(s.q(`UPDATE lane_inbox SET escalation_batch=? WHERE id=?`), "claimed", item.ID); err != nil {
			t.Fatal(err)
		}
		if err := s.quarantineBatch("claimed", "test refusal"); err != nil {
			t.Fatal(err)
		}
		items, err := s.unclaimedUnread()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, i := range items {
			found = found || i.ID == item.ID
		}
		if !found {
			t.Fatalf("item claimed by a quarantined batch is not unclaimed: %+v", items)
		}
	})
}

// A store opened from before hk 7117 holds quarantined unsent batches. Opening
// it again makes them terminal, and leaves sent batches and quarantine rows as
// they were. SQLite only: the Postgres and MySQL twins are migrations 0047 and
// 0044, checked below.
func TestLaneInboxOpenMakesExistingQuarantinedBatchesTerminal_hk7117(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	s, err := openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO lane_inbox_batches(id,lane,text,sent) VALUES('old-refused','lane-a','x',0),('old-sent','lane-a','y',1),('old-pending','lane-a','z',0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO lane_inbox_batch_quarantine(batch_id,reason) VALUES('old-refused','pre-hk7117'),('old-sent','pre-hk7117')`); err != nil {
		t.Fatal(err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = openLaneInboxStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for id, want := range map[string]int{"old-refused": 2, "old-sent": 1, "old-pending": 0} {
		if got := batchSent(t, s, id); got != want {
			t.Errorf("%s after reopen: sent=%d, want %d", id, got, want)
		}
	}
}

// Both migration files carry the same data change and parse as one guarded
// statement, so a run on Postgres or MySQL does what the SQLite open does.
func TestLaneInboxQuarantineTerminalMigrationsExist_hk7117(t *testing.T) {
	pg, err := hubPostgresMigrations.ReadFile("hub_postgres_migrations/0047_lane_inbox_quarantine_terminal.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(pg), "SET sent=2 WHERE sent=0 AND id IN (SELECT batch_id FROM lane_inbox_batch_quarantine)") {
		t.Errorf("postgres 0047 does not mark quarantined unsent batches terminal:\n%s", pg)
	}
	body, err := hubMySQLMigrations.ReadFile("hub_mysql_migrations/0044_lane_inbox_quarantine_terminal.sql")
	if err != nil {
		t.Fatal(err)
	}
	m, err := parseHubMySQLMigration(string(body))
	if err != nil {
		t.Fatalf("mysql 0044 does not parse: %v", err)
	}
	if !strings.Contains(m.statement, "SET b.sent=2 WHERE b.sent=0") {
		t.Errorf("mysql 0044 statement = %q, want the terminal update", m.statement)
	}
}
