package panewire

import (
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// The held and digest passes (hk 6081) each have a partial index on SQLite
// and Postgres and a composite key on MySQL. These names must match in every
// dialect's DDL.
var laneInboxPassIndexes = []string{"lane_inbox_hold_pending", "lane_inbox_digest_pending"}

// hk 6081 review: the indexes must exist on every dialect, and the schema for
// each comes only from its migration files (Postgres and MySQL) or from
// openLaneInboxStore (SQLite). Like the t5776 check, this reads the files, so it
// runs without a database.
func TestLaneInboxPassIndexesHaveAMigrationOnEveryDialect_hk6081(t *testing.T) {
	postgresCreate := regexp.MustCompile(`CREATE INDEX IF NOT EXISTS (\w+) ON lane_inbox\(`)
	mysqlKey := regexp.MustCompile(`ADD KEY (\w+) \(`)
	for _, c := range []struct {
		name    string
		sources []string
		find    *regexp.Regexp
	}{
		{"postgres", migrationSources(t, hubPostgresMigrations, "hub_postgres_migrations/*.sql"), postgresCreate},
		{"mysql", migrationSources(t, hubMySQLMigrations, "hub_mysql_migrations/*.sql"), mysqlKey},
	} {
		found := map[string]bool{}
		for _, source := range c.sources {
			for _, m := range c.find.FindAllStringSubmatch(source, -1) {
				found[m[1]] = true
			}
		}
		for _, index := range laneInboxPassIndexes {
			if !found[index] {
				t.Errorf("%s migrations do not create lane_inbox index %s", c.name, index)
			}
		}
	}
}

// The SQLite plans of both pass statements search the new partial indexes and
// never scan lane_inbox itself. This fails on the base schema, where both plans
// are "SCAN lane_inbox". Postgres and MySQL planners depend on table size, so
// their plans are not asserted here.
func TestLaneInboxPassQueriesUseTheirPartialIndexes_hk6081(t *testing.T) {
	s, err := openLaneInboxStore(filepath.Join(t.TempDir(), "inbox.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	for _, c := range []struct{ name, query, index string }{
		{"held pass", laneInboxHeldQuery, "lane_inbox_hold_pending"},
		{"digest pass", laneInboxDigestDueQuery, "lane_inbox_digest_pending"},
	} {
		rows, err := s.db.Query(`EXPLAIN QUERY PLAN ` + c.query)
		if err != nil {
			t.Fatalf("%s: explain: %v", c.name, err)
		}
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			plan = append(plan, detail)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		joined := strings.Join(plan, "\n")
		if !strings.Contains(joined, "USING INDEX "+c.index) {
			t.Errorf("%s plan does not search %s:\n%s", c.name, c.index, joined)
		}
		if slices.Contains(plan, "SCAN lane_inbox") {
			t.Errorf("%s plan scans lane_inbox in full:\n%s", c.name, joined)
		}
	}
}

// passFixture is the lane_inbox state both passes read. Each row is named by
// its key; the sets are the ids the statements' predicates select, in id order.
type passFixture struct {
	at     time.Time
	held   []int64 // rows makeHeldBatches may claim
	digest []int64 // rows the digest query selects (a digest is due for some)
	claim  []int64 // rows the two passes claim at at+2m, in id order
}

// buildPassFixture writes the rows, each one a case of the predicates: pending,
// acked, superseded, already batched, not yet due. Every other predicate of
// both statements is covered by a row that differs only in that field.
func buildPassFixture(t *testing.T, s *laneInboxStore) passFixture {
	t.Helper()
	at := time.UnixMilli(1_700_000_000_000).UTC()
	ms := at.UnixMilli()
	put := func(key, lane, class string, window, hold time.Duration) int64 {
		t.Helper()
		var holds []time.Duration
		if hold > 0 {
			holds = append(holds, hold)
		}
		item, err := s.put(key, lane, "lane.event", class, quietEvent(key, "fixture "+key), at, 0, window, holds...)
		if err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		return item.ID
	}
	update := func(id int64, column string, value any) {
		t.Helper()
		if _, err := s.db.Exec(s.q(`UPDATE lane_inbox SET `+column+`=? WHERE id=?`), value, id); err != nil {
			t.Fatalf("update %s: %v", column, err)
		}
	}
	// Digest candidates: a due one (lane-a) and one whose window ends later
	// (lane-c), which the pass must leave for its own time.
	dueDigest := put("digest-due", "lane-a", "interrupt", time.Minute, 0)
	laterDigest := put("digest-later", "lane-c", "interrupt", 5*time.Minute, 0)
	update(put("digest-acked", "lane-a", "interrupt", time.Minute, 0), "acked_at", ms)
	update(put("digest-superseded", "lane-a", "interrupt", time.Minute, 0), "superseded_at", ms)
	update(put("digest-batched", "lane-a", "interrupt", time.Minute, 0), "digest_batch", "batch-old")
	// Held candidates: an interrupt and an inbox row in two lanes. A hold
	// clears digest_at, so the interrupt is not a digest candidate.
	heldInterrupt := put("held-interrupt", "lane-a", "interrupt", time.Minute, 10*time.Minute)
	heldInbox := put("held-inbox", "lane-b", "inbox", 0, 10*time.Minute)
	update(put("held-acked", "lane-b", "inbox", 0, 10*time.Minute), "acked_at", ms)
	update(put("held-superseded", "lane-b", "inbox", 0, 10*time.Minute), "superseded_at", ms)
	update(put("held-batched", "lane-b", "inbox", 0, 10*time.Minute), "digest_batch", "batch-old")
	return passFixture{
		at:     at,
		held:   []int64{heldInterrupt, heldInbox},
		digest: []int64{dueDigest, laterDigest},
		claim:  []int64{dueDigest, heldInterrupt, heldInbox},
	}
}

// laneInboxIDs runs one pass's scan, unchanged, and returns the ids it selects,
// in order.
func laneInboxIDs(t *testing.T, s *laneInboxStore, query string) []int64 {
	t.Helper()
	rows, err := s.db.Query(query)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		item, err := scanLaneInbox(rows)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, item.ID)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return ids
}

// The two passes select and claim the same rows on every backend (SQLite,
// Postgres and MySQL in CI). The fixture expects the sets the base predicates
// select, so this fails if the migration or index change altered a result.
func TestLaneInboxPassesClaimTheSameRowsOnEveryBackend_hk6081(t *testing.T) {
	forEachLaneInboxBackend(t, func(t *testing.T, open func() *laneInboxStore) {
		s := open()
		f := buildPassFixture(t, s)
		if got := laneInboxIDs(t, s, laneInboxHeldQuery); !slices.Equal(got, f.held) {
			t.Fatalf("held scan = %v, want %v", got, f.held)
		}
		if got := laneInboxIDs(t, s, laneInboxDigestDueQuery); !slices.Equal(got, f.digest) {
			t.Fatalf("digest scan = %v, want %v", got, f.digest)
		}

		// Two minutes in: the digest window of digest-due has ended, the one
		// of digest-later has not, and the held rows are still held (their
		// lane is not working, so the pass claims them now).
		now := f.at.Add(2 * time.Minute)
		due, err := s.makeBatchesDue(now)
		if err != nil {
			t.Fatalf("digest pass: %v", err)
		}
		if want := f.at.Add(5 * time.Minute).UnixMilli(); due != want {
			t.Errorf("digest pass due = %d, want the later window's end %d", due, want)
		}
		h := &HubServer{inbox: &hubInbox{store: s}, now: func() time.Time { return now }}
		heldDue, watch, err := h.makeHeldBatches()
		if err != nil {
			t.Fatalf("held pass: %v", err)
		}
		if heldDue != 0 || len(watch) != 0 {
			t.Errorf("held pass due=%d watch=%v, want none for lanes that are not working", heldDue, watch)
		}

		var claimed []int64
		rows, err := s.db.Query(`SELECT id FROM lane_inbox WHERE digest_batch<>'' AND digest_batch<>'batch-old' ORDER BY id`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			claimed = append(claimed, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(claimed, f.claim) {
			t.Fatalf("claimed rows = %v, want %v", claimed, f.claim)
		}
	})
}
