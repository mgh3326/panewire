package arbiter

import (
	"database/sql"
	"slices"
	"testing"
	"time"
)

func epochRow(t *testing.T, s *Store, job, pool string) (epoch int64, found bool) {
	t.Helper()
	db := rawDB(t, s)
	err := db.QueryRow("SELECT epoch FROM quota_pool_epochs WHERE job_id = ? AND pool = ?", job, pool).Scan(&epoch)
	if err == sql.ErrNoRows {
		return 0, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return epoch, true
}

// rawDB opens the ledger the way an older binary or the script does: plain
// SQLite, no knowledge of the side table.
func rawDB(t *testing.T, s *Store) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", fileURI(s.DB, "rw"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func recordEpoch(t *testing.T, s *Store, job, pool string, epoch uint64) {
	t.Helper()
	if _, e := s.RecordQuotaPool(ctx, QuotaRun{Job: job, Profile: "cmd-x", Gate: Gate{Pool: pool}, Epoch: epoch}); e != nil {
		t.Fatalf("record %s/%s epoch %d: %v", job, pool, epoch, e)
	}
}

func poolsOf(t *testing.T, s *Store, job string) []string {
	t.Helper()
	pools, e := s.QuotaPools(ctx, job)
	if e != nil {
		t.Fatal(e)
	}
	return pools
}

func TestQuotaEpochReleaseTakesOnlyTheMatchingEpoch_t5294(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	recordEpoch(t, s, "j1", "example", 3)

	// An older attempt's release, and a newer one's, leave the row alone.
	for _, epoch := range []uint64{1, 2, 4} {
		released, e := s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", epoch, func() bool { return true })
		if e != nil || released {
			t.Fatalf("epoch %d released=%v err=%v: a row of epoch 3 is no other attempt's", epoch, released, e)
		}
	}
	if !slices.Equal(poolsOf(t, s, "j1"), []string{"example"}) {
		t.Fatal("row removed by another attempt's release")
	}
	if got := eventKinds(t, s, "j1"); slices.Contains(got, "quota_pool.release") {
		t.Fatalf("a refused release recorded an event: %v", got)
	}

	released, e := s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 3, nil)
	if e != nil || !released {
		t.Fatalf("released=%v err=%v", released, e)
	}
	if len(poolsOf(t, s, "j1")) != 0 {
		t.Fatal("row left behind")
	}
	if _, found := epochRow(t, s, "j1", "example"); found {
		t.Fatal("side row outlived its quota row")
	}
	evs, _ := s.Events(ctx, "j1")
	last := evs[len(evs)-1]
	if last.Kind != "quota_pool.release" || last.Payload["epoch"] != float64(3) {
		t.Fatalf("release event %+v", last)
	}
	if jv, _ := s.JobGet(ctx, "j1"); jv.State != "released" {
		t.Fatalf("state %s", jv.State)
	}
	_, e = s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 3, nil)
	wantCode(t, e, ExitFencing)
	_, e = s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 0, nil)
	wantCode(t, e, ExitUsage)
}

func TestQuotaEpochHigherAttemptTakesOverALowerRow_t5294(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	recordEpoch(t, s, "j1", "example", 3)

	for _, epoch := range []uint64{3, 2} { // the same attempt twice, and an older one
		_, e := s.RecordQuotaPool(ctx, QuotaRun{Job: "j1", Profile: "cmd-x", Gate: Gate{Pool: "example"}, Epoch: epoch})
		wantCode(t, e, ExitDuplicate)
	}
	_, e := s.RecordQuotaPool(ctx, QuotaRun{Job: "j1", Profile: "cmd-x", Gate: Gate{Pool: "example"}})
	wantCode(t, e, ExitDuplicate) // an epoch-less caller never takes a row over

	recordEpoch(t, s, "j1", "example", 5)
	if epoch, _ := epochRow(t, s, "j1", "example"); epoch != 5 {
		t.Fatalf("epoch %d, want 5", epoch)
	}
	evs, _ := s.Events(ctx, "j1")
	if p := evs[len(evs)-1].Payload; p["epoch"] != float64(5) || p["superseded_epoch"] != float64(3) {
		t.Fatalf("record event %v", p)
	}
	if released, e := s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 3, nil); e != nil || released {
		t.Fatalf("the superseded attempt released the new row: %v %v", released, e)
	}
	if released, e := s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 5, nil); e != nil || !released {
		t.Fatalf("released=%v err=%v", released, e)
	}
}

// A row with no epoch is released by a terminal end only; the attempt's own
// epoch matches nothing.
func TestQuotaEpochLessRowWaitsForTerminalEnd_t5294(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	recordEpoch(t, s, "j1", "example", 0)
	if _, found := epochRow(t, s, "j1", "example"); found {
		t.Fatal("an epoch-less record wrote a side row")
	}
	asked := 0
	for _, terminal := range []func() bool{nil, func() bool { asked++; return false }} {
		released, e := s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 1, terminal)
		if e != nil || released {
			t.Fatalf("released=%v err=%v without a terminal end", released, e)
		}
	}
	if asked != 1 {
		t.Fatalf("terminal asked %d times", asked)
	}
	released, e := s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 1, func() bool { return true })
	if e != nil || !released || len(poolsOf(t, s, "j1")) != 0 {
		t.Fatalf("released=%v err=%v", released, e)
	}
	// A row with an epoch never asks.
	recordEpoch(t, s, "j1", "example", 2)
	if released, e = s.ReleaseQuotaPoolEpoch(ctx, "j1", "example", 2, func() bool { t.Fatal("terminal asked for a row with an epoch"); return false }); e != nil || !released {
		t.Fatalf("released=%v err=%v", released, e)
	}
}

func TestQuotaEpochGCStaleCleanupDropsTheSideRow_t5294(t *testing.T) {
	s, clk := newStore(t)
	claim(t, s, "gone")
	recordEpoch(t, s, "gone", "example", 2)
	clk.advance(11 * time.Minute)
	r, e := s.GC(ctx, agentsOf())
	if e != nil || len(r.StaleQuota) != 1 {
		t.Fatalf("%+v %v", r, e)
	}
	if _, found := epochRow(t, s, "gone", "example"); found {
		t.Fatal("gc left the side row behind")
	}
}

// The ledger stays the version-2 file the script wrote: the schema version is
// untouched, and every statement the script and an older binary issue still
// works with the side table present.
func TestQuotaEpochSideTableKeepsVersion2ReadableByOlderBinaries_t5294(t *testing.T) {
	s := copyFixture(t) // written by the agent-skills arbiter script
	db := rawDB(t, s)
	var before []string
	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		before = append(before, name)
	}
	rows.Close()
	if slices.Contains(before, "quota_pool_epochs") {
		t.Fatalf("fixture already has the side table: %v", before)
	}

	// The new binary acquires and releases with epochs on the script's file.
	claim(t, s, "job-new")
	recordEpoch(t, s, "job-new", "example", 4)
	if _, found := epochRow(t, s, "job-new", "example"); !found {
		t.Fatal("no side row")
	}

	var version int
	if err := db.QueryRow("SELECT schema_version FROM meta WHERE id = 1").Scan(&version); err != nil || version != SchemaVersion || SchemaVersion != 2 {
		t.Fatalf("schema_version=%d err=%v", version, err)
	}
	// The script's own reads and writes of quota_pool_records, as it issues them.
	var n int
	if err := db.QueryRow("SELECT 1 FROM sqlite_master LIMIT 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow("SELECT count(*) FROM (SELECT job_id, pool, profile, started_at FROM quota_pool_records ORDER BY job_id, pool)").Scan(&count); err != nil || count != 2 {
		t.Fatalf("legacy read: %d rows, err=%v", count, err)
	}
	if _, err := db.Exec("DELETE FROM quota_pool_records WHERE job_id = ? AND pool = ?", "job-new", "example"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO quota_pool_records (job_id, pool, profile, started_at) VALUES (?, ?, ?, ?)", "job-new", "example", "cmd-x", iso(s.now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	// The older binary deleted and recorded the row again, leaving the dead
	// attempt's side row: it must not give the new row an epoch.
	if released, e := s.ReleaseQuotaPoolEpoch(ctx, "job-new", "example", 4, nil); e != nil || released {
		t.Fatalf("a re-recorded row inherited the old epoch: released=%v err=%v", released, e)
	}
	if released, e := s.ReleaseQuotaPoolEpoch(ctx, "job-new", "example", 4, func() bool { return true }); e != nil || !released {
		t.Fatalf("released=%v err=%v", released, e)
	}
	// The new binary still reads the script's row, which has no epoch.
	st, e := s.Status(ctx, "", "")
	if e != nil || len(st.Quota) != 1 || st.Quota[0].Job != "job-a" {
		t.Fatalf("%+v %v", st.Quota, e)
	}
	if released, e := s.ReleaseQuotaPoolEpoch(ctx, "job-a", "example", 1, nil); e != nil || released {
		t.Fatalf("a script-written row was released by an epoch: %v %v", released, e)
	}
	if released, e := s.ReleaseQuotaPoolEpoch(ctx, "job-a", "example", 1, func() bool { return true }); e != nil || !released {
		t.Fatalf("released=%v err=%v", released, e)
	}
}
