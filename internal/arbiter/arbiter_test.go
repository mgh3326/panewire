package arbiter

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

var ctx = context.Background()

// clock is a settable test clock.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

// newStore is an empty ledger under a temp dir.
func newStore(t *testing.T) (*Store, *clock) {
	t.Helper()
	root := t.TempDir()
	clk := &clock{time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	return &Store{DB: StatePath(filepath.Join(root, "data")), Inbox: filepath.Join(root, "jobs"), Now: clk.now}, clk
}

func claim(t *testing.T, s *Store, job string) {
	t.Helper()
	if _, e := s.Claim(ctx, Claim{Job: job, Lane: "lane1", T: "T1", AgentLabel: job}); e != nil {
		t.Fatalf("claim %s: %v", job, e)
	}
}

func wantCode(t *testing.T, e error, code int) {
	t.Helper()
	if e == nil || Code(e) != code {
		t.Fatalf("want exit %d, got %v", code, e)
	}
}

func eventKinds(t *testing.T, s *Store, job string) []string {
	t.Helper()
	evs, e := s.Events(ctx, job)
	if e != nil {
		t.Fatal(e)
	}
	var kinds []string
	for _, ev := range evs {
		kinds = append(kinds, ev.Kind)
	}
	return kinds
}

func artifactNames(t *testing.T, s *Store, job string) []string {
	t.Helper()
	entries, e := os.ReadDir(s.eventsDir(job))
	if e != nil {
		t.Fatal(e)
	}
	var names []string
	for _, ent := range entries {
		names = append(names, ent.Name())
	}
	return names
}

// ---- the existing arbiter state is read as it is

// copyFixture copies testdata/arbiter-v2 (state written by the agent-skills
// arbiter script, schema 2: four jobs, a quota_pool record, an unexpired path
// lease, an expired linear_permit lease and a released job) into a temp dir.
func copyFixture(t *testing.T) *Store {
	t.Helper()
	src, dst := filepath.Join("testdata", "arbiter-v2"), t.TempDir()
	e := filepath.WalkDir(src, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if e != nil {
		t.Fatal(e)
	}
	clk := &clock{time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)}
	return &Store{DB: filepath.Join(dst, "state.db"), Inbox: filepath.Join(dst, "jobs"), Now: clk.now}
}

func TestFixtureStateIsReadWithoutMigration(t *testing.T) {
	s := copyFixture(t)
	st, e := s.Status(ctx, "", "")
	if e != nil {
		t.Fatal(e)
	}
	if len(st.Jobs) != 4 || len(st.Leases) != 2 || len(st.Quota) != 1 {
		t.Fatalf("jobs=%d leases=%d quota=%d", len(st.Jobs), len(st.Leases), len(st.Quota))
	}
	got := map[string]string{}
	for _, j := range st.Jobs {
		got[j.Job] = j.State
	}
	if got["job-a"] != "leased" || got["job-d"] != "released" {
		t.Fatalf("states %v", got)
	}
	for _, l := range st.Leases {
		switch l.Kind {
		case KindPath:
			if l.Resource != "/repo/one" || l.Expired || l.Token != "1-…" {
				t.Fatalf("path lease %+v", l)
			}
		case KindLinearPermit:
			if !l.Expired || l.Token != "2-…" {
				t.Fatalf("permit lease %+v", l)
			}
		}
	}
	if q := st.Quota[0]; q.Job != "job-a" || q.Pool != "example" || q.Profile != "cmd-x" {
		t.Fatalf("quota %+v", q)
	}
	a, e := s.JobGet(ctx, "job-a")
	if e != nil {
		t.Fatal(e)
	}
	if a.AgentLabel != "worker-a" || a.Receipt["pane_id"] != "w:p1" || a.Receipt["workspace"] != "ws" || a.Receipt["profile"] != "cmd-x" {
		t.Fatalf("job-a %+v", a)
	}
	if !slices.Equal(a.RecentEventKinds, []string{"job.claim", "quota_pool.record", "job.spawned"}) {
		t.Fatalf("kinds %v", a.RecentEventKinds)
	}
	d, e := s.JobGet(ctx, "job-d")
	if e != nil || d.State != "released" || d.Receipt != nil {
		t.Fatalf("job-d %+v %v", d, e)
	}
	_, e = s.JobGet(ctx, "nope")
	wantCode(t, e, ExitNotFound)
	evs, e := s.Events(ctx, "job-a")
	if e != nil || len(evs) != 3 || evs[2].Payload["tab_id"] != "w:t1" {
		t.Fatalf("events %v %v", evs, e)
	}
}

func TestFixtureAcceptsNewWritesAlongsideOld(t *testing.T) {
	s := copyFixture(t)
	// Numbering continues after the script's artifacts.
	seq, e := s.AppendEvent(ctx, "job-a", "job.note", map[string]any{"n": 1}, nil)
	if e != nil || seq != 4 {
		t.Fatalf("seq=%d %v", seq, e)
	}
	if names := artifactNames(t, s, "job-a"); !slices.Contains(names, "00004-job.note.json") {
		t.Fatalf("artifacts %v", names)
	}
	// A released job is reclaimed in place and keeps its history.
	r, e := s.Claim(ctx, Claim{Job: "job-d", Lane: "lane9", T: "T2", ReclaimReleased: true})
	if e != nil || !r.Reclaimed || r.PreviousCreatedAt != "2026-01-01T00:00:00+00:00" || r.CreatedAt != r.PreviousCreatedAt {
		t.Fatalf("reclaim %+v %v", r, e)
	}
	if got := eventKinds(t, s, "job-d"); got[len(got)-1] != "job.reclaim" {
		t.Fatalf("kinds %v", got)
	}
	// The expired permit is re-granted over its archived generation; the live
	// path lease still refuses.
	l, e := s.Acquire(ctx, LeaseReq{Job: "job-d", Resource: "permit-1", Kind: KindLinearPermit})
	if e != nil || l.Superseded != "job-c" || !strings.HasPrefix(l.Token, "3-") {
		t.Fatalf("lease %+v %v", l, e)
	}
	_, e = s.Acquire(ctx, LeaseReq{Job: "job-d", Resource: "/repo/one", Kind: KindPath})
	wantCode(t, e, ExitDenied)
}

func TestSchemaMismatchIsRefused(t *testing.T) {
	s := copyFixture(t)
	c, e := s.rw(ctx, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.exec("UPDATE meta SET schema_version = 3"); e != nil {
		t.Fatal(e)
	}
	c.Close()
	_, e = s.Status(ctx, "", "")
	wantCode(t, e, ExitStorage)
	if !strings.Contains(e.Error(), "schema mismatch") {
		t.Fatal(e)
	}
}

// ---- claim

func TestClaimRegistersAndRefusesDuplicates(t *testing.T) {
	s, _ := newStore(t)
	r, e := s.Claim(ctx, Claim{Job: "j1", Lane: "lane", T: "T2", TaskID: "42", Role: "builder", ParentLane: "boss"})
	if e != nil || r.EventSeq != 1 || r.State != "claimed" || r.AgentLabel != "j1" {
		t.Fatalf("%+v %v", r, e)
	}
	_, e = s.Claim(ctx, Claim{Job: "j1", Lane: "lane", T: "T2"})
	wantCode(t, e, ExitDuplicate)
	// A claimed (not released) job is not reclaimable.
	_, e = s.Claim(ctx, Claim{Job: "j1", Lane: "lane", T: "T2", ReclaimReleased: true})
	wantCode(t, e, ExitDuplicate)
	evs, _ := s.Events(ctx, "j1")
	if evs[0].Payload["task_id"] != float64(42) || evs[0].Payload["parent_lane"] != "boss" {
		t.Fatalf("payload %v", evs[0].Payload)
	}
	_, e = s.Claim(ctx, Claim{Job: "j2", Lane: "lane", T: "T2", Role: "builder"})
	wantCode(t, e, ExitUsage)
	_, e = s.Claim(ctx, Claim{Job: "j3", Lane: "lane", T: "T9"})
	wantCode(t, e, ExitUsage)
	_, e = s.Claim(ctx, Claim{Job: "../x", Lane: "lane", T: "T1"})
	wantCode(t, e, ExitUsage)
}

// A claim role is implementer (the default) or builder; the retired names are
// refused, not mapped (operator decision 208: no aliases).
func TestClaimRoleVocabulary(t *testing.T) {
	s, _ := newStore(t)
	r, e := s.Claim(ctx, Claim{Job: "j1", Lane: "lane", T: "T2"})
	if e != nil || r.Role != "implementer" {
		t.Fatalf("default role %+v %v", r, e)
	}
	for _, role := range []string{"worker", "captain", "tester", "counsel"} {
		_, e = s.Claim(ctx, Claim{Job: "j-" + role, Lane: "lane", T: "T2", Role: role, ParentLane: "boss"})
		wantCode(t, e, ExitUsage)
	}
	r, e = s.Claim(ctx, Claim{Job: "jb", Lane: "lane", T: "T2", Role: "builder", ParentLane: "boss"})
	if e != nil || r.Role != "builder" {
		t.Fatalf("builder claim %+v %v", r, e)
	}
}

// ---- events

func TestEventAppendAndRead(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	for i, kind := range []string{"job.one", "job.two"} {
		seq, e := s.AppendEvent(ctx, "j1", kind, map[string]any{"i": i, "note": "<&>"}, nil)
		if e != nil || seq != i+2 {
			t.Fatalf("seq=%d %v", seq, e)
		}
	}
	if got := eventKinds(t, s, "j1"); !slices.Equal(got, []string{"job.claim", "job.one", "job.two"}) {
		t.Fatalf("kinds %v", got)
	}
	if names := artifactNames(t, s, "j1"); !slices.Equal(names, []string{"00001-job.claim.json", "00002-job.one.json", "00003-job.two.json"}) {
		t.Fatalf("artifacts %v", names)
	}
	body, _ := os.ReadFile(filepath.Join(s.eventsDir("j1"), "00002-job.one.json"))
	if !strings.Contains(string(body), `"note":"<&>"`) || !strings.HasSuffix(string(body), "\n") {
		t.Fatalf("body %q", body)
	}
	evs, _ := s.Events(ctx, "j1")
	if evs[1].Payload["note"] != "<&>" || evs[1].Seq != 2 || evs[1].CreatedAt != "2026-03-01T12:00:00+00:00" {
		t.Fatalf("%+v", evs[1])
	}
}

func TestEventSeqFollowsOtherWritersFiles(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	// wrk numbers files in the same directory; the database alone would pick 2.
	if e := os.WriteFile(filepath.Join(s.eventsDir("j1"), "00007-wrk.note.json"), []byte("{}\n"), 0o644); e != nil {
		t.Fatal(e)
	}
	seq, e := s.AppendEvent(ctx, "j1", "job.note", nil, nil)
	if e != nil || seq != 8 {
		t.Fatalf("seq=%d %v", seq, e)
	}
}

func TestEventExplicitSeqDuplicateIsRefused(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	two := 2
	if _, e := s.AppendEvent(ctx, "j1", "job.a", nil, &two); e != nil {
		t.Fatal(e)
	}
	_, e := s.AppendEvent(ctx, "j1", "job.b", nil, &two)
	wantCode(t, e, ExitDuplicate)
	if got := artifactNames(t, s, "j1"); len(got) != 2 {
		t.Fatalf("a refused event left an artifact: %v", got)
	}
}

func TestClaimKindsCannotBeForged(t *testing.T) {
	s, _ := newStore(t)
	for _, kind := range []string{"job.claim", "job.claimed", "job.reclaim", "x.claim", "a b.claim"} {
		_, e := s.AppendEvent(ctx, "j1", kind, nil, nil)
		wantCode(t, e, ExitUsage)
	}
	if _, e := os.Stat(s.DB); e == nil {
		t.Fatal("a refused claim kind created state")
	}
}

// A job claimed on another host has no local row; its events are still kept.
func TestEventNeedsNoLocalClaim(t *testing.T) {
	s, _ := newStore(t)
	seq, e := s.AppendEvent(ctx, "remote-job", "job.placement", map[string]any{"host": "elsewhere"}, nil)
	if e != nil || seq != 1 {
		t.Fatalf("seq=%d %v", seq, e)
	}
	if got := eventKinds(t, s, "remote-job"); !slices.Equal(got, []string{"job.placement"}) {
		t.Fatalf("kinds %v", got)
	}
	_, e = s.JobGet(ctx, "remote-job")
	wantCode(t, e, ExitNotFound)
}

// ---- leases, per kind

func TestExclusiveLeaseLifecycle(t *testing.T) {
	for _, kind := range []string{KindPath, KindLinearPermit} {
		t.Run(kind, func(t *testing.T) {
			s, clk := newStore(t)
			claim(t, s, "j1")
			claim(t, s, "j2")
			l, e := s.Acquire(ctx, LeaseReq{Job: "j1", Resource: "res", Kind: kind})
			if e != nil || !strings.HasPrefix(l.Token, "1-") {
				t.Fatalf("%+v %v", l, e)
			}
			if want := clk.now().Add(DefaultTTL[kind]); l.ExpiresAt != iso(want) {
				t.Fatalf("expires %s want %s", l.ExpiresAt, iso(want))
			}
			// conflict
			_, e = s.Acquire(ctx, LeaseReq{Job: "j2", Resource: "res", Kind: kind})
			wantCode(t, e, ExitDenied)
			// the same resource under the other kind is independent
			other := KindPath
			if kind == KindPath {
				other = KindLinearPermit
			}
			if _, e = s.Acquire(ctx, LeaseReq{Job: "j2", Resource: "res", Kind: other}); e != nil {
				t.Fatal(e)
			}
			// release fencing: wrong job, wrong token, missing token
			wantCode(t, s.Release(ctx, "j2", "res", kind, l.Token, false), ExitFencing)
			wantCode(t, s.Release(ctx, "j1", "res", kind, "1-wrong", false), ExitFencing)
			wantCode(t, s.Release(ctx, "j1", "res", kind, "", false), ExitUsage)
			if e = s.Release(ctx, "j1", "res", kind, l.Token, false); e != nil {
				t.Fatal(e)
			}
			wantCode(t, s.Release(ctx, "j1", "res", kind, l.Token, false), ExitFencing)
			if _, e = s.Acquire(ctx, LeaseReq{Job: "j2", Resource: "res", Kind: kind}); e != nil {
				t.Fatalf("a released lease is free: %v", e)
			}
			st, _ := s.Status(ctx, "j1", "")
			if len(st.History) != 1 || st.History[0].State != "released" || strings.Contains(st.History[0].Token, "-w") {
				t.Fatalf("history %+v", st.History)
			}
			if got := eventKinds(t, s, "j1"); !slices.Equal(got, []string{"job.claim", "lease.acquire", "lease.release"}) {
				t.Fatalf("kinds %v", got)
			}
		})
	}
}

func TestExclusiveLeaseExpiryIsSupersededAndArchived(t *testing.T) {
	for _, kind := range []string{KindPath, KindLinearPermit} {
		t.Run(kind, func(t *testing.T) {
			s, clk := newStore(t)
			claim(t, s, "j1")
			claim(t, s, "j2")
			if _, e := s.Acquire(ctx, LeaseReq{Job: "j1", Resource: "res", Kind: kind, TTL: 5 * time.Minute, HasTTL: true}); e != nil {
				t.Fatal(e)
			}
			clk.advance(5*time.Minute - time.Second)
			_, e := s.Acquire(ctx, LeaseReq{Job: "j2", Resource: "res", Kind: kind})
			wantCode(t, e, ExitDenied)
			clk.advance(time.Second) // expires_at <= now is expired
			l, e := s.Acquire(ctx, LeaseReq{Job: "j2", Resource: "res", Kind: kind})
			if e != nil || l.Superseded != "j1" || !strings.HasPrefix(l.Token, "2-") {
				t.Fatalf("%+v %v", l, e)
			}
			st, _ := s.Status(ctx, "", "")
			if len(st.History) != 1 || st.History[0].State != "superseded" || st.History[0].Job != "j1" {
				t.Fatalf("history %+v", st.History)
			}
			// the stale owner can no longer release
			wantCode(t, s.Release(ctx, "j1", "res", kind, "1-x", false), ExitFencing)
		})
	}
}

func TestForcedReleaseNeedsNoToken(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	if _, e := s.Acquire(ctx, LeaseReq{Job: "j1", Resource: "res", Kind: KindPath}); e != nil {
		t.Fatal(e)
	}
	if e := s.Release(ctx, "operator", "res", KindPath, "", true); e != nil {
		t.Fatal(e)
	}
	st, _ := s.Status(ctx, "", "")
	if len(st.Leases) != 0 || st.History[0].State != "force-released" {
		t.Fatalf("%+v", st)
	}
	if got := eventKinds(t, s, "j1"); got[len(got)-1] != "lease.release.force" {
		t.Fatalf("kinds %v", got)
	}
}

func TestLeaseNeedsARegisteredJob(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	_, e := s.Acquire(ctx, LeaseReq{Job: "ghost", Resource: "res", Kind: KindPath})
	wantCode(t, e, ExitDenied)
	_, e = s.Acquire(ctx, LeaseReq{Job: "j1", Resource: "res", Kind: KindQuotaPool})
	wantCode(t, e, ExitUsage)
	_, e = s.RecordQuotaPool(ctx, QuotaRun{Job: "ghost", Profile: "p", Gate: Gate{Pool: "pool"}})
	wantCode(t, e, ExitDenied)
}

func TestQuotaPoolRecordLifecycle(t *testing.T) {
	s, clk := newStore(t)
	claim(t, s, "j1")
	gate, e := ParseGate("cmd-x", "pool=example profile=cmd-x ref_resolution=unverified-claim requested_by=op", "")
	if e != nil {
		t.Fatal(e)
	}
	r, e := s.RecordQuotaPool(ctx, QuotaRun{Job: "j1", Profile: "cmd-x", LaunchProfile: "cmd-x@high", Gate: gate})
	if e != nil || r.Pool != "example" || r.StartedAt != iso(clk.now()) {
		t.Fatalf("%+v %v", r, e)
	}
	_, e = s.RecordQuotaPool(ctx, QuotaRun{Job: "j1", Profile: "cmd-x", Gate: gate})
	wantCode(t, e, ExitDuplicate)
	pools, _ := s.QuotaPools(ctx, "j1")
	if !slices.Equal(pools, []string{"example"}) {
		t.Fatalf("pools %v", pools)
	}
	jv, _ := s.JobGet(ctx, "j1")
	if jv.State != "leased" {
		t.Fatalf("state %s", jv.State)
	}
	evs, _ := s.Events(ctx, "j1")
	p := evs[1].Payload
	if p["launch_profile"] != "cmd-x@high" || p["ref_resolution"] != "unverified-claim" || p["requested_by"] != "op" {
		t.Fatalf("payload %v", p)
	}
	// There is no TTL: time passing does not expire a record.
	clk.advance(30 * 24 * time.Hour)
	if pools, _ = s.QuotaPools(ctx, "j1"); len(pools) != 1 {
		t.Fatal("quota_pool record expired")
	}
	wantCode(t, s.ReleaseQuotaPool(ctx, "j1", "other", false), ExitFencing)
	if e = s.ReleaseQuotaPool(ctx, "j1", "example", false); e != nil {
		t.Fatal(e)
	}
	wantCode(t, s.ReleaseQuotaPool(ctx, "j1", "example", false), ExitFencing)
	jv, _ = s.JobGet(ctx, "j1")
	if jv.State != "released" {
		t.Fatalf("state %s", jv.State)
	}
	// a released job is reclaimable and may record again
	if _, e = s.Claim(ctx, Claim{Job: "j1", Lane: "lane1", T: "T1", ReclaimReleased: true}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.RecordQuotaPool(ctx, QuotaRun{Job: "j1", Profile: "cmd-x", Gate: gate}); e != nil {
		t.Fatal(e)
	}
}

func TestQuotaPoolsOfAnAbsentLedgerIsEmpty(t *testing.T) {
	s, _ := newStore(t)
	if pools, e := s.QuotaPools(ctx, "j"); e != nil || len(pools) != 0 {
		t.Fatalf("%v %v", pools, e)
	}
	if _, e := os.Stat(s.DB); e == nil {
		t.Fatal("a read created the ledger")
	}
}

func TestParseGate(t *testing.T) {
	g, e := ParseGate("p", "ok pool=a profile=p\n# pool=a profile=p ref_resolution=x\nref_resolution=x", "")
	if e != nil || g.Pool != "a" || g.Audit["ref_resolution"] != "x" {
		t.Fatalf("%+v %v", g, e)
	}
	for name, text := range map[string]string{
		"profile differs": "pool=a profile=other",
		"no pool":         "profile=p",
		"injected audit":  "pool=a ref_resolution=unverified ref_resolution=verified",
	} {
		_, e := ParseGate("p", text, "")
		if Code(e) != ExitStorage {
			t.Errorf("%s: %v", name, e)
		}
	}
}

// ---- gc

func agentsOf(list ...Agent) AgentSource {
	return func(context.Context) ([]Agent, error) { return list, nil }
}

func quotaJob(t *testing.T, s *Store, job, label string) {
	t.Helper()
	if _, e := s.Claim(ctx, Claim{Job: job, Lane: "lane1", T: "T1", AgentLabel: label}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.RecordQuotaPool(ctx, QuotaRun{Job: job, Profile: "cmd-x", Gate: Gate{Pool: "example"}}); e != nil {
		t.Fatal(e)
	}
}

func TestGCExpiresExclusiveLeasesWithoutDeletingRows(t *testing.T) {
	s, clk := newStore(t)
	claim(t, s, "j1")
	claim(t, s, "j2")
	if _, e := s.Acquire(ctx, LeaseReq{Job: "j1", Resource: "a", Kind: KindPath, TTL: time.Minute, HasTTL: true}); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Acquire(ctx, LeaseReq{Job: "j2", Resource: "b", Kind: KindLinearPermit}); e != nil {
		t.Fatal(e)
	}
	clk.advance(2 * time.Minute)
	r, e := s.GC(ctx, agentsOf())
	if e != nil || r.Checked != 2 || r.Expired != 1 || len(r.Transitioned) != 1 || r.Transitioned[0].Job != "j1" {
		t.Fatalf("%+v %v", r, e)
	}
	st, _ := s.Status(ctx, "", "")
	if len(st.Leases) != 2 || len(st.History) != 1 || st.History[0].State != "expired" {
		t.Fatalf("%+v", st)
	}
	// Idempotent: the generation is archived once and not re-evented.
	r, e = s.GC(ctx, agentsOf())
	if e != nil || len(r.Transitioned) != 0 || r.Expired != 1 {
		t.Fatalf("%+v %v", r, e)
	}
	if n := strings.Count(strings.Join(eventKinds(t, s, "j1"), ","), "lease.expire"); n != 1 {
		t.Fatalf("lease.expire events: %d", n)
	}
}

func TestGCStaleQuotaRecordAndOrphanRelease(t *testing.T) {
	s, clk := newStore(t)
	quotaJob(t, s, "gone", "gone-label")
	quotaJob(t, s, "alive", "alive-label")
	clk.advance(11 * time.Minute)
	r, e := s.GC(ctx, agentsOf(Agent{Status: "idle", Name: "alive-label"}, Agent{Status: "working", Name: "other"}))
	if e != nil {
		t.Fatal(e)
	}
	if len(r.StaleQuota) != 1 || r.StaleQuota[0].Job != "gone" || len(r.OrphanReleased) != 1 || r.OrphanReleased[0].Job != "gone" || r.OrphanReleased[0].Reason != "job absent from herdr agent list" {
		t.Fatalf("%+v", r)
	}
	if r.HerdrAgents != 2 || r.LiveAgents != 1 {
		t.Fatalf("agents %+v", r)
	}
	if jv, _ := s.JobGet(ctx, "gone"); jv.State != "released" {
		t.Fatalf("gone: %s", jv.State)
	}
	if jv, _ := s.JobGet(ctx, "alive"); jv.State != "leased" {
		t.Fatalf("alive: %s", jv.State)
	}
	if pools, _ := s.QuotaPools(ctx, "alive"); len(pools) != 1 {
		t.Fatal("a listed agent lost its record")
	}
	if got := eventKinds(t, s, "gone"); !slices.Equal(got[len(got)-2:], []string{"quota_pool.stale_cleanup", "job.orphan_release"}) {
		t.Fatalf("kinds %v", got)
	}
}

func TestGCReleasesRowlessLeasedJobsOnlyWhenAgentIsAbsent(t *testing.T) {
	s, clk := newStore(t)
	for _, j := range []string{"a", "b"} {
		claim(t, s, j)
		if _, e := s.Acquire(ctx, LeaseReq{Job: j, Resource: j, Kind: KindPath, TTL: time.Minute, HasTTL: true}); e != nil {
			t.Fatal(e)
		}
	}
	clk.advance(11 * time.Minute) // both leases expired, both jobs past grace
	// gc archives the expired lease; the lease row stays, so neither job is row-less.
	r, e := s.GC(ctx, agentsOf())
	if e != nil || len(r.OrphanReleased) != 0 {
		t.Fatalf("%+v %v", r, e)
	}
	// A leased job holding nothing at all: force-release its lease first.
	if e = s.Release(ctx, "op", "a", KindPath, "", true); e != nil {
		t.Fatal(e)
	}
	c, _ := s.rw(ctx, false)
	c.exec("UPDATE jobs SET state = 'leased' WHERE job_id = 'a'")
	c.Close()
	clk.advance(11 * time.Minute)
	r, e = s.GC(ctx, agentsOf(Agent{Status: "working", Name: "a"}))
	if e != nil || len(r.OrphanReleased) != 0 {
		t.Fatalf("a listed agent is not an orphan: %+v %v", r, e)
	}
	r, e = s.GC(ctx, agentsOf())
	if e != nil || len(r.OrphanReleased) != 1 || r.OrphanReleased[0].Job != "a" {
		t.Fatalf("%+v %v", r, e)
	}
}

func TestGCStartupGraceSkipsFreshJobs(t *testing.T) {
	s, clk := newStore(t)
	quotaJob(t, s, "fresh", "fresh")
	clk.advance(9 * time.Minute)
	r, e := s.GC(ctx, agentsOf())
	if e != nil || len(r.StaleQuota) != 0 || len(r.Skipped) != 1 || !strings.Contains(r.Skipped[0].Reason, "startup grace") {
		t.Fatalf("%+v %v", r, e)
	}
	if pools, _ := s.QuotaPools(ctx, "fresh"); len(pools) != 1 {
		t.Fatal("a job inside the grace lost its record")
	}
}

func TestGCFailsClosedOnUnknownAgentOrUnmappedClaim(t *testing.T) {
	s, clk := newStore(t)
	quotaJob(t, s, "x", "x")
	clk.advance(time.Hour)
	_, e := s.GC(ctx, agentsOf(Agent{Status: "unknown", Name: "x"}))
	wantCode(t, e, ExitStorage)
	_, e = s.GC(ctx, agentsOf(Agent{Status: "weird", Name: "x"}))
	wantCode(t, e, ExitStorage)
	if pools, _ := s.QuotaPools(ctx, "x"); len(pools) != 1 {
		t.Fatal("a refused gc changed state")
	}
	// A claim from before the label mapping cannot be matched, so nothing is guessed.
	c, _ := s.rw(ctx, false)
	c.exec(`UPDATE events SET payload = '{"payload":{"owner_lane":"l"}}' WHERE job_id = 'x' AND kind = 'job.claim'`)
	c.Close()
	_, e = s.GC(ctx, agentsOf())
	wantCode(t, e, ExitStorage)
	if !strings.Contains(e.Error(), "unmapped=x") {
		t.Fatal(e)
	}
}

func TestGCSnapshotGuard(t *testing.T) {
	s, clk := newStore(t)
	quotaJob(t, s, "x", "x")
	clk.advance(time.Hour)
	// The job is released and reclaimed while the Herdr list is being read.
	racing := func(context.Context) ([]Agent, error) {
		if e := s.ReleaseQuotaPool(ctx, "x", "example", false); e != nil {
			t.Fatal(e)
		}
		if _, e := s.Claim(ctx, Claim{Job: "x", Lane: "l", T: "T1", ReclaimReleased: true}); e != nil {
			t.Fatal(e)
		}
		if _, e := s.RecordQuotaPool(ctx, QuotaRun{Job: "x", Profile: "cmd-x", Gate: Gate{Pool: "example"}}); e != nil {
			t.Fatal(e)
		}
		return nil, nil
	}
	r, e := s.GC(ctx, racing)
	if e != nil || len(r.StaleQuota) != 0 || len(r.Skipped) != 1 || !strings.Contains(r.Skipped[0].Reason, "changed after Herdr snapshot") {
		t.Fatalf("%+v %v", r, e)
	}
	if pools, _ := s.QuotaPools(ctx, "x"); len(pools) != 1 {
		t.Fatal("a reclaimed job lost its record")
	}
}

func TestReleaseOrphan(t *testing.T) {
	s, clk := newStore(t)
	claim(t, s, "x")
	if _, e := s.RecordQuotaPool(ctx, QuotaRun{Job: "x", Profile: "cmd-x", Gate: Gate{Pool: "example"}}); e != nil {
		t.Fatal(e)
	}
	_, e := s.ReleaseOrphan(ctx, "ghost", agentsOf())
	wantCode(t, e, ExitNotFound)
	clk.advance(time.Hour)
	// It still holds a quota_pool record: not an orphan.
	_, e = s.ReleaseOrphan(ctx, "x", agentsOf())
	wantCode(t, e, ExitFencing)
	if e = s.ReleaseQuotaPool(ctx, "x", "example", false); e != nil {
		t.Fatal(e)
	}
	// released, not leased
	_, e = s.ReleaseOrphan(ctx, "x", agentsOf())
	wantCode(t, e, ExitFencing)
	c, _ := s.rw(ctx, false)
	c.exec("UPDATE jobs SET state = 'leased' WHERE job_id = 'x'")
	c.Close()
	_, e = s.ReleaseOrphan(ctx, "x", agentsOf(Agent{Status: "idle", Name: "x"}))
	wantCode(t, e, ExitFencing)
	_, e = s.ReleaseOrphan(ctx, "x", agentsOf(Agent{Status: "unknown", Name: "x"}))
	wantCode(t, e, ExitStorage)
	o, e := s.ReleaseOrphan(ctx, "x", agentsOf())
	if e != nil || o.PriorState != "leased" {
		t.Fatalf("%+v %v", o, e)
	}
	evs, _ := s.Events(ctx, "x")
	if last := evs[len(evs)-1]; last.Kind != "job.orphan_release" || last.Payload["forced"] != true {
		t.Fatalf("%+v", last)
	}
}

// ---- a job is released only when it holds nothing

func TestJobStaysLeasedWhileAnotherResourceIsHeld(t *testing.T) {
	s, clk := newStore(t)
	claim(t, s, "j1")
	for _, pool := range []string{"pool-a", "pool-b"} {
		if _, e := s.RecordQuotaPool(ctx, QuotaRun{Job: "j1", Profile: "p", Gate: Gate{Pool: pool}}); e != nil {
			t.Fatal(e)
		}
	}
	l, e := s.Acquire(ctx, LeaseReq{Job: "j1", Resource: "res", Kind: KindPath, TTL: time.Hour, HasTTL: true})
	if e != nil {
		t.Fatal(e)
	}
	state := func() string { v, _ := s.JobGet(ctx, "j1"); return v.State }
	if e = s.ReleaseQuotaPool(ctx, "j1", "pool-a", false); e != nil || state() != "leased" {
		t.Fatalf("one of two pools released: %s %v", state(), e)
	}
	if e = s.Release(ctx, "j1", "res", KindPath, l.Token, false); e != nil || state() != "leased" {
		t.Fatalf("the lease released while a pool is held: %s %v", state(), e)
	}
	_, e = s.Claim(ctx, Claim{Job: "j1", Lane: "l", T: "T1", ReclaimReleased: true})
	wantCode(t, e, ExitDuplicate)
	if e = s.ReleaseQuotaPool(ctx, "j1", "pool-b", false); e != nil || state() != "released" {
		t.Fatalf("nothing held, still %s %v", state(), e)
	}
	// An expired lease holds nothing: releasing the last pool settles the job.
	claim(t, s, "j2")
	if _, e = s.Acquire(ctx, LeaseReq{Job: "j2", Resource: "res2", Kind: KindLinearPermit, TTL: time.Minute, HasTTL: true}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.RecordQuotaPool(ctx, QuotaRun{Job: "j2", Profile: "p", Gate: Gate{Pool: "pool-a"}}); e != nil {
		t.Fatal(e)
	}
	clk.advance(time.Hour)
	if e = s.ReleaseQuotaPool(ctx, "j2", "pool-a", false); e != nil {
		t.Fatal(e)
	}
	if v, _ := s.JobGet(ctx, "j2"); v.State != "released" {
		t.Fatalf("an expired lease kept the job leased: %s", v.State)
	}
}

// ---- a new ledger is private to its owner

func TestNewLedgerIsOwnerOnly(t *testing.T) {
	s, _ := newStore(t)
	claim(t, s, "j1")
	c, e := s.rw(ctx, false)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	if _, e = c.exec("INSERT INTO events (job_id, seq, kind, payload, created_at) VALUES ('x', 1, 'k', '{}', 't')"); e != nil {
		t.Fatal(e)
	}
	mode := func(p string) os.FileMode {
		st, e := os.Stat(p)
		if e != nil {
			t.Fatalf("%s: %v", p, e)
		}
		return st.Mode().Perm()
	}
	if m := mode(filepath.Dir(s.DB)); m != 0o700 {
		t.Fatalf("state dir mode %o", m)
	}
	for _, p := range []string{s.DB, s.DB + "-wal", s.DB + "-shm"} {
		if m := mode(p); m&0o077 != 0 {
			t.Fatalf("%s is readable by others: %o", filepath.Base(p), m)
		}
	}
}
