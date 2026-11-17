package fleet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// hubRevocationRecord is the exact record the node writes when the hub
// reassigns a job to another node: writeHubRevocation (hub_jobs_client.go)
// stores a type-only job.revoked with the new ownership epoch, in the file
// <seq>-job.revoked.json. hub_revocation_record_t5062_test.go in the root
// package pins this body against the real function.
const hubRevocationRecord = `{"type":"job.revoked","job_id":"j","epoch":2}`

// writeHubRevocationRecord writes the revocation record after the job's
// existing events, as writeHubRevocation does, and returns its path.
func writeHubRevocationRecord(t *testing.T, a *App, id string) string {
	t.Helper()
	dir := filepath.Join(a.Jobs, id, "events")
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%05d-job.revoked.json", len(files)+1))
	if err := os.WriteFile(path, []byte(strings.Replace(hubRevocationRecord, `"j"`, `"`+id+`"`, 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	// The record carries no timestamp: its mtime is its time, so stamp it
	// where putEvent stamps the next event.
	stamp := a.Now().Add(time.Duration(len(files)) * time.Nanosecond)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	return path
}

func putsOf(s *fakeJobStore) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if strings.HasPrefix(c, "put ") {
			n++
		}
	}
	return n
}

func closedPanes(f *fakeRunner) []string {
	var closed []string
	for _, c := range f.Calls {
		if line := strings.Join(c.Args, " "); strings.HasPrefix(line, "pane close") {
			closed = append(closed, line)
		}
	}
	return closed
}

// reapRevoked reaps a reviewer job whose hk record is spawned after the given
// terminal step, with its pane listed idle. It returns the store, its put
// count and the panes reap closed.
func reapRevoked(t *testing.T, terminal func(a *App)) (*fakeJobStore, []string) {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	base := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"idle"}]}}`}
		}
		return base(c)
	}
	store := a.JobStore.(*fakeJobStore)
	if err := store.Claim(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	store.records["j"]["state"] = "spawned"
	seedJob(t, a, "j", "reviewer")
	terminal(a)
	reapJob(t, a)
	return store, closedPanes(f)
}

// A node that lost a job to a hub reassignment closes its own pane on reap but
// leaves the shared hk record, which the new owner holds (hk 5062).
func TestReapAfterHubRevocationLeavesHKRecord_t5062(t *testing.T) {
	store, closed := reapRevoked(t, func(a *App) { writeHubRevocationRecord(t, a, "j") })
	if got := store.record(t, "j")["state"]; got != "spawned" || putsOf(store) != 0 {
		t.Fatalf("hk state=%v puts=%d, want the new owner's record untouched", got, putsOf(store))
	}
	if len(closed) != 1 {
		t.Fatalf("panes closed = %v, want the local pane closed", closed)
	}
}

// The same end by a local job.revoked (panewire job close) or job.lost is this
// node's own, and reap still writes lost.
func TestReapWithoutHubRevocationStillWritesLost_t5062(t *testing.T) {
	for _, kind := range []string{"job.revoked", "job.lost"} {
		t.Run(kind, func(t *testing.T) {
			store, closed := reapRevoked(t, func(a *App) { putEvent(t, a, "j", kind, map[string]any{}) })
			if got := store.record(t, "j")["state"]; got != "lost" || putsOf(store) != 1 || len(closed) != 1 {
				t.Fatalf("hk state=%v puts=%d closed=%v, want lost written once and the pane closed", got, putsOf(store), closed)
			}
		})
	}
}

// Reap's usage write is a write to the shared record too: a reassigned job's
// usage belongs to the new owner's session, not this node's.
func TestReapAfterHubRevocationWritesNoUsage_t5062(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	store := usageJob(t, a, claudeSession())
	store.mu.Lock()
	store.calls = nil
	store.mu.Unlock()
	writeHubRevocationRecord(t, a, "j")
	reapJob(t, a)
	rec := store.record(t, "j")
	if _, ok := rec["usage"]; ok || rec["state"] != "spawned" || putsOf(store) != 0 {
		t.Fatalf("record = %v puts=%d, want the new owner's record untouched", rec, putsOf(store))
	}
}

// The task pane sweep closes a revoked job's pane, with the hk write left out.
func TestSweepAfterHubRevocationLeavesHKRecord_t5062(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "developing", "started"
	path := writeHubRevocationRecord(t, f.a, "j")
	// The record carries no timestamp: its mtime is its time. Pin it to the
	// fixture clock, as a real revocation is stamped by the real clock.
	stamp := f.now.Add(time.Second)
	if err := os.Chtimes(path, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	f.at(0)
	f.at(60)
	f.at(240)
	if got := f.store.records["j"]["state"]; got != "spawned" || putsOf(f.store) != 0 {
		t.Fatalf("hk state=%v puts=%d, want the new owner's record untouched", got, putsOf(f.store))
	}
	if closed := f.closed(); !slices.Equal(closed, []string{"p-j"}) {
		t.Fatalf("closed = %v, want the local pane closed", closed)
	}
}

// A job that ends by a local job.revoked still gets its hk record ended by the
// sweep.
func TestSweepWithoutHubRevocationStillWritesHK_t5062(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "developing", "started"
	putEvent(t, f.a, "j", "job.revoked", map[string]any{})
	f.at(0)
	f.at(60)
	f.at(240)
	if got := f.store.records["j"]["state"]; got != "lost" || putsOf(f.store) == 0 || len(f.closed()) != 1 {
		t.Fatalf("hk state=%v puts=%d closed=%v, want lost written and the pane closed", got, putsOf(f.store), f.closed())
	}
}

// A quota stop whose hand-off grace ran out ends the job with a bare hk lost
// write; a job the hub reassigned in the meantime is left to its new owner.
func TestQuotaStopLostAfterHubRevocationLeavesHKRecord_t5062(t *testing.T) {
	for _, tt := range []struct {
		name       string
		reassigned bool
		wantState  string
		wantPuts   int
	}{{"reassigned", true, "spawned", 0}, {"own end", false, "lost", 1}} {
		t.Run(tt.name, func(t *testing.T) {
			a := testApp(t)
			ledgerRunner(t, a)
			store := a.JobStore.(*fakeJobStore)
			if err := store.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			store.calls = nil
			store.mu.Unlock()
			seedJob(t, a, "j", "implementer")
			if tt.reassigned {
				writeHubRevocationRecord(t, a, "j")
			}
			j, err := a.job("j")
			if err != nil {
				t.Fatal(err)
			}
			if err := a.quotaStopLost(t.Context(), j, quotastop.Stop{Pool: "example"}, false); err != nil {
				t.Fatal(err)
			}
			if got := store.record(t, "j")["state"]; got != tt.wantState || putsOf(store) != tt.wantPuts {
				t.Fatalf("hk state=%v puts=%d, want %s with %d put(s)", got, putsOf(store), tt.wantState, tt.wantPuts)
			}
		})
	}
}

// Job.Reassigned holds from the hub revocation to the next claim or spawn,
// across a local end or reprompt, and only a hub revocation sets it.
func TestJobReassignedFollowsHubRevocation_t5062(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	reassigned := func(id string) bool {
		t.Helper()
		j, err := a.job(id)
		if err != nil {
			t.Fatal(err)
		}
		return j.Reassigned
	}
	seedJob(t, a, "j", "implementer")
	if reassigned("j") {
		t.Fatal("a fresh job is reassigned")
	}
	putEvent(t, a, "j", "job.revoked", map[string]any{})
	if reassigned("j") {
		t.Fatal("a local job.revoked is reassigned")
	}
	seedJob(t, a, "j", "implementer")
	// A type-only record without the hub's epoch is no reassignment.
	files, _ := os.ReadDir(filepath.Join(a.Jobs, "j", "events"))
	noEpoch := filepath.Join(a.Jobs, "j", "events", fmt.Sprintf("%05d-job.revoked.json", len(files)+1))
	if err := os.WriteFile(noEpoch, []byte(`{"type":"job.revoked","job_id":"j"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	stamp := a.Now().Add(time.Duration(len(files)) * time.Nanosecond)
	if err := os.Chtimes(noEpoch, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	if reassigned("j") {
		t.Fatal("a revocation record with no epoch is reassigned")
	}
	seedJob(t, a, "j", "implementer")
	writeHubRevocationRecord(t, a, "j")
	if !reassigned("j") {
		t.Fatal("a hub revocation is not reassigned")
	}
	putEvent(t, a, "j", "job.lost", map[string]any{})
	putEvent(t, a, "j", "job.reprompted", map[string]any{})
	if !reassigned("j") {
		t.Fatal("a later local end or reprompt cleared the reassignment")
	}
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p2", "tab_id": "w:t2", "label": "agent-b"})
	if reassigned("j") {
		t.Fatal("a new spawn kept the reassignment")
	}
}

// The hub writes a revocation without the job end lock, so the Job a writer
// holds can predate it. Each shared write re-reads the ledger just before it
// (hubReassigned): these writers get a snapshot taken before the revocation.
func TestSharedWritersRecheckLedgerAfterStaleSnapshot_t5062(t *testing.T) {
	stale := func(t *testing.T) (*App, *fakeJobStore, Job) {
		t.Helper()
		a := testApp(t)
		ledgerRunner(t, a)
		store := a.JobStore.(*fakeJobStore)
		if err := store.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
			t.Fatal(err)
		}
		seedJob(t, a, "j", "implementer")
		putEvent(t, a, "j", "job.lost", map[string]any{})
		j, err := a.job("j")
		if err != nil || j.Reassigned {
			t.Fatalf("snapshot err=%v reassigned=%v", err, j.Reassigned)
		}
		writeHubRevocationRecord(t, a, "j")
		store.mu.Lock()
		store.calls = nil
		store.mu.Unlock()
		return a, store, j
	}
	untouched := func(t *testing.T, store *fakeJobStore) {
		t.Helper()
		if got := store.record(t, "j")["state"]; got != "spawned" || putsOf(store) != 0 {
			t.Fatalf("hk state=%v puts=%d, want the new owner's record untouched", got, putsOf(store))
		}
	}
	t.Run("reap", func(t *testing.T) {
		a, store, j := stale(t)
		a.markEndedJobLost(t.Context(), j)
		untouched(t, store)
	})
	t.Run("sweep", func(t *testing.T) {
		a, store, j := stale(t)
		a.markSweptJob(t.Context(), j, "lost")
		untouched(t, store)
	})
	t.Run("usage", func(t *testing.T) {
		a, store, j := stale(t)
		a.writeJobSession(t.Context(), j, jobSession{})
		untouched(t, store)
	})
	t.Run("quota stop", func(t *testing.T) {
		a, store, j := stale(t)
		if err := a.quotaStopLost(t.Context(), j, quotastop.Stop{Pool: "example"}, false); err != nil {
			t.Fatal(err)
		}
		untouched(t, store)
	})
}

// A revocation that lands while reap reads hk (after the job was folded) still
// stops the write: the hk read is where the window is widest.
func TestReapRevocationDuringHKReadLeavesHKRecord_t5062(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	base := a.JobStore.(*fakeJobStore)
	store := &revokeOnGetStore{fakeJobStore: base, revoke: func() { writeHubRevocationRecord(t, a, "j") }}
	a.JobStore = store
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "implementer")
	putEvent(t, a, "j", "job.lost", map[string]any{})
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	base.mu.Lock()
	base.calls = nil
	base.mu.Unlock()
	a.markEndedJobLost(t.Context(), j)
	if !store.revoked || base.record(t, "j")["state"] != "spawned" || putsOf(base) != 0 {
		t.Fatalf("revoked=%v hk state=%v puts=%d, want the new owner's record untouched", store.revoked, base.record(t, "j")["state"], putsOf(base))
	}
}

// revokeOnGetStore lands the hub revocation during the first hk read.
type revokeOnGetStore struct {
	*fakeJobStore
	revoke  func()
	revoked bool
}

func (s *revokeOnGetStore) Get(ctx context.Context, id string) (map[string]any, error) {
	rec, err := s.fakeJobStore.Get(ctx, id)
	if !s.revoked {
		s.revoked = true
		s.revoke()
	}
	return rec, err
}
