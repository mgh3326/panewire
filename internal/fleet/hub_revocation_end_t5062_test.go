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

// A worker pane left on the node the hub took the job from must not complete
// or escalate the new owner's record: the command is refused, whether the
// revocation is already in the ledger or lands while the report uploads (the
// hub writes it without the job's locks).
func TestDoneAndEscalateAfterHubRevocationLeaveHKRecord_t5062(t *testing.T) {
	commands := map[string][]string{
		"done":     {"--report"},
		"escalate": {"--report", "--question", "which way?"},
	}
	for _, kind := range []string{"done", "escalate"} {
		for _, during := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/revoked-during-upload=%v", kind, during), func(t *testing.T) {
				r := newDoneRig(t)
				r.write("report\n")
				revoke := func() { writeHubRevocationRecord(t, r.a, "j") }
				if during {
					revoked := false
					r.hk.Handle(func(req hkRequest) (int, string, bool) {
						if !revoked && strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
							revoked = true
							revoke()
						}
						return 0, "", false
					})
				} else {
					revoke()
				}
				r.store.mu.Lock()
				r.store.calls = nil
				r.store.mu.Unlock()
				uploads := 0
				if !during {
					r.hk.Handle(func(req hkRequest) (int, string, bool) {
						if strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
							uploads++
						}
						return 0, "", false
					})
				}
				args := []string{"j"}
				for _, a := range commands[kind] {
					args = append(args, a)
					if a == "--report" {
						args = append(args, r.path)
					}
				}
				err := r.a.lifecycle(t.Context(), kind, args)
				if code(err) != exitStateConflict || !strings.Contains(err.Error(), "reassigned") {
					t.Fatalf("err = %v, want the reassignment refusal", err)
				}
				if got := r.store.record(t, "j")["state"]; got != "spawned" || putsOf(r.store) != 0 {
					t.Fatalf("hk state=%v puts=%d, want the new owner's record untouched", got, putsOf(r.store))
				}
				if uploads != 0 {
					t.Fatalf("%d report upload(s) before the refusal, want none for a revocation already in the ledger", uploads)
				}
			})
		}
	}
}

// Ownership is read in ledger sequence order, not timestamp order: a node
// clock stepped backwards gives the revocation record an mtime before the
// spawn it followed, and the timestamp fold would clear the reassignment.
func TestHubReassignedReadsLedgerSequenceOrder_t5062(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	seedJob(t, a, "j", "implementer")
	path := writeHubRevocationRecord(t, a, "j")
	past := a.Now().Add(-time.Hour)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatal(err)
	}
	if folded, err := a.job("j"); err != nil || !folded.Reassigned {
		t.Fatalf("fold err=%v reassigned=%v, want the revocation to stand though its mtime precedes the spawn", err, folded.Reassigned)
	}
	if !a.hubReassigned(Job{ID: "j"}) {
		t.Fatal("hubReassigned missed a revocation that follows the spawn in ledger order")
	}
}

// recordJobUsage of a live job also writes the session's model to the record;
// a revocation that lands while it reads the harness session stops that write
// too, not only the usage one.
func TestRecordJobUsageModelWriteAfterHubRevocation_t5062(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	claudeFixture(t, a.Home)
	store := usageJob(t, a, claudeSession())
	stale, err := a.job("j")
	if err != nil || stale.Terminal || stale.Reassigned {
		t.Fatalf("snapshot err=%v terminal=%v reassigned=%v", err, stale.Terminal, stale.Reassigned)
	}
	writeHubRevocationRecord(t, a, "j")
	store.mu.Lock()
	store.calls = nil
	store.mu.Unlock()
	a.recordJobUsage(t.Context(), stale, false)
	if rec := store.record(t, "j"); rec["model"] != nil || rec["usage"] != nil || putsOf(store) != 0 {
		t.Fatalf("record = %v puts=%d, want the new owner's record untouched", rec, putsOf(store))
	}
}

// A revocation that lands while done reads the harness session (after the
// upload's check) still stops the terminal PUT, and so does one that lands
// between the PUTs of strictCompletedPutUntil's retries or at the pin of a
// retry whose settlement is already receipted.
func TestDoneAndEscalateRevokedDuringSessionRead_t5062(t *testing.T) {
	for _, kind := range []string{"done", "escalate"} {
		t.Run(kind, func(t *testing.T) {
			a := testApp(t)
			f := ledgerRunner(t, a)
			hk := useFakeHK(t)
			noHKCLI(t, f)
			hk.Handle(hkCommentAnswer)
			claudeFixture(t, a.Home)
			store := usageJob(t, a, claudeSession())
			revoked := false
			base := f.Do
			f.Do = func(c Command) Result {
				if !revoked && c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
					revoked = true
					writeHubRevocationRecord(t, a, "j")
				}
				return base(c)
			}
			path := filepath.Join(a.Jobs, "j", "report.md")
			if err := os.WriteFile(path, []byte("report\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			store.calls = nil
			store.mu.Unlock()
			args := []string{"j", "--report", path}
			if kind == "escalate" {
				args = append(args, "--question", "which way?")
			}
			err := a.lifecycle(t.Context(), kind, args)
			if !revoked {
				t.Fatal("the session read never ran, so the revocation never landed")
			}
			if code(err) != exitStateConflict || !strings.Contains(err.Error(), "reassigned") {
				t.Fatalf("err = %v, want the reassignment refusal", err)
			}
			if got := store.record(t, "j")["state"]; got != "spawned" || putsOf(store) != 0 {
				t.Fatalf("hk state=%v puts=%d, want the new owner's record untouched", got, putsOf(store))
			}
		})
	}
}

// putRevokeStore lands the revocation with the first PUT, which it answers
// with an ambiguous failure so the writer retries.
type putRevokeStore struct {
	*fakeJobStore
	revoke func()
	puts   int
}

func (s *putRevokeStore) Put(ctx context.Context, id string, fields map[string]any) error {
	s.puts++
	if s.puts == 1 {
		s.revoke()
		return &BackendError{Status: 503}
	}
	return s.fakeJobStore.Put(ctx, id, fields)
}

func TestStrictCompletedPutRechecksBeforeRetry_t5062(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	base := a.JobStore.(*fakeJobStore)
	store := &putRevokeStore{fakeJobStore: base, revoke: func() { writeHubRevocationRecord(t, a, "j") }}
	a.JobStore = store
	if err := base.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "implementer")
	_, err := a.strictCompletedPutUntil(t.Context(), "j", map[string]any{"state": "completed"}, nil, nil, time.Now().Add(time.Minute))
	if !isDoneResultFailure(err, "job_reassigned") || store.puts != 1 || base.record(t, "j")["state"] != "spawned" {
		t.Fatalf("err=%v puts=%d state=%v, want one PUT (before the revocation) and then the refusal", err, store.puts, base.record(t, "j")["state"])
	}
}

// A retry whose settlement is receipted skips the completed write; a
// revocation landing during its report upload still stops the pin.
func TestDoneRetryRevokedDuringUploadDoesNotPin_t5062(t *testing.T) {
	r := newDoneRig(t)
	r.write("report\n")
	digest := reportDigest([]byte("report\n"))
	putEvent(t, r.a, "j", "job.completed", map[string]any{"report": r.path, "report_sha256": digest, "question": ""})
	putEvent(t, r.a, "j", eventRecordSettled, map[string]any{"report_sha256": digest, "pinned": false})
	r.store.records["j"]["state"] = "completed"
	revoked := false
	r.hk.Handle(func(req hkRequest) (int, string, bool) {
		if !revoked && strings.HasPrefix(req.Route(), "PUT /v1/documents/") {
			revoked = true
			writeHubRevocationRecord(t, r.a, "j")
		}
		return 0, "", false
	})
	r.store.mu.Lock()
	r.store.calls = nil
	r.store.mu.Unlock()
	err := r.done()
	if !revoked || code(err) != exitStateConflict || !strings.Contains(err.Error(), "reassigned") {
		t.Fatalf("revoked=%v err=%v, want the reassignment refusal", revoked, err)
	}
	if rec := r.store.record(t, "j"); rec["report_doc"] != nil || putsOf(r.store) != 0 {
		t.Fatalf("record=%v puts=%d, want no pin written", rec, putsOf(r.store))
	}
}

// The spawn repair PUT is a write to the record too: a revocation that lands
// during the read-back that finds the record still claimed stops it.
func TestStrictCompletedPutRechecksBeforeSpawnRepair_t5062(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	base := a.JobStore.(*fakeJobStore)
	store := &revokeOnGetStore{fakeJobStore: base, revoke: func() { writeHubRevocationRecord(t, a, "j") }}
	a.JobStore = store
	if err := base.Claim(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	seedJob(t, a, "j", "implementer")
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	base.putErr = &BackendError{Status: 503}
	_, err = a.strictCompletedPutUntil(t.Context(), "j", map[string]any{"state": "completed"}, nil, spawnReceiptForRepair(j.Events), time.Now().Add(time.Minute))
	puts := putsOf(base)
	if !isDoneResultFailure(err, "job_reassigned") || puts != 1 {
		t.Fatalf("err=%v puts=%d, want the one PUT before the revocation and then the refusal, no repair PUT", err, puts)
	}
}

// done reports the refusal and announces nothing when the revocation lands
// during the terminal PUT's retries: a failed completed write alone is a
// warning, so the refusal must not be taken for one.
func TestDoneRevokedBetweenTerminalPutRetriesAnnouncesNothing_t5062(t *testing.T) {
	r := newDoneRig(t)
	r.write("report\n")
	store := &putRevokeStore{fakeJobStore: r.store, revoke: func() { writeHubRevocationRecord(t, r.a, "j") }}
	r.a.JobStore = store
	err := r.done()
	if !isDoneResultFailure(err, "job_reassigned") {
		t.Fatalf("err = %v, want the reassignment refusal", err)
	}
	if got := len(r.events("job.completed")); got != 0 || r.store.record(t, "j")["state"] != "spawned" {
		t.Fatalf("job.completed events=%d state=%v, want nothing announced and the record untouched", got, r.store.record(t, "j")["state"])
	}
}
