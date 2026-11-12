package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type spawnedEventFaultStore struct {
	JobStore
	fault func()
}

func (s spawnedEventFaultStore) Put(ctx context.Context, id string, fields map[string]any) error {
	err := s.JobStore.Put(ctx, id, fields)
	if err == nil && fields["state"] == "spawned" {
		s.fault()
	}
	return err
}

func TestSpawnEventFailureKeepsCleanupMarkerAndLease(t *testing.T) {
	a, s, runner, _ := cmdFixture(t)
	store := a.JobStore.(*fakeJobStore)
	originalDB := a.Arbiter.DB
	blocked := filepath.Join(a.Home, "blocked")
	if err := os.WriteFile(blocked, nil, 0600); err != nil {
		t.Fatal(err)
	}
	a.JobStore = spawnedEventFaultStore{JobStore: store, fault: func() { a.Arbiter.DB = filepath.Join(blocked, "state.db") }}
	safe, observed := true, false
	a.ObserveRetryable = func(v bool) { safe = v; observed = true }
	err := a.Spawn(t.Context(), s)
	if err == nil || !observed || safe {
		t.Fatalf("failure not marked uncertain: %v %v %v", err, observed, safe)
	}
	// hk refuses a result without state=completed, and this fault also blocks the
	// local event, so ObserveRetryable above is what keeps the hub from a retry.
	if record := store.record(t, s.Job); record["state"] != "spawned" || record["result"] != nil {
		t.Fatalf("record: %+v", record)
	}
	a.Arbiter.DB = originalDB
	_, pools := ledgerState(t, a, s.Job)
	if len(pools) == 0 {
		t.Fatal("released working pane quota")
	}
	for _, c := range runner.Calls {
		if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "pane close") {
			t.Fatal("closed working pane")
		}
	}
}

func TestSpawnClaimMeasurementsIncludeLocalLease(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	placement, leased := 0, false
	a.ObserveStage = func(stage string, _ time.Duration) {
		if stage == "placement" {
			placement++
		}
		if stage == "claim" {
			if pools, err := a.ledger().QuotaPools(t.Context(), s.Job); err == nil && len(pools) > 0 {
				leased = true
			}
		}
	}
	if err := a.Spawn(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if placement != 1 || !leased {
		t.Fatalf("placement=%d local lease observed=%v", placement, leased)
	}
}

func TestLifecycleUsesExplicitNodeRoot(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	a.Jobs = root
	seedJob(t, a, "j", "worker")
	store := a.JobStore.(*fakeJobStore)
	if err := store.claimSpawned(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "j", "report.md")
	if err := os.WriteFile(path, []byte("done\n"), 0600); err != nil {
		t.Fatal(err)
	}
	a.Jobs = filepath.Join(a.Home, "wrong-root")
	if err := a.executeLifecycleAt(t.Context(), root, []string{"done", "j", "--report", path}); err != nil {
		t.Fatal(err)
	}
	if store.record(t, "j")["state"] != "completed" {
		t.Fatal("custom-root completion failed")
	}
}

func TestSubmissionReceiptAtomicConcurrentPublication(t *testing.T) {
	a := &App{Jobs: t.TempDir()}
	base := Submission{RequestID: "00000000-0000-4000-8000-000000000001", Args: []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "job", "--job", "job", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "worker"}, Brief: "brief", Repository: Repository{Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work/project"}, OriginMachine: "machine-a", DeadlineSeconds: 600}
	if err := os.MkdirAll(filepath.Join(a.Jobs, "job"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.Jobs, "job", ".submission-crashed.tmp"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := base
			req.RequestID = fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
			saved, err := a.saveSubmission(req)
			if err != nil {
				errs <- err
				return
			}
			ids <- saved.RequestID
		}(i)
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	winner := ""
	for id := range ids {
		if winner == "" {
			winner = id
		}
		if id != winner {
			t.Fatal("multiple request keys published")
		}
	}
	raw, err := os.ReadFile(filepath.Join(a.Jobs, "job", "submission.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Submission
	if json.Unmarshal(raw, &saved) != nil || saved.RequestID != winner {
		t.Fatal("partial receipt published")
	}
	base.Brief = "changed"
	if _, err = a.saveSubmission(base); ErrorCode(err) != exitDuplicate {
		t.Fatalf("overwrote existing receipt: %v", err)
	}
}

func TestPrepareSubmissionCanonicalLongJobIdentityAndEarlySubdirectoryRefusal(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Local["machine_id"] = "source-a"
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	s.Prompt = filepath.Join(a.Home, "submission-brief.md")
	if err := os.WriteFile(s.Prompt, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Label, s.T, s.Owner, s.Host, s.OverrideReason, s.Containment = "job-a", "T1", "parent", "h1", "fixture", "hk:12"
	a.Repository = &Repository{Name: "repo-a", Origin: "https://git.invalid/org/repo-a.git", LocalRoot: s.CWD}
	names := map[string]bool{}
	for _, tail := range []string{"first", "second"} {
		s.Job = strings.Repeat("a", 120) + tail
		req, err := a.PrepareSubmission(t.Context(), s)
		if err != nil {
			t.Fatal(err)
		}
		name := req.Repository.Checkout.Dir
		if names[name] || len(name) > 128 {
			t.Fatalf("checkout identity collision/length: %s", name)
		}
		names[name] = true
	}
	a.Repository.Relative = "src"
	if _, err := a.PrepareSubmission(t.Context(), s); err == nil || !strings.Contains(err.Error(), "checkout root") {
		t.Fatalf("subdirectory not refused early: %v", err)
	}
	if len(hub.spawns) != 0 {
		t.Fatal("preparation launched a node")
	}
}
