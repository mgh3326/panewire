package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

func TestQuotaPoolReconcileReleasesGoneJobAndKeepsLiveJob(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"gone-job", "live-job"} {
		label := "label-" + job
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "gone-job", "default")
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "live-job", "custom")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaEmptySince["default"] = now.Add(-arbiter.OrphanStartupGrace)
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	sessions := map[string]int{}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		sessions[session]++
		if session == "custom" {
			return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{Name: "label-live-job", AgentStatus: "working"}}}, nil
		}
		return []nodeHerdrRow{}, nil
	}
	client.reconcileNodeQuotaPools(ctx)
	if sessions["default"] != 1 || sessions["custom"] != 1 || len(sessions) != 2 {
		t.Fatalf("read Herdr sessions %v, want default and custom exactly once", sessions)
	}

	if pools, err := store.QuotaPools(ctx, "gone-job"); err != nil || len(pools) != 0 {
		t.Fatalf("gone job pools=%v err=%v, want released", pools, err)
	}
	if pools, err := store.QuotaPools(ctx, "live-job"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("live job pools=%v err=%v, want retained", pools, err)
	}
}

func writeQuotaPoolReconcileSpawnedEvent(t *testing.T, jobs, job, session string) {
	writeQuotaPoolReconcileSpawnedEventWithPane(t, jobs, job, session, "")
}

func writeQuotaPoolReconcileSpawnedEventWithPane(t *testing.T, jobs, job, session, pane string) {
	t.Helper()
	dir := filepath.Join(jobs, job, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	payload := map[string]any{"herdr_session": session}
	if pane != "" {
		payload["pane_id"] = pane
	}
	event := map[string]any{
		"kind":    "job.spawned",
		"payload": payload,
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00003-job.spawned.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestQuotaPoolReconcileRunsAtStartupAndOnEachTick(t *testing.T) {
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: t.TempDir(), AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time)
	client.quotaReconcile.ticks = ticks
	var passes atomic.Int32
	client.quotaReconcile.pass = func(context.Context) { passes.Add(1) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { client.runQuotaPoolReconcile(ctx); close(done) }()
	eventually(t, time.Second, "startup reconciliation", func() bool { return passes.Load() == 1 })
	ticks <- time.Now()
	eventually(t, time.Second, "periodic reconciliation", func() bool { return passes.Load() == 2 })
	cancel()
	eventually(t, time.Second, "reconciliation loop shutdown", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
}

func TestQuotaPoolReconcileLeavesRecordsWhenRecordedSessionIsUnreadable(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	if _, err := store.Claim(ctx, arbiter.Claim{Job: "held-job", Lane: "owner", T: "T2", AgentLabel: "label-held-job"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: "held-job", Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "held-job", "custom")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	var readCalls atomic.Int32
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		readCalls.Add(1)
		if session != "custom" {
			t.Fatalf("read Herdr session %q, want custom", session)
		}
		return nil, errors.New("herdr unavailable")
	}
	client.reconcileNodeQuotaPools(ctx)
	if readCalls.Load() != 1 {
		t.Fatalf("agents reader called %d times, want 1", readCalls.Load())
	}

	if pools, err := store.QuotaPools(ctx, "held-job"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("held job pools=%v err=%v, want retained after failed session read", pools, err)
	}
}

func TestQuotaPoolReconcileFailsClosedWhenJobDirectoryIsMissing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	if _, err := store.Claim(ctx, arbiter.Claim{Job: "missing-dir-job", Lane: "owner", T: "T2", AgentLabel: "label-missing-dir"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: "missing-dir-job", Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "missing-dir-job", "custom")
	if err := os.RemoveAll(filepath.Join(jobs, "missing-dir-job")); err != nil {
		t.Fatal(err)
	}
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(context.Context, string) ([]nodeHerdrRow, error) {
		t.Fatal("read Herdr even though a candidate job's session ledger is missing")
		return nil, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, "missing-dir-job"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("missing-dir job pools=%v err=%v, want retained", pools, err)
	}
}

func TestQuotaPoolReconcileRefusesEmptySnapshotWhenJobsHeld(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	job := "held-job"
	label := "label-" + job
	if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, job, "default")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		return []nodeHerdrRow{}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("held job pools=%v err=%v, want retained after empty agent snapshot", pools, err)
	}
	if jv, err := store.JobGet(ctx, job); err != nil || jv.State != "leased" {
		t.Fatalf("held job state=%q err=%v, want leased", jv.State, err)
	}
}

func TestQuotaPoolReconcileDisabledByDefault(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	job := "gone-job"
	label := "label-" + job
	if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, job, "default")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{Name: "other", AgentStatus: "idle"}}}, nil
	}
	var passes atomic.Int32
	client.quotaReconcile.pass = func(context.Context) { passes.Add(1) }
	client.runQuotaPoolReconcile(ctx)
	if passes.Load() != 0 {
		t.Fatalf("runQuotaPoolReconcile ran %d passes, want 0 when disabled by default", passes.Load())
	}
	client.reconcileNodeQuotaPools(ctx)
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("job pools=%v err=%v, want retained when reconcile is disabled", pools, err)
	}
}

func TestQuotaPoolReconcileDaemonFlag(t *testing.T) {
	root := t.TempDir()
	envPath := filepath.Join(root, "node.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=node-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--socket", filepath.Join(root, "daemon.sock"), "--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath}
	daemon, code, err := newDaemonForCLI(base, daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil || code != ExitOK || daemon.cfg.Hub.Client.quotaPoolReconcile {
		t.Fatalf("default flag: daemon=%v code=%d err=%v, want quotaPoolReconcile=false", daemon, code, err)
	}
	daemon, code, err = newDaemonForCLI(append(append([]string(nil), base...), "--quota-pool-reconcile"), daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil || code != ExitOK || !daemon.cfg.Hub.Client.quotaPoolReconcile {
		t.Fatalf("opt-in flag: daemon=%v code=%d err=%v, want quotaPoolReconcile=true", daemon, code, err)
	}
}

func TestQuotaPoolReconcileEmptySessionHoldsAndReleasesOther(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"job-side", "gone-work", "live-work"} {
		label := "label-" + job
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "job-side", "side")
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "gone-work", "work")
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "live-work", "work")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		if session == "side" {
			return []nodeHerdrRow{}, nil
		}
		return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{Name: "label-live-work", AgentStatus: "working"}}}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	// job-side in empty session is unverified & skipped (retained)
	if pools, err := store.QuotaPools(ctx, "job-side"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("job-side pools=%v err=%v, want retained", pools, err)
	}
	// live-work is present (retained)
	if pools, err := store.QuotaPools(ctx, "live-work"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("live-work pools=%v err=%v, want retained", pools, err)
	}
	// gone-work in session "work" is absent from rows and released
	if pools, err := store.QuotaPools(ctx, "gone-work"); err != nil || len(pools) != 0 {
		t.Fatalf("gone-work pools=%v err=%v, want released", pools, err)
	}
}

func TestQuotaPoolReconcileEmptySessionReleasesAfterTenMinutesContinuous(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	job := "single-job"
	label := "label-" + job
	if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, job, "single")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	var rowsToReturn []nodeHerdrRow
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		return rowsToReturn, nil
	}

	// Pass 1: empty read at t=0 -> unverified, retained
	rowsToReturn = []nodeHerdrRow{}
	client.reconcileNodeQuotaPools(ctx)
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 {
		t.Fatalf("pass 1: pools=%v, want retained", pools)
	}

	// Pass 2: advance 9m50s -> still within 10m grace, retained
	now = now.Add(9*time.Minute + 50*time.Second)
	client.reconcileNodeQuotaPools(ctx)
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 {
		t.Fatalf("pass 2 (9m50s): pools=%v, want retained", pools)
	}

	// Non-empty read in between resets the emptySince window
	rowsToReturn = []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{Name: label, AgentStatus: "working"}}}
	now = now.Add(10 * time.Second)
	client.reconcileNodeQuotaPools(ctx)
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 {
		t.Fatalf("pass with live agent: pools=%v, want retained", pools)
	}

	// New empty read: resets window to 0
	rowsToReturn = []nodeHerdrRow{}
	now = now.Add(10 * time.Second)
	client.reconcileNodeQuotaPools(ctx)
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 {
		t.Fatalf("reset pass 1: pools=%v, want retained", pools)
	}

	// Advance 5 minutes: still retained
	now = now.Add(5 * time.Minute)
	client.reconcileNodeQuotaPools(ctx)
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 {
		t.Fatalf("reset pass 2: pools=%v, want retained", pools)
	}

	// Advance past 10 continuous minutes: empty read accepted as absence, job released!
	now = now.Add(5*time.Minute + time.Second)
	client.reconcileNodeQuotaPools(ctx)
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 0 {
		t.Fatalf("after 10m continuous empty: pools=%v, want released", pools)
	}
}

func TestQuotaPoolReconcileUnnamedRowOnJobPaneHoldsJobAndReleasesOther(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"held-a", "gone-b"} {
		label := "label-" + job
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolReconcileSpawnedEventWithPane(t, jobs, "held-a", "custom", "w1:pA")
	writeQuotaPoolReconcileSpawnedEventWithPane(t, jobs, "gone-b", "custom", "w1:pB")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		return []nodeHerdrRow{
			{nodeAgentRow: nodeAgentRow{PaneID: "w1:pA", Name: "", AgentStatus: "working"}},
		}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	// held-a has unnamed row on its pane: unverified & skipped (retained)
	if pools, err := store.QuotaPools(ctx, "held-a"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("held-a pools=%v err=%v, want retained", pools, err)
	}
	// gone-b is absent and released despite being in the same session!
	if pools, err := store.QuotaPools(ctx, "gone-b"); err != nil || len(pools) != 0 {
		t.Fatalf("gone-b pools=%v err=%v, want released", pools, err)
	}
}

func TestQuotaPoolReconcileUnnamedRowWithoutPaneHoldsSessionAndReleasesOther(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"held-a", "gone-b", "live-b"} {
		label := "label-" + job
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "held-a", "sess-a")
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "gone-b", "sess-b")
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "live-b", "sess-b")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		if session == "sess-a" {
			// Unnamed row without pane id in sess-a
			return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{PaneID: "", Name: "", AgentStatus: "working"}}}, nil
		}
		// sess-b has live-b listed, gone-b absent
		return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{Name: "label-live-b", AgentStatus: "working"}}}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	// held-a in sess-a is retained
	if pools, err := store.QuotaPools(ctx, "held-a"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("held-a pools=%v err=%v, want retained", pools, err)
	}
	// live-b in sess-b is retained
	if pools, err := store.QuotaPools(ctx, "live-b"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("live-b pools=%v err=%v, want retained", pools, err)
	}
	// gone-b in sess-b is released
	if pools, err := store.QuotaPools(ctx, "gone-b"); err != nil || len(pools) != 0 {
		t.Fatalf("gone-b pools=%v err=%v, want released", pools, err)
	}
}

func TestQuotaPoolReconcileUnnamedRowOnUnboundPaneDropped(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	job := "gone-job"
	label := "label-" + job
	if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: label}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolReconcileSpawnedEventWithPane(t, jobs, job, "custom", "w1:p1")
	// sess-other provides another session with a live agent so custom is not the sole empty session
	other := "live-other"
	if _, err := store.Claim(ctx, arbiter.Claim{Job: other, Lane: "owner", T: "T2", AgentLabel: "label-" + other}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: other, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolReconcileSpawnedEventWithPane(t, jobs, other, "sess-other", "w1:pOther")

	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		if session == "sess-other" {
			return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{Name: "label-live-other", AgentStatus: "working"}}}, nil
		}
		// session "custom" lists an unnamed row on an unbound pane "w1:p99" (not w1:p1)
		return []nodeHerdrRow{
			{nodeAgentRow: nodeAgentRow{PaneID: "w1:p99", Name: "", AgentStatus: "working"}},
		}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	// Unnamed row on unbound pane is dropped; gone-job is released
	if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 0 {
		t.Fatalf("gone-job pools=%v err=%v, want released when unnamed row is on unbound pane", pools, err)
	}
	if pools, err := store.QuotaPools(ctx, other); err != nil || len(pools) != 1 {
		t.Fatalf("live-other pools=%v err=%v, want retained", pools, err)
	}
}
