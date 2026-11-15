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
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
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
	t.Helper()
	dir := filepath.Join(jobs, job, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	event := map[string]any{
		"kind":    "job.spawned",
		"payload": map[string]any{"herdr_session": session},
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
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: t.TempDir(), AllowInsecureForTests: true})
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
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		if session != "custom" {
			t.Fatalf("read Herdr session %q, want custom", session)
		}
		return nil, errors.New("herdr unavailable")
	}
	client.reconcileNodeQuotaPools(ctx)

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
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true})
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
