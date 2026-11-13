package panewire

import (
	"context"
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
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: jobs, AllowInsecureForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.nodeHerdr = func(context.Context, ...string) ([]byte, error) {
		return []byte(nodeAgentList(nodeAgentJSON("w1:p1", "label-live-job", "working", "/work/live"))), nil
	}
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, "gone-job"); err != nil || len(pools) != 0 {
		t.Fatalf("gone job pools=%v err=%v, want released", pools, err)
	}
	if pools, err := store.QuotaPools(ctx, "live-job"); err != nil || len(pools) != 1 || pools[0] != "example" {
		t.Fatalf("live job pools=%v err=%v, want retained", pools, err)
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
