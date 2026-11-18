package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

// A held job whose events/ directory is missing cannot prove it does not own an
// unnamed row's pane. The owner lookup must fail the pass, not read the missing
// ledger as "no binding" (PR #701 review, high). Nothing is released.
func TestQuotaPoolReconcileUnnamedRowFailsClosedOnHeldJobWithoutLedger(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"pane-job", "ghost-job"} {
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: "label-" + job}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolJobEventNamed(t, jobs, "pane-job", "00003-job.spawned.json", map[string]any{
		"kind":    "job.spawned",
		"payload": map[string]any{"herdr_session": "custom", "pane_id": "pane-x"},
	})
	// ghost-job's ledger is gone: its events/ directory is missing.
	if err := os.RemoveAll(filepath.Join(jobs, "ghost-job", "events")); err != nil {
		t.Fatal(err)
	}
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	client.quotaReconcile.agents = func(context.Context, string) ([]nodeHerdrRow, error) {
		return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{PaneID: "pane-x", AgentStatus: "working"}}}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	for _, job := range []string{"pane-job", "ghost-job"} {
		if pools, err := store.QuotaPools(ctx, job); err != nil || len(pools) != 1 {
			t.Fatalf("%s pools=%v err=%v, want retained: the pass must fail closed on a held ledger it cannot read", job, pools, err)
		}
	}
}
