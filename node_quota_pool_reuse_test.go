package panewire

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

// Herdr can reuse a pane ID. A job that was bound to the pane and has since
// ended (completed here) no longer holds it, so an unnamed row on the reused ID
// must not hold that job. Otherwise its stale quota record is never released
// (PR #701 review, medium).
func TestQuotaPoolReconcileUnnamedRowIgnoresEndedJobOnReusedPane(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	if _, err := store.Claim(ctx, arbiter.Claim{Job: "old-job", Lane: "owner", T: "T2", AgentLabel: "label-old-job"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: "old-job", Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	writeQuotaPoolJobEventNamed(t, jobs, "old-job", "00003-job.spawned.json", map[string]any{
		"kind":    "job.spawned",
		"payload": map[string]any{"herdr_session": "custom", "pane_id": "pane-x"},
	})
	writeQuotaPoolJobEventNamed(t, jobs, "old-job", "00004-job.completed.json", map[string]any{
		"kind":    "job.completed",
		"payload": map[string]any{},
	})
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
		// The reused pane ID now belongs to an unnamed row of a new agent.
		return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{PaneID: "pane-x", AgentStatus: "working"}}}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, "old-job"); err != nil || len(pools) != 0 {
		t.Fatalf("old-job pools=%v err=%v, want released: it ended and no longer holds pane-x", pools, err)
	}
}
