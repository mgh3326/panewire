package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

// The pane index reads only binding events it can name from their file name; a
// job's own ledger reads every event by its kind. An unnamed row on a pane that
// the job's ledger binds must hold that job even when the index misses the
// binding. Before the owner lookup read the ledger, the row was dropped and the
// job was released (PR #701 review).
func TestQuotaPoolReconcileUnnamedRowFindsOwnerFromLedger(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"pane-job", "gone-job"} {
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: "label-" + job}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "gone-job", "custom")
	// pane-job's binding sits in a file the pane index does not scan.
	writeQuotaPoolJobEventNamed(t, jobs, "pane-job", "00004-job.spawn.json", map[string]any{
		"kind":    "job.spawned",
		"payload": map[string]any{"herdr_session": "custom", "pane_id": "pane-x"},
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
		return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{PaneID: "pane-x", AgentStatus: "working"}}}, nil
	}
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, "pane-job"); err != nil || len(pools) != 1 {
		t.Fatalf("pane-job pools=%v err=%v, want retained: its ledger binds the unnamed row's pane", pools, err)
	}
	if pools, err := store.QuotaPools(ctx, "gone-job"); err != nil || len(pools) != 0 {
		t.Fatalf("gone-job pools=%v err=%v, want released: its session has no row for it", pools, err)
	}
}

// writeQuotaPoolJobEventNamed writes one event of job under the given file name.
func writeQuotaPoolJobEventNamed(t *testing.T, jobs, job, name string, event map[string]any) {
	t.Helper()
	dir := filepath.Join(jobs, job, "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
		t.Fatal(err)
	}
}
