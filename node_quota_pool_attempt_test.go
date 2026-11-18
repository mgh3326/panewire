package panewire

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

// quotaPoolAttemptFixture claims one job with a quota record, writes its ledger
// events (kind, pane) in order, and returns a node client over a live unnamed
// row on pane-x. The pass runs at +20m.
func quotaPoolAttemptFixture(t *testing.T, kinds ...string) (*HubClient, *arbiter.Store) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	if _, err := store.Claim(ctx, arbiter.Claim{Job: "attempt-job", Lane: "owner", T: "T2", AgentLabel: "label-attempt-job"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: "attempt-job", Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
		t.Fatal(err)
	}
	for i, kind := range kinds {
		payload := map[string]any{}
		if kind == "job.spawned" {
			payload = map[string]any{"herdr_session": "custom", "pane_id": "pane-x"}
		}
		writeQuotaPoolJobEventNamed(t, jobs, "attempt-job", fmt.Sprintf("%04d-%s.json", i+3, kind), map[string]any{"kind": kind, "payload": payload})
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
	return client, store
}

// A job that was lost (quota stop) and then reprompted resumes on its pane. Its
// unnamed row on that pane is its own, so the job must stay held (PR #701 review).
func TestQuotaPoolReconcileRepromptedJobKeepsItsPaneAfterLoss(t *testing.T) {
	client, store := quotaPoolAttemptFixture(t, "job.spawned", "job.lost", "job.reprompted")
	client.reconcileNodeQuotaPools(context.Background())
	if pools, err := store.QuotaPools(context.Background(), "attempt-job"); err != nil || len(pools) != 1 {
		t.Fatalf("attempt-job pools=%v err=%v, want retained: a reprompt reopens its attempt on the same pane", pools, err)
	}
}

// job.joined leaves the job owning its pane (docs/pane-sweep.md): an unnamed row
// on that live pane still holds the job.
func TestQuotaPoolReconcileJoinedJobKeepsItsPane(t *testing.T) {
	client, store := quotaPoolAttemptFixture(t, "job.spawned", "job.joined")
	client.reconcileNodeQuotaPools(context.Background())
	if pools, err := store.QuotaPools(context.Background(), "attempt-job"); err != nil || len(pools) != 1 {
		t.Fatalf("attempt-job pools=%v err=%v, want retained: a joined job still owns its live pane", pools, err)
	}
}
