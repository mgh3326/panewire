package panewire

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

// A session that stops holding jobs must end its empty-read window. Otherwise a
// job that later joins that session inherits the old window, and its first empty
// read can release it without 10 minutes of its own confirmation (PR #701
// review, mergewake medium).
func TestQuotaPoolReconcileEmptyWindowEndsWhenSessionStopsHoldingJobs(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"solo-job", "late-job"} {
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: "label-" + job}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "solo-job", "solo")
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "late-job", "other")
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
		if session == "other" {
			return []nodeHerdrRow{{nodeAgentRow: nodeAgentRow{Name: "label-late-job", AgentStatus: "working"}}}, nil
		}
		return []nodeHerdrRow{}, nil
	}

	// At +20m solo lists nothing while holding solo-job: its window starts.
	client.reconcileNodeQuotaPools(ctx)

	// solo-job ends: a forced quota release leaves it released with nothing held.
	if err := store.ReleaseQuotaPool(ctx, "solo-job", "example", true); err != nil {
		t.Fatal(err)
	}
	// At +25m nothing holds solo, so the pass does not read it.
	now = base.Add(25 * time.Minute)
	client.reconcileNodeQuotaPools(ctx)

	// late-job is bound to solo at +31m. Its claim is 31 minutes old, so only the
	// confirmation can hold it; solo's window started at +20m, which is not its own.
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "late-job", "solo")
	client.sessionCacheMu.Lock()
	client.jobSessions = nil
	client.sessionCacheMu.Unlock()
	now = base.Add(31 * time.Minute)
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, "late-job"); err != nil || len(pools) != 1 {
		t.Fatalf("late-job pools=%v err=%v, want retained: its first empty read starts its own 10-minute window", pools, err)
	}
}
