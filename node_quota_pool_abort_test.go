package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/fleet"
)

// An aborted pass did not read the sessions after the one that failed, so their
// empty-read windows cannot show 10 minutes of continuous emptiness. The window
// of an unread session must end with the pass (PR #701 review).
func TestQuotaPoolReconcileAbortedPassEndsEmptyWindowOfUnreadSession(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	base := time.Date(2026, 11, 13, 0, 0, 0, 0, time.UTC)
	now := base
	store := &arbiter.Store{DB: arbiter.StatePath(filepath.Join(root, "data")), Inbox: jobs, Now: func() time.Time { return now }}
	for _, job := range []string{"a-job", "z-job"} {
		if _, err := store.Claim(ctx, arbiter.Claim{Job: job, Lane: "owner", T: "T2", AgentLabel: "label-" + job}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordQuotaPool(ctx, arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: "example"}}); err != nil {
			t.Fatal(err)
		}
	}
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "a-job", "a")
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "z-job", "z")
	now = base.Add(20 * time.Minute)
	client, err := NewHubClient(HubClientConfig{URL: "ws://127.0.0.1", MachineID: "node-a", Token: "fixture", JobsInboxRoot: root, AllowInsecureForTests: true, QuotaPoolReconcile: true})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return now }
	client.quotaReconcile.app = func() (*fleet.App, error) {
		return &fleet.App{Arbiter: store}, nil
	}
	failA := false
	client.quotaReconcile.agents = func(_ context.Context, session string) ([]nodeHerdrRow, error) {
		if session == "a" && failA {
			return nil, errors.New("herdr unavailable")
		}
		return []nodeHerdrRow{}, nil
	}

	// +20m: both sessions read empty; z's window starts here.
	client.reconcileNodeQuotaPools(ctx)
	// +25m: a fails, so the pass aborts before z is read.
	now = base.Add(25 * time.Minute)
	failA = true
	client.reconcileNodeQuotaPools(ctx)
	// +30m: both read empty again. z's first read after the abort starts its own window.
	now = base.Add(30 * time.Minute)
	failA = false
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, "z-job"); err != nil || len(pools) != 1 {
		t.Fatalf("z-job pools=%v err=%v, want retained: z was not read at +25m, so its window restarts at +30m", pools, err)
	}
}

// A pass that aborts before any session is read (here, a held job's ledger is
// gone) must end the window of the session it would have read. A job that joins
// that session later then gets its own 10-minute confirmation (PR #701 review).
func TestQuotaPoolReconcileAbortBeforeSessionReadsEndsEmptyWindow(t *testing.T) {
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

	// +20m: solo reads empty while holding solo-job; its window starts.
	client.reconcileNodeQuotaPools(ctx)

	// +25m: late-job's ledger is gone, so the pass aborts before any session read.
	// The session cache answers for 10 wall-clock seconds, so clear it first.
	if err := os.RemoveAll(filepath.Join(jobs, "late-job")); err != nil {
		t.Fatal(err)
	}
	client.sessionCacheMu.Lock()
	client.jobSessions = nil
	client.sessionCacheMu.Unlock()
	now = base.Add(25 * time.Minute)
	client.reconcileNodeQuotaPools(ctx)

	// +31m: late-job's ledger is back, bound to solo. Its claim is 31 minutes old,
	// so only its own confirmation can hold it. solo's window from +20m is not its.
	writeQuotaPoolReconcileSpawnedEvent(t, jobs, "late-job", "solo")
	client.sessionCacheMu.Lock()
	client.jobSessions = nil
	client.sessionCacheMu.Unlock()
	now = base.Add(31 * time.Minute)
	client.reconcileNodeQuotaPools(ctx)

	if pools, err := store.QuotaPools(ctx, "late-job"); err != nil || len(pools) != 1 {
		t.Fatalf("late-job pools=%v err=%v, want retained: the aborted +25m pass must end solo's window", pools, err)
	}
}
