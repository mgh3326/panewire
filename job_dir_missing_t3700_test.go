package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// hk 3700 (hk 3685 ticket A): a job dir without events/ is no evidence of a
// lease, and job.mark_lost never reads an absent ledger as "not completed".
// Every fixture lives under t.TempDir().

// Idle shutdown: a job dir with no events/ contributes no lease, so the node
// can idle out.
func TestIdleShutdownJobDirWithoutEventsHoldsNoLease(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "jobs", "job-noevents"), 0700); err != nil {
		t.Fatal(err)
	}
	leases, err := idleShutdownJobLeases(root, nil, context.Background())
	if err != nil || len(leases) != 0 {
		t.Fatalf("job dir without events/: leases=%v err=%v", leases, err)
	}
}

// Idle shutdown: an events/ that exists but cannot be read keeps the lease
// (the conservative answer). Root reads a mode-000 directory, so the case is
// skipped there; the reason is the environment, not the code.
func TestIdleShutdownJobDirUnreadableEventsStillHoldsLease(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a mode-000 events/ directory; the unreadable case cannot arise")
	}
	root := t.TempDir()
	sealed := filepath.Join(root, "jobs", "job-sealed", "events")
	if err := os.MkdirAll(sealed, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sealed, 0000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, 0700) })
	leases, err := idleShutdownJobLeases(root, nil, context.Background())
	if err != nil || len(leases) != 1 || leases[0].JobID != "job-sealed" {
		t.Fatalf("unreadable events/: leases=%v err=%v", leases, err)
	}
}

// job.mark_lost: when the job dir or its events/ is missing the ledger is
// unknown, so the command refuses with ledger_missing and writes nothing,
// even though herdr lists no pane for the job.
func TestNodeJobMarkLostRefusesWithoutLedger(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(inbox string)
	}{
		{"events missing", func(inbox string) {
			if err := os.MkdirAll(filepath.Join(inbox, "jobs", "job-a"), 0700); err != nil {
				t.Fatal(err)
			}
		}},
		{"job dir missing", func(inbox string) {
			if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inbox := t.TempDir()
			tt.setup(inbox)
			client, store, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a"})
			if code != "" || result["outcome"] != nodeJobMarkLostLedgerMissing {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if writes := store.writes(); len(writes) != 0 {
				t.Fatalf("hk writes=%v; a ledger-unknown job must not be marked lost", writes)
			}
			if store.state("job-a") != "spawned" {
				t.Fatalf("hk state = %v, want it unchanged at spawned", store.state("job-a"))
			}
			if fake.called("agent", "list") != 0 || fake.called("pane", "list") != 0 {
				t.Fatalf("herdr was asked before the ledger was read: calls=%v", fake.calls)
			}
		})
	}
}
