package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

// hk 3700 (hk 3685 ticket A): a job dir without events/ is no evidence of a
// lease, and job.mark_lost never reads an absent ledger as "not completed".
// Every fixture lives under t.TempDir().

// Idle shutdown: a job dir with no events/ contributes no lease, so the node
// can idle out.
// Idle shutdown: an events/ that exists but cannot be read keeps the lease
// (the conservative answer). Root reads a mode-000 directory, so the case is
// skipped there; the reason is the environment, not the code.
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
