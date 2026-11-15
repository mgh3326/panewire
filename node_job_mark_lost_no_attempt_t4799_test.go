package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

// claimJournalSpawn records an executing journal row for a spawn of job under
// label, the row a node writes when it starts one.
func claimJournalSpawn(t *testing.T, client *HubClient, requestID, job, label string) {
	t.Helper()
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	defer client.releaseSpawnJournal()
	claimed, _, err := journal.claim(t.Context(), hubOutboundMessage{RequestID: requestID, Args: []string{"spawn", "--job", job, "-l", label}})
	if err != nil || !claimed {
		t.Fatalf("journal claim=%v err=%v", claimed, err)
	}
}

// hk 4799: with the job's ledger gone, the node answers the hub's mark-lost
// with the proof it needs — no spawn journal row and no active job name the
// job — and still writes nothing to hk.
func TestNodeJobMarkLostProvesNoLiveAttempt(t *testing.T) {
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
		t.Fatal(err)
	}
	client, store, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostNoLiveAttempt {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 || store.state("job-a") != "spawned" {
		t.Fatalf("hk state=%v writes=%v", store.state("job-a"), store.writes())
	}
	if len(fake.calls) != 0 {
		t.Fatalf("herdr asked for a missing ledger: %v", fake.calls)
	}
}

// A spawn this node started for the same job under another label is an attempt
// that may still be live, so the ledger-less answer stays a refusal.
func TestNodeJobMarkLostHoldsWhenANewerSpawnIsExecuting(t *testing.T) {
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
		t.Fatal(err)
	}
	client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	claimJournalSpawn(t, client, "d676220b-e763-4af1-aefd-000000000003", "job-a", "job-a-r2")
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostLedgerMissing {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 || store.state("job-a") != "spawned" {
		t.Fatalf("hk state=%v writes=%v", store.state("job-a"), store.writes())
	}
}

// Either record naming the job, under any label, is a live attempt.
func TestJobAttemptLiveReadsBothRecords(t *testing.T) {
	executing := []executingSpawn{{RequestID: "request-r2", Job: "job-a", Label: "job-a-r2"}}
	active := []HubActiveJob{{JobID: "job-a", AgentLabel: "job-a-r2"}}
	for _, tc := range []struct {
		name  string
		rows  []executingSpawn
		jobs  []HubActiveJob
		alive bool
	}{
		{"neither record names the job", nil, []HubActiveJob{{JobID: "job-b"}}, false},
		{"an executing spawn under another label", executing, nil, true},
		{"an active job under another label", nil, active, true},
		{"both records name the job", executing, active, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := jobAttemptLive("job-a", tc.rows, tc.jobs); got != tc.alive {
				t.Fatalf("jobAttemptLive=%v, want %v", got, tc.alive)
			}
		})
	}
}

// A journal the node cannot open is no proof: the answer stays the bare
// ledger_missing, which keeps the caller's hold.
func TestNodeJobMarkLostHoldsWhenTheProofIsUnreadable(t *testing.T) {
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
		t.Fatal(err)
	}
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, nil, 0600); err != nil {
		t.Fatal(err)
	}
	client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	client.spawnJournalDir = filepath.Join(blocker, "spawn")
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostLedgerMissing {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 || store.state("job-a") != "spawned" {
		t.Fatalf("hk state=%v writes=%v", store.state("job-a"), store.writes())
	}
}

// scanActiveJobsForLimit keeps a jobs read error, which the heartbeat's list
// reads as "nothing active": the proof must never take that as an empty list.
func TestScanActiveJobsForLimitKeepsTheReadError(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "jobs"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if active, err := scanActiveJobsForLimit(t.Context(), root, nil, 0); err == nil {
		t.Fatalf("active=%v, want the jobs read error", active)
	}
	if active := scanHubActiveJobsForLimit(t.Context(), root, nil, 0); active != nil {
		t.Fatalf("heartbeat scan=%v, want nil", active)
	}
}
