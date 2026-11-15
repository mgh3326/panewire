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
	claimJournalSpawnArgs(t, client, requestID, []string{"--job", job, "-l", label})
}

// claimJournalSpawnArgs is claimJournalSpawn for a request's own argv.
func claimJournalSpawnArgs(t *testing.T, client *HubClient, requestID string, args []string) {
	t.Helper()
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	defer client.releaseSpawnJournal()
	claimed, _, err := journal.claim(t.Context(), hubOutboundMessage{RequestID: requestID, Args: args})
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

// The operator --purpose is free text and sent before --job and -l, so the
// journal's identity columns come from the strict flag/value pairs: a purpose
// beginning with --job= or -l= cannot rename the row (finding r4578127795).
func TestNodeSpawnJournalRecordsStrictSpawnIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"the purpose forges the job", []string{"--purpose", "--job=job-b", "--job", "job-a", "-l", "job-a"}},
		{"the purpose forges the label", []string{"--purpose", "-l=job-b", "--job", "job-a", "-l", "job-a"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _, _ := markLostClient(t, t.TempDir(), nil, "")
			claimJournalSpawnArgs(t, client, "d676220b-e763-4af1-aefd-000000000004", tc.args)
			journal, err := client.acquireSpawnJournal()
			if err != nil {
				t.Fatal(err)
			}
			defer client.releaseSpawnJournal()
			rows, err := journal.executing(t.Context())
			if err != nil || len(rows) != 1 {
				t.Fatalf("rows=%+v err=%v", rows, err)
			}
			if rows[0].Job != "job-a" || rows[0].Label != "job-a" {
				t.Fatalf("row=%+v, want job-a/job-a", rows[0])
			}
		})
	}
}

// A spawn running for job-a is a live attempt even when the operator purpose
// begins with --job=: the journal records the real pair, so the proof keeps
// the hold instead of freeing the reservation beside a live attempt.
func TestNodeJobMarkLostHoldsWhenThePurposeForgesTheJob(t *testing.T) {
	inbox := t.TempDir()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
		t.Fatal(err)
	}
	client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	claimJournalSpawnArgs(t, client, "d676220b-e763-4af1-aefd-000000000005", []string{"--purpose", "--job=job-b", "--job", "job-a", "-l", "job-a"})
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostLedgerMissing {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 || store.state("job-a") != "spawned" {
		t.Fatalf("hk state=%v writes=%v", store.state("job-a"), store.writes())
	}
}

// A journal row an older build wrote keeps the lax identity, so the proof
// cannot trust its job id: it holds for any job while that row is still
// executing, and a strict row for another job never holds.
func TestNodeJobMarkLostHoldsWhileALegacyRowExecutes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, client *HubClient)
		want  string
	}{
		{"a strict row for another job does not hold", func(t *testing.T, client *HubClient) {
			claimJournalSpawnArgs(t, client, "d676220b-e763-4af1-aefd-000000000006", []string{"--job", "job-b", "-l", "job-b"})
		}, nodeJobMarkLostNoLiveAttempt},
		{"a row from an older build holds", func(t *testing.T, client *HubClient) {
			journal, err := client.acquireSpawnJournal()
			if err != nil {
				t.Fatal(err)
			}
			defer client.releaseSpawnJournal()
			if _, err := journal.db.ExecContext(t.Context(), `INSERT INTO spawn_inbox(request_id,fingerprint,status,job_id,label) VALUES('legacy-request','fingerprint','executing','job-b','job-b')`); err != nil {
				t.Fatal(err)
			}
		}, nodeJobMarkLostLedgerMissing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inbox := t.TempDir()
			if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0700); err != nil {
				t.Fatal(err)
			}
			client, store, _ := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
			tc.write(t, client)
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", Label: "job-a"})
			if code != "" || result["outcome"] != tc.want {
				t.Fatalf("code=%q result=%v, want %q", code, result, tc.want)
			}
			if len(store.writes()) != 0 || store.state("job-a") != "spawned" {
				t.Fatalf("hk state=%v writes=%v", store.state("job-a"), store.writes())
			}
		})
	}
}

// A re-claim recorded in the job's ledger is read there, never by the
// no-attempt proof: the ledger answers newer-claim for a claim under another
// label, and the hub keeps the hold. The proof runs only for a job with no
// ledger left, where the active list's age window has no claim of this job to
// hide.
func TestNodeJobMarkLostHoldsOnANewerClaimUnderAnotherLabel(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a",
		map[string]any{"kind": "job.claim", "payload": map[string]any{"agent_label": "job-a"}},
		map[string]any{"kind": "job.reclaim", "payload": map[string]any{"agent_label": "job-a-r2"}},
	)
	client, store, fake := markLostClient(t, inbox, []string{nodeAgentList()}, markLostEmptyPanes)
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, nodeJobMarkLostArgs{Job: "job-a", Label: "job-a"})
	if code != "" || result["outcome"] != nodeJobMarkLostNewerClaim {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 {
		t.Fatalf("hk writes=%v", store.writes())
	}
	if len(fake.calls) != 0 {
		t.Fatalf("herdr asked before the ledger decided: %v", fake.calls)
	}
}

// Either record naming the job, under any label, is a live attempt.
func TestJobAttemptLiveReadsBothRecords(t *testing.T) {
	executing := []executingSpawn{{RequestID: "request-r2", Job: "job-a", Label: "job-a-r2", Strict: true}}
	active := []HubActiveJob{{JobID: "job-a", AgentLabel: "job-a-r2"}}
	for _, tc := range []struct {
		name  string
		rows  []executingSpawn
		jobs  []HubActiveJob
		alive bool
	}{
		{"neither record names the job", nil, []HubActiveJob{{JobID: "job-b"}}, false},
		{"a strict row for another job", []executingSpawn{{RequestID: "request-r3", Job: "job-b", Label: "job-b", Strict: true}}, nil, false},
		{"a row from an older build", []executingSpawn{{RequestID: "legacy", Job: "job-b", Label: "job-b"}}, nil, true},
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
