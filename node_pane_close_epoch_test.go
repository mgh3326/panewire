package panewire

import (
	"os"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestCloseOpRequiresCurrentOwnershipEpoch(t *testing.T) {
	client := closeOpNode(t, t.TempDir(), idlePaneHerdr())
	client.assignedJobs = map[string]uint64{closeOpJob: 3}
	args := closeOpArgs("epoch-op")
	args["epoch"] = 2
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "epoch_mismatch" {
		t.Fatalf("stale epoch: code=%q", code)
	}
	args["epoch"] = 3
	if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "" || result["outcome"] != fleet.CloseOpClosed {
		t.Fatalf("current epoch: result=%v code=%q", result, code)
	}
}

func TestCloseOpRevokedEpochSurvivesNodeRestart(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	addCloseOpEvent(t, inbox, "00003-job.revoked.json", `{"type":"job.revoked","job_id":"job-1","epoch":2}`)
	// The assignment map starts empty after restart, but the durable
	// revocation still rejects the old owner epoch.
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("stale-op")); code != "epoch_mismatch" {
		t.Fatalf("revoked epoch accepted after restart: %q", code)
	}
}

func TestCloseOpReplayKeepsItsRegisteredEpoch(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	seedNodeCloseOp(t, inbox, "old-op", fleet.CloseOpRegistered)
	client.assignedJobs = map[string]uint64{closeOpJob: 3}
	args := closeOpArgs("old-op")
	args["epoch"] = 3
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "invalid_args" {
		t.Fatalf("old request adopted a new epoch: %q", code)
	}
}

func TestCloseOpRestartRecoversRegisteredEpoch(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, goneHerdr())
	seedNodeCloseOp(t, inbox, "old-op", fleet.CloseOpClosed)
	job, err := os.OpenRoot(seedCloseOpJob(t, inbox))
	if err != nil {
		t.Fatal(err)
	}
	op, found, err := fleet.ReadCloseOp(job, "old-op")
	if err != nil || !found {
		t.Fatalf("read journal: found=%v err=%v", found, err)
	}
	op.Epoch = 3
	if err := fleet.WriteCloseOp(job, op); err != nil {
		t.Fatal(err)
	}
	job.Close()
	args := closeOpArgs("old-op")
	args["epoch"] = 3
	if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "" || result["outcome"] != fleet.CloseOpClosed || result["replayed"] != true {
		t.Fatalf("restart replay: result=%v code=%q", result, code)
	}
	client.assignedJobs = map[string]uint64{closeOpJob: 2}
	if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "" || result["outcome"] != fleet.CloseOpClosed || result["replayed"] != true {
		t.Fatalf("replay with an older assignment: result=%v code=%q", result, code)
	}
}

func TestCloseOpRestartRecoversEpochFromUnregisteredIntent(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	client.nodeJobs = &quotaJobs{records: map[string]map[string]any{closeOpJob: {"state": "spawned"}}}
	addCloseOpEvent(t, inbox, "00003-job.hub_close_intent.json", `{"kind":"job.hub_close_intent","payload":{"request_id":"intent-op","attempt":"00002-job.spawned.json","epoch":"3","pane_id":"w1:p1","label":"job-a","end_reason":"quota_stop pool=devin-free"}}`)
	args := closeOpArgs("intent-op")
	args["epoch"] = 3
	if result, code := nodeRun(t, client, nodeCommandPaneCloseOp, args); code != "" || result["outcome"] != fleet.CloseOpClosed {
		t.Fatalf("unregistered intent replay: result=%v code=%q", result, code)
	}
}

func TestCloseStatusReportsOnlyDurablySettledRequest(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, goneHerdr())
	seedNodeCloseOp(t, inbox, "settled-op", fleet.CloseOpClosed)
	status := func() map[string]any {
		t.Helper()
		result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("settled-op"))
		if code != "" {
			t.Fatalf("status: %q", code)
		}
		return result
	}
	if result := status(); result["settled"] != false {
		t.Fatalf("unsettled close: %v", result)
	}
	addCloseOpEvent(t, inbox, "00003-job.lost.json", `{"kind":"job.lost","payload":{"reason":"hub_close","final":true,"request_id":"another-op"}}`)
	if result := status(); result["settled"] != false {
		t.Fatalf("different request: %v", result)
	}
	addCloseOpEvent(t, inbox, "00004-job.lost.json", `{"kind":"job.lost","payload":{"reason":"hub_close","final":true,"request_id":"settled-op"}}`)
	if result := status(); result["settled"] != true {
		t.Fatalf("settled close: %v", result)
	}
}

func TestCloseStatusSettlesOlderUnknownWhenRetrySettles(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, goneHerdr())
	seedNodeCloseOp(t, inbox, "old-op", fleet.CloseOpUnknown)
	addCloseOpEvent(t, inbox, "00003-job.hub_close_intent.json", `{"kind":"job.hub_close_intent","payload":{"request_id":"old-op","attempt":"00002-job.spawned.json","pane_id":"w1:p1","label":"job-a"}}`)
	addCloseOpEvent(t, inbox, "00004-job.hub_close_intent.json", `{"kind":"job.hub_close_intent","payload":{"request_id":"retry-op","attempt":"00002-job.spawned.json","pane_id":"w1:p1","label":"job-a"}}`)
	addCloseOpEvent(t, inbox, "00005-job.lost.json", `{"kind":"job.lost","payload":{"reason":"hub_close","final":true,"request_id":"retry-op"}}`)
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, closeOpRef("old-op"))
	if code != "" || result["settled"] != true {
		t.Fatalf("older request not settled: result=%v code=%q", result, code)
	}
}
