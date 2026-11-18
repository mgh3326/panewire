package panewire

import (
	"testing"
	"time"
)

func TestJobOwnerFenceCommittedGrantSurvivesJournalCleanup_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}
	if err := hub.saveOwnerTransfer(intent); err != nil {
		t.Fatal(err)
	}
	hub.recoverOwnerTransfer(t.Context(), intent)
	if exists, err := hub.hasOwnerTransfer(intent); err != nil || exists {
		t.Fatalf("committed transfer journal retained: exists=%v err=%v", exists, err)
	}
	dir := hub.ownerTransferDir
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, agent := ownerFenceHub(t, hk)
	restarted.ownerTransferDir = dir
	delete(restarted.jobs, "fenced-job")
	restarted.sendHeartbeatDirectives("machine-b", agent)
	var assignment hubJobAssignedEvent
	if !waitUntil(3*time.Second, func() bool {
		select {
		case assignment = <-agent.assignments:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("durable successor was not redelivered after restart")
	}
	if assignment.JobID != "fenced-job" || assignment.Epoch != 2 {
		t.Fatalf("redelivered assignment=%+v", assignment)
	}
	restarted.mu.Lock()
	pending := restarted.pendingRevocations["machine-a"]["fenced-job"]
	restarted.mu.Unlock()
	if pending.Epoch != 2 {
		t.Fatalf("predecessor revocation was not restored: %+v", pending)
	}
}

func TestJobOwnerFenceRecoveryRetainsAllPredecessors_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-c", "owner_epoch": float64(3)}}
	hub, _ := ownerFenceHub(t, hk)
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-b", Epoch: 2, From: "machine-a"}, false); err != nil {
		t.Fatal(err)
	}
	hub.jobs["fenced-job"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "fenced-job", Epoch: 2}, Node: "machine-b", FencedNodes: map[string]uint64{"machine-a": 2}}
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-b", To: "machine-c", Epoch: 2}
	if err := hub.saveOwnerTransfer(intent); err != nil {
		t.Fatal(err)
	}
	hub.recoverOwnerTransfer(t.Context(), intent)
	hub.mu.Lock()
	for _, predecessor := range []string{"machine-a", "machine-b"} {
		if got := hub.pendingRevocations[predecessor]["fenced-job"].Epoch; got != 3 {
			hub.mu.Unlock()
			t.Fatalf("immediate revocation for %s=%d, want 3", predecessor, got)
		}
	}
	hub.mu.Unlock()
	dir := hub.ownerTransferDir
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, agent := ownerFenceHub(t, hk)
	restarted.ownerTransferDir = dir
	delete(restarted.jobs, "fenced-job")
	restarted.sendHeartbeatDirectives("machine-c", agent)
	if !waitUntil(3*time.Second, func() bool {
		select {
		case assignment := <-agent.assignments:
			return assignment.JobID == "fenced-job" && assignment.Epoch == 3
		default:
			return false
		}
	}) {
		t.Fatal("new owner was not recovered")
	}
	restarted.mu.Lock()
	defer restarted.mu.Unlock()
	for _, predecessor := range []string{"machine-a", "machine-b"} {
		if got := restarted.pendingRevocations[predecessor]["fenced-job"].Epoch; got != 3 {
			t.Fatalf("revocation for %s after restart=%d, want 3", predecessor, got)
		}
	}
}

func TestJobOwnerFenceHeartbeatRestoresHistoryOnce_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, agent := ownerFenceHub(t, hk)
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-b", Epoch: 2, From: "machine-a", Fenced: []string{"machine-a"}}, false); err != nil {
		t.Fatal(err)
	}
	delete(hub.jobs, "fenced-job")
	hub.observeActiveJobs("machine-b", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-b", Epoch: 2}}, hub.now())
	hub.sendHeartbeatDirectives("machine-b", agent)
	eventually(t, 3*time.Second, "predecessor revocation after heartbeat", func() bool {
		hub.mu.Lock()
		defer hub.mu.Unlock()
		return hub.pendingRevocations["machine-a"]["fenced-job"].Epoch == 2
	})
	waitOwnerWork(t, hub, "machine-b", false)
	if !hub.acknowledgeRevocation("machine-a", hubJobEventPayload{JobID: "fenced-job", Epoch: 2}) {
		t.Fatal("could not acknowledge recovered revocation")
	}
	hub.sendHeartbeatDirectives("machine-b", agent)
	waitOwnerWork(t, hub, "machine-b", false)
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if pending := hub.pendingRevocations["machine-a"]["fenced-job"]; pending.Epoch != 0 {
		t.Fatalf("acknowledged revocation was queued again: %+v", pending)
	}
}
