package panewire

import (
	"io"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestJobOwnerFencePendingTransferCannotReadmitOldGrant_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	if err := hub.saveOwnerTransfer(hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	delete(hub.jobs, "fenced-job")
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: unavailableOwnerStore{hk}, Out: out, Err: errout}, nil
	}
	hub.observeActiveJobs("machine-a", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}}, hub.now())
	if job := hub.jobs["fenced-job"]; job != nil {
		t.Fatalf("pending committed transfer admitted predecessor during HK outage: %+v", job)
	}
}

func TestJobOwnerFenceHeldGrantAdmittedDuringHKOutage_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, _ := ownerFenceHub(t, hk)
	delete(hub.jobs, "fenced-job")
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: unavailableOwnerStore{hk}, Out: out, Err: errout}, nil
	}
	hub.observeActiveJobs("machine-a", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}}, hub.now())
	if job := hub.jobs["fenced-job"]; job == nil || job.Node != "machine-a" || job.Epoch != 1 {
		t.Fatalf("known grant unavailable during HK outage: %+v", job)
	}
}

func TestJobOwnerFenceTerminalOwnerNotReadmitted_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "completed", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-b", Epoch: 2}, false); err != nil {
		t.Fatal(err)
	}
	delete(hub.jobs, "fenced-job")
	hub.observeActiveJobs("machine-b", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-b", Epoch: 2}}, hub.now())
	if job := hub.jobs["fenced-job"]; job != nil {
		t.Fatalf("terminal HK owner was readmitted as active: %+v", job)
	}
}

func TestJobOwnerFenceTerminalCleanupCannotReadmitPredecessor_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "completed", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}
	if err := hub.saveOwnerTransfer(intent); err != nil {
		t.Fatal(err)
	}
	hub.recoverOwnerTransfer(t.Context(), intent)
	if exists, err := hub.hasOwnerTransfer(intent); err != nil || exists {
		t.Fatalf("terminal intent not retired: exists=%v err=%v", exists, err)
	}
	delete(hub.jobs, "fenced-job")
	hub.nodes["machine-a"] = &hubNodeRecord{}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: unavailableOwnerStore{hk}, Out: out, Err: errout}, nil
	}
	hub.observeActiveJobs("machine-a", []HubActiveJob{{JobID: "fenced-job", AgentLabel: "worker-a", Epoch: 1}}, hub.now())
	if job := hub.jobs["fenced-job"]; job != nil {
		t.Fatalf("terminal cleanup readmitted predecessor during HK outage: %+v", job)
	}
}
