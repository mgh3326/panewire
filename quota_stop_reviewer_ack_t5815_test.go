package panewire

import (
	"context"
	"testing"
	"time"
)

// An acknowledgement written exactly at the bound (created + quotaWaitAdoptBound)
// adopts the reviewer's wait on both branches of the handshake (hk 5815): the
// bound is inclusive. Changing either `<=` to `<` turns these tests red.

// The request is in the ledger and its wait_requested write landed: the main branch.
func TestQuotaReviewerWaitAckAtTheBoundAdoptsOnTheRequestedBranch(t *testing.T) {
	rig := newWaitRig(t)
	rig.decide()
	rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
	bound := time.UnixMilli(rig.decisionCreated(t)).Add(quotaWaitAdoptBound)
	rig.appendAck(t, bound, false)
	rig.manager.Tick(context.Background(), bound.Add(time.Second))
	if !rig.waitAdopted(t) {
		t.Fatal("an acknowledgement exactly at the bound did not adopt the wait")
	}
	rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
	if len(rig.announce) != 0 {
		t.Fatalf("handed over after an acknowledgement at the bound: announced=%d", len(rig.announce))
	}
}

// The daemon exited before wait_requested was written: the lost-request branch.
func TestQuotaReviewerWaitAckAtTheBoundAdoptsAfterALostRequest(t *testing.T) {
	rig := newWaitRig(t)
	rig.decide()
	rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
	bound := time.UnixMilli(rig.decisionCreated(t)).Add(quotaWaitAdoptBound)
	if _, err := rig.store.db.Exec(`UPDATE quota_stop_decisions SET wait_requested=0`); err != nil {
		t.Fatal(err)
	}
	rig.appendAck(t, bound, false)
	rig.manager.Tick(context.Background(), bound.Add(time.Second))
	if !rig.waitAdopted(t) {
		t.Fatal("an acknowledgement exactly at the bound did not adopt the lost request's wait")
	}
	rig.wantDecision(t, "wait", "reset_within_reviewer_wait")
	if len(rig.announce) != 0 {
		t.Fatalf("handed over after an acknowledgement at the bound: announced=%d", len(rig.announce))
	}
}
