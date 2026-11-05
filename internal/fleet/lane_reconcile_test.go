package fleet

import (
	"context"
	"strings"
	"testing"
)

func TestLaneReconcileAfterLandingFailure(t *testing.T) {
	a, s, f, _, _ := laneRegisterFixture(t, Result{Code: 6, Err: "refused"}, Result{Code: 6, Err: "refused"}, Result{})
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if spawnedReceipt(t, a, s.Job)["lane_unregistered"] != true {
		t.Fatal("landing did not fail registration")
	}
	if err := a.reconcileNodeLane(context.Background(), s.Job, a.machine()); err != nil {
		t.Fatal(err)
	}
	if got := laneAddCalls(f); got != 3 {
		t.Fatalf("registration attempts = %d", got)
	}
	events, err := a.events(s.Job)
	if err != nil {
		t.Fatal(err)
	}
	if events[len(events)-1].Kind != "job.lane_registered" {
		t.Fatalf("last event = %s", events[len(events)-1].Kind)
	}
}

func TestLaneReconcileRecordsFailureReason(t *testing.T) {
	a, s, _, _, _ := laneRegisterFixture(t, Result{Code: 6, Err: "refused token=secret"})
	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if err := a.reconcileNodeLane(context.Background(), s.Job, a.machine()); err == nil {
		t.Fatal("expected refusal")
	}
	events, err := a.events(s.Job)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Kind != "job.lane_registration_failed" || !strings.Contains(str(last.Payload, "lane_error"), "<redacted>") || strings.Contains(str(last.Payload, "lane_error"), "token=secret") {
		t.Fatalf("failure event = %+v", last)
	}
}
