package panewire

// #1001: three one-line fixes the #989 tester verified. CE1 and CE2 are the
// tester's counterexamples adopted as regression tests; the scan test pins
// SHOULD-4 (an id-less ack for a non-lane-event job must never run the
// full-history transport-id lookup inside the socket read loop).

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
)

// t1001TwoLanes puts two lanes on one machine: the director-1 / b953 shape
// (mac-personal hosts both).
func t1001TwoLanes(paneA, paneB string) string {
	encoded, _ := json.Marshal(map[string]any{"lanes": map[string]reportRelayRoute{
		t989Lane:     {Machine: "host-a", Pane: paneA},
		"lane-other": {Machine: "host-a", Pane: paneB},
	}})
	return string(encoded)
}

// CE1: a live window may only be closed by an ack that names the window's
// own event. The late path already enforces the job binding; the live window
// used pane equality as its only row binding and #989 removed it, so a
// same-machine ack whose id names row R but whose job names another lane's
// event (on another pane) would close R without the job check.
func TestT1001JobMismatchedAckSparesLiveWindow(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	if err := os.WriteFile(rig.lanePath, []byte(t1001TwoLanes("w1:pA", "w1:pB")), 0600); err != nil {
		t.Fatal(err)
	}
	rig.sendLaneEvent("t1001-ce1", "only for lane-t989")
	directive := rig.directive()
	other := laneEventTransportID("lane-other", "t1001-ce1-other")
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: other, Pane: "w1:pB", OriginalEventID: directive.EventID})
	if row := rig.row(directive.EventID); row.DeliveredAt != "" {
		t.Fatalf("CE1: an ack naming another lane's job closed the live window's row: delivered_to=%q", row.DeliveredTo)
	}
}

// CE2: a hold accepted on a moved pane must still be droppable by the node
// that holds it; otherwise the window stays held=true, never expires and
// never spends its attempt.
func TestT1001MovedHoldDropRetiresWindow(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.sendLaneEvent("t1001-ce2", "held on a moved pane")
	directive := rig.directive()
	key := relayPendingKey(directive.EventID, directive.JobID)
	if !rig.hub.rememberRelayHeld("host-a", relayHeldPayload{EventID: directive.EventID, JobID: directive.JobID, Pane: "w1:pB", Lane: t989Lane}) {
		t.Fatal("hold on the moved pane was refused")
	}
	rig.hub.mu.Lock()
	held := rig.hub.r19a.relayPending[key].held
	rig.hub.mu.Unlock()
	if !held {
		t.Fatal("the window was not marked held")
	}
	dropped := rig.hub.consumeRelayDropped("host-a", relayDroppedPayload{JobID: directive.JobID, Pane: "w1:pB", Lane: t989Lane, OriginalEventID: directive.EventID, Reason: "ttl"})
	rig.hub.mu.Lock()
	_, stillPending := rig.hub.r19a.relayPending[key]
	rig.hub.mu.Unlock()
	if !dropped || stillPending {
		t.Fatalf("CE2: the holding node's own drop was refused (dropped=%v) and the held window survives (pending=%v)", dropped, stillPending)
	}
}

// SHOULD-4: an id-less ack resolves its row through the lane.event transport
// id the job_id echoes. A job_id without that prefix can never match, so the
// synchronous full-history scan must not run for it — one WARN, zero GETs.
func TestT1001IdlessNonLaneJobSkipsHistoryScan(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.sendLaneEvent("t1001-scan", "history exists to be scanned")
	rig.directive()
	before := rig.fake.count(http.MethodGet, "/v1/relay/events")
	warns := func() int { return strings.Count(rig.logs.String(), "WARN") }
	warnsBefore := warns()
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: "job-t1001-legacy", Pane: "w1:pA"})
	if got := rig.fake.count(http.MethodGet, "/v1/relay/events") - before; got != 0 {
		t.Fatalf("id-less ack for a non-lane-event job ran %d relay-history reads, want 0", got)
	}
	if got := warns() - warnsBefore; got != 1 {
		t.Fatalf("id-less ack for a non-lane-event job left %d WARNs, want 1: %s", got, rig.logs.String())
	}
}

// An id-less ack binds only lane.event rows; a job.* row that shares the
// ack's job_id is never closed by it (the #989 tester's I5).
func TestT1001IdlessAckNeverClosesJobRows(t *testing.T) {
	rig := t989Start(t, "w1:pA")
	rig.fake.seedUndelivered(handoffkeepRelayEvent{ID: 900, Kind: "job.completed", JobID: "job-t1001", Epoch: 1, OwnerLane: t989Lane, Machine: "host-a", PaneID: "w1:pA", EventID: "ev-1"})
	t650Send(t, rig.hub, "host-a", rig.dest, "relay.delivered", relayAckPayload{JobID: "job-t1001", Pane: "w1:pA"})
	if row := rig.row(900); row.DeliveredAt != "" {
		t.Fatalf("an id-less ack closed a job.completed row: delivered_to=%q", row.DeliveredTo)
	}
}
