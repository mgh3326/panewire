package panewire

// hk 3425: a worker's completion is a record, not a design-lane notice. The
// hub keeps the job record, the report and the job events, but a design lane
// is not injected or filed for job.completed, or for a ready/merged lane event.

import (
	"bytes"
	"strings"
	"testing"
)

const designCompletionLanes = `{"lanes":{
 "panewire-design":{"machine":"host-a","pane":"workspace:design"},
 "worker-lane":{"machine":"host-a","pane":"workspace:pane-1","parent":"panewire-design"},
 "operator-desk":{"machine":"host-a","pane":"workspace:desk"}
}}`

func designCompletionHub(t *testing.T, logs *bytes.Buffer) (*HubServer, *fakeHandoffkeep, *hubAgent) {
	t.Helper()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h := quietDesignHub(t, hk, designCompletionLanes, logs)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	h.mu.Lock()
	h.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", state: "connected", agent: agent}
	h.mu.Unlock()
	return h, fake, agent
}

func designCompletionRetiredTo(fake *fakeHandoffkeep, want string) bool {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, to := range fake.deliveredTo {
		if to == want {
			return true
		}
	}
	return false
}

// (a) owner panewire-design, worker done: nothing is injected or filed for the
// design lane, the producer's outbox is acknowledged, and the durable row is
// retired instead of replayed.
func TestQuietDesignLaneWorkerCompletionNotDelivered(t *testing.T) {
	var logs bytes.Buffer
	h, fake, agent := designCompletionHub(t, &logs)

	h.relayJobEventFrom("host-a", "job.completed", hubJobEventPayload{
		JobID: "worker-job-1", Epoch: 1, OwnerLane: "panewire-design", Label: "worker-1", Host: "host-a",
		ReportPath: "report.md", ReportLastLine: "done", EventID: "ev-completed",
	})

	select {
	case d := <-agent.relays:
		t.Fatalf("design lane received %+v", d)
	default:
	}
	if items, err := h.inbox.store.list("panewire-design", false); err != nil || len(items) != 0 {
		t.Fatalf("design lane inbox items=%+v err=%v", items, err)
	}
	select {
	case <-agent.persisted:
	default:
		t.Fatal("the producer was not acknowledged")
	}
	if !designCompletionRetiredTo(fake, "hub/design-lane") {
		t.Fatalf("completion row was not retired: %v", fake.deliveredTo)
	}
	if got := strings.Count(logs.String(), "completion notice suppressed for design lane"); got != 1 {
		t.Fatalf("suppression DEBUG lines=%d: %s", got, logs.String())
	}
}

// (b) a ready or merged lane event reaches no design lane, while the same ready
// event still reaches the operator desk.
func TestQuietDesignLaneReadyAndMergedNotDelivered(t *testing.T) {
	var logs bytes.Buffer
	h, _, agent := designCompletionHub(t, &logs)

	event := func(lane, id, notifyType string) hubJobEventPayload {
		return hubJobEventPayload{JobID: laneEventTransportID(lane, id), Epoch: 1, OwnerLane: lane, EventID: id, NotifyType: notifyType, Text: "worker finished"}
	}
	for _, notifyType := range []string{"ready", "merged"} {
		if r := h.relayLaneEvent(event("panewire-design", "design-"+notifyType, notifyType), agent); r.ID == 0 {
			t.Fatalf("%s relay=%+v", notifyType, r)
		}
		select {
		case d := <-agent.relays:
			t.Fatalf("design lane received the %s event: %+v", notifyType, d)
		default:
		}
	}
	if items, err := h.inbox.store.list("panewire-design", false); err != nil || len(items) != 0 {
		t.Fatalf("design lane inbox items=%+v err=%v", items, err)
	}
	if got := strings.Count(logs.String(), "completion notice suppressed for design lane"); got != 2 {
		t.Fatalf("suppression DEBUG lines=%d: %s", got, logs.String())
	}

	h.relayLaneEvent(event("operator-desk", "desk-ready", "ready"), agent)
	select {
	case d := <-agent.relays:
		if d.Pane != "workspace:desk" {
			t.Fatalf("desk directive=%+v", d)
		}
	default:
		t.Fatal("the operator desk did not receive the ready event")
	}
}

// (c) decisions and failures still reach a design lane; only the completion
// notices are held back.
func TestQuietDesignLaneCompletionSuppressionKeepsDecisionsAndFailures(t *testing.T) {
	h, _, agent := designCompletionHub(t, &bytes.Buffer{})
	for _, event := range []struct {
		id, notifyType, text string
	}{
		{"spawn-failed", "automation-failed", "[panewire] SPAWN_FAILED job-1 request=r1 host=none profile=none rc=70 stage=launch replay=no reason=spawn_failed fix=Check the checkout."},
		{"blocked", "blocked", "BLOCKED review needs an operator decision"},
		{"decision", "decision", "ESC which release do we ship?"},
	} {
		e := hubJobEventPayload{JobID: laneEventTransportID("panewire-design", event.id), Epoch: 1, OwnerLane: "panewire-design", EventID: event.id, NotifyType: event.notifyType, Text: event.text}
		if r := h.relayLaneEvent(e, agent); r.ID == 0 || r.PersistFailed {
			t.Fatalf("%s relay=%+v", event.id, r)
		}
	}
	if got := len(agent.relays); got != 3 {
		t.Fatalf("design lane received %d interrupts, want 3", got)
	}
}

// A completion row stored before this change is retired on replay, not injected
// into the design lane.
func TestQuietDesignLaneOldCompletionRecordIsRetiredOnReplay(t *testing.T) {
	h, fake, agent := designCompletionHub(t, &bytes.Buffer{})
	record := handoffkeepRelayEvent{
		ID: 900, Kind: "job.completed", JobID: "worker-job-old", Epoch: 1,
		OwnerLane: "panewire-design", ReportPath: "report.md", EventID: "ev-old",
	}
	fake.seedUndelivered(record)
	if !h.replayInbox(record) {
		t.Fatal("the completion record was not settled by the inbox")
	}
	select {
	case d := <-agent.relays:
		t.Fatalf("design lane received the replayed completion: %+v", d)
	default:
	}
	if !designCompletionRetiredTo(fake, "hub/design-lane") {
		t.Fatalf("replayed completion row was not retired: %v", fake.deliveredTo)
	}
}
