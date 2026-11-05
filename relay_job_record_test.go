package panewire

import "testing"

func TestLaneEventJobRecordFallback(t *testing.T) {
	f := newR19eFixture(t)
	event := hubJobEventPayload{OwnerLane: "missing", JobID: "producer", EventID: "event"}
	job := &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job", AgentLabel: "missing", Pane: "w1:p9"}, Node: r19eLaneNode}
	if _, _, ok := f.hub.resolveRelayRoute("lane.event", event); ok {
		t.Fatal("unexpected route without job")
	}
	f.hub.jobs["job"] = job
	route, agent, ok := f.hub.resolveRelayRoute("lane.event", event)
	if !ok || agent == nil || route.Pane != "w1:p9" {
		t.Fatalf("unique job route: route=%+v routed=%v", route, ok)
	}
	if !f.hub.injectLaneRelayEvent(event, route, agent, 0, "test") {
		t.Fatal("fallback injection was not queued")
	}
	select {
	case delivery := <-f.relays:
		if delivery.Pane != "w1:p9" {
			t.Fatalf("delivery pane = %q", delivery.Pane)
		}
	default:
		t.Fatal("fallback delivery did not reach node")
	}
	f.hub.jobs["other"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "other", AgentLabel: "missing", Pane: "w1:p8"}, Node: r19eLaneNode}
	if _, _, ok := f.hub.resolveRelayRoute("lane.event", event); ok {
		t.Fatal("ambiguous jobs routed")
	}
	f.hub.jobs["other"].Completed = true
	job.Completed = true
	if _, _, ok := f.hub.resolveRelayRoute("lane.event", event); ok {
		t.Fatal("terminal jobs routed")
	}
}
