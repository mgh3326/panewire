package panewire

import (
	"io"
	"testing"
)

func TestPoolActionSupersededFreesFenceWithoutNotice(t *testing.T) {
	f, action := poolActionFixture(t)
	response := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": "superseded", "attempt": "00004-job.reprompted.json"}}
	if err := runPoolActionStep(t, f.hub, f.node, action, response); err != nil {
		t.Fatal(err)
	}
	if events := spawnFailedInjections(f.destination); len(events) != 0 {
		t.Fatalf("superseded close sent a notice: %+v", events)
	}
	assertPoolActionFence(t, f, action, true)
}

func TestPoolActionSupersededPermitsFreshAction(t *testing.T) {
	f, action := poolActionFixture(t)
	response := poolActionReply{op: nodeCommandPaneCloseStatus, result: map[string]any{"state": "superseded", "attempt": "00004-job.reprompted.json"}}
	if err := runPoolActionStep(t, f.hub, f.node, action, response); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	f.hub.jobs[f.job].Orphaned = false
	f.hub.mu.Unlock()
	app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.hub.createPoolAction(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, f.holder, app, nil); err != nil {
		t.Fatalf("new attempt remained fenced: %v", err)
	}
	if next := readOnlyPoolAction(t, f.hub); next.ID == action.ID {
		t.Fatal("new action reused the superseded request")
	}
}
