package panewire

// hk 3434: a consult answer is announced to the lane that asked for it as a
// lane.event --type ready whose text starts with [consult-done] (rstack
// skills/ops/references/consult.md §2). A claude requester waits for that
// event in its pane (relay.md §3-1), so a design lane that asked for advice
// must still be told about it. hk 3426 quieted every ready lane.event to a
// design lane, this one included. Since hk 3602 the answer is filed and wakes
// the lane with the one-line wake rather than being typed.

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestQuietDesignConsultDoneWakes(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h, agent := t3426Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), hk, t3426Records)

	for _, tc := range []struct{ name, deliver string }{
		{"policy", ""},
		{"explicit interrupt", "interrupt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "consult-migration-" + tc.deliver
			e := hubJobEventPayload{JobID: laneEventTransportID(t3426Design, id), Epoch: 1, OwnerLane: t3426Design, EventID: id, NotifyType: "ready", DeliveryClass: tc.deliver,
				Text: "[consult-done] ship the migration first | ~/work/herdr-inbox/jobs/consult-migration/answer.md"}
			if r := h.relayLaneEvent(e, agent); r.ID == 0 || r.PersistFailed {
				t.Fatalf("relay=%+v", r)
			}
			got := t3426Injected(agent)
			if len(got) != 1 || !strings.Contains(got[0].Text, "(most urgent: ready)") {
				t.Fatalf("injections=%+v, want a ready wake", got)
			}
			t3602AssertWoken(t, h, got, t3426Design, "w1:design", "lane.event", e.JobID, "ready")
		})
	}

	// The exception is the consult answer's own marker at the start of the
	// text; a worker READY that merely mentions it stays in the inbox.
	ready := t3426Ready(t3426Design, "ready-mentions-consult")
	ready.Text = "READY hk3404 PR #12 (applied the [consult-done] advice)"
	if r := h.relayLaneEvent(ready, agent); r.ID == 0 || r.PersistFailed {
		t.Fatalf("ready relay=%+v", r)
	}
	if got := t3426Injected(agent); len(got) != 0 {
		t.Fatalf("a worker READY injected the design lane: %+v", got)
	}
	if item := t3426Item(t, h, t3426Design, "lane.event", ready.JobID); item.Class != "inbox" {
		t.Fatalf("worker READY stored as %+v, want an inbox item", item)
	}
}
