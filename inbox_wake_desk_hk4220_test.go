package panewire

import (
	"testing"
)

// hk4220FileUnread files one wake-worthy lane event without attempting a wake,
// so the test itself can run the wake over a routes snapshot it staged.
func hk4220FileUnread(t *testing.T, r *t3602Rig, lane string) {
	t.Helper()
	e := t3602Event(lane, "order", "please review this order")
	if _, err := r.h.inbox.store.fileEventUnread(relayEventDedupeKey("lane.event", e), lane, "lane.event", e, r.clock.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

// TestInboxWakeDeskPanelessDecisionAndDeskRouteShareOneSnapshot pins hk 4220:
// the wake decides paneless and resolves the desk route from one routes read,
// so a lanes-file write after that read cannot send the line somewhere the
// decision never saw.
func TestInboxWakeDeskPanelessDecisionAndDeskRouteShareOneSnapshot(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	routes[t3602Design] = reportRelayRoute{Sink: true}
	routes[deskPolicyLane] = reportRelayRoute{Machine: "host-a", Pane: "w1:desk-before"}
	t4150Routes(t, r, routes)
	snapshot := r.h.inboxWakeRoutes()
	// Swapped after the wake's one read: on disk the design lane is live again
	// and the desk has moved, so a reload would decide and send elsewhere.
	routes[t3602Design] = reportRelayRoute{Machine: "host-a", Pane: "w1:design"}
	routes[deskPolicyLane] = reportRelayRoute{Machine: "host-a", Pane: "w1:desk-after"}
	t4150Routes(t, r, routes)
	swapped := loadReportRelayRoutes(r.h.reportRelayPath)
	if swapped[t3602Design].Sink || swapped[deskPolicyLane].Pane != "w1:desk-after" {
		t.Fatalf("swapped lanes file=%+v, want a live design lane and w1:desk-after", swapped)
	}
	t4150Snapshot(t, r, "w1:design", "w1:desk-before", "w1:desk-after")
	hk4220FileUnread(t, r, t3602Design)
	if outcome := r.h.sendInboxWakeWithRoutes(snapshot, t3602Design, "design", false); outcome != inboxWakeSent {
		t.Fatalf("wake outcome=%v, want sent", outcome)
	}
	t3602OneWake(t, t3426Injected(r.agent), "w1:desk-before", t4150DeskWake(t3602Design, 1, "order"))
	ev := t4150DeskEvent(t, r, t3602Design, false)
	want := "lane=" + t3602Design + " class=design unread=1 most_urgent=order desk=" + deskPolicyLane + " machine=host-a pane=w1:desk-before"
	if ev.Detail != want {
		t.Fatalf("desk event detail=%q, want %q", ev.Detail, want)
	}
}

// TestInboxWakeDeskLaneRouteAndPanelessDecisionShareOneSnapshot is the other
// half of hk 4220: the lane route the wake first resolves and the paneless
// decision that may keep it also read the same routes snapshot.
func TestInboxWakeDeskLaneRouteAndPanelessDecisionShareOneSnapshot(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	t4150Routes(t, r, routes)
	snapshot := r.h.inboxWakeRoutes()
	// Swapped after the one read: the design lane is paneless on disk now.
	delete(routes, t3602Design)
	t4150Routes(t, r, routes)
	if _, hasRow := loadReportRelayRoutes(r.h.reportRelayPath)[t3602Design]; hasRow {
		t.Fatal("swapped lanes file still routes the design lane")
	}
	hk4220FileUnread(t, r, t3602Design)
	if outcome := r.h.sendInboxWakeWithRoutes(snapshot, t3602Design, "design", false); outcome != inboxWakeSent {
		t.Fatalf("wake outcome=%v, want sent", outcome)
	}
	t3602OneWake(t, t3426Injected(r.agent), "w1:design", t3602Wake(t3602Design, 1, "order"))
	for _, ev := range r.wakeEvents() {
		if ev.Reason == "desk_fallback" {
			t.Fatalf("wake fell back to the desk from a later read: %+v", ev)
		}
	}
}
