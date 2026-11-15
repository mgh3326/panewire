package panewire

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

const t4150DeskWakeTail = " | no pane: desk takes it | panewire inbox list --lane "

func t4150DeskWake(lane string, unread int, most string) string {
	return "[inbox] " + lane + ": " + strconv.Itoa(unread) + " unread (most urgent: " + most + ")" + t4150DeskWakeTail + lane
}

func t4150Routes(t *testing.T, r *t3602Rig, routes map[string]reportRelayRoute) {
	t.Helper()
	b, err := json.Marshal(reportRelayRoutes{Lanes: routes})
	if err != nil {
		t.Fatal(err)
	}
	path := r20LanesFile(t, string(b))
	r.h.reportRelayPath = path
}

func t4150BaseRoutes() map[string]reportRelayRoute {
	return map[string]reportRelayRoute{
		t3602Design:          {Machine: "host-a", Pane: "w1:design"},
		deskPolicyLane:       {Machine: "host-a", Pane: "w1:desk"},
		"work-operator-desk": {Machine: "host-a", Pane: "w1:workdesk"},
		"worker-3404":        {Machine: "host-a", Pane: "w1:worker", Parent: t3602Design},
		"review-3404":        {Machine: "host-a", Pane: "w1:review", Parent: t3602Design},
		"trial-3404":         {Machine: "host-a", Pane: "w1:trial", Parent: t3602Design},
	}
}

func t4150Snapshot(t *testing.T, r *t3602Rig, panes ...string) {
	t.Helper()
	sessions := make([]HubSession, 0, len(panes))
	for _, pane := range panes {
		sessions = append(sessions, HubSession{PaneID: pane, Status: "idle", Harness: "claude"})
	}
	r.h.mu.Lock()
	node := r.h.nodes["host-a"]
	node.state = "connected"
	node.sessionSnapshot = &hubSessionSnapshotRecord{sessions: sessions, snapshotStatus: hubSnapshotStatusOK, receivedAt: r.clock.Now()}
	r.h.mu.Unlock()
}

func t4150Event(t *testing.T, r *t3602Rig, lane string) []hubRelayInjectEvent {
	t.Helper()
	e := t3602Event(lane, "order", "please review this order")
	return r.fileQuiet(t, e, r.agent)
}

func t4150DeskEvent(t *testing.T, r *t3602Rig, lane string, startup bool) fleetEvent {
	t.Helper()
	for _, ev := range r.wakeEvents() {
		if ev.Job == lane && ev.Outcome == "sent" && ev.Reason == "desk_fallback" {
			if startup && !strings.Contains(ev.Detail, "startup=true") {
				t.Fatalf("startup desk event detail=%q", ev.Detail)
			}
			return ev
		}
	}
	t.Fatalf("no desk_fallback event for %s: %+v", lane, r.wakeEvents())
	return fleetEvent{}
}

func TestInboxWakeDesignWithoutRowFallsBackToDesk(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	delete(routes, t3602Design)
	t4150Routes(t, r, routes)
	got := t4150Event(t, r, t3602Design)
	t3602OneWake(t, got, "w1:desk", t4150DeskWake(t3602Design, 1, "order"))
	ev := t4150DeskEvent(t, r, t3602Design, false)
	if ev.Detail != "lane=handoffkeep-design class=design unread=1 most_urgent=order desk=operator-desk machine=host-a pane=w1:desk" {
		t.Fatalf("desk event=%+v", ev)
	}
	item := t3602Unread(t, r.h, t3602Design, "lane.event", t3426ItemJobID(t, r.h, t3602Design))
	if item.ReadAt != 0 {
		t.Fatalf("design inbox item was read: %+v", item)
	}
	counts, err := r.h.inbox.store.counts(deskPolicyLane)
	if err != nil || counts[deskPolicyLane] != 0 {
		t.Fatalf("desk unread counts=%v err=%v", counts, err)
	}
	longLane := strings.Repeat("a", 128)
	if text := t4150DeskWake(longLane, 1, "decision"); !validHubNoteText(text) {
		t.Fatalf("128-byte lane wake is invalid (%d bytes): %q", len(text), text)
	}
}

func TestInboxWakeDesignWithoutRowKeepsUnroutableLiveJobOwed(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	delete(routes, t3602Design)
	t4150Routes(t, r, routes)
	r.h.mu.Lock()
	r.h.jobs["job-4150"] = &hubJobRecord{
		HubActiveJob: HubActiveJob{JobID: "job-4150", AgentLabel: t3602Design, OwnerLane: t3602Design, Pane: "w1:job", Epoch: 1},
		Node:         "host-a",
		LastSeen:     r.clock.Now(),
	}
	r.h.nodes["host-a"].agent = nil
	r.h.mu.Unlock()
	if got := t4150Event(t, r, t3602Design); len(got) != 0 {
		t.Fatalf("unroutable live job injections=%+v", got)
	}
	if got := r.wakeEvents(); len(got) != 1 || got[0].Outcome != "skipped" || got[0].Reason != "unrouted" {
		t.Fatalf("unroutable live job events=%+v", got)
	}
	r.h.inbox.wakes.mu.Lock()
	state := r.h.inbox.wakes.lanes[t3602Design]
	owed := state != nil && state.pending && state.unrouted
	r.h.inbox.wakes.mu.Unlock()
	if !owed {
		t.Fatal("unroutable live job wake was not left owed")
	}
	r.h.mu.Lock()
	r.h.nodes["host-a"].agent = r.agent
	r.h.mu.Unlock()
	r.h.retryUnroutedInboxWakes()
	r.h.flushInboxWakes()
	t3602OneWake(t, t3426Injected(r.agent), "w1:job", t3602Wake(t3602Design, 1, "order"))
}

func TestInboxWakeDesignWithProvenGonePaneFallsBackToDesk(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	t4150Routes(t, r, routes)
	t4150Snapshot(t, r, "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial")
	obs, ok := r.h.observeNodePanes("host-a")
	if !ok || r.h.laneProvablyGone(t3602Design, routes[t3602Design], obs) {
		t.Fatal("initial absence did not seed the design row write time")
	}
	r.clock.advance(laneWriteSettle + time.Second)
	t4150Snapshot(t, r, "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial")
	got := t4150Event(t, r, t3602Design)
	t3602OneWake(t, got, "w1:desk", t4150DeskWake(t3602Design, 1, "order"))
	t4150DeskEvent(t, r, t3602Design, false)
}

func TestInboxWakeDesignProvenGonePaneWithLiveJobKeepsLaneRoute(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	t4150Routes(t, r, routes)
	t4150Snapshot(t, r, "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial", "w1:job")
	obs, ok := r.h.observeNodePanes("host-a")
	if !ok || r.h.laneProvablyGone(t3602Design, routes[t3602Design], obs) {
		t.Fatal("initial absence did not seed the design row write time")
	}
	r.clock.advance(laneWriteSettle + time.Second)
	t4150Snapshot(t, r, "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial", "w1:job")
	r.h.mu.Lock()
	r.h.jobs["job-4150-pane-alive"] = &hubJobRecord{
		HubActiveJob: HubActiveJob{JobID: "job-4150-pane-alive", AgentLabel: t3602Design, OwnerLane: t3602Design, Pane: "w1:job", Epoch: 1},
		Node:         "host-a",
		LastSeen:     r.clock.Now(),
	}
	r.h.mu.Unlock()
	got := t4150Event(t, r, t3602Design)
	t3602OneWake(t, got, "w1:job", t3602Wake(t3602Design, 1, "order"))
	for _, ev := range r.wakeEvents() {
		if ev.Reason == "desk_fallback" {
			t.Fatalf("live job was diverted to desk: %+v", ev)
		}
	}
}

func TestInboxWakeDesignProvenGonePaneWithUnroutableLiveJobStaysOwed(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	t4150Routes(t, r, routes)
	t4150Snapshot(t, r, "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial", "w1:job")
	obs, ok := r.h.observeNodePanes("host-a")
	if !ok || r.h.laneProvablyGone(t3602Design, routes[t3602Design], obs) {
		t.Fatal("initial absence did not seed the design row write time")
	}
	r.clock.advance(laneWriteSettle + time.Second)
	t4150Snapshot(t, r, "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial", "w1:job")
	r.h.mu.Lock()
	r.h.jobs["job-4150-unroutable"] = &hubJobRecord{
		HubActiveJob: HubActiveJob{JobID: "job-4150-unroutable", AgentLabel: t3602Design, OwnerLane: t3602Design, Pane: "w1:job", Epoch: 1},
		Node:         "host-a",
		LastSeen:     r.clock.Now(),
	}
	r.h.nodes["host-a"].agent = nil
	r.h.mu.Unlock()
	if got := t4150Event(t, r, t3602Design); len(got) != 0 {
		t.Fatalf("unroutable live job injections=%+v", got)
	}
	if got := r.wakeEvents(); len(got) != 1 || got[0].Outcome != "skipped" || got[0].Reason != "unrouted" {
		t.Fatalf("unroutable live job events=%+v", got)
	}
	r.h.inbox.wakes.mu.Lock()
	state := r.h.inbox.wakes.lanes[t3602Design]
	owed := state != nil && state.pending && state.unrouted
	r.h.inbox.wakes.mu.Unlock()
	if !owed {
		t.Fatal("unroutable live job wake was not left owed")
	}
}

func TestInboxWakeDesignStaleSnapshotKeepsLaneRoute(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	t4150Routes(t, r, routes)
	r.h.laneLife.writeOrSeed(t3602Design, r.clock.Now().UTC())
	r.clock.advance(laneWriteSettle + time.Second)
	t4150Snapshot(t, r, "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial")
	r.h.mu.Lock()
	r.h.nodes["host-a"].sessionSnapshot.receivedAt = r.clock.Now().Add(-sessionsFindSnapshotMaxAge - time.Second)
	r.h.mu.Unlock()
	got := t4150Event(t, r, t3602Design)
	t3602OneWake(t, got, "w1:design", t3602Wake(t3602Design, 1, "order"))
}

func TestInboxWakeDesignDeskWindowChoosesTargetAfresh(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	delete(routes, t3602Design)
	t4150Routes(t, r, routes)
	got := t4150Event(t, r, t3602Design)
	t3602OneWake(t, got, "w1:desk", t4150DeskWake(t3602Design, 1, "order"))
	if got := t4150Event(t, r, t3602Design); len(got) != 0 {
		t.Fatalf("inside-window injections=%+v", got)
	}
	routes[t3602Design] = reportRelayRoute{Machine: "host-a", Pane: "w1:design"}
	t4150Routes(t, r, routes)
	t4150Snapshot(t, r, "w1:design", "w1:desk", "w1:workdesk", "w1:worker", "w1:review", "w1:trial")
	r.clock.advance(r.h.inbox.policy.Relay.wakeWindow)
	r.h.flushInboxWakes()
	t3602OneWake(t, t3426Injected(r.agent), "w1:design", t3602Wake(t3602Design, 2, "order"))
}

func TestInboxWakeDesignDeskUnreachableRetriesOnHello(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	delete(routes, t3602Design)
	delete(routes, deskPolicyLane)
	t4150Routes(t, r, routes)
	t4150Event(t, r, t3602Design)
	if got := t3426Injected(r.agent); len(got) != 0 {
		t.Fatalf("unreachable desk injections=%+v", got)
	}
	if got := r.wakeEvents(); len(got) != 1 || got[0].Reason != "unrouted" || got[0].Outcome != "skipped" {
		t.Fatalf("unreachable desk events=%+v", got)
	}
	routes[deskPolicyLane] = reportRelayRoute{Machine: "host-a", Pane: "w1:desk"}
	t4150Routes(t, r, routes)
	r.h.retryUnroutedInboxWakes()
	r.h.flushInboxWakes()
	t3602OneWake(t, t3426Injected(r.agent), "w1:desk", t4150DeskWake(t3602Design, 1, "order"))
}

func TestInboxWakeDesignStartupSeedFallsBackToDesk(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	delete(routes, t3602Design)
	t4150Routes(t, r, routes)
	e := t3602Event(t3602Design, "order", "please review this order")
	if _, err := r.h.inbox.store.fileEventUnread(relayEventDedupeKey("lane.event", e), t3602Design, "lane.event", e, r.clock.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	r.h.seedStartupInboxWakes()
	r.clock.advance(r.h.inbox.policy.Relay.wakeWindow)
	r.h.flushInboxWakes()
	t3602OneWake(t, t3426Injected(r.agent), "w1:desk", t4150DeskWake(t3602Design, 1, "order"))
	t4150DeskEvent(t, r, t3602Design, true)
}

func TestInboxWakeDesignLivePaneLineIsUnchanged(t *testing.T) {
	r := newT3602Rig(t, "default")
	got := t4150Event(t, r, t3602Design)
	t3602OneWake(t, got, "w1:design", t3602Wake(t3602Design, 1, "order"))
}

func TestInboxWakePanelessNonDesignLanesDoNotFallback(t *testing.T) {
	r := newT3602Rig(t, t3602TrialPolicy(t))
	routes := t4150BaseRoutes()
	delete(routes, "worker-3404")
	delete(routes, "trial-3404")
	delete(routes, deskPolicyLane)
	t4150Routes(t, r, routes)
	for _, lane := range []string{"worker-3404", "trial-3404", deskPolicyLane} {
		e := t3602Event(lane, "order", "please review this order")
		if _, err := r.h.inbox.store.fileEventUnread(relayEventDedupeKey("lane.event", e), lane, "lane.event", e, r.clock.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		class := r.h.inbox.policy.inboxOnlyClass(lane, r.clock.Now())
		r.h.sendInboxWake(lane, class, false)
	}
	if got := t3426Injected(r.agent); len(got) != 0 {
		t.Fatalf("non-design desk injections=%+v", got)
	}
}

func TestInboxWakeDesignDisconnectedNodeDoesNotFallback(t *testing.T) {
	r := newT3602Rig(t, "default")
	routes := t4150BaseRoutes()
	t4150Routes(t, r, routes)
	r.h.mu.Lock()
	delete(r.h.nodes, "host-a")
	r.h.mu.Unlock()
	if got := t4150Event(t, r, t3602Design); len(got) != 0 {
		t.Fatalf("disconnected node injections=%+v", got)
	}
	if got := r.wakeEvents(); len(got) != 1 || got[0].Reason != "unrouted" || got[0].Outcome != "skipped" {
		t.Fatalf("disconnected events=%+v", got)
	}
}

func t3426ItemJobID(t *testing.T, h *HubServer, lane string) string {
	t.Helper()
	items, err := h.inbox.store.list(lane, true)
	if err != nil || len(items) != 1 {
		t.Fatalf("%s unread items=%+v err=%v", lane, items, err)
	}
	return items[0].Event.JobID
}
