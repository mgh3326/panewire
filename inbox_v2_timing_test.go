package panewire

import (
	"path/filepath"
	"testing"
	"time"
)

// setReceiverSession swaps the node snapshot so lane's pane reports the given
// status and agent CLI. A nil snapshot leaves the receiver unreported.
func setReceiverSession(h *HubServer, machine string, sessions []HubSession, at time.Time) {
	var record *hubSessionSnapshotRecord
	if sessions != nil {
		record = &hubSessionSnapshotRecord{sessions: sessions, snapshotStatus: hubSnapshotStatusOK, receivedAt: at}
	}
	h.nodes[machine].sessionSnapshot = record
}

func receiverSession(pane, status, harness string) []HubSession {
	return []HubSession{{PaneID: pane, Status: status, Harness: harness}}
}

func TestOwnerCompletionPromotes(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)

	// Owner completion: job.completed to the owner lane is immediate even
	// though its class is inbox.
	e := hubJobEventPayload{JobID: "job-1", Epoch: 1, OwnerLane: "lane-a", Label: "agent", Host: "host-a", ReportPath: "report.md", ReportLastLine: "VERDICT: DONE"}
	h.relayJobEventFrom("host-a", "job.completed", e)
	if len(agent.relays) != 1 {
		t.Fatal("owned completion deferred")
	}
	counts, _ := h.inbox.store.counts("lane-a")
	if counts["lane-a"] != 0 {
		t.Fatal("owned completion left an unread inbox item")
	}
	items, _ := h.inbox.store.list("lane-a", false)
	if len(items) != 1 || items[0].Class != "interrupt" {
		t.Fatalf("stored=%+v", items)
	}

	// Parent completion: job.joined reports the worker's join to the owner's
	// parent lane — also a completion the receiver owns.
	e2 := hubJobEventPayload{JobID: "job-2", Epoch: 1, OwnerLane: "child", Label: "agent", Host: "host-a", ReportPath: "report.md"}
	h.relayJobEventFrom("host-a", "job.joined", e2)
	if len(agent.relays) != 2 {
		t.Fatal("parent join deferred")
	}

	// Observer: a lane merely told that a job finished keeps its own class —
	// the lane.event text "done" stays an inbox item for a busy supported
	// receiver.
	e3 := quietEvent("obs-1", "[worker] done | report posted")
	e3.JobID = laneEventTransportID("lane-a", "obs-1")
	r := h.relayLaneEvent(e3, agent)
	if !r.Routed || len(agent.relays) != 2 {
		t.Fatal("observer completion did not defer")
	}
	counts, _ = h.inbox.store.counts("lane-a")
	if counts["lane-a"] != 1 {
		t.Fatal(counts)
	}
	if fake.attemptsFor("job.completed", "job-1", 1, "report.md", "") != 1 {
		t.Fatal("completed not persisted")
	}
}

func TestReceiverCapabilityAndIdleRules(t *testing.T) {
	cases := []struct {
		name     string
		sessions []HubSession
		want     string // "interrupt" for immediate, "inbox" for deferred
	}{
		{"supported idle", receiverSession("w1:p1", "idle", "claude"), "interrupt"},
		{"supported done", receiverSession("w1:p1", "done", "claude"), "interrupt"},
		{"supported working", receiverSession("w1:p1", "working", "claude"), "inbox"},
		{"supported blocked", receiverSession("w1:p1", "blocked", "claude"), "inbox"},
		{"supported unknown status", receiverSession("w1:p1", "unknown", "claude"), "inbox"},
		{"unsupported cli", receiverSession("w1:p1", "idle", "grok"), "interrupt"},
		{"unlisted cli", receiverSession("w1:p1", "idle", "mystery"), "interrupt"},
		{"unreported cli", receiverSession("w1:p1", "idle", ""), "interrupt"},
		{"pane missing from snapshot", receiverSession("w9:p9", "idle", "claude"), "interrupt"},
		{"no snapshot", nil, "interrupt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now()
			h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
			setReceiverSession(h, "host-a", tc.sessions, now)
			e := quietEvent("probe-"+tc.name, "FYI notice")
			h.relayLaneEvent(e, agent)
			items, _ := h.inbox.store.list("lane-a", false)
			var got string
			if len(items) == 1 {
				got = items[0].Class
			}
			if got != tc.want {
				t.Fatalf("class=%s want %s (relays=%d)", got, tc.want, len(agent.relays))
			}
			wantRelays := 0
			if tc.want == "interrupt" {
				wantRelays = 1
			}
			if len(agent.relays) != wantRelays {
				t.Fatalf("relays=%d want %d", len(agent.relays), wantRelays)
			}
		})
	}
}

func TestReceiverSnapshotFreshnessGate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(h *HubServer, now time.Time)
		want   string
	}{
		{"stale snapshot", func(h *HubServer, now time.Time) {
			setReceiverSession(h, "host-a", receiverSession("w1:p1", "working", "claude"), now.Add(-2*sessionsFindSnapshotMaxAge))
		}, "interrupt"},
		{"future snapshot", func(h *HubServer, now time.Time) {
			setReceiverSession(h, "host-a", receiverSession("w1:p1", "working", "claude"), now.Add(time.Minute))
		}, "interrupt"},
		{"unavailable snapshot", func(h *HubServer, now time.Time) {
			h.nodes["host-a"].sessionSnapshot = &hubSessionSnapshotRecord{sessions: receiverSession("w1:p1", "working", "claude"), snapshotStatus: hubSnapshotStatusUnavailable, receivedAt: now}
		}, "interrupt"},
		{"truncated snapshot", func(h *HubServer, now time.Time) {
			h.nodes["host-a"].sessionSnapshot = &hubSessionSnapshotRecord{sessions: receiverSession("w1:p1", "working", "claude"), snapshotStatus: hubSnapshotStatusOK, truncated: true, receivedAt: now}
		}, "interrupt"},
		{"disconnected node", func(h *HubServer, now time.Time) {
			h.nodes["host-a"].state = "disconnected"
		}, "interrupt"},
		{"stale node", func(h *HubServer, now time.Time) {
			h.nodes["host-a"].state = "stale"
		}, "interrupt"},
		{"unrouted lane", nil, "interrupt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now()
			h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
			if tc.mutate != nil {
				tc.mutate(h, now)
			}
			lane := "lane-a"
			e := quietEvent("fresh-"+tc.name, "FYI notice")
			wantRelays := 1
			if tc.name == "unrouted lane" {
				// Unrouted lanes promote like any unknown receiver — the row is
				// stored interrupt — but delivery itself still fails unrouted.
				e.OwnerLane, e.JobID = "nowhere", laneEventTransportID("nowhere", "fresh-"+tc.name)
				lane, wantRelays = "nowhere", 0
			}
			h.relayLaneEvent(e, agent)
			items, _ := h.inbox.store.list(lane, false)
			if len(items) != 1 || items[0].Class != tc.want {
				t.Fatalf("items=%+v", items)
			}
			if len(agent.relays) != wantRelays {
				t.Fatalf("relays=%d want %d", len(agent.relays), wantRelays)
			}
		})
	}
}

func TestDigestStillFiresForBusyReceiver(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
	h.relayLaneEvent(quietEvent("busy-1", "FYI notice"), agent)
	if len(agent.relays) != 0 {
		t.Fatal("busy supported receiver was interrupted")
	}
	// Deadline passes; the digest flush is the safety net for deferred items.
	now = now.Add(31 * time.Minute)
	h.flushInbox()
	if len(agent.relays) != 1 {
		t.Fatalf("digest did not fire: relays=%d", len(agent.relays))
	}
}

func TestMustActInterruptsEveryReceiver(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	now := time.Now()
	h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), "default", hk, &now)
	e := quietEvent("blocker", "BLOCKER disk full")
	e.DeliveryClass = "inbox" // explicit class cannot demote must-act either
	h.relayLaneEvent(e, agent)
	if len(agent.relays) != 1 {
		t.Fatal("must-act deferred")
	}
}

func TestAgentsPolicyParsing(t *testing.T) {
	p, err := parseDeliveryPolicy([]byte("default: interrupt\nagents:\n  grok: supported\n  claude: unsupported\nrules: []\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !p.agentSupported("grok") || p.agentSupported("claude") || p.agentSupported("pi") {
		t.Fatalf("agents=%v", p.Agents)
	}
	// Defaults: claude supported, everything else unsupported.
	p, err = loadDeliveryPolicy("default")
	if err != nil {
		t.Fatal(err)
	}
	if !p.agentSupported("claude") || p.agentSupported("codex") || p.agentSupported("") {
		t.Fatalf("defaults=%v", p.Agents)
	}
	for _, raw := range []string{
		"default: interrupt\nagents: {claude: bogus}",
		"default: interrupt\nagents: {'': supported}",
		"default: interrupt\nagents: {'bad cli': supported}",
		"default: interrupt\nagents: [claude]",
	} {
		if _, err := parseDeliveryPolicy([]byte(raw)); err == nil {
			t.Fatalf("invalid agents block accepted: %q", raw)
		}
	}
}

func TestHarnessRidesTheSessionWire(t *testing.T) {
	states := []HerdrAgentState{{PaneID: "w1:p1", Status: "idle", Harness: "claude", Revision: 1, SourceStateChangeSeq: 2}}
	sessions := hubSessionSnapshotsFromStates(states)
	if len(sessions) != 1 || sessions[0].Harness != "claude" {
		t.Fatalf("%+v", sessions)
	}
	raw := []byte(`[{"pane_id":"w1:p1","workspace_id":"w1","label":"a","status":"idle","revision":1,"state_change_seq":2,"harness":"codex"}]`)
	decoded, ok := decodeHubSessionSnapshots(raw)
	if !ok || len(decoded) != 1 || decoded[0].Harness != "codex" {
		t.Fatalf("decoded=%+v ok=%v", decoded, ok)
	}
	// Unknown twelfth field and null harness are rejected, absent is fine.
	if _, ok := decodeHubSessionSnapshots([]byte(`[{"pane_id":"w1:p1","workspace_id":"w1","label":"a","status":"idle","revision":1,"state_change_seq":2,"harness":"x","extra":1}]`)); ok {
		t.Fatal("extra field accepted")
	}
	if _, ok := decodeHubSessionSnapshots([]byte(`[{"pane_id":"w1:p1","workspace_id":"w1","label":"a","status":"idle","revision":1,"state_change_seq":2,"harness":null}]`)); ok {
		t.Fatal("null harness accepted")
	}
	if _, ok := decodeHubSessionSnapshots([]byte(`[{"pane_id":"w1:p1","workspace_id":"w1","label":"a","status":"idle","revision":1,"state_change_seq":2}]`)); !ok {
		t.Fatal("absent harness rejected")
	}
	// An overlong harness survives as unreported, not invalid.
	states[0].Harness = string(make([]byte, 600))
	if got := hubSessionSnapshotsFromStates(states); len(got) != 1 || got[0].Harness != "" {
		t.Fatalf("%+v", got)
	}
}
