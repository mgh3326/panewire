package panewire

// hk 3410: design-session lanes get decisions and failures only. The hub
// classifies them from relay.design_lanes, suppresses idle-wake and the inbox
// summary for them, and a restart replay now consults the receiver's read as
// well as its ack for every lane.

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const quietDesignPolicy = `default: interrupt
deadlines: {inbox: 30m, interrupt: 0s}
digest_window: 0s
relay:
  design_lanes: ["*-design"]
rules: []
lanes:
  panewire-design:
    inbox_summary: {digest_prefix: "inbox/panewire-design/digest/", max_chars: 300}
  desk-lane:
    inbox_summary: {digest_prefix: "inbox/desk-lane/digest/", max_chars: 300}
`

// quietDesignLanes routes a design lane, a worker reporting to it, and a second
// worker reporting to a normal owner lane.
const quietDesignLanes = `{"lanes":{
 "panewire-design":{"machine":"host-a","pane":"workspace:design"},
 "worker-lane":{"machine":"host-a","pane":"workspace:pane-1","parent":"panewire-design"},
 "plain-owner":{"machine":"host-a","pane":"workspace:owner"},
 "plain-worker":{"machine":"host-a","pane":"workspace:plain","parent":"plain-owner"},
 "plain-lane":{"machine":"host-a","pane":"workspace:plainlane"}
}}`

func quietDesignHub(t *testing.T, hk *handoffkeepRelayClient, lanes string, logs io.Writer) *HubServer {
	t.Helper()
	return quietDesignHubAt(t, hk, lanes, filepath.Join(t.TempDir(), "inbox.sqlite3"), logs)
}

func quietDesignHubAt(t *testing.T, hk *handoffkeepRelayClient, lanes, inboxPath string, logs io.Writer) *HubServer {
	t.Helper()
	policy := filepath.Join(t.TempDir(), "delivery.yaml")
	if err := os.WriteFile(policy, []byte(quietDesignPolicy), 0600); err != nil {
		t.Fatal(err)
	}
	logger := slog.Default()
	if logs != nil {
		logger = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	h, err := NewHubServer(HubServerConfig{
		Tokens:             map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath:    r20LanesFile(t, lanes),
		DeliveryPolicyPath: policy,
		InboxStorePath:     inboxPath,
		Logger:             logger,
		handoffkeep:        hk,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}

func quietDesignPolicyOf(t *testing.T, raw string) *deliveryPolicy {
	t.Helper()
	p, err := parseDeliveryPolicy([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// (e) design_lanes parses exact names and globs, defaults to *-design, and
// refuses a malformed pattern at load.
func TestQuietDesignLaneConfig(t *testing.T) {
	builtin := quietDesignPolicyOf(t, "default: interrupt\n")
	custom := quietDesignPolicyOf(t, "default: interrupt\nrelay:\n  design_lanes: [\"*-design\", \"hk-design\", \"team-?-design\"]\n")
	off := quietDesignPolicyOf(t, "default: interrupt\nrelay:\n  design_lanes: []\n")
	everything := quietDesignPolicyOf(t, "default: interrupt\nrelay:\n  design_lanes: [\"*\"]\n")
	for _, tc := range []struct {
		name    string
		policy  *deliveryPolicy
		lane    string
		matches bool
	}{
		{"default glob", builtin, "panewire-design", true},
		{"default other", builtin, "lane-a", false},
		{"exact name", custom, "hk-design", true},
		{"single-char glob", custom, "team-a-design", true},
		{"glob does not span sections", custom, "team-a-design-extra", false},
		{"explicit empty list", off, "panewire-design", false},
		// The desk owns its summary scheduler; a pattern that would match it
		// never makes it a design lane.
		{"desk excluded", everything, deskPolicyLane, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.policy.designLane(tc.lane); got != tc.matches {
				t.Fatalf("designLane(%q)=%t, want %t", tc.lane, got, tc.matches)
			}
		})
	}
	for _, raw := range []string{
		"default: interrupt\nrelay:\n  design_lanes: [\"[\"]\n",
		"default: interrupt\nrelay:\n  design_lanes: [\"\"]\n",
		"default: interrupt\nrelay:\n  design_lanes: [\"a\\n\"]\n",
	} {
		if _, err := parseDeliveryPolicy([]byte(raw)); err == nil {
			t.Fatalf("invalid design_lanes accepted: %q", raw)
		}
	}
}

// (a) an idle-wake whose owner lane is a design lane produces no event, no
// injection and no inbox row; a worker lane reporting to a normal owner is
// unchanged.
func TestQuietDesignLaneIdleWakeIsSuppressed(t *testing.T) {
	var logs bytes.Buffer
	h := quietDesignHub(t, nil, quietDesignLanes, &logs)
	rig := newIdleWakeTestRig(t, time.Second)

	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.now = rig.now.Add(2 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	design := h.idleWakeRoute("host-a", rig.requests[0])
	if design.Eligible || design.Reason != idleWakeReasonDesignLane {
		t.Fatalf("design-lane decision=%+v", design)
	}
	rig.manager.ApplyRoute(context.Background(), design)
	if len(rig.emitted) != 0 || countIdleWakeFiles(t, rig.manager.inboxRoot) != 0 {
		t.Fatalf("design lane emitted=%d files=%d", len(rig.emitted), countIdleWakeFiles(t, rig.manager.inboxRoot))
	}
	if items, err := h.inbox.store.list("panewire-design", false); err != nil || len(items) != 0 {
		t.Fatalf("design lane inbox items=%+v err=%v", items, err)
	}
	if got := strings.Count(logs.String(), "idle-wake suppressed for design lane"); got != 1 {
		t.Fatalf("suppression DEBUG lines=%d: %s", got, logs.String())
	}

	// A pane reporting to a normal owner lane follows the unchanged path.
	observe := func(status string, revision int64, at time.Time) {
		t.Helper()
		if err := rig.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:plain", WorkspaceID: "workspace", Label: "worker-synthetic", Status: status, Revision: revision}, at); err != nil {
			t.Fatal(err)
		}
	}
	observe("working", 1, rig.now)
	rig.now = rig.now.Add(time.Second)
	observe("idle", 2, rig.now)
	rig.now = rig.now.Add(2 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	if len(rig.requests) != 2 {
		t.Fatalf("route requests=%d, want 2", len(rig.requests))
	}
	worker := h.idleWakeRoute("host-a", rig.requests[1])
	if !worker.Eligible || worker.Lane != "plain-owner" {
		t.Fatalf("worker-lane decision=%+v", worker)
	}
	rig.manager.ApplyRoute(context.Background(), worker)
	if len(rig.emitted) != 1 || rig.emitted[0].OwnerLane != "plain-owner" {
		t.Fatalf("worker lane emitted=%+v", rig.emitted)
	}
	if got := strings.Count(logs.String(), "idle-wake suppressed for design lane"); got != 1 {
		t.Fatalf("a non-design lane logged a suppression: %s", logs.String())
	}
}

// (b) the inbox summary is never appended to a design lane's interrupt, and is
// still appended to a non-design lane's.
func TestQuietDesignLaneInboxSummaryNotSurfaced(t *testing.T) {
	h := quietDesignHub(t, nil, quietDesignLanes, nil)
	kind := "lane.event"
	design := hubJobEventPayload{OwnerLane: "panewire-design", EventID: "summary-design", Text: "note"}
	designLine := "[inbox] 1 new: report 1 | hk:doc inbox/panewire-design/digest/x"
	if _, err := h.inbox.store.saveInboxSummary(relayEventDedupeKey(kind, design), "panewire-design", designLine, 1); err != nil {
		t.Fatal(err)
	}
	if got := h.appendInboxSummary(kind, design, "panewire-design", "directive"); got != "directive" {
		t.Fatalf("design lane summary surfaced: %q", got)
	}

	desk := hubJobEventPayload{OwnerLane: "desk-lane", EventID: "summary-desk", Text: "note"}
	deskLine := "[inbox] 1 new: report 1 | hk:doc inbox/desk-lane/digest/x"
	if _, err := h.inbox.store.saveInboxSummary(relayEventDedupeKey(kind, desk), "desk-lane", deskLine, 1); err != nil {
		t.Fatal(err)
	}
	if got, want := h.appendInboxSummary(kind, desk, "desk-lane", "directive"), "directive"+inboxSummaryJoin+deskLine; got != want {
		t.Fatalf("non-design lane summary=%q, want %q", got, want)
	}
}

// (b) continued: the "N unread inbox items" escalation digest is never made for
// a design lane, while a normal lane still escalates. Existing scheduled items
// are defused when the class is enabled at startup.
func TestQuietDesignLaneGetsNoUnreadDigest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h := quietDesignHubAt(t, nil, quietDesignLanes, path, nil)
	now := time.Now().UTC()
	// A supported, working receiver keeps both events inbox-class instead of
	// promoting them to an immediate interrupt.
	sessions := []HubSession{{PaneID: "workspace:design", Status: "working", Harness: "claude"}, {PaneID: "workspace:plainlane", Status: "working", Harness: "claude"}}
	h.mu.Lock()
	h.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", state: "connected", sessionSnapshot: &hubSessionSnapshotRecord{sessions: sessions, snapshotStatus: hubSnapshotStatusOK, receivedAt: now}}
	h.mu.Unlock()
	relay := func(lane, id string) {
		t.Helper()
		e := hubJobEventPayload{JobID: laneEventTransportID(lane, id), Epoch: 1, OwnerLane: lane, EventID: id, NotifyType: "report", Text: "FYI progress note"}
		if r := h.relayLaneEvent(e, nil); r.ID == 0 || r.PersistFailed {
			t.Fatalf("%s relay=%+v", id, r)
		}
	}
	relay("panewire-design", "digest-design")
	relay("plain-lane", "digest-plain")

	due := func(hub *HubServer, lane string) int64 {
		t.Helper()
		var at int64
		if err := hub.inbox.store.db.QueryRow(`SELECT due_at FROM lane_inbox WHERE lane=?`, lane).Scan(&at); err != nil {
			t.Fatal(err)
		}
		return at
	}
	if got := due(h, "panewire-design"); got != 0 {
		t.Fatalf("design lane item is scheduled to escalate: due_at=%d", got)
	}
	if got := due(h, "plain-lane"); got == 0 {
		t.Fatal("plain lane item lost its deadline")
	}
	if err := h.inbox.store.makeBatches(now.Add(31 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	batches, err := h.inbox.store.pendingBatches()
	if err != nil || len(batches) != 1 || batches[0].Lane != "plain-lane" {
		t.Fatalf("batches=%+v err=%v, want only the plain lane", batches, err)
	}
	if items, err := h.inbox.store.list("panewire-design", true); err != nil || len(items) != 1 {
		t.Fatalf("design lane unread items=%+v err=%v", items, err)
	}

	// A pre-existing scheduled item is defused by the startup sweep.
	if _, err := h.inbox.store.db.Exec(`UPDATE lane_inbox SET due_at=1,digest_at=1 WHERE lane='panewire-design'`); err != nil {
		t.Fatal(err)
	}
	h.Close()
	h2 := quietDesignHubAt(t, nil, quietDesignLanes, path, nil)
	if got := due(h2, "panewire-design"); got != 0 {
		t.Fatalf("startup sweep left a design item scheduled: due_at=%d", got)
	}
	if err := h2.inbox.store.makeBatches(now.Add(31 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	batches, err = h2.inbox.store.pendingBatches()
	for _, b := range batches {
		if b.Lane == "panewire-design" {
			t.Fatalf("design lane escalated after restart: %+v", b)
		}
	}
}

// (d) the notices a design lane must keep still reach it: SPAWN_FAILED
// (automation-failed), BLOCKED and a decision question.
func TestQuietDesignLaneKeepsDecisionsAndFailures(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h := quietDesignHub(t, hk, quietDesignLanes, nil)
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	h.mu.Lock()
	h.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", state: "connected", agent: agent}
	h.mu.Unlock()

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

// (c) a restart with one acked row and one unconfirmed row replays only the
// unconfirmed one. Removing the settled ack filter replays both.
func TestQuietRestartReplaySkipsAckedRowForEveryLane(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	now := time.Now().UTC()
	h, agent := quietHub(t, path, "default", hk, &now)

	// Both rows are plain interrupts: the store stamps read at insert, so only
	// the ack is receiver evidence for the first row and the second row is
	// genuinely unconfirmed. Removing the acked filter replays both.
	send := func(id, text string) int64 {
		t.Helper()
		e := quietEvent(id, text)
		if r := h.relayLaneEvent(e, agent); r.ID == 0 || r.PersistFailed {
			t.Fatalf("%s relay=%+v", id, r)
		}
		rowID := holdReceive(t, agent).EventID
		if !h.markRelayEventUnconfirmed(relayPending{eventID: rowID, kind: "lane.event", event: e}) {
			t.Fatalf("%s marker was not written", id)
		}
		return rowID
	}
	ackedID := send("acked-row", "host disk failed on the review host")
	var inboxID int64
	if err := h.inbox.store.db.QueryRow(`SELECT id FROM lane_inbox WHERE event_id=?`, "acked-row").Scan(&inboxID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.inbox.store.ack(inboxID, "lane-a", false, now); err != nil {
		t.Fatal(err)
	}
	unconfirmedID := send("unconfirmed-row", "the unconfirmed host warning")
	h.Close()

	h2, agent2 := quietHub(t, path, "default", hk, &now)
	h2.replayUndeliveredRelayEvents(context.Background())
	h2.replayUndeliveredLaneEvents(context.Background())

	var replayed []hubRelayInjectEvent
	for {
		select {
		case d := <-agent2.relays:
			replayed = append(replayed, d)
			continue
		default:
		}
		break
	}
	if len(replayed) != 1 || replayed[0].EventID != unconfirmedID || !strings.HasPrefix(replayed[0].Text, "[replayed after hub restart") {
		t.Fatalf("restart replayed %+v, want only the unconfirmed row %d with the label", replayed, unconfirmedID)
	}
	if got := fake.deliveredToFor(ackedID); got != "hub/"+relayReplayRetiredPane+relayUnconfirmedInboxRetireReason {
		t.Fatalf("acked row delivered_to=%q, want the settled-inbox retire", got)
	}
}
