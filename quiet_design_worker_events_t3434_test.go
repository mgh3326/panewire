package panewire

// hk 3434: a worker's own routine lane.event to its design owner lane stays in
// the lane's inbox; every other one is classified as before. The node names the emitting pane on the
// lane.event wire (pane_id) to a hub that advertised it; the hub counts the
// event a worker's when the sending node runs a live worker job in that pane.
// Everything it cannot attribute that way is delivered as before.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const (
	t3434WorkerJob  = "hk3430-mcp-chatgpt"
	t3434WorkerPane = "w1:worker"
	t3434ConsultJob = "consult-hk3434"
	t3434ConsultPan = "w1:consult"
	t3434DesignPane = "w1:design"
	t3434DeskPane   = "w1:desk"
)

// t3434Hub is the 3426 fixture with the producer node named host-a, a second
// connected node host-b, and live jobs: the worker and a counsel (consult) job
// on host-a, and a job on host-b in a pane whose id is also used on host-a.
// Every spawned role claims as worker, so that is the role the hub sees.
func t3434Hub(t *testing.T, path string) (*HubServer, *hubAgent, *fakeHandoffkeep) {
	t.Helper()
	fake, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h, agent := t3426Hub(t, path, hk, t3426Records)
	agent.machineID = "host-a"
	other := &hubAgent{machineID: "host-b", relays: make(chan hubRelayInjectEvent, 32), persisted: make(chan hubRelayPersistedEvent, 32)}
	h.mu.Lock()
	h.nodes["host-b"] = &hubNodeRecord{machineID: "host-b", state: "connected", agent: other}
	h.mu.Unlock()
	now := time.Now()
	h.observeActiveJobs("host-a", []HubActiveJob{
		{JobID: t3434WorkerJob, AgentLabel: t3434WorkerJob, Epoch: 1, OwnerLane: t3426Design, Pane: t3434WorkerPane, Role: "implementer"},
		{JobID: t3434ConsultJob, AgentLabel: t3434ConsultJob, Epoch: 1, OwnerLane: t3426Design, Pane: t3434ConsultPan, Role: "implementer"},
		{JobID: "hk3434-done", AgentLabel: "hk3434-done", Epoch: 1, OwnerLane: t3426Design, Pane: "w1:finished", Role: "implementer"},
		{JobID: "hk3434-captain", AgentLabel: "hk3434-captain", Epoch: 1, OwnerLane: t3426Design, Pane: "w1:captain", Role: "captain"},
		{JobID: "hk3434-builder", AgentLabel: "hk3434-builder", Epoch: 1, OwnerLane: t3426Design, Pane: "w1:builder"},
	}, now)
	h.observeActiveJobs("host-b", []HubActiveJob{
		{JobID: "hk3434-remote", AgentLabel: "hk3434-remote", Epoch: 1, OwnerLane: t3426Design, Pane: "w1:remote", Role: "implementer"},
	}, now)
	// hk3434-done has ended: the hub marked it completed and the node's next
	// heartbeat no longer lists it.
	h.mu.Lock()
	h.jobs["hk3434-done"].Completed = true
	h.mu.Unlock()
	return h, agent, fake
}

var t3434Seq int

func t3434Event(lane, pane, notifyType, text string) hubJobEventPayload {
	t3434Seq++
	id := "t3434-" + strconv.Itoa(t3434Seq)
	return hubJobEventPayload{JobID: laneEventTransportID(lane, id), Epoch: 1, OwnerLane: lane, EventID: id, NotifyType: notifyType, PaneID: pane, Text: text}
}

// t3434Quiet relays e from host-a and fails unless it is a quiet inbox item:
// accepted, not injected, unread with no deadline or digest, and written to no
// hk relay row.
func t3434Quiet(t *testing.T, h *HubServer, agent *hubAgent, fake *fakeHandoffkeep, e hubJobEventPayload) {
	t.Helper()
	if r := h.relayLaneEvent(e, agent); r.ID == 0 || r.PersistFailed || !r.Routed || r.Machine != "inbox" {
		t.Fatalf("%q relay=%+v, want an inbox item", e.Text, r)
	}
	if got := t3426Injected(agent); len(got) != 0 {
		t.Fatalf("%q (type %q, deliver %q) injected the design lane: %+v", e.Text, e.NotifyType, e.DeliveryClass, got)
	}
	item := t3426Item(t, h, e.OwnerLane, "lane.event", e.JobID)
	if item.Class != "inbox" || item.ReadAt != 0 || item.DueAt != 0 || item.DigestAt != 0 {
		t.Fatalf("%q stored as %+v, want an unread inbox item with no deadline", e.Text, item)
	}
	if rows := t3426RelayRows(fake, e.JobID); rows != 0 {
		t.Fatalf("%q hk relay rows=%d, want 0", e.Text, rows)
	}
}

// t3434Woken relays e from sender and fails unless e was filed unread in the
// design lane's inbox and the lane got its one-line wake in pane instead of
// the body (hk 3602), naming most as the most urgent kind.
func t3434Woken(t *testing.T, h *HubServer, sender, receiver *hubAgent, e hubJobEventPayload, pane, most string) {
	t.Helper()
	if r := h.relayLaneEvent(e, sender); r.PersistFailed || r.Machine != "inbox" {
		t.Fatalf("%q relay=%+v, want an inbox item", e.Text, r)
	}
	t3602AssertWoken(t, h, t3426Injected(receiver), e.OwnerLane, pane, "lane.event", e.JobID, most)
}

// (1, 2) A worker's own progress lane.event to its design lane -- untyped as
// hk3430-mcp-chatgpt sent it, or typed progress, report, info, merged, or
// untyped with --deliver interrupt -- stays in the design lane's inbox.
func TestQuietDesignWorkerLaneEventIsNotInjected(t *testing.T) {
	h, agent, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	for _, tc := range []struct{ notifyType, deliver, text string }{
		{"", "", "hk 3430: PR #221 open https://github.com/mgh3326/handoffkeep/pull/221"},
		{"", "", "#221 merged; follow-up #222 open"},
		{"progress", "", "hk 3430: tests green, pushing"},
		{"report", "", "hk 3430: report written /jobs/hk3430/report.md"},
		{"info", "", "hk 3430: rebased on main"},
		{"merged", "", "hk 3430: #221 merged"},
		{"", "interrupt", "hk 3430: PR #221 open, please look"},
		{"progress", "interrupt", "hk 3430: still working"},
	} {
		e := t3434Event(t3426Design, t3434WorkerPane, tc.notifyType, tc.text)
		e.DeliveryClass = tc.deliver
		t3434Quiet(t, h, agent, fake, e)
	}
}

// (3) A worker's decision and blocked lane.events still wake its design lane;
// since hk 3602 the body is filed and the lane gets the one-line wake.
func TestQuietDesignWorkerDecisionAndBlockedWake(t *testing.T) {
	h, agent, _ := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	for _, tc := range []struct{ notifyType, text string }{
		{"decision", "ESC hk 3430: which migration order do we ship?"},
		{"blocked", "BLOCKED hk 3430: CI needs an operator secret"},
	} {
		t3434Woken(t, h, agent, agent, t3434Event(t3426Design, t3434WorkerPane, tc.notifyType, tc.text), t3434DesignPane, tc.notifyType)
	}
}

// (4) Everything the hub cannot attribute to a live worker's own pane on the
// sending node is not quieted. Since hk 3602 it is filed like everything else
// and what was typed before wakes the design lane, except a declared info
// record (here progress), which is filed without a wake.
func TestQuietDesignUnattributedLaneEventsWake(t *testing.T) {
	h, agent, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	progress := "hk 3430: PR #221 open"
	// Untyped text with no action tag wakes the lane ranked info: the hub
	// cannot tell it is routine.
	for _, tc := range []struct {
		name string
		e    hubJobEventPayload
		most string
	}{
		{"no pane_id (an older node)", t3434Event(t3426Design, "", "", progress), "info"},
		{"pane with no live job", t3434Event(t3426Design, "w1:p99", "", progress), "info"},
		{"pane of a job on another node", t3434Event(t3426Design, "w1:remote", "", progress), "info"},
		{"pane of a completed job", t3434Event(t3426Design, "w1:finished", "", progress), "info"},
		{"pane of a captain job", t3434Event(t3426Design, "w1:captain", "", progress), "info"},
		{"pane of a builder job (no role on the hub)", t3434Event(t3426Design, "w1:builder", "", progress), "info"},
		{"TASK_LINK_FAILED from the design session's own pane", t3434Event(t3426Design, t3434DesignPane, "automation-failed", "[panewire] TASK_LINK_FAILED job=job-2 task=7 reason=task claim exited with status 1; link the task manually"), "automation-failed"},
		{"design session's own note on the worker's node", t3434Event(t3426Design, t3434DesignPane, "", "note to self: review #221 after lunch"), "info"},
		{"desk's own note on the worker's node", t3434Event(t3426Design, t3434DeskPane, "progress", "desk: hk 3430 is next"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.e.NotifyType != "progress" {
				t3434Woken(t, h, agent, agent, tc.e, t3434DesignPane, tc.most)
				return
			}
			t3434Quiet(t, h, agent, fake, tc.e)
		})
	}

	// Hub-originated notices have no sending node, whatever pane they name.
	for _, text := range []string{
		"[panewire] SPAWN_FAILED job-1 request=r1 host=none profile=none rc=70 stage=launch replay=no reason=spawn_failed fix=Check the checkout.",
		"[delivery unconfirmed] event 7 (lane.event, producer id p-1) for lane handoffkeep-design was not confirmed on host-a pane w1:design: timeout. The worker may not have it; read the pane or re-send. Reported once.",
	} {
		t3434Woken(t, h, nil, agent, t3434Event(t3426Design, t3434WorkerPane, "automation-failed", text), t3434DesignPane, "automation-failed")
	}
}

// (5) The [consult-done] answer from a counsel (consult) job's pane wakes the
// design lane that asked; the same pane's routine progress does not.
func TestQuietDesignConsultDoneFromCounselWakes(t *testing.T) {
	h, agent, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	t3434Woken(t, h, agent, agent, t3434Event(t3426Design, t3434ConsultPan, "ready", "[consult-done] ship the hub first | /jobs/consult-hk3434/answer.md"), t3434DesignPane, "ready")
	t3434Woken(t, h, agent, agent, t3434Event(t3426Design, t3434ConsultPan, "", "[consult-done] ship the hub first | /jobs/consult-hk3434/answer.md"), t3434DesignPane, "ready")
	t3434Quiet(t, h, agent, fake, t3434Event(t3426Design, t3434ConsultPan, "", "consult: reading the code"))
}

// (6) A worker's lane.event to a lane that is not a design lane is classified
// exactly as the same event without a sender pane.
func TestQuietDesignWorkerLaneEventToOtherLanesUnchanged(t *testing.T) {
	type outcome struct {
		injected int
		class    string
		read     bool
	}
	run := func(t *testing.T, lane, notifyType, deliver, pane string) outcome {
		h, agent, _ := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
		e := t3434Event(lane, pane, notifyType, "hk 3430: PR #221 open")
		e.DeliveryClass = deliver
		if r := h.relayLaneEvent(e, agent); r.PersistFailed {
			t.Fatalf("relay=%+v", r)
		}
		got := outcome{injected: len(t3426Injected(agent))}
		items, err := h.inbox.store.list(lane, true)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if item.Event.JobID == e.JobID {
				got.class, got.read = item.Class, item.ReadAt != 0
			}
		}
		return got
	}
	for _, lane := range []string{"operator-desk", "director-1"} {
		for _, notifyType := range []string{"", "progress", "info", "ready", "decision"} {
			for _, deliver := range []string{"", "interrupt"} {
				name := lane + "/" + notifyType + "/" + deliver
				t.Run(name, func(t *testing.T) {
					if with, without := run(t, lane, notifyType, deliver, t3434WorkerPane), run(t, lane, notifyType, deliver, ""); with != without {
						t.Fatalf("with the worker's pane %+v, without %+v", with, without)
					}
				})
			}
		}
	}
}

// (7) A quieted worker event is not put in the pane by a hub restart: not by
// the restart replay, not by a digest, and not when the node, reconnected
// before the new hub advertised pane_id, resends it without one.
func TestQuietDesignWorkerLaneEventStaysQuietAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, agent, fake := t3434Hub(t, path)
	e := t3434Event(t3426Design, t3434WorkerPane, "", "hk 3430: PR #221 open")
	t3434Quiet(t, h, agent, fake, e)

	h.Close()
	h2, agent2, _ := t3434Hub(t, path)
	h2.replayUndeliveredRelayEvents(context.Background())
	h2.replayUndeliveredLaneEvents(context.Background())
	h2.flushInbox()
	if err := h2.inbox.store.makeBatches(time.Now().Add(31 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	h2.flushInbox()
	if got := t3426Injected(agent2); len(got) != 0 {
		t.Fatalf("restart replay or digest injected the design lane: %+v", got)
	}
	for _, retry := range []hubJobEventPayload{e, func() hubJobEventPayload { r := e; r.PaneID = ""; return r }()} {
		if r := h2.relayLaneEvent(retry, agent2); r.ID == 0 || r.PersistFailed {
			t.Fatalf("retry relay=%+v", r)
		}
		if got := t3426Injected(agent2); len(got) != 0 {
			t.Fatalf("a retry (pane %q) injected the design lane: %+v", retry.PaneID, got)
		}
	}
	if item := t3426Item(t, h2, t3426Design, "lane.event", e.JobID); item.Class != "inbox" || item.ReadAt != 0 {
		t.Fatalf("after restart stored as %+v", item)
	}
}

// The hub decodes the sender pane; a value it cannot use is no pane, never a
// lost event.
func TestLaneEventPayloadCarriesSenderPane(t *testing.T) {
	for _, tc := range []struct{ payload, pane string }{
		{`{"owner_lane":"lane-a","event_id":"e-1","text":"hi","pane_id":"w1:p7"}`, "w1:p7"},
		{`{"owner_lane":"lane-a","event_id":"e-1","text":"hi"}`, ""},
		{`{"owner_lane":"lane-a","event_id":"e-1","text":"hi","pane_id":"w1:\np7"}`, ""},
		{`{"owner_lane":"lane-a","event_id":"e-1","text":"hi","pane_id":7}`, ""},
	} {
		event, ok := decodeHubLaneEventPayload([]byte(tc.payload))
		if !ok || event.PaneID != tc.pane {
			t.Fatalf("%s decoded ok=%t pane=%q, want pane %q", tc.payload, ok, event.PaneID, tc.pane)
		}
		if _, ok := parseHubInbound([]byte(`{"type":"event","kind":"lane.event","payload":` + tc.payload + `}`)); !ok {
			t.Fatalf("%s refused at the envelope", tc.payload)
		}
	}
}

// t3434LaneEvents runs a fake hub that, when advertise is set, sends
// hub.features with lane_event_pane on connect, and collects every lane.event
// payload it receives.
func t3434LaneEvents(t *testing.T, advertise bool) (string, func() []map[string]json.RawMessage) {
	t.Helper()
	var mu sync.Mutex
	var payloads []map[string]json.RawMessage
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer func() { _ = connection.CloseNow() }()
		ctx := request.Context()
		if advertise {
			_ = wsjson.Write(ctx, connection, map[string]any{"type": "hub.features", "features": []string{"host_signals", "readiness", "lane_event_pane"}})
		}
		for {
			var message map[string]json.RawMessage
			if wsjson.Read(ctx, connection, &message) != nil {
				return
			}
			var kind, eventKind string
			_ = json.Unmarshal(message["type"], &kind)
			_ = json.Unmarshal(message["kind"], &eventKind)
			switch {
			case kind == "ping":
				_ = wsjson.Write(ctx, connection, map[string]string{"type": "pong"})
			case kind == "event" && eventKind == "lane.event":
				var payload map[string]json.RawMessage
				_ = json.Unmarshal(message["payload"], &payload)
				mu.Lock()
				payloads = append(payloads, payload)
				mu.Unlock()
			}
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, func() []map[string]json.RawMessage {
		mu.Lock()
		defer mu.Unlock()
		return append([]map[string]json.RawMessage(nil), payloads...)
	}
}

func t3434WriteLaneRecord(t *testing.T, root, id, pane, notifyType string) {
	t.Helper()
	record := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339), OwnerLane: t3426Design, EventID: id, PaneID: pane, NotifyType: notifyType, Text: "hk 3430: PR #221 open"}
	if _, err := writeLaneEmitRecord(root, record); err != nil {
		t.Fatal(err)
	}
}

func t3434Payload(payloads []map[string]json.RawMessage, id string) (map[string]json.RawMessage, bool) {
	for _, p := range payloads {
		var got string
		if json.Unmarshal(p["event_id"], &got) == nil && got == id {
			return p, true
		}
	}
	return nil, false
}

// (1, 8) A node names the emitting pane on the lane.event wire to a hub that
// advertised lane_event_pane. A node's own notice about a pane -- a stall
// notice, an idle-wake -- names no sender pane.
func TestNodeSendsLaneEventPaneToAHubThatAdvertisedIt(t *testing.T) {
	url, received := t3434LaneEvents(t, true)
	root := t.TempDir()
	fsRunNode(t, url, func(client *HubClient) { client.jobsInboxRoot = root })
	// The node learns the feature from its first read; a record scanned before
	// that goes without the pane, as it would to an older hub. Emit until one
	// lands after it.
	var withPane string
	attempt := 0
	eventually(t, 20*time.Second, "a lane.event carrying pane_id", func() bool {
		if withPane != "" {
			return true
		}
		payloads := received()
		for i := 1; i <= attempt; i++ {
			if p, ok := t3434Payload(payloads, "worker-"+strconv.Itoa(i)); ok && string(p["pane_id"]) == `"`+t3434WorkerPane+`"` {
				withPane = "worker-" + strconv.Itoa(i)
				return true
			}
		}
		if attempt == 0 || len(payloads) >= attempt {
			attempt++
			t3434WriteLaneRecord(t, root, "worker-"+strconv.Itoa(attempt), t3434WorkerPane, "")
		}
		return false
	})
	t3434WriteLaneRecord(t, root, "stall:0123456789abcdef0123456789abcdef", t3434WorkerPane, "")
	t3434WriteLaneRecord(t, root, "idle-wake-1", t3434WorkerPane, "idle-wake")
	for _, id := range []string{"stall:0123456789abcdef0123456789abcdef", "idle-wake-1"} {
		eventually(t, 10*time.Second, id+" sent", func() bool { _, ok := t3434Payload(received(), id); return ok })
		if p, _ := t3434Payload(received(), id); p["pane_id"] != nil {
			t.Fatalf("%s named a sender pane: %s", id, p["pane_id"])
		}
	}
}

// (8) A hub from before this change never advertises lane_event_pane, so the
// node never sends the key that hub's decoder refuses. The frozen vocabulary
// is that hub's decodeHubLaneEventPayload (694fd56 hub_jobs.go:165).
func TestNewNodeLaneEventIsAcceptedByAnOldHub(t *testing.T) {
	url, received := t3434LaneEvents(t, false)
	root := t.TempDir()
	fsRunNode(t, url, func(client *HubClient) { client.jobsInboxRoot = root })
	t3434WriteLaneRecord(t, root, "worker-old-hub", t3434WorkerPane, "progress")
	eventually(t, 10*time.Second, "the lane.event", func() bool { _, ok := t3434Payload(received(), "worker-old-hub"); return ok })
	p, _ := t3434Payload(received(), "worker-old-hub")
	oldHub := map[string]bool{"owner_lane": true, "event_id": true, "text": true, "epoch": true, "truncated": true, "replay": true, "delivery_class": true, "notify_type": true, "cause": true, "head": true, "pr": true}
	if len(p) < 3 || len(p) > 11 {
		t.Fatalf("an old hub refuses %d fields: %v", len(p), p)
	}
	for name := range p {
		if !oldHub[name] {
			t.Errorf("an old hub would refuse the lane.event for %q", name)
		}
	}
}

// End to end over a real socket: a real hub advertises lane_event_pane, a real
// node that runs the worker job sends its pane, and the hub keeps the
// worker's progress in the design lane's inbox while its decision wakes the
// design pane with one line (hk 3602).
func TestQuietDesignWorkerLaneEventEndToEnd(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	h, err := NewHubServer(HubServerConfig{
		Tokens:             map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath:    r20LanesFile(t, t3426Lanes),
		DeliveryPolicyPath: "default",
		InboxStorePath:     filepath.Join(t.TempDir(), "inbox.sqlite3"),
		handoffkeep:        hk,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)

	root := t.TempDir()
	events := filepath.Join(root, "jobs", t3434WorkerJob, "events")
	if err := os.MkdirAll(events, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	for name, record := range map[string]string{
		"00001-job.claimed.json": `{"type":"job.claimed","job_id":"` + t3434WorkerJob + `","epoch":1,"agent_label":"` + t3434WorkerJob + `","owner_lane":"` + t3426Design + `","role":"implementer","created_at":"` + now + `"}`,
		"00002-job.spawned.json": `{"type":"job.spawned","job_id":"` + t3434WorkerJob + `","pane_id":"` + t3434WorkerPane + `","created_at":"` + now + `"}`,
	} {
		if err := os.WriteFile(filepath.Join(events, name), []byte(record), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	var injected []string
	client, err := NewHubClient(HubClientConfig{
		URL: r6WSURL(server.URL, ""), MachineID: "host-a", Token: "node", Accepting: true,
		AllowInsecureForTests: true, PingInterval: 20 * time.Millisecond, Warn: func(string) {},
		relayInject: func(_ context.Context, pane, text string) bool {
			mu.Lock()
			defer mu.Unlock()
			injected = append(injected, pane+" "+text)
			return true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	client.jobsInboxRoot = root
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); client.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	eventually(t, 10*time.Second, "the hub sees the worker job and the node the feature", func() bool {
		peer := client.signalsPeer.Load()
		return peer != nil && peer.acceptsLaneEventPane.Load() && h.laneEventFromWorker("host-a", t3434WorkerPane, t3426Design)
	})
	progress := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: now, OwnerLane: t3426Design, EventID: "e2e-progress", PaneID: t3434WorkerPane, Text: "hk 3430: PR #221 open"}
	decision := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: now, OwnerLane: t3426Design, EventID: "e2e-decision", PaneID: t3434WorkerPane, NotifyType: "decision", Text: "ESC hk 3430: which order do we ship?"}
	if _, err := writeLaneEmitRecord(root, progress); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the progress note in the design lane's inbox", func() bool {
		items, err := h.inbox.store.list(t3426Design, false)
		if err != nil {
			return false
		}
		for _, item := range items {
			if item.Kind == "lane.event" && item.Event.EventID == "e2e-progress" {
				return item.Class == "inbox" && item.ReadAt == 0
			}
		}
		return false
	})
	if _, err := writeLaneEmitRecord(root, decision); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "the decision's wake in the design pane", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(injected) > 0
	})
	mu.Lock()
	defer mu.Unlock()
	// Since hk 3602 the decision is filed and the node types the wake line.
	if len(injected) != 1 || !strings.HasPrefix(injected[0], t3434DesignPane+" ") || !strings.Contains(injected[0], "[inbox] "+t3426Design+": 2 unread (most urgent: decision) | panewire inbox list --lane "+t3426Design) || strings.Contains(injected[0], "which order do we ship") {
		t.Fatalf("injections=%q, want only the decision's wake in %s", injected, t3434DesignPane)
	}
}

// t3434ParentedLanes is t3426Lanes with the design lane naming a parent, as a
// design lane that reports to the desk does: the shape under which a worker
// directive type (findings, order) to the lane counts as isWorkerDirective.
const t3434ParentedLanes = `{"lanes":{
 "handoffkeep-design":{"machine":"host-a","pane":"w1:design","parent":"operator-desk"},
 "operator-desk":{"machine":"host-a","pane":"w1:desk"},
 "director-1":{"machine":"host-a","pane":"w1:director"},
 "worker-3404":{"machine":"host-a","pane":"w1:worker","parent":"handoffkeep-design"}
}}`

// t3434Routes runs f once on the plain t3426 lanes and once with the design
// lane naming a parent, so a route-based override after designQuiet shows.
func t3434Routes(t *testing.T, f func(t *testing.T, h *HubServer, agent *hubAgent, fake *fakeHandoffkeep)) {
	for _, tc := range []struct{ name, lanes string }{{"design lane without a parent", t3426Lanes}, {"design lane with a parent", t3434ParentedLanes}} {
		t.Run(tc.name, func(t *testing.T) {
			h, agent, fake := t3434Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
			if err := os.WriteFile(h.reportRelayPath, []byte(tc.lanes), 0o600); err != nil {
				t.Fatal(err)
			}
			f(t, h, agent, fake)
		})
	}
}

// t3434WorkerActions are a worker's lane.events to its design lane that need
// a person: an untyped action tag, and every declared type that is not
// routine.
var t3434WorkerActions = []struct{ notifyType, text, most string }{
	{"", "ESC need a decision | options: ship the hub first, or the node first", "decision"},
	{"", "BLOCKER hk 3430: CI needs an operator secret", "blocked"},
	{"", "hk 3430: BOUNCE, the brief contradicts the contract", "blocked"},
	{"", "URGENT hk 3430: main is red", "decision"},
	{"", "hk 3430: needs_decision on the migration order", "decision"},
	{"decision", "ESC hk 3430: which migration order do we ship?", "decision"},
	{"blocked", "BLOCKED hk 3430: CI needs an operator secret", "blocked"},
	{"findings", "hk 3430: review findings on #221 | /jobs/hk3430/findings.md", "findings"},
	{"order", "hk 3430: rebase #222 on #221 first", "order"},
	{"automation-failed", "hk 3430: mergewake watch failed on #221", "automation-failed"},
	{"deploy-approval", "hk 3430: #221 needs a deploy approval", "decision"},
	{"watch-hold", "hk 3430: watch hold on #221", "blocked"},
	{"culprit-hold", "hk 3430: #221 is the culprit of a revert", "blocked"},
}

// t3434WorkerRoutine are a worker's routine lane.events to its design lane:
// untyped progress with no action tag (an untyped READY is a completion
// record), and the routine declared types.
var t3434WorkerRoutine = []struct{ notifyType, text string }{
	{"", "READY | PR #1 | report"},
	{"", "hk 3430: PR #221 open"},
	{"", "#221 merged; follow-up #222 open"},
	{"progress", "hk 3430: tests green, pushing"},
	{"report", "hk 3430: report written /jobs/hk3430/report.md"},
	{"info", "hk 3430: rebased on main"},
	{"merged", "hk 3430: #221 merged"},
	{"ack", "hk 3430: ack, starting"},
	{"closed", "hk 3430: #220 closed as superseded"},
	{"ready", "READY hk 3430 | PR #221 | /jobs/hk3430/report.md"},
}

// A worker's action lane.event wakes its design lane, whether or not the
// design lane's route names a parent.
func TestQuietDesignWorkerActionLaneEventsWake(t *testing.T) {
	t3434Routes(t, func(t *testing.T, h *HubServer, agent *hubAgent, _ *fakeHandoffkeep) {
		for _, tc := range t3434WorkerActions {
			t.Run("type="+tc.notifyType+"/"+tc.text, func(t *testing.T) {
				t3434Woken(t, h, agent, agent, t3434Event(t3426Design, t3434WorkerPane, tc.notifyType, tc.text), t3434DesignPane, tc.most)
			})
		}
	})
}

// A worker's routine lane.event stays in its design lane's inbox, whether or
// not the design lane's route names a parent, and also with --deliver
// interrupt.
func TestQuietDesignWorkerRoutineLaneEventsAreQuiet(t *testing.T) {
	t3434Routes(t, func(t *testing.T, h *HubServer, agent *hubAgent, fake *fakeHandoffkeep) {
		for _, deliver := range []string{"", "interrupt"} {
			for _, tc := range t3434WorkerRoutine {
				t.Run("deliver="+deliver+"/type="+tc.notifyType+"/"+tc.text, func(t *testing.T) {
					e := t3434Event(t3426Design, t3434WorkerPane, tc.notifyType, tc.text)
					e.DeliveryClass = deliver
					t3434Quiet(t, h, agent, fake, e)
				})
			}
		}
	})
}

// designQuiet is the one decision point, and it agrees with the documented
// rule: for a worker's lane.event, every declared type quiets exactly when it
// is routine, so a type added to the vocabulary interrupts until someone
// decides otherwise.
func TestQuietDesignWorkerLaneEventTypesMatchTheDocumentedRule(t *testing.T) {
	p, err := loadDeliveryPolicy("default")
	if err != nil {
		t.Fatal(err)
	}
	routine := map[string]bool{"progress": true, "report": true, "info": true, "merged": true, "ack": true, "closed": true, "ready": true}
	noTask, worker := func() bool { return false }, func() bool { return true }
	for notifyType := range hubNotifyTypes {
		e := hubJobEventPayload{OwnerLane: t3426Design, NotifyType: notifyType, PaneID: t3434WorkerPane, Text: "hk 3430: status"}
		if got := p.designQuiet(t3426Design, "lane.event", e, noTask, worker); got != routine[notifyType] {
			t.Errorf("a worker's %q lane.event to a design lane: designQuiet=%t, want %t", notifyType, got, routine[notifyType])
		}
	}
}
