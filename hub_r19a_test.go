package panewire

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestR19HierarchicalLanesAndLegacyRoutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(path, []byte(`{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1","parent":"captain"},"captain":{"machine":"host-a","pane":"w1:p2","parent":null}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	lanes := loadReportRelayRoutes(path)
	if lanes["lane-a"].Parent != "captain" || lanes["captain"].Pane != "w1:p2" {
		t.Fatalf("lanes=%+v", lanes)
	}
	legacy := filepath.Join(t.TempDir(), "routes.json")
	if err := os.WriteFile(legacy, []byte(`{"routes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got := loadReportRelayRoutes(legacy)["lane-a"]; got.Machine != "host-a" || got.Parent != "" {
		t.Fatalf("legacy route=%+v", got)
	}
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token"}, ReportRelayPath: path, RelayAckTimeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 3)}
	h.nodes["host-a"] = &hubNodeRecord{agent: agent}
	event := hubJobEventPayload{JobID: "job-a", Epoch: 1, OwnerLane: "lane-a", Label: "worker", Host: "host-a", ReportPath: "report.md", ReportLastLine: "DONE", Reason: "needs parent"}
	h.relayJobCompletion(event)
	if got := <-agent.relays; got.Pane != "w1:p1" {
		t.Fatalf("completed pane=%q", got.Pane)
	}
	event.JobID = "job-b"
	h.relayJobEvent("job.escalate", event)
	if got := <-agent.relays; got.Pane != "w1:p2" {
		t.Fatalf("escalation parent pane=%q", got.Pane)
	}
	noParent := filepath.Join(t.TempDir(), "no-parent.json")
	if err := os.WriteFile(noParent, []byte(`{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	h.reportRelayPath = noParent
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sub := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 1)}
	h.subscribers[sub] = struct{}{}
	event.JobID = "job-c"
	h.relayJobEvent("job.joined", event)
	select {
	case got := <-sub.messages:
		if got.event == nil || got.event.Kind != "relay.unrouted" {
			t.Fatalf("parentless=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("parentless escalation was silently routed")
	}
}

func TestR19RelayAckTimeoutBroadcastsOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lanes.json")
	if err := os.WriteFile(path, []byte(`{"lanes":{"lane-a":{"machine":"host-a","pane":"w1:p1"}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token"}, ReportRelayPath: path, RelayAckTimeout: 25 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 1)}
	h.nodes["host-a"] = &hubNodeRecord{agent: agent}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sub := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 3)}
	h.subscribers[sub] = struct{}{}
	event := hubJobEventPayload{JobID: "job-a", Epoch: 1, OwnerLane: "lane-a", Label: "worker", Host: "host-a", ReportPath: "report.md", ReportLastLine: "DONE"}
	h.relayJobCompletion(event)
	<-agent.relays
	select {
	case got := <-sub.messages:
		if got.event == nil || got.event.Kind != "relay.unconfirmed" || !bytes.Contains(got.event.Payload, []byte(`"reason":"ack_timeout"`)) {
			t.Fatalf("timeout event=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("missing ack timeout event")
	}
	time.Sleep(60 * time.Millisecond)
	select {
	case got := <-sub.messages:
		t.Fatalf("duplicate timeout=%+v", got)
	default:
	}
}

func TestR19AcceptingOverrideControlsPlacementAndUI(t *testing.T) {
	overrides := filepath.Join(t.TempDir(), "accepting.json")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token"}, AcceptingOverridesPath: overrides})
	if err != nil {
		t.Fatal(err)
	}
	h.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", accepting: true, state: "connected", agent: &hubAgent{}, activeJobs: map[string]HubActiveJob{}, remoteMeta: map[string]string{}}
	set := func(mode string) int {
		request := httptest.NewRequest(http.MethodPost, "/v1/nodes/host-a/accepting", bytes.NewBufferString(`{"mode":"`+mode+`"}`))
		request.Header.Set("Authorization", "Bearer operator-token")
		response := httptest.NewRecorder()
		h.Handler().ServeHTTP(response, request)
		return response.Code
	}
	if got := set("off"); got != http.StatusOK {
		t.Fatalf("off status=%d", got)
	}
	policy := PlacementPolicy{LocalMachine: "host-a", MaxActiveJobs: 1, LoadRatio: 1}
	if result := h.makePlacement(policy, placementMetrics{}, "hub-only", time.Now()); result.Decision != "unavailable" || result.Reason != "unavailable" || !strings.Contains(result.Candidates[0].Reason, "not_accepting") {
		t.Fatalf("off placement=%+v", result)
	}
	if data := h.uiData(); data.Nodes[0].AcceptingEffective || data.Nodes[0].AcceptingOverride != "off" {
		t.Fatalf("UI override=%+v", data.Nodes[0])
	}
	if got := set("auto"); got != http.StatusOK {
		t.Fatalf("auto status=%d", got)
	}
	h.nodes["host-a"].accepting = false
	if data := h.uiData(); data.Nodes[0].AcceptingEffective {
		t.Fatalf("auto did not return to hello value: %+v", data.Nodes[0])
	}
	b, err := os.ReadFile(overrides)
	if err != nil || !bytes.Contains(b, []byte(`"host-a":"auto"`)) {
		t.Fatalf("override file=%q err=%v", b, err)
	}
}

func TestR19RelayInjectDoesNotBlockClientReadLoop(t *testing.T) {
	pong := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		var ignored map[string]any
		_ = wsjson.Read(t.Context(), conn, &ignored)
		_ = wsjson.Read(t.Context(), conn, &ignored)
		_ = wsjson.Write(t.Context(), conn, map[string]any{"type": "relay.inject", "job_id": "job-a", "pane": "w1:p1", "text": "one line"})
		_ = wsjson.Write(t.Context(), conn, map[string]any{"type": "ping"})
		for {
			var message map[string]any
			if wsjson.Read(t.Context(), conn, &message) != nil {
				return
			}
			if message["type"] == "pong" {
				pong <- struct{}{}
				return
			}
		}
	}))
	defer server.Close()
	client, err := NewHubClient(HubClientConfig{URL: r6WSURL(server.URL, ""), MachineID: "host-a", Token: "node-token", AllowInsecureForTests: true, RelayInjectTimeout: 25 * time.Millisecond, PingInterval: time.Hour, relayInject: func(ctx context.Context, _, _ string) bool { <-ctx.Done(); return false }})
	if err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- client.serve(ctx, conn) }()
	select {
	case <-pong:
	case <-time.After(time.Second):
		t.Fatal("relay inject blocked node read loop")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("client did not stop")
	}
}

func TestR19EscalationPayloadRequiresReason(t *testing.T) {
	if _, ok := decodeHubJobEscalationPayload([]byte(`{"job_id":"job-a","epoch":1,"owner_lane":"lane-a"}`)); ok {
		t.Fatal("escalation without reason accepted")
	}
	if _, ok := decodeRelayAckPayload([]byte(`{"job_id":"job-a","pane":"w1:p1","reason":"ack_timeout","extra":true}`)); ok {
		t.Fatal("ack payload carrier accepted")
	}
}

func TestR19NodeScansFlatEscalationRecord(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "00001-job.escalate.json"), []byte(`{"kind":"job.escalate","epoch":1,"owner_lane":"lane-a","parent_lane":"ignored-by-hub","label":"worker","host":"host-a","report_path":"report.md","report_last_line":"waiting","reason":"needs captain"}`), 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{jobsInboxRoot: root, completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, assignedJobs: map[string]uint64{}}
	events := client.jobCompletionEvents()
	if len(events) != 1 || events[0].Kind != "job.escalate" {
		t.Fatalf("escalation scan=%+v", events)
	}
	if event, ok := decodeHubJobEscalationPayload(events[0].Payload); !ok || event.Reason != "needs captain" || event.OwnerLane != "lane-a" {
		t.Fatalf("escalation payload=%s", events[0].Payload)
	}
}
