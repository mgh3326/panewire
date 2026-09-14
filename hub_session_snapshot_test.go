package panewire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func task268HeartbeatClient(t *testing.T, collector func(context.Context) ([]HerdrAgentState, error)) *HubClient {
	t.Helper()
	client, err := NewHubClient(HubClientConfig{
		URL:                   "ws://fixture.invalid",
		MachineID:             "node-synthetic",
		Token:                 "token-synthetic",
		AllowInsecureForTests: true,
		PingInterval:          time.Hour,
		hostLoadCollector: func(context.Context) (HubHostLoad, error) {
			return HubHostLoad{}, errors.New("synthetic host load unavailable")
		},
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) {
			return nil, errors.New("synthetic host memory unavailable")
		},
		sessionCollector: collector,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func task268DecodeHeartbeat(t *testing.T, event hubClientEvent) (hubHeartbeatPayload, map[string]json.RawMessage) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &fields); err != nil {
		t.Fatalf("heartbeat JSON: %v", err)
	}
	heartbeat, ok := decodeHubHeartbeatPayload(event.Payload)
	if !ok {
		t.Fatalf("heartbeat decoder rejected node payload: %s", event.Payload)
	}
	return heartbeat, fields
}

func task268HeartbeatEnvelope(t *testing.T, heartbeat hubHeartbeatPayload) []byte {
	t.Helper()
	payload, err := json.Marshal(heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(map[string]any{"type": "event", "kind": "heartbeat", "payload": json.RawMessage(payload)})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func task268SendHeartbeat(t *testing.T, hub *HubServer, machineID string, agent *hubAgent, heartbeat hubHeartbeatPayload) {
	t.Helper()
	hub.handleAgentMessage(machineID, "fixture", agent, task268HeartbeatEnvelope(t, heartbeat))
}

func TestTask268AC1AgentListWireAllowlistDropsPrivateFields(t *testing.T) {
	fixtureAgents := []map[string]any{
		{
			"pane_id":           "workspace:pane-1",
			"workspace_id":      "workspace",
			"label":             "worker-synthetic",
			"agent_status":      "working",
			"revision":          7,
			"state_change_seq":  11,
			"interactive_ready": false,
			"cwd":               "/synthetic/secret-path",
			"foreground_cwd":    "/synthetic/foreground-secret",
			"terminal_title":    "Synthetic private terminal title",
			"terminal_id":       "terminal-synthetic",
			"agent_session":     "session-uuid-synthetic",
			"prompt":            "synthetic prompt secret",
			"transcript":        "synthetic transcript secret",
		},
		{
			"pane_id":          "workspace:pane-2",
			"workspace_id":     "workspace",
			"label":            "worker-synthetic-2",
			"agent_status":     "idle",
			"revision":         8,
			"state_change_seq": 12,
		},
	}
	socket := task268HerdrAgentListSocket(t, fixtureAgents)
	herdr, err := NewHerdrClient(socket)
	if err != nil {
		t.Fatal(err)
	}
	states, err := herdr.AgentStates(context.Background())
	herdr.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 || states[0].InteractiveReady == nil || *states[0].InteractiveReady {
		t.Fatalf("agent.list interactive readiness was not preserved as tri-state: %+v", states)
	}
	if states[1].InteractiveReady != nil {
		t.Fatalf("missing interactive_ready became false: %+v", states[1])
	}

	client := task268HeartbeatClient(t, func(context.Context) ([]HerdrAgentState, error) { return states, nil })
	event := client.heartbeatEvent(context.Background())
	payload := string(event.Payload)
	for _, forbidden := range []string{
		"prompt", "transcript", "output", "cwd", "foreground_cwd", "terminal_title", "terminal_id", "agent_session",
		"/synthetic/secret-path", "/synthetic/foreground-secret", "Synthetic private terminal title", "terminal-synthetic", "session-uuid-synthetic", "synthetic prompt secret", "synthetic transcript secret",
	} {
		if strings.Contains(payload, forbidden) {
			t.Fatalf("forbidden key/value %q crossed the heartbeat wire: %s", forbidden, payload)
		}
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(event.Payload, &wire); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(wire["sessions"], []byte(`"interactive_ready":false`)) {
		t.Fatalf("explicit false readiness was omitted: %s", wire["sessions"])
	}
	if bytes.Contains(wire["sessions"], []byte(`"interactive_ready":null`)) {
		t.Fatalf("missing readiness was serialized as null: %s", wire["sessions"])
	}
}

func TestTask268AC2SnapshotFailureIsNotEmptySnapshot(t *testing.T) {
	failed := task268HeartbeatClient(t, func(context.Context) ([]HerdrAgentState, error) {
		return nil, errors.New("synthetic herdr unavailable")
	})
	failedEvent := failed.heartbeatEvent(context.Background())
	var failedFields map[string]json.RawMessage
	if err := json.Unmarshal(failedEvent.Payload, &failedFields); err != nil {
		t.Fatal(err)
	}
	var failedHeartbeat hubHeartbeatPayload
	if err := json.Unmarshal(failedEvent.Payload, &failedHeartbeat); err != nil {
		t.Fatal(err)
	}
	if failedHeartbeat.SnapshotStatus != hubSnapshotStatusUnavailable {
		t.Fatalf("failed collector status=%q, want unavailable", failedHeartbeat.SnapshotStatus)
	}
	if rawSessions, exists := failedFields["sessions"]; exists && !isJSONNull(rawSessions) {
		t.Fatalf("failed collector was represented as a session array: sessions=%s heartbeat=%s", rawSessions, failedEvent.Payload)
	}
	if _, ok := decodeHubHeartbeatPayload(failedEvent.Payload); !ok {
		t.Fatalf("valid unavailable snapshot was rejected by the hub decoder: %s", failedEvent.Payload)
	}

	empty := task268HeartbeatClient(t, func(context.Context) ([]HerdrAgentState, error) {
		return []HerdrAgentState{}, nil
	})
	emptyHeartbeat, emptyFields := task268DecodeHeartbeat(t, empty.heartbeatEvent(context.Background()))
	if emptyHeartbeat.SnapshotStatus != hubSnapshotStatusOK || emptyHeartbeat.Sessions == nil || len(*emptyHeartbeat.Sessions) != 0 {
		t.Fatalf("empty successful collector=%+v", emptyHeartbeat)
	}
	if string(emptyFields["sessions"]) != "[]" {
		t.Fatalf("empty successful collector was not serialized as []: %s", emptyFields["sessions"])
	}
	if _, exists := emptyFields["snapshot_status"]; !exists {
		t.Fatal("successful snapshot omitted snapshot_status")
	}
	if _, exists := failedFields["sessions"]; exists == true {
		t.Fatal("unavailable and empty snapshots were not distinguishable")
	}

	legacy := task268HeartbeatClient(t, nil)
	_, legacyFields := task268DecodeHeartbeat(t, legacy.heartbeatEvent(context.Background()))
	for _, name := range []string{"sessions", "snapshot_status", "truncated"} {
		if _, exists := legacyFields[name]; exists {
			t.Fatalf("nil session hook changed legacy heartbeat with %q: %s", name, legacy.heartbeatEvent(context.Background()).Payload)
		}
	}
}

func TestTask268AC3SnapshotReplacesAndStalesFromHubClock(t *testing.T) {
	clock := time.Date(2026, 9, 14, 13, 55, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens:            map[string]string{"operator": "operator-synthetic", "node-synthetic": "token-synthetic"},
		Now:               func() time.Time { return clock },
		StaleAfter:        time.Minute,
		KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("node-synthetic", "version-synthetic", "fixture", agent, true)
	hub.nodes["node-synthetic"].lastKeepaliveSent = clock

	ready := false
	snapshotA := []HubSession{{PaneID: "workspace:pane-a", WorkspaceID: "workspace", Label: "worker-synthetic-a", Status: "working", InteractiveReady: &ready, Revision: 1, StateChangeSeq: 2}}
	task268SendHeartbeat(t, hub, "node-synthetic", agent, hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, Sessions: &snapshotA, SnapshotStatus: hubSnapshotStatusOK})
	first := hub.Nodes()
	if len(first) != 1 || first[0].SessionSnapshot == nil || len(first[0].SessionSnapshot.Sessions) != 1 || first[0].SessionSnapshot.Sessions[0].PaneID != "workspace:pane-a" {
		t.Fatalf("snapshot A was not stored: %+v", first)
	}
	if !first[0].SessionSnapshot.ReceivedAt.Equal(clock) || first[0].SessionSnapshot.Stale {
		t.Fatalf("snapshot A metadata=%+v, want hub clock and fresh", first[0].SessionSnapshot)
	}

	clock = clock.Add(10 * time.Second)
	snapshotB := []HubSession{{PaneID: "workspace:pane-b", WorkspaceID: "workspace", Label: "worker-synthetic-b", Status: "idle", Revision: 3, StateChangeSeq: 4}}
	task268SendHeartbeat(t, hub, "node-synthetic", agent, hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, Sessions: &snapshotB, SnapshotStatus: hubSnapshotStatusOK})
	second := hub.Nodes()
	if len(second) != 1 || second[0].SessionSnapshot == nil || len(second[0].SessionSnapshot.Sessions) != 1 || second[0].SessionSnapshot.Sessions[0].PaneID != "workspace:pane-b" {
		t.Fatalf("snapshot B did not replace A: %+v", second)
	}
	if !second[0].SessionSnapshot.ReceivedAt.Equal(clock) || second[0].SessionSnapshot.Sessions[0].PaneID == "workspace:pane-a" {
		t.Fatalf("snapshot B metadata=%+v", second[0].SessionSnapshot)
	}

	// A legacy heartbeat has no replacement target; it must retain B.
	task268SendHeartbeat(t, hub, "node-synthetic", agent, hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}})
	retained := hub.Nodes()
	if retained[0].SessionSnapshot == nil || retained[0].SessionSnapshot.Sessions[0].PaneID != "workspace:pane-b" || !retained[0].SessionSnapshot.ReceivedAt.Equal(clock) {
		t.Fatalf("legacy heartbeat erased the stored snapshot: %+v", retained[0].SessionSnapshot)
	}

	clock = clock.Add(time.Minute)
	hub.Sweep()
	stale := hub.Nodes()
	if len(stale) != 1 || stale[0].State != "stale" || stale[0].SessionSnapshot == nil || !stale[0].SessionSnapshot.Stale || stale[0].SessionSnapshot.Sessions[0].PaneID != "workspace:pane-b" {
		t.Fatalf("stale transition deleted or failed to mark snapshot: %+v", stale)
	}
}

func TestTask268AC4NodesEndpointNestsSnapshotProjection(t *testing.T) {
	clock := time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC)
	hub, err := NewHubServer(HubServerConfig{
		Tokens:            map[string]string{"operator": "operator-synthetic", "node-synthetic": "token-synthetic"},
		Now:               func() time.Time { return clock },
		KeepaliveInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("node-synthetic", "version-synthetic", "fixture", agent, false)
	hub.nodes["node-synthetic"].lastKeepaliveSent = clock
	sessions := []HubSession{{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Revision: 5, StateChangeSeq: 9}}
	task268SendHeartbeat(t, hub, "node-synthetic", agent, hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, Sessions: &sessions, SnapshotStatus: hubSnapshotStatusOK, Truncated: true})
	clock = clock.Add(time.Minute)
	hub.Sweep()

	request := httptest.NewRequest(http.MethodGet, "/v1/nodes", nil)
	request.Header.Set(hubAuthorizationHeader, "Bearer operator-synthetic")
	response := httptest.NewRecorder()
	hub.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /v1/nodes status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Nodes []map[string]json.RawMessage `json:"nodes"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || len(body.Nodes) != 1 {
		t.Fatalf("nodes response=%s err=%v", response.Body.String(), err)
	}
	var snapshot map[string]json.RawMessage
	if err := json.Unmarshal(body.Nodes[0]["session_snapshot"], &snapshot); err != nil {
		t.Fatalf("session_snapshot was not nested JSON: %v body=%s", err, response.Body.String())
	}
	for _, name := range []string{"sessions", "snapshot_status", "truncated", "received_at", "stale"} {
		if _, exists := snapshot[name]; !exists {
			t.Fatalf("nested snapshot omitted %q: %s", name, response.Body.String())
		}
	}
	if string(snapshot["snapshot_status"]) != `"ok"` || string(snapshot["truncated"]) != "true" || string(snapshot["stale"]) != "true" || !bytes.Contains(snapshot["sessions"], []byte("workspace:pane-1")) {
		t.Fatalf("nested snapshot values=%s", body.Nodes[0]["session_snapshot"])
	}
	if !bytes.Contains(snapshot["received_at"], []byte("2026-09-14T14:00:00Z")) {
		t.Fatalf("nested snapshot received_at=%s", snapshot["received_at"])
	}
}

// This frozen copy is the pre-task heartbeat vocabulary. It models the old
// hub decoder only for the hub-first rollout proof; production code is not
// changed to reintroduce the rejected legacy behavior.
var task268LegacyHeartbeatFields = [...]string{"status", "checks", "host_load", "load_error", "host_memory", "quota", "active_jobs", "holds_active"}

func task268LegacyHubAcceptsHeartbeat(payload []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || fields == nil {
		return false
	}
	for name := range fields {
		allowed := false
		for _, legacy := range task268LegacyHeartbeatFields {
			if name == legacy {
				allowed = true
				break
			}
		}
		if !allowed {
			return false
		}
	}
	return true
}

func TestTask268AC5HubFirstRolloutAndLegacyNodeCompatibility(t *testing.T) {
	client := task268HeartbeatClient(t, func(context.Context) ([]HerdrAgentState, error) {
		return []HerdrAgentState{{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle"}}, nil
	})
	newNodePayload := client.heartbeatEvent(context.Background()).Payload
	if task268LegacyHubAcceptsHeartbeat(newNodePayload) {
		t.Fatalf("hub-first rollout proof failed: frozen legacy allowlist accepted new node payload %s", newNodePayload)
	}

	legacyNodePayload, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{"service": HubCheckOK}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeHubHeartbeatPayload(legacyNodePayload); !ok {
		t.Fatalf("new hub rejected legacy node payload: %s", legacyNodePayload)
	}
}

func TestTask268AC6SessionCapByteLimitAndDeterministicTruncation(t *testing.T) {
	states := make([]HerdrAgentState, hubMaxHeartbeatSessions+5)
	for index := range states {
		states[index] = HerdrAgentState{
			PaneID:               fmt.Sprintf("workspace:pane-%03d", index),
			WorkspaceID:          "workspace",
			Label:                "worker-synthetic",
			Status:               "working",
			Revision:             int64(index + 1),
			SourceStateChangeSeq: int64(index + 1),
		}
	}
	// Deliberately reverse the collector input; the wire order must remain pane-id sorted.
	for left, right := 0, len(states)-1; left < right; left, right = left+1, right-1 {
		states[left], states[right] = states[right], states[left]
	}
	client := task268HeartbeatClient(t, func(context.Context) ([]HerdrAgentState, error) { return states, nil })
	event := client.heartbeatEvent(context.Background())
	heartbeat, _ := task268DecodeHeartbeat(t, event)
	if heartbeat.Sessions == nil || len(*heartbeat.Sessions) != hubMaxHeartbeatSessions || !heartbeat.Truncated {
		t.Fatalf("session cap result=%+v", heartbeat)
	}
	if len(event.Payload) >= hubMaxMessageBytes {
		t.Fatalf("heartbeat payload len=%d reached limit=%d", len(event.Payload), hubMaxMessageBytes)
	}
	if (*heartbeat.Sessions)[0].PaneID != "workspace:pane-000" || (*heartbeat.Sessions)[hubMaxHeartbeatSessions-1].PaneID != fmt.Sprintf("workspace:pane-%03d", hubMaxHeartbeatSessions-1) {
		t.Fatalf("session cap order is not deterministic: first=%q last=%q", (*heartbeat.Sessions)[0].PaneID, (*heartbeat.Sessions)[hubMaxHeartbeatSessions-1].PaneID)
	}

	sortedStates := append([]HerdrAgentState(nil), states...)
	// The collector's reversed input is sorted by the node projection. Compare
	// against another input order to pin deterministic selection and encoding.
	for left, right := 0, len(sortedStates)-1; left < right; left, right = left+1, right-1 {
		sortedStates[left], sortedStates[right] = sortedStates[right], sortedStates[left]
	}
	secondPayload := marshalHubHeartbeatWithSessions(hubHeartbeatPayload{
		Status:         "alive",
		Checks:         map[string]HubCheckStatus{},
		LoadError:      "synthetic host load unavailable",
		Quota:          unavailableHubQuotaSnapshot(),
		SnapshotStatus: hubSnapshotStatusOK,
	}, sortedStates)
	if !bytes.Equal(event.Payload, secondPayload) {
		t.Fatalf("session truncation was input-order dependent:\nfirst=%s\nsecond=%s", event.Payload, secondPayload)
	}

	short := task268HeartbeatClient(t, func(context.Context) ([]HerdrAgentState, error) {
		return []HerdrAgentState{{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle"}}, nil
	})
	shortHeartbeat, shortFields := task268DecodeHeartbeat(t, short.heartbeatEvent(context.Background()))
	if shortHeartbeat.Truncated || string(shortFields["truncated"]) == "true" {
		t.Fatalf("non-truncated snapshot advertised truncation: %s", short.heartbeatEvent(context.Background()).Payload)
	}

	overLimit := make([]HubSession, hubMaxHeartbeatSessions+1)
	for index := range overLimit {
		overLimit[index] = HubSession{PaneID: fmt.Sprintf("workspace:pane-%03d", index), WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle"}
	}
	rawOverLimit, err := json.Marshal(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, Sessions: &overLimit, SnapshotStatus: hubSnapshotStatusOK})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeHubHeartbeatPayload(rawOverLimit); ok {
		t.Fatalf("hub decoder accepted %d sessions above cap %d", len(overLimit), hubMaxHeartbeatSessions)
	}
}

func TestTask268B1HangingHerdrSocketYieldsUnavailable(t *testing.T) {
	directory, err := os.MkdirTemp("/tmp", "pw-task268-hang-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	listener, err := net.Listen("unix", filepath.Join(directory, "herdr.sock"))
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan net.Conn, 8)
	t.Cleanup(func() {
		_ = listener.Close()
		for {
			select {
			case conn := <-held:
				_ = conn.Close()
			default:
				return
			}
		}
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held <- conn
		}
	}()

	client := task268HeartbeatClient(t, hubSessionSnapshotHook(listener.Addr().String()))
	done := make(chan hubClientEvent, 1)
	go func() { done <- client.heartbeatEvent(context.Background()) }()
	select {
	case event := <-done:
		heartbeat, fields := task268DecodeHeartbeat(t, event)
		if heartbeat.SnapshotStatus != hubSnapshotStatusUnavailable {
			t.Fatalf("hung herdr socket status=%q, want unavailable: %s", heartbeat.SnapshotStatus, event.Payload)
		}
		if heartbeat.Sessions != nil {
			t.Fatalf("hung herdr socket was represented as a session array: %s", event.Payload)
		}
		if raw, exists := fields["sessions"]; exists && !isJSONNull(raw) {
			t.Fatalf("hung herdr socket serialized sessions=%s, want absent/null: %s", raw, event.Payload)
		}
	case <-time.After(4 * hubSessionSnapshotTimeout):
		t.Fatalf("heartbeatEvent still blocked after %s on a hung herdr socket (hubSessionSnapshotTimeout=%s)", 4*hubSessionSnapshotTimeout, hubSessionSnapshotTimeout)
	}
}

func task268HerdrAgentListSocket(t *testing.T, agents []map[string]any) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "pw-task268-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	listener, err := net.Listen("unix", filepath.Join(directory, "herdr.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			go func(connection net.Conn) {
				defer connection.Close()
				scanner := bufio.NewScanner(connection)
				for scanner.Scan() {
					var request map[string]any
					if json.Unmarshal(scanner.Bytes(), &request) != nil {
						continue
					}
					result := any(map[string]any{"tabs": []any{}})
					if request["method"] == "agent.list" {
						result = map[string]any{"agents": agents}
					}
					response, err := json.Marshal(map[string]any{"id": request["id"], "result": result})
					if err != nil {
						return
					}
					_, _ = fmt.Fprintf(connection, "%s\n", response)
				}
			}(connection)
		}
	}()
	return listener.Addr().String()
}
