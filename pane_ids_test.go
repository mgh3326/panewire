package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestHeartbeatPaneIDsDecode(t *testing.T) {
	raw := []byte(`{"status":"alive","checks":{},"sessions":[],"snapshot_status":"ok","truncated":true,"pane_ids":["w1:p1"],"pane_ids_complete":true}`)
	if _, ok := decodeHubHeartbeatPayload(raw); !ok {
		t.Fatal("heartbeat with complete pane IDs rejected")
	}
}

func TestHeartbeatPaneIDsLargeSnapshot(t *testing.T) {
	states := make([]HerdrAgentState, 106)
	for i := range states {
		states[i] = HerdrAgentState{PaneID: fmt.Sprintf("w1:p%03d", i), Status: "idle"}
	}
	payload := marshalHubHeartbeatWithSessions(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, SnapshotStatus: hubSnapshotStatusOK}, states)
	beat, ok := decodeHubHeartbeatPayload(payload)
	if !ok {
		t.Fatalf("heartbeat rejected: %s", payload)
	}
	if beat.PaneIDs == nil || len(*beat.PaneIDs) != 106 || beat.PaneIDsComplete == nil || !*beat.PaneIDsComplete || beat.Sessions == nil || len(*beat.Sessions) != 64 || !beat.Truncated {
		t.Fatalf("incorrect snapshot: %+v", beat)
	}
	envelope, _ := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: "heartbeat", Payload: payload}))
	if len(envelope) >= hubMaxMessageBytes {
		t.Fatalf("envelope too large: %d", len(envelope))
	}
}

func TestLaneReleasePaneIDs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ids   string
		sweep bool
		cap   bool
	}{
		{"complete", `,"pane_ids":["w1:p3"],"pane_ids_complete":true`, true, true},
		{"partial", `,"pane_ids":[],"pane_ids_complete":false`, false, false},
		{"old", ``, false, false},
	} {
		for _, mode := range []string{"sweep", "cap"} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				hub, clock, path, _ := laneReleaseHub(t, laneReleaseCapFile, func(c *HubServerConfig) { c.LanesNodeRowCap = 3 })
				laneReleaseAge(t, hub, clock, path)
				install := func() {
					raw := []byte(`{"status":"alive","checks":{},"sessions":[],"snapshot_status":"ok","truncated":true` + tc.ids + `}`)
					beat, ok := decodeHubHeartbeatPayload(raw)
					if !ok {
						t.Fatal("heartbeat rejected")
					}
					hub.mu.Lock()
					hub.nodes["machine-a"] = &hubNodeRecord{machineID: "machine-a", state: "connected", sessionSnapshot: hubSessionSnapshotRecordFromHeartbeat(beat, clock.at)}
					hub.mu.Unlock()
				}
				install()
				if mode == "sweep" {
					hub.releaseDeadLanes()
					clock.at = clock.at.Add(laneReleaseGrace)
					install()
					got := hub.releaseDeadLanes()
					if tc.sweep && !reflect.DeepEqual(got, []string{"dead-1", "dead-2"}) {
						t.Fatalf("released %v", got)
					}
					if !tc.sweep && len(got) != 0 {
						t.Fatalf("untrusted list released %v", got)
					}
				} else {
					writer := lanesNodePut(t, hub, "fresh-1", `{"machine":"machine-a","pane":"w1:p4"}`)
					want := http.StatusTooManyRequests
					if tc.cap {
						want = http.StatusCreated
					}
					if writer.Code != want {
						t.Fatalf("status=%d want=%d", writer.Code, want)
					}
				}
				for _, lane := range laneReleaseLanes(t, path) {
					if lane == "live-1" {
						return
					}
				}
				t.Fatal("pane present only in IDs was released")
			})
		}
	}
}

func TestHeartbeatPaneIDsBudget(t *testing.T) {
	states := make([]HerdrAgentState, 1000)
	for i := range states {
		states[i] = HerdrAgentState{PaneID: fmt.Sprintf("w1:p%04d-%s", i, strings.Repeat("x", 100)), Status: "idle"}
	}
	payload := marshalHubHeartbeatWithSessions(hubHeartbeatPayload{Status: "alive", Checks: map[string]HubCheckStatus{}, SnapshotStatus: hubSnapshotStatusOK}, states)
	beat, ok := decodeHubHeartbeatPayload(payload)
	if !ok || beat.PaneIDs == nil || beat.PaneIDsComplete == nil || *beat.PaneIDsComplete || len(*beat.PaneIDs) >= len(states) {
		t.Fatal("oversized list must be accepted as incomplete")
	}
	envelope, _ := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: "heartbeat", Payload: payload}))
	if len(envelope) >= hubMaxMessageBytes {
		t.Fatalf("envelope too large: %d", len(envelope))
	}
}
