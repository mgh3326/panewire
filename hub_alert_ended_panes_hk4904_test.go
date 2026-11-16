package panewire

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// endedPanesHeartbeat sends one heartbeat carrying the node sweep's count of
// ended-job panes still open past a day (hubFeatureEndedPanes, hk 4904).
func endedPanesHeartbeat(t *testing.T, hub *HubServer, machineID string, agent *hubAgent, open int) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"type": "event", "kind": "heartbeat",
		"payload": map[string]any{"status": "alive", "ended_panes": open},
	})
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage(machineID, "hk4904", agent, payload)
}

// The hub raises one alarm per host when a node reports finished panes it still
// holds, and closes the episode when the count drops to zero. Both go through
// the shared two-observation debounce, and a second host has its own alarm.
func TestEndedPanesAlarmRaisesOncePerHostAndClears(t *testing.T) {
	clock := time.Date(2026, 11, 16, 5, 0, 0, 0, time.UTC)
	capture := newHubAlertLog(nil)
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": r6OperatorToken, "node-a": r6NodeAToken, "node-b": r6NodeBToken},
		Now:    func() time.Time { return clock },
		Logger: capture.Logger(),
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.connect("node-a", "hk4904", "fixture", agent, false)
	endedPanesHeartbeat(t, hub, "node-a", agent, 14)
	if got := capture.Alerts(); len(got) != 0 {
		t.Fatalf("one observation alerted: %+v", got)
	}
	endedPanesHeartbeat(t, hub, "node-a", agent, 14)
	alerts := capture.Alerts()
	if len(alerts) != 1 || alerts[0].MachineID != "node-a" || alerts[0].Reason != hubAlertReasonEndedPanes || alerts[0].Recovery {
		t.Fatalf("alerts = %+v", alerts)
	}
	// One alarm per host: another count above zero is the same episode.
	endedPanesHeartbeat(t, hub, "node-a", agent, 9)
	if got := capture.Alerts(); len(got) != 1 {
		t.Fatalf("a second alarm was raised for one host: %+v", got)
	}
	// A second host raises its own.
	other := &hubAgent{}
	hub.connect("node-b", "hk4904", "fixture", other, false)
	endedPanesHeartbeat(t, hub, "node-b", other, 1)
	endedPanesHeartbeat(t, hub, "node-b", other, 1)
	alerts = capture.Alerts()
	if len(alerts) != 2 || alerts[1].MachineID != "node-b" || alerts[1].Reason != hubAlertReasonEndedPanes || alerts[1].Recovery {
		t.Fatalf("a second host did not raise its own alarm: %+v", alerts)
	}
	// Two zero observations clear a host; one more zero says nothing new.
	endedPanesHeartbeat(t, hub, "node-a", agent, 0)
	endedPanesHeartbeat(t, hub, "node-a", agent, 0)
	alerts = capture.Alerts()
	if len(alerts) != 3 || !alerts[2].Recovery || alerts[2].MachineID != "node-a" || alerts[2].Reason != hubAlertReasonEndedPanes {
		t.Fatalf("the cleared host was not reported recovered: %+v", alerts)
	}
	endedPanesHeartbeat(t, hub, "node-a", agent, 0)
	if got := capture.Alerts(); len(got) != 3 {
		t.Fatalf("a cleared host alerted again: %+v", got)
	}
}

// The heartbeat is a closed payload: a count outside the allowed range drops the
// heartbeat, and a heartbeat without the count leaves the host's state alone.
func TestEndedPanesHeartbeatFieldIsBounded(t *testing.T) {
	cases := []struct {
		name  string
		field string
		valid bool
	}{
		{"zero", `"ended_panes":0`, true},
		{"a count", `"ended_panes":14`, true},
		{"negative", `"ended_panes":-1`, false},
		{"over the bound", fmt.Sprintf(`"ended_panes":%d`, hubEndedPanesMax+1), false},
		{"null", `"ended_panes":null`, false},
	}
	for _, c := range cases {
		heartbeat, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive",` + c.field + `}`))
		if ok != c.valid {
			t.Fatalf("%s: accepted=%v want %v", c.name, ok, c.valid)
		}
		if ok && (heartbeat.EndedPanes == nil || *heartbeat.EndedPanes < 0) {
			t.Fatalf("%s: count = %v", c.name, heartbeat.EndedPanes)
		}
	}
	if heartbeat, ok := decodeHubHeartbeatPayload([]byte(`{"status":"alive"}`)); !ok || heartbeat.EndedPanes != nil {
		t.Fatalf("missing count = %v accepted=%v", heartbeat.EndedPanes, ok)
	}
}

// The count travels only to a hub that advertised hubFeatureEndedPanes — an
// older hub rejects a heartbeat field it does not know — and only once a sweep
// pass has measured it. The hub's feature frame is what opens that gate.
func TestEndedPanesCountTravelsOnlyToAHubThatReadsIt(t *testing.T) {
	if !hasName(hubFeaturesFrame().Features, hubFeatureEndedPanes) {
		t.Fatalf("hub.features does not list %s", hubFeatureEndedPanes)
	}
	client := &HubClient{}
	if got := client.endedPanesForHeartbeat(); got != nil {
		t.Fatalf("an unmeasured node reported %v", *got)
	}
	client.endedPanesOpen.Store(3)
	client.endedPanesMeasured.Store(true)
	if got := client.endedPanesForHeartbeat(); got != nil {
		t.Fatalf("a count went to a hub that did not advertise it: %v", *got)
	}
	peer := &hubClientConnection{}
	peer.acceptsEndedPanes.Store(true)
	client.signalsPeer.Store(peer)
	if got := client.endedPanesForHeartbeat(); got == nil || *got != 3 {
		t.Fatalf("count = %v", got)
	}
}
