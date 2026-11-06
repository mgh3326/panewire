package panewire

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// A proven delivery leaves one INFO line naming the event by id, so the hub
// journal answers "was it delivered" without a database read. No text is
// logged.
func TestRelayDeliveredLeavesOneInfoLine(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "fixture-operator-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	sink := captureHubLog(hub)
	event := hubJobEventPayload{JobID: "lane-event-x", OwnerLane: "worker-a", EventID: "evt-delivered-1", Text: "secret-looking body ghp_notreal"}
	hub.markRelayEventDelivered(relayPending{machine: "machine-a", pane: "w1:p2", kind: "lane.event", event: event})
	var found map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
		var line map[string]any
		if json.Unmarshal([]byte(raw), &line) == nil && line["msg"] == "relay delivered" && line["route"] == "ack" {
			if found != nil {
				t.Fatalf("more than one delivered line: %s", sink.String())
			}
			found = line
		}
	}
	if found == nil || found["level"] != "INFO" || found["event_id"] != "evt-delivered-1" || found["lane"] != "worker-a" || found["pane"] != "w1:p2" || found["machine"] != "machine-a" {
		t.Fatalf("delivered line = %v (log %s)", found, sink.String())
	}
	if strings.Contains(sink.String(), "ghp_notreal") {
		t.Fatal("event text reached the log")
	}
}
