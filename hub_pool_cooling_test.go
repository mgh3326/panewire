package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQuotaStopCoolingRelayAndExpiry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(time.Hour)
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "job-a", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	event := map[string]any{"kind": "job.lost", "created_at": now.Format(time.RFC3339), "payload": map[string]any{"reason": "quota_stop", "pool": "devin-free", "reset_at": reset.Format(time.RFC3339)}}
	b, _ := json.Marshal(event)
	if err := os.WriteFile(filepath.Join(dir, "0001-job.lost.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	events := scanHubRelayEvents(root)
	if len(events) != 1 || !strings.Contains(events[0].Reason, "pool=devin-free reset_at=") {
		t.Fatalf("events: %+v", events)
	}
	client := &HubClient{}
	wire, ok := client.relayEventForSend(events[0])
	if !ok {
		t.Fatal("stop was not queued")
	}
	decoded, valid := decodeHubJobEscalationPayload(wire.Payload)
	if !valid {
		t.Fatalf("invalid hub payload: %s", wire.Payload)
	}
	h := &HubServer{}
	h.observePoolCooling(decoded.Reason, now)
	if got := h.fleetSnapshot(now).Cooling["devin-free"]; !got.Equal(reset) {
		t.Fatalf("reset %v", got)
	}
	if len(h.fleetSnapshot(reset).Cooling) != 0 {
		t.Fatal("cooling remains at reset")
	}
	h.observePoolCooling("quota_stop pool=devin-free", now)
	if got := h.fleetSnapshot(now).Cooling["devin-free"]; !got.Equal(now.Add(5 * time.Minute)) {
		t.Fatalf("default reset %v", got)
	}
}
