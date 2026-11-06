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

func TestOwnerlessQuotaStopAcknowledgedOnce(t *testing.T) {
	_, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub, agent := r20t5Hub(t, r20t7Lanes, hk, 8)
	inbox := t.TempDir()
	r20t7WriteEvent(t, inbox, "cooling-job", "00001-job.lost.json", `{"kind":"job.lost","payload":{"reason":"quota_stop","pool":"devin-free"}}`)
	store := NewMemoryStore(t)
	defer store.Close()
	node := r20Node(inbox, store)
	events := r20Sent(node)
	if len(events) != 1 {
		t.Fatalf("events: %v", events)
	}
	_, acks := r20t7Deliver(t, hub, agent, node, events[0])
	if len(acks) != 1 || acks[0].Type != "pool.cooling.ack" {
		t.Fatalf("acks: %+v", acks)
	}
	raw, err := json.Marshal(acks[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := parseHubOutbound(raw); !ok {
		t.Fatalf("ack cannot be decoded: %s", raw)
	}
	routed := acks[0]
	routed.Type = "relay.persisted"
	raw, _ = json.Marshal(routed)
	if _, ok := parseHubOutbound(raw); !ok {
		t.Fatalf("routed job.lost ack cannot be decoded: %s", raw)
	}
	restarted := r20t7Node(inbox, store, time.Now().Add(2*relayOutboxBackoff))
	if again := restarted.jobCompletionEvents(); len(again) != 0 {
		t.Fatalf("resent: %+v", again)
	}
}

func TestMalformedQuotaStopResetFallsBack(t *testing.T) {
	now := time.Now().UTC()
	h := &HubServer{}
	h.observePoolCooling("quota_stop pool=devin-free reset_at=invalid", now)
	if got := h.fleetSnapshot(now).Cooling["devin-free"]; !got.Equal(now.Add(5 * time.Minute)) {
		t.Fatalf("reset %v", got)
	}
}
