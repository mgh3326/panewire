package panewire

import (
	"path/filepath"
	"testing"
)

// A relay row the removed operator chat wrote is retired before any inbox
// migration or lane injection, even on an inbox-enabled lane. Replay must
// neither file it as an unread inbox item nor inject it.
func TestLegacyChatRowRetiresBeforeInboxMigration(t *testing.T) {
	fake, hk, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	h, err := NewHubServer(HubServerConfig{
		Tokens:             map[string]string{"operator": "op", "host-a": "node"},
		ReportRelayPath:    r20LanesFile(t, `{"lanes":{"operator-desk":{"machine":"host-a","pane":"w1:p1"}}}`),
		DeliveryPolicyPath: "default",
		InboxStorePath:     filepath.Join(t.TempDir(), "inbox.sqlite3"),
		handoffkeep:        hk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	agent := &hubAgent{relays: make(chan hubRelayInjectEvent, 2), persisted: make(chan hubRelayPersistedEvent, 2)}
	h.nodes["host-a"] = &hubNodeRecord{agent: agent}

	// "FYI" classifies as an inbox item under the default policy, so an
	// unretired row would be filed rather than injected.
	legacy := t961LaneRow(91, "operator-desk", "chat-7", t961tRecent(), 1)
	legacy.Text = "FYI"
	fake.seedUndelivered(legacy)
	h.replayRelayEvent(legacy, "startup")

	if got := fake.deliveredToFor(91); got != "hub/chat-terminal" {
		t.Fatalf("legacy chat row delivered_to=%q, want hub/chat-terminal", got)
	}
	if len(agent.relays) != 0 {
		t.Fatal("legacy chat row was injected")
	}
	counts, err := h.inbox.store.counts("operator-desk")
	if err != nil {
		t.Fatal(err)
	}
	if counts["operator-desk"] != 0 {
		t.Fatalf("legacy chat row was filed to the inbox: %v", counts)
	}
}
