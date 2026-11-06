package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIdlePromoteOffDelivery(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sessions []HubSession
	}{
		{"idle", receiverSession("w1:p1", "idle", "claude")},
		{"unsupported", receiverSession("w1:p1", "working", "unknown")},
		{"unreported", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, hk, closeServer := newFakeHandoffkeep(t)
			defer closeServer()
			now := time.Now().UTC().Truncate(time.Millisecond)
			policyPath := filepath.Join(t.TempDir(), "policy.yaml")
			policy := "default: inbox\ndeadlines: {inbox: 30m}\nlanes:\n  lane-a:\n    idle_promote: false\n"
			if err := os.WriteFile(policyPath, []byte(policy), 0600); err != nil {
				t.Fatal(err)
			}
			h, agent := quietHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), policyPath, hk, &now)
			setReceiverSession(h, "host-a", tc.sessions, now)
			h.relayLaneEvent(quietEvent("notice", "FYI notice"), agent)
			items, err := h.inbox.store.list("lane-a", true)
			if err != nil || len(items) != 1 || items[0].Class != "inbox" || items[0].DueAt != now.Add(30*time.Minute).UnixMilli() || len(agent.relays) != 0 {
				t.Fatalf("items=%+v err=%v relays=%d", items, err, len(agent.relays))
			}
			interrupt := quietEvent("decision", "choose a path")
			interrupt.NotifyType = "decision"
			h.relayLaneEvent(interrupt, agent)
			if len(agent.relays) != 1 {
				t.Fatalf("interrupt relays=%d", len(agent.relays))
			}
			<-agent.relays
			now = now.Add(31 * time.Minute)
			h.flushInbox()
			h.flushInbox()
			if len(agent.relays) != 1 {
				t.Fatalf("deadline relays=%d", len(agent.relays))
			}
			if got := <-agent.relays; !strings.Contains(got.Text, "1 unread inbox items") {
				t.Fatalf("deadline injection=%+v", got)
			}
		})
	}
}

func TestIdlePromotePolicyValidation(t *testing.T) {
	for _, tc := range []struct {
		name, policy string
		wantError    bool
	}{
		{"off and summary", "lanes: {lane-a: {idle_promote: false, inbox_summary: {digest_prefix: inbox/lane-a/digest/}}}", true},
		{"on and summary", "lanes: {lane-a: {idle_promote: true, inbox_summary: {digest_prefix: inbox/lane-a/digest/}}}", false},
		{"invalid boolean", "lanes: {lane-a: {idle_promote: nope}}", true},
		{"global option", "idle_promote: false", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseDeliveryPolicy([]byte("default: inbox\n" + tc.policy))
			if (err != nil) != tc.wantError {
				t.Fatalf("error=%v", err)
			}
			if tc.name == "off and summary" && err != nil && (!strings.Contains(err.Error(), "idle_promote") || !strings.Contains(err.Error(), "inbox_summary")) {
				t.Fatalf("unclear error: %v", err)
			}
		})
	}
}
