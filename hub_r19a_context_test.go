package panewire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestR19aRelayContextAndLanesJSONAuthority(t *testing.T) {
	event := hubJobEventPayload{Label: "worker", Host: "host-a", Question: strings.Repeat("q", 300), ReportPath: "report.md"}
	escalated := relayTextForKind("job.escalate", event)
	if !strings.Contains(escalated, "[escalate] worker (host-a) :: Q: ") || !strings.Contains(escalated, strings.Repeat("q", 240)) || !strings.Contains(escalated, " … → report.md") || !strings.HasSuffix(escalated, " (전문: report.md)") || len(escalated) > 512 {
		t.Fatalf("escalation text=%q", escalated)
	}
	joined := relayTextForKind("job.joined", hubJobEventPayload{Label: "captain", PR: "#32", Head: "0123456789abcdef", ReportPath: "report.md"})
	if joined != "[joined] captain :: PR #32 @ 012345678 → report.md" {
		t.Fatalf("joined text=%q", joined)
	}
	var inbox hubInboxEvent
	if err := json.Unmarshal([]byte(`{"kind":"job.escalate","owner_lane":"captain","question":"why","pane_id":"flat","payload":{"question":"nested","pane_id":"nested","pr":"#32","head":"0123456789"}}`), &inbox); err != nil {
		t.Fatal(err)
	}
	if inbox.question() != "why" || inbox.paneID() != "flat" || inbox.pr() != "#32" || inbox.head() != "0123456789" {
		t.Fatalf("inbox=%+v", inbox)
	}
}

func TestR19aUnavailablePlacementSerializesNull(t *testing.T) {
	b, err := json.Marshal(PlacementResult{Decision: "unavailable", Reason: "unavailable", Source: "hub-only"})
	if err != nil || !strings.Contains(string(b), `"decision":null`) {
		t.Fatalf("placement=%s err=%v", b, err)
	}
}

func TestR19aRelayAckTimeoutIsOneShotPerJob(t *testing.T) {
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "operator-token", "host-a": "node-token"}, RelayAckTimeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sub := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 2)}
	h.subscribers[sub] = struct{}{}
	h.startRelayAck("job-a", "host-a", "pane-a")
	h.expireRelayAck("job-a")
	h.startRelayAck("job-a", "host-a", "pane-a")
	h.expireRelayAck("job-a")
	if got := <-sub.messages; got.event == nil || got.event.Kind != "relay.unconfirmed" {
		t.Fatalf("first timeout=%+v", got)
	}
	select {
	case got := <-sub.messages:
		t.Fatalf("second timeout=%+v", got)
	default:
	}
}
