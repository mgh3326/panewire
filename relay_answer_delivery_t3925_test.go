package panewire

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRelayClaudeAnswerSettlesIntoQueue_t3925(t *testing.T) {
	text := task687Text(767025, "[event] answer: proceed")
	pending := task626Claude([]string{"● Working"}, "❯ "+text)
	queued := task626Claude([]string{"● Working", "❯ " + text, "  ctrl+enter to send now"}, "❯ Press up to edit queued messages")
	_, calls := task683FakeHerdr(t, "claude", []string{task626ClaudeEmpty, pending, pending, queued}, []string{"", "", "", queued})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectQueued {
		t.Fatalf("answer outcome=%+v, want queued after render settles", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 1 {
		t.Fatalf("calls=%v, want one prompt and one return", got)
	}
}

func TestRelayOmpAnswerPresendReadFailureNamesCause_t3925(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-omp-read-failure", "order", "answer")
	dir := t.TempDir()
	installFakeHerdr(t, dir, "#!/bin/sh\ncase \"$2\" in\nget) echo '{\"result\":{\"agent\":{\"agent\":\"omp\",\"agent_status\":\"idle\"}}}' ;;\nread) exit 1 ;;\nesac\n")
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))
	_, payloads := rig.drainEvents()
	if got := payloads["relay.dropped"].Reason; got != "pane_read_failed" {
		t.Fatalf("drop reason=%q, want pane_read_failed", got)
	}
}

func TestRelayAnswerUncertainNamesAction_t3925(t *testing.T) {
	for _, policy := range []string{relayDeliverDirective, "now", "idle"} {
		t.Run(policy, func(t *testing.T) {
			events := make(chan hubClientEvent, 8)
			client := &HubClient{relayInjectVerdict: func(context.Context, string, string, []string) relayInjectResult {
				return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: "omp", Evidence: "return_failed"}
			}}
			client.setRelayEmitter(func(event hubClientEvent) { events <- event })
			client.relayBusyManager().deliver(context.Background(), []relayHeld{{Pane: "w1:p1", Lane: "worker", JobID: "job", EventID: 3925, Text: "answer", DeliverPolicy: policy, HeldSince: time.Now()}}, false)
			close(events)
			for event := range events {
				if event.Kind == "relay.dropped" {
					var drop relayDroppedPayload
					if err := json.Unmarshal(event.Payload, &drop); err != nil {
						t.Fatal(err)
					}
					if !strings.Contains(drop.Reason, "check_pane") {
						t.Fatalf("drop reason=%q, want an actionable check_pane reason", drop.Reason)
					}
					return
				}
			}
			t.Fatal("answer produced no terminal reason")
		})
	}
}

func TestRelayPriorUncertainSendCannotBecomeReadFailure_t3925(t *testing.T) {
	calls := 0
	events := make(chan hubClientEvent, 8)
	client := &HubClient{relayInjectVerdict: func(context.Context, string, string, []string) relayInjectResult {
		calls++
		if calls == 1 {
			return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: "claude", Evidence: "unproven:composer_empty"}
		}
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: "claude", Evidence: "presend:unproven:read_failed"}
	}}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().deliver(context.Background(), []relayHeld{{Pane: "w1:p1", Lane: "worker", JobID: "job", EventID: 3926, Text: "answer", DeliverPolicy: relayDeliverDirective, HeldSince: time.Now()}}, false)
	close(events)
	for event := range events {
		if event.Kind == "relay.dropped" {
			var drop relayDroppedPayload
			if err := json.Unmarshal(event.Payload, &drop); err != nil {
				t.Fatal(err)
			}
			if drop.Reason != "delivery_uncertain_check_pane" {
				t.Fatalf("drop reason=%q after a possibly sent first attempt", drop.Reason)
			}
			return
		}
	}
	t.Fatal("answer produced no terminal reason")
}

func TestRelayOmpUnprovenAnswerDoesNotRetype_t3925(t *testing.T) {
	previous := relayObserveWindow
	relayObserveWindow = 50 * time.Millisecond
	t.Cleanup(func() { relayObserveWindow = previous })
	sc := t3276WorkingUnprovable()["omp-working"]
	rig := t3276Start(t)
	directive := rig.send("ev-omp-uncertain", "decision", "answer")
	pane := t3276InstallPane(t, sc, t3276TypedFor(directive), false)
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))
	if got := pane.calls(t, "prompt"); got != 1 {
		t.Fatalf("unproven answer typed %d times; want once", got)
	}
}

func TestRelayWaitingDecisionAnswersReportFailure_t3925(t *testing.T) {
	for _, notify := range []string{"order", "decision"} {
		t.Run(notify, func(t *testing.T) {
			rig := t3276Start(t)
			directive := rig.send("ev-waiting-"+notify, notify, "answer to the waiting job")
			rig.node.relayInjectVerdict = func(context.Context, string, string, []string) relayInjectResult {
				return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: "omp", Evidence: "return_failed"}
			}
			rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))
			_, payloads := rig.drainEvents()
			if got := payloads["relay.dropped"].Reason; got != "delivery_uncertain_check_pane" {
				t.Fatalf("%s drop reason=%q", notify, got)
			}
			got := rig.escalations()
			if len(got) != 1 || !strings.Contains(got[0].Text, "Check the pane before resending") {
				t.Fatalf("%s escalation=%+v", notify, got)
			}
		})
	}
}
