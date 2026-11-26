package panewire

// hk 6820: a send herdr rejects leaves devin's pane in doubt and any other
// harness's untouched (devinRelayInjectGuarded). These tests run the node's own
// delivery with a rejecting verdict and hand the hub the frames it emitted.

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// t6820RejectedFrames are the frames a node emits for one idle-policy row whose
// every send herdr rejects on harness.
func t6820RejectedFrames(t *testing.T, d hubRelayInjectEvent, harness string) []hubClientEvent {
	t.Helper()
	events := make(chan hubClientEvent, 16)
	node := &HubClient{relayInjectVerdict: func(context.Context, string, string, []string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectRetryable, Harness: harness, Evidence: "prompt_failed"}
	}}
	node.setRelayEmitter(func(event hubClientEvent) { events <- event })
	node.relayBusyManager().deliver(context.Background(), []relayHeld{{Pane: d.Pane, Lane: d.Lane, JobID: d.JobID, EventID: d.EventID, Text: d.Text, HeldSince: time.Now()}}, false)
	close(events)
	var frames []hubClientEvent
	for event := range events {
		frames = append(frames, event)
	}
	if len(frames) < 2 || frames[len(frames)-1].Kind != "relay.dropped" {
		t.Fatalf("node frames for %q: %+v", harness, frames)
	}
	return frames
}

func t6820Frame(t *testing.T, rig *t6384Rig, kind string, payload []byte) {
	t.Helper()
	t650Send(t, rig.hub, "host-a", rig.agent, kind, json.RawMessage(payload))
}

// t6820LabelledCopies runs passes past every backoff, answering each copy as
// the node would, and counts the copies queued.
func t6820LabelledCopies(t *testing.T, rig *t6384Rig, first hubRelayInjectEvent, harness string) int {
	t.Helper()
	copies, d := 0, first
	for i, wait := range []time.Duration{0, time.Minute, 5 * time.Minute, 15 * time.Minute, obligationParkRecheck} {
		rig.hub.redeliverWorkingExpiredOnIdle("host-a", t6820Wake(d.Pane, uint64(i+1)))
		rig.pass(wait)
		for _, queued := range t3426Injected(rig.agent) {
			copies, d = copies+1, queued
			for _, frame := range t6820RejectedFrames(t, d, harness) {
				t6820Frame(t, rig, frame.Kind, frame.Payload)
			}
		}
	}
	return copies
}

// A devin row whose sends herdr rejected may hold part of the text: one
// labelled copy, ever, and no revive. A harness whose rejected send types
// nothing keeps the budget. Killed by: reading a rejected send as safe on
// every harness; reading it as maybe typed on every harness.
func TestRejectedSendIsMaybeTypedOnDevinOnly(t *testing.T) {
	for harness, want := range map[string]int{"devin": 1, "": 1, "claude": relayReplayMaxAttempts - 1} {
		t.Run("harness="+harness, func(t *testing.T) {
			rig := newT6384Rig(t)
			d := rig.send("lane-a", "rejected-send")
			for _, frame := range t6820RejectedFrames(t, d, harness) {
				t6820Frame(t, rig, frame.Kind, frame.Payload)
			}
			if got := t6820LabelledCopies(t, rig, d, harness); got != want {
				t.Fatalf("copies=%d, want %d (obligation %+v)", got, want, rig.obligation(d.EventID))
			}
			if c := t6820CountsOf(t, rig, d.EventID); c.revived != 0 || c.requested != 0 {
				t.Fatalf("a rejected send was revived: %+v", c)
			}
		})
	}
}

// The later drops of the same delivery do not make devin's text safe: a failed
// read on the node's retry, or the drop alone with its report lost. Killed by:
// letting that drop settle the report; ignoring the harness when the drop
// expires the ack window.
func TestDevinRejectedSendStaysMaybeTyped(t *testing.T) {
	report := func(t *testing.T, rig *t6384Rig, d hubRelayInjectEvent, reason string) {
		t650Send(t, rig.hub, "host-a", rig.agent, "relay.unconfirmed", relayAckPayload{JobID: d.JobID, Pane: d.Pane, OriginalEventID: d.EventID, Reason: reason})
	}
	drop := func(t *testing.T, rig *t6384Rig, d hubRelayInjectEvent, reason string) {
		t650Send(t, rig.hub, "host-a", rig.agent, "relay.dropped", relayDroppedPayload{JobID: d.JobID, Pane: d.Pane, Lane: d.Lane, OriginalEventID: d.EventID, Reason: reason, Harness: "devin"})
	}
	for name, run := range map[string]func(*testing.T, *t6384Rig, hubRelayInjectEvent){
		"then a failed read": func(t *testing.T, rig *t6384Rig, d hubRelayInjectEvent) {
			report(t, rig, d, "prompt_failed")
			report(t, rig, d, "maybe_in_pane presend:unproven:read_failed")
			drop(t, rig, d, relayDroppedPaneReadFailed)
		},
		"drop only": func(t *testing.T, rig *t6384Rig, d hubRelayInjectEvent) {
			drop(t, rig, d, relayDroppedRetriesExhausted)
			eventually(t, 2*time.Second, "the dropped window to close", func() bool {
				rig.hub.mu.Lock()
				defer rig.hub.mu.Unlock()
				_, open := rig.hub.relayControl.relayPending[relayPendingKey(d.EventID, d.JobID)]
				return !open
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newT6384Rig(t)
			d := rig.send("lane-a", "devin-doubt")
			run(t, rig, d)
			rig.pass(0)
			copy := rig.injected()
			report(t, rig, copy, "prompt_failed")
			drop(t, rig, copy, relayDroppedRetriesExhausted)
			t6820NoMoreCopiesAfter(t, rig, copy.Pane)
		})
	}
}

func t6820NoMoreCopiesAfter(t *testing.T, rig *t6384Rig, pane string) {
	t.Helper()
	for i, wait := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, obligationParkRecheck} {
		rig.hub.redeliverWorkingExpiredOnIdle("host-a", t6820Wake(pane, uint64(i+1)))
		rig.pass(wait)
		if got := t3426Injected(rig.agent); len(got) != 0 {
			t.Fatalf("a further copy was queued after %v: %+v", wait, got)
		}
	}
}

// A budget copy whose send was rejected is held by the node past the backoff.
// The row waits for the drop and then retries on. Killed by: ending the row on
// the report, ahead of its drop.
func TestHeldRejectedCopyWaitsForItsDrop(t *testing.T) {
	rig := newT6384Rig(t)
	d := rig.send("lane-a", "held-rejection")
	frames := t6820RejectedFrames(t, d, "claude")
	for _, frame := range frames {
		t6820Frame(t, rig, frame.Kind, frame.Payload)
	}
	rig.pass(0)
	copy := rig.injected()
	frames = t6820RejectedFrames(t, copy, "claude")
	t6820Frame(t, rig, frames[0].Kind, frames[0].Payload)
	t650Send(t, rig.hub, "host-a", rig.agent, "relay.held", relayHeldPayload{EventID: copy.EventID, JobID: copy.JobID, Pane: copy.Pane, Lane: "lane-a",
		Reason: "retry", HeldSince: rig.clock.Now().Format(time.RFC3339Nano), DeliverPolicy: relayDeliverDirective})
	rig.pass(5 * time.Minute)
	if o := rig.obligation(d.EventID); o.State != obligationOwed || len(rig.agent.relays) != 0 {
		t.Fatalf("while the node holds the rejected copy: %+v queued=%d", o, len(rig.agent.relays))
	}
	for _, frame := range frames[1:] {
		t6820Frame(t, rig, frame.Kind, frame.Payload)
	}
	rig.pass(5 * time.Minute)
	if next := rig.injected(); next.EventID != d.EventID {
		t.Fatalf("next copy=%+v", next)
	}
}
