package panewire

// hk 6338: an operator HTTP ingress event is exempt from the inbox-only rule
// (hk 3602) only when it asks for immediate delivery (deliver=now or
// delivery_class=interrupt) or carries no notify_type. A typed ingress event
// that asks for neither is filed like a node-relayed one: unread in the lane's
// inbox, never typed, with the coalesced one-line wake when it is wake-worthy.

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func t6338Ingress(lane, eventID, text string, extra map[string]any) map[string]any {
	body := map[string]any{"kind": "lane.event", "lane": lane, "event_id": eventID, "text": text, "label": "operator-web"}
	for key, value := range extra {
		body[key] = value
	}
	return body
}

// t6338Post posts one operator ingress event and fails unless the hub accepted it.
func t6338Post(t *testing.T, r *t3602Rig, body map[string]any) {
	t.Helper()
	if writer := r24PostIngress(t, r.h, "op", body); writer.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
}

// t6338AwaitTyped waits for the body text to be typed into pane. The wait ends
// on the injection the assertion needs, not on an earlier step.
func t6338AwaitTyped(t *testing.T, r *t3602Rig, pane, text string) {
	t.Helper()
	var got []hubRelayInjectEvent
	eventually(t, 5*time.Second, "the body typed into "+pane, func() bool {
		got = append(got, t3426Injected(r.agent)...)
		for _, injected := range got {
			if injected.Pane == pane && strings.Contains(injected.Text, text) {
				return true
			}
		}
		return false
	})
}

// (a) A typed ingress info to an idle design lane is filed unread, not typed,
// and -- being pure info -- wakes nothing.
func TestIngressInfoToDesignLaneIsFiledNotTyped(t *testing.T) {
	r := newT3602Rig(t, "default")
	const eventID = "t6338-info"
	t6338Post(t, r, t6338Ingress(t3602Design, eventID, "hk 6338 routine notice", map[string]any{"notify_type": "info"}))
	if got := t3426Injected(r.agent); len(got) != 0 {
		t.Fatalf("typed into the design pane: %+v", got)
	}
	item := t3426Item(t, r.h, t3602Design, "lane.event", laneEventTransportID(t3602Design, eventID))
	if item.Class != "inbox" || item.ReadAt != 0 || item.AckedAt != 0 || item.DueAt != 0 || item.DigestAt != 0 || item.HoldUntil != 0 {
		t.Fatalf("stored as %+v, want an unread inbox item with no deadline, digest or hold", item)
	}
	if ev := r.wakeEvents(); len(ev) != 0 {
		t.Fatalf("pure info recorded wakes %+v", ev)
	}
}

// (b) An ingress info that asks for immediate delivery is still typed, as it
// is today: deliver=now, and separately delivery_class=interrupt.
func TestIngressImmediateInfoStaysTypedOnDesignLane(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra map[string]any
	}{
		{"deliver-now", map[string]any{"notify_type": "info", "deliver": "now"}},
		{"delivery-class-interrupt", map[string]any{"notify_type": "info", "delivery_class": "interrupt"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newT3602Rig(t, "default")
			text := "hk 6338 immediate " + tc.name
			t6338Post(t, r, t6338Ingress(t3602Design, "t6338-"+tc.name, text, tc.extra))
			t6338AwaitTyped(t, r, "w1:design", text)
		})
	}
}

// (c) An untyped ingress event -- the operator's own words -- is typed as it
// is today.
func TestIngressUntypedStaysTypedOnDesignLane(t *testing.T) {
	r := newT3602Rig(t, "default")
	text := "hk 6338 operator words"
	t6338Post(t, r, t6338Ingress(t3602Design, "t6338-untyped", text, nil))
	t6338AwaitTyped(t, r, "w1:design", text)
}

// (d) A typed ingress order (wake-worthy) to a design lane is filed unread,
// never typed, and produces the lane's one-line wake at once.
func TestIngressOrderToDesignLaneIsFiledAndWakes(t *testing.T) {
	r := newT3602Rig(t, "default")
	const eventID = "t6338-order"
	const text = "ORDER hk 6338: hold the release"
	t6338Post(t, r, t6338Ingress(t3602Design, eventID, text, map[string]any{"notify_type": "order"}))
	t3602OneWake(t, t3426Injected(r.agent), "w1:design", t3602Wake(t3602Design, 1, "order"))
	item := t3426Item(t, r.h, t3602Design, "lane.event", laneEventTransportID(t3602Design, eventID))
	if item.Class != "inbox" || item.ReadAt != 0 || item.AckedAt != 0 {
		t.Fatalf("stored as %+v, want an unread inbox item", item)
	}
	if ev := r.wakeEvents(); len(ev) != 1 || ev[0].Outcome != "sent" || ev[0].Job != t3602Design || ev[0].Reason != "design" {
		t.Fatalf("inbox_wake fleet events=%+v, want one sent for the design lane", ev)
	}
}

// (e) A lane that is not inbox-only receives a typed ingress info as it does
// today: typed into its pane.
func TestIngressWorkerLaneKeepsTypedDelivery(t *testing.T) {
	r := newT3602Rig(t, "default")
	text := "hk 6338 worker-lane notice"
	t6338Post(t, r, t6338Ingress("worker-3404", "t6338-worker", text, map[string]any{"notify_type": "info"}))
	t6338AwaitTyped(t, r, "w1:worker", text)
}

// The rule itself: only an ingress event that asks for immediate delivery or
// is untyped is exempt. A node-relayed event is never exempt through this
// function, whatever its deliver or class.
func TestInboxOnlyExemptIngressRule(t *testing.T) {
	for _, tc := range []struct {
		name string
		e    hubJobEventPayload
		want bool
	}{
		{"ingress untyped", hubJobEventPayload{operatorIngress: true}, true},
		{"ingress typed info", hubJobEventPayload{operatorIngress: true, NotifyType: "info"}, false},
		{"ingress typed order", hubJobEventPayload{operatorIngress: true, NotifyType: "order"}, false},
		{"ingress info deliver now", hubJobEventPayload{operatorIngress: true, NotifyType: "info", DeliverPolicy: "now"}, true},
		{"ingress info delivery class interrupt", hubJobEventPayload{operatorIngress: true, NotifyType: "info", DeliveryClass: "interrupt"}, true},
		{"ingress info delivery class inbox", hubJobEventPayload{operatorIngress: true, NotifyType: "info", DeliveryClass: "inbox"}, false},
		{"ingress info deliver idle", hubJobEventPayload{operatorIngress: true, NotifyType: "info", DeliverPolicy: "idle"}, false},
		{"ingress info deliver directive", hubJobEventPayload{operatorIngress: true, NotifyType: "info", DeliverPolicy: "directive"}, false},
		{"node relay untyped", hubJobEventPayload{NotifyType: ""}, false},
		{"node relay deliver now", hubJobEventPayload{DeliverPolicy: "now"}, false},
		{"node relay delivery class interrupt", hubJobEventPayload{DeliveryClass: "interrupt"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := inboxOnlyExempt(tc.e); got != tc.want {
				t.Fatalf("inboxOnlyExempt(%+v) = %t, want %t", tc.e, got, tc.want)
			}
		})
	}
}
