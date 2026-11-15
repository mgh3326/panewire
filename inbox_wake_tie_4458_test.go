package panewire

import "testing"

// TestInboxWakeMostUrgentOrderWinsDecisionTie covers hk 4458: order and
// decision share one urgency rank, and the wake line's "most urgent" kind must
// not depend on which of the two sits first in the inbox.
func TestInboxWakeMostUrgentOrderWinsDecisionTie(t *testing.T) {
	order := laneInboxItem{Kind: "notify", Event: hubJobEventPayload{NotifyType: "order"}}
	decision := laneInboxItem{Kind: "notify", Event: hubJobEventPayload{NotifyType: "decision"}}
	for name, items := range map[string][]laneInboxItem{
		"decision first": {decision, order},
		"order first":    {order, decision},
	} {
		if got := inboxWakeMostUrgent(items); got != "order" {
			t.Fatalf("%s: most urgent = %q, want order", name, got)
		}
	}
	if got := inboxWakeMostUrgent([]laneInboxItem{decision}); got != "decision" {
		t.Fatalf("decision alone: most urgent = %q, want decision", got)
	}
}
