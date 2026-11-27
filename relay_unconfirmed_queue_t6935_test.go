package panewire

// hk 6935 (hk 6893): a storeless hub's one labelled replay is spent by its
// retire, and nothing may be queued after that retire. When the pane's
// injection queue has no room, the replay is deferred before the retire: the
// row stays unspent, and the next trigger with room queues its one labelled
// copy. Killed by: retiring the row before the room check.

import (
	"context"
	"testing"
)

func TestUnconfirmedReplayWaitsForQueueRoom(t *testing.T) {
	rig := t1002Start(t)
	directive := rig.sendLaneEvent("ev-6935-queue", "queue body")
	eventID := directive.EventID
	rig.unconfirmed(directive, "maybe_in_pane")

	// A restart whose destination queue has no room for another directive.
	rig.startHub()
	rig.dest.relays = make(chan hubRelayInjectEvent, 1)
	rig.dest.relays <- hubRelayInjectEvent{Type: "relay.inject", JobID: "filler", Text: "filler"}
	ctx := context.Background()
	rig.hub.replayUndeliveredRelayEvents(ctx)
	rig.hub.replayUndeliveredLaneEvents(ctx)

	if got := len(rig.dest.relays); got != 1 {
		t.Fatalf("queue holds %d directives with the queue full, want the filler alone", got)
	}
	if row := rig.row(eventID); row.DeliveredAt != "" {
		t.Fatalf("the replay retired its row with the queue full: %+v", row)
	}
	if markers := rig.markers(); len(markers) != 1 || markers[0].DeliveredAt != "" {
		t.Fatalf("marker with the queue full: %+v, want one live marker", markers)
	}

	// The queue drains: the next trigger queues the one labelled copy.
	<-rig.dest.relays
	rig.hub.replayUndeliveredRelayEvents(ctx)
	rig.hub.replayUndeliveredLaneEvents(ctx)
	if injects := rig.t1034Labelled(eventID); len(injects) != 1 {
		t.Fatalf("labelled copies after the queue drained = %d, want 1", len(injects))
	}
	if row := rig.row(eventID); row.DeliveredTo != "hub/replay-retired:"+relayUnconfirmedRetireReason {
		t.Fatalf("row after the replay: delivered_to=%q", row.DeliveredTo)
	}
	for restart := 1; restart <= 2; restart++ {
		rig.restart()
		if injects := rig.injects(); len(injects) != 0 {
			t.Fatalf("restart %d after the replay injected %d rows, want 0", restart, len(injects))
		}
	}
}
