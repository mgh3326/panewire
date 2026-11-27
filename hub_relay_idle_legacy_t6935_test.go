package panewire

// hk 6935: on a hub with no store, an idle wake re-delivers the oldest
// remembered working-expired row of its pane that is still inside the replay
// window. A row past the window is dropped from memory, and the row behind it
// is still the one this wake re-delivers. Killed by: returning at the expired
// row instead of looking past it.

import (
	"testing"
	"time"
)

func TestLegacyIdleWakeSkipsExpiredOldestRow(t *testing.T) {
	rig := t1002Start(t)
	stale := rig.sendLaneEvent("ev-6935-stale", "stale body")
	fresh := rig.sendLaneEvent("ev-6935-fresh", "fresh body")
	if stale.EventID >= fresh.EventID {
		t.Fatalf("stale row %d is not the older one of %d", stale.EventID, fresh.EventID)
	}
	now := rig.hub.now().UTC()
	rig.hub.mu.Lock()
	rig.hub.relayControl.legacyIdle = map[int64]legacyIdleRow{
		stale.EventID: {lane: t1002Lane, droppedAt: now.Add(-relayReplayMaxAge - time.Minute)},
		fresh.EventID: {lane: t1002Lane, droppedAt: now},
	}
	rig.hub.mu.Unlock()

	rig.hub.redeliverWorkingExpiredOnIdle("host-a", idleWakeRouteRequest{Pane: "w1:pA", EventID: "idle:w1:session:1", StateChangeSeq: 1})

	got := rig.injects()
	if len(got) != 1 || got[0].EventID != fresh.EventID {
		t.Fatalf("the idle wake re-delivered %+v, want the fresh row %d alone", got, fresh.EventID)
	}
	if row := rig.row(stale.EventID); row.DeliveredAt != "" {
		t.Fatalf("the expired row was re-delivered: %+v", row)
	}
}
