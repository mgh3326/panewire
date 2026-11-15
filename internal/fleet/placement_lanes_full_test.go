package fleet

import "testing"

// A machine whose node credential already owns its whole lane row cap is
// published by the hub with the lanes_full reason. The fleet must not read that
// host as usable: its next lane write is refused, so a pane there comes up
// LANE_UNREGISTERED (hk 4658).
func TestViewUsableRefusesLanesFull(t *testing.T) {
	if viewUsable(HostView{Name: "capped", Connected: true, Reason: "lanes_full"}, false) {
		t.Fatal("a lanes-full view is not usable")
	}
	if !viewUsable(HostView{Name: "ok", Connected: true, Reason: "available"}, false) {
		t.Fatal("a plain connected view is usable")
	}
}
