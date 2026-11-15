package panewire

import (
	"testing"
	"time"
)

// A paid Devin stop names the catalog pool devin, whose rows also carry the six
// SWE-2 free rungs. It cools the paid band alone, so the free rungs stay
// placeable while only the paid window is spent (hk 4582); a free-band stop
// keeps its own key and leaves the paid band alone.
func TestPaidDevinStopCoolsOnlyThePaidBand_T4582(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(6 * time.Hour)
	h := &HubServer{}
	h.observePoolCooling("quota_stop pool=devin reset_at="+reset.Format(time.RFC3339), now)

	snap := h.fleetSnapshot(now)
	if got := snap.Cooling["devin-paid"]; !got.Equal(reset) {
		t.Fatalf("paid band cooling = %v, want %v", got, reset)
	}
	if got, ok := snap.Cooling["devin"]; ok {
		t.Fatalf("a paid stop must not cool the catalog pool devin (it holds the free rungs too): %v", got)
	}

	h.observePoolCooling("quota_stop pool=devin-free reset_at="+reset.Format(time.RFC3339), now)
	snap = h.fleetSnapshot(now)
	if got := snap.Cooling["devin-free"]; !got.Equal(reset) {
		t.Fatalf("free band cooling = %v, want %v", got, reset)
	}
	if got := snap.Cooling["devin-paid"]; !got.Equal(reset) {
		t.Fatalf("a free-band stop changed the paid band's cooling to %v", got)
	}
}
