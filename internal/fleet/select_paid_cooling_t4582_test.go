package fleet

import (
	"context"
	"testing"
	"time"
)

// devinBandRows is the devin pool as the catalog carries it: the SWE-2 free
// rungs and the paid rungs share the one pool, plus another pool to move to.
func devinBandRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "devin-ds41", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "deepseek-v4-1-flash-high"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "claude-sonnet-5-5"},
	}
}

func devinBandFixture(t *testing.T) (*App, Spawn) {
	t.Helper()
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(4)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, q := selFixture(t, "A", devinBandRows(), pools, []string{"devin", "claude"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "devin-ds41": "devin", "sonnet": "claude"})
	return a, s
}

// A paid Devin weekly stop holds the paid rungs, not the whole devin pool: the
// six SWE-2 free rungs of that catalog pool draw on the free band and stay
// placeable while only the paid window is spent (hk 4582).
func TestSelectPaidCoolingLeavesFreeRungsEligible_T4582(t *testing.T) {
	a, s := devinBandFixture(t)
	now := a.Now()
	clock := now
	a.Now = func() time.Time { return clock }
	reset := now.Add(6 * time.Hour)
	a.State = &fakeStateReader{snap: FleetSnapshot{Cooling: map[string]time.Time{"devin-paid": reset}}}

	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-ds41", "high")["outcome"]; got != "dropped:pool_cooling" {
		t.Fatalf("a paid stop must drop the paid rung as pool_cooling: %v", got)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got == "dropped:pool_cooling" {
		t.Fatalf("a paid stop must leave the free rung eligible: %v", got)
	}
	if sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("the free rung stays placeable while only the paid window is spent: chosen=%s", sel.chosen.seatName)
	}

	// Past the paid window's reset the paid rung returns on its own.
	clock = reset.Add(time.Minute)
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-ds41", "high")["outcome"]; got == "dropped:pool_cooling" {
		t.Fatalf("the paid rung must be admitted again after the reset: %v", got)
	}
}

// The free band's stop is the mirror image: it holds the free rungs and leaves
// the paid rungs of the same catalog pool eligible.
func TestSelectFreeBandCoolingLeavesPaidRungsEligible_T4582(t *testing.T) {
	a, s := devinBandFixture(t)
	reset := a.Now().Add(time.Hour)
	a.State = &fakeStateReader{snap: FleetSnapshot{Cooling: map[string]time.Time{"devin-free": reset}}}

	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "dropped:pool_cooling" {
		t.Fatalf("a free-band stop must drop the free rung as pool_cooling: %v", got)
	}
	if got := candOutcome(sel.record(), "devin-ds41", "high")["outcome"]; got == "dropped:pool_cooling" {
		t.Fatalf("a free-band stop must leave the paid rung eligible: %v", got)
	}
	if sel.chosen.seatName != "devin-ds41" {
		t.Fatalf("the paid rung stays placeable while only the free band is held: chosen=%s", sel.chosen.seatName)
	}
}
