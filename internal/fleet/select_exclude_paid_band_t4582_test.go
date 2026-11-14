package fleet

import (
	"context"
	"testing"
)

// --exclude-pool devin-paid excludes the paid-band rungs alone. The six SWE-2
// free rungs of the same catalog pool stay eligible, so a replacement after a
// paid stop never spends the free band and never takes a paid rung that stops
// on the same window (hk 4582). Excluding the catalog pool devin still drops
// every devin rung.
func TestSelectExcludePoolDevinPaidDropsOnlyPaidRungs_T4582(t *testing.T) {
	a, s := devinBandFixture(t)
	s.ExcludePools = "devin-paid"
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := candOutcome(sel.record(), "devin-ds41", "high")["outcome"]; got != "dropped:pool_excluded" {
		t.Fatalf("--exclude-pool devin-paid must exclude the paid rung: %v", got)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got == "dropped:pool_excluded" {
		t.Fatalf("--exclude-pool devin-paid must leave the free rung eligible: %v", got)
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["outcome"]; got == "dropped:pool_excluded" {
		t.Fatalf("--exclude-pool devin-paid is not a catalog pool and must not touch another pool: %v", got)
	}
	if sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("the free rung must still be chosen: %s", sel.chosen.seatName)
	}

	// The catalog pool still excludes every rung that draws on it.
	a, s = devinBandFixture(t)
	s.ExcludePools = "devin"
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"devin-swe2", "devin-ds41"} {
		if got := candOutcome(sel.record(), profile, "high")["outcome"]; got != "dropped:pool_excluded" {
			t.Fatalf("--exclude-pool devin must exclude %s: %v", profile, got)
		}
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("with the whole devin pool excluded the selection must move to another pool: %s", sel.chosen.seatName)
	}
}
