package fleet

import (
	"context"
	"testing"
)

// The reserved devin-free key names the SWE-2 free band, not a catalog pool.
// A quota-stopped replacement carries the stopped record's pool in
// --exclude-pool, so excluding devin-free must drop the free rungs (hk 4622);
// excluding the catalog pool devin must still drop every devin rung.
func TestSelectExcludePoolDevinFreeDropsTheFreeRungs_T4622(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "devin-swe2-medium", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2-medium"},
		{Profile: "devin-ds41", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "deepseek-v4-1-flash-high"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "claude-sonnet-5-5"},
	}
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(4)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	gate := gatePools(map[string]string{"devin-swe2": "devin", "devin-swe2-medium": "devin", "devin-ds41": "devin", "sonnet": "claude"})

	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude"})
	q.GateDo = gate
	s.ExcludePools = "devin-free"
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"devin-swe2", "devin-swe2-medium"} {
		if got := candOutcome(sel.record(), profile, "high")["outcome"]; got != "dropped:pool_excluded" {
			t.Fatalf("--exclude-pool devin-free must exclude the free rung %s: %v", profile, got)
		}
	}
	if got := candOutcome(sel.record(), "devin-ds41", "high")["outcome"]; got == "dropped:pool_excluded" {
		t.Fatalf("--exclude-pool devin-free must not exclude the paid devin rung: %v", got)
	}
	if sel.chosen.seatName == "devin-swe2" || sel.chosen.seatName == "devin-swe2-medium" {
		t.Fatalf("a free rung was chosen although the free band is excluded: %s", sel.chosen.seatName)
	}

	// The catalog pool still excludes every rung that draws on it.
	a, s, q = selFixture(t, "A", rows, pools, []string{"devin", "claude"})
	q.GateDo = gate
	s.ExcludePools = "devin"
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []string{"devin-swe2", "devin-swe2-medium", "devin-ds41"} {
		if got := candOutcome(sel.record(), profile, "high")["outcome"]; got != "dropped:pool_excluded" {
			t.Fatalf("--exclude-pool devin must exclude %s: %v", profile, got)
		}
	}
}
