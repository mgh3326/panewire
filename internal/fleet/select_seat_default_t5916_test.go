package fleet

import (
	"context"
	"testing"
)

// hk 5916: the embedded pool order names no seat override, so Command Code's
// B and C seat is its catalog's own row, omp-musespark13c, on every host.

// seatDefaultRows is the Command Code pool's rows for one task grade: the
// DeepSeek A+ row that the old embedded seat named, plus a Muse Spark row at
// the task's grade. The Muse Spark row is the pool's lowest fit.
func seatDefaultRows(grade string) []CatalogRow {
	return []CatalogRow{
		{Profile: "omp-dsflash41", Effort: "high", Grade: "A+", Pool: "commandcode", Gate: "default", ModelID: "m"},
		{Profile: "omp-musespark13c", Effort: "high", Grade: grade, Pool: "commandcode", Gate: "default", ModelID: "m"},
		{Profile: "codex-luna", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "haiku", Effort: "medium", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

// With the embedded policy (no pool_policy named) Command Code takes its B and
// C task on omp-musespark13c, the pool's seat, and its seat is the lowest fit.
func TestEmbeddedPolicyCommandCodeSeatIsMuseSpark_T5916(t *testing.T) {
	pools := map[string]PoolView{
		"commandcode": rankedView(-0.083),
		"codex":       rankedView(-0.59),
		"claude":      rankedView(-0.176),
	}
	for _, grade := range []string{"B", "C"} {
		t.Run(grade, func(t *testing.T) {
			a, s, q := selFixture(t, grade, seatDefaultRows(grade), pools, []string{"commandcode", "codex", "claude"})
			q.GateDo = gatePools(map[string]string{"omp-dsflash41": "commandcode", "omp-musespark13c": "commandcode", "codex-luna": "codex", "haiku": "claude"})
			a.Config.Local["omp_commandcode"] = true
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			if got := chosenRung(sel); got != "omp-musespark13c@high" {
				t.Fatalf("grade %s chose %s, want omp-musespark13c@high", grade, got)
			}
			if got := candOutcome(sel.record(), "omp-musespark13c", "high")["seat_source"]; got != "lowest_fit" {
				t.Fatalf("seat_source = %v, want lowest_fit (no policy seat)", got)
			}
		})
	}
}
