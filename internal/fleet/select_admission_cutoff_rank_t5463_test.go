package fleet

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// hk 5463: selection ranks a pool whose reading of some window is at or above
// its admission cutoff behind every pool admission would accept. On 2026-11-19
// a reviewer request ranked codex first at 7d 99% (admission's cutoff) and then
// waited on a codex refresh while commandcode, agy and openai-api had room.

func cutoffRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "codex-luna", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "codex-terra61", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "omp-dsflash41", Effort: "high", Grade: "A+", Pool: "commandcode", Gate: "default", ModelID: "m"},
		{Profile: "agy-flash38-high", Effort: "high", Grade: "A", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-high"},
		ompRow("omp-sol", "openai/gpt-6.1-sol", "openai-api"),
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	}
}

func cutoffPools(now time.Time, codex7d, commandcode7d float64) map[string]PoolView {
	used := func(pct float64) PoolView {
		return PoolView{Status: "ok", Class: "spend", Blocking: ptrF(pct), Projected: ptrF(pct), Windows: []ViewWindow{{Window: "7d", UsedPct: pct}}}
	}
	cc := used(commandcode7d)
	cc.Blocking = ptrF(commandcode7d)
	return map[string]PoolView{
		// codex 7d 99% with a far reset: its projection stays low only because
		// the reading says 99% used, so Blocking is the cutoff's evidence.
		"codex":       used(codex7d),
		"commandcode": cc,
		"agy":         used(0),
		"claude":      used(100),
		"devin":       used(100),
		"openai-api": {Status: "ok", Class: "credit", Credit: &CreditBalance{
			LeftPct: 58, MeasuredAt: now.Add(-time.Hour), ExpiresAt: now.Add(10 * 24 * time.Hour)}},
	}
}

func cutoffReviewer(t *testing.T, codex7d, commandcode7d float64, rows []CatalogRow) (*selPlan, error) {
	t.Helper()
	a, s, q := selFixture(t, "B", rows, nil, []string{"codex", "commandcode", "agy", "claude", "devin", "openai-api"})
	q.PoolViews = cutoffPools(a.Now(), codex7d, commandcode7d)
	a.Config.Local["omp_commandcode"] = true
	serveOMPRouting(t, a)
	s.Role, s.ParentFamily = "reviewer", "anthropic"
	q.GateDo = gatePools(map[string]string{"codex-luna": "codex", "codex-terra61": "codex", "omp-dsflash41": "commandcode", "agy-flash38-high": "agy", "omp-sol": "openai-api", "opus": "claude", "devin-swe2": "devin"})
	return a.selectProfile(context.Background(), s)
}

// The 2026-11-19 reviewer request: codex 7d 99% (far reset), commandcode
// 5h 7.7% / 7d 55.5%, agy 0%, openai-api credit 58%, claude and devin 100%.
// Codex is at admission's cutoff, so it never ranks first.
func TestSelectReviewerSkipsPoolAtAdmissionCutoff_T5463(t *testing.T) {
	sel, e := cutoffReviewer(t, 99, 55.5, cutoffRows())
	if e != nil {
		t.Fatal(e)
	}
	if got := sel.chosen.row.Pool; got == "codex" {
		t.Fatalf("chose %s from the pool at its admission cutoff", sel.chosen.row.Profile)
	}
	rec := sel.record()
	for _, effort := range []string{"high"} {
		for _, profile := range []string{"codex-luna", "codex-terra61"} {
			row := candOutcome(rec, profile, effort)
			if row["at_admission_cutoff"] != true || row["admission_cutoff_pct"] != 99.0 {
				t.Fatalf("%s must name at_admission_cutoff with its cutoff: %v", profile, row)
			}
		}
	}
	// The pool is still a candidate, ranked behind the rest.
	var pools []string
	for _, c := range sel.ranked {
		if !slices.Contains(pools, c.row.Pool) {
			pools = append(pools, c.row.Pool)
		}
	}
	if last := pools[len(pools)-1]; last != "codex" {
		t.Fatalf("codex must rank last, order %v", pools)
	}
	if rule, _ := rec["rule"].(string); !strings.Contains(rule, "at_admission_cutoff") {
		t.Fatalf("the rule text must name at_admission_cutoff: %q", rule)
	}
}

// Command Code's cutoff is 90, not 99: at 7d 91% it ranks behind a pool with
// room. The cutoff is read from the admission policy, so a configured cutoff
// moves it.
func TestSelectCommandcodeCutoffComesFromAdmissionPolicy_T5463(t *testing.T) {
	a, s, q := selFixture(t, "B", cutoffRows()[2:4], nil, []string{"commandcode", "agy"})
	q.PoolViews = cutoffPools(a.Now(), 0, 91)
	a.Config.Local["omp_commandcode"] = true
	q.GateDo = gatePools(map[string]string{"omp-dsflash41": "commandcode", "agy-flash38-high": "agy"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if sel.chosen.row.Pool != "agy" {
		t.Fatalf("chose %s, want the pool with room", sel.chosen.row.Profile)
	}
	if got := sel.poolCutoff("commandcode"); got != 90 {
		t.Fatalf("commandcode cutoff = %g, want 90", got)
	}
	if got := sel.poolCutoff("codex"); got != 99 {
		t.Fatalf("codex cutoff = %g, want 99", got)
	}
}

// A pool above 90 but below 99 other than commandcode is not at its cutoff:
// the key does not turn into a flat 90.
func TestSelectPoolBelowItsCutoffKeepsItsRank_T5463(t *testing.T) {
	sel, e := cutoffReviewer(t, 95, 55.5, cutoffRows())
	if e != nil {
		t.Fatal(e)
	}
	if row := candOutcome(sel.record(), "codex-luna", "high"); row["at_admission_cutoff"] != nil {
		t.Fatalf("codex at 95%% is below its 99 cutoff: %v", row)
	}
}

// When every fitting pool is at its cutoff the key ties and the choice is the
// one the order picked before: no new no-candidate outcome. (A commandcode
// reading at its cutoff is dropped outright by the commandcode rules, so the
// pools here are codex and agy.)
func TestSelectAllPoolsAtCutoffKeepsOrder_T5463(t *testing.T) {
	pick := func(t *testing.T, used float64) (*selPlan, string) {
		a, s, q := selFixture(t, "B", cutoffRows()[:2:2], nil, []string{"codex", "agy"})
		q.CatalogRows = append(q.CatalogRows, cutoffRows()[3])
		q.PoolViews = map[string]PoolView{"codex": cutoffPools(a.Now(), used, 0)["codex"], "agy": cutoffPools(a.Now(), used, 0)["codex"]}
		q.GateDo = gatePools(map[string]string{"codex-luna": "codex", "codex-terra61": "codex", "agy-flash38-high": "agy"})
		sel, e := a.selectProfile(context.Background(), s)
		if e != nil {
			t.Fatalf("selection refused at %g%%: %v", used, e)
		}
		return sel, sel.chosen.row.Profile
	}
	atSel, at := pick(t, 99)
	if !atSel.chosen.atCutoff {
		t.Fatal("the chosen pool should be at its cutoff in this case")
	}
	if _, below := pick(t, 98); at != below {
		t.Fatalf("all at cutoff chose %s, the same readings below cutoff chose %s", at, below)
	}
}
