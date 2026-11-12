package fleet

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"
)

// wasteCase reproduces the 2026-10-31 decision: claude at 31% used of a
// 7-day window resetting in ~9.4 h (projected 31.8%) against codex at 13%
// resetting in ~95 h (projected 30%). The free rung is held (its gate
// refuses), so the choice is between the two waste-tier pools.
func wasteRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "codex-luna", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "m"},
	}
}

func wastePools(claudeUsed, claudeProj, claudeUrg, claudeReset, codexUsed, codexProj, codexUrg, codexReset float64, withUrgency bool) map[string]PoolView {
	claude := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(claudeUsed), Projected: ptrF(claudeProj)}
	codex := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(codexUsed), Projected: ptrF(codexProj)}
	if withUrgency {
		claude.Urgency, claude.ResetHours = ptrF(claudeUrg), ptrF(claudeReset)
		codex.Urgency, codex.ResetHours = ptrF(codexUrg), ptrF(codexReset)
	}
	return map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "claude": claude, "codex": codex}
}

// wasteCase20261031 is the recorded 2026-10-31 reading: claude 31% used,
// projected 31.8% at a reset ~9.4 h out (~68% of the week lost); codex 13%
// used, projected 30% with ~95 h left.
func wastePools20261031() map[string]PoolView {
	return wastePools(31, 31.8, 7.26, 9.4, 13, 30, 0.74, 95, true)
}

func wasteFixture(t *testing.T, pools map[string]PoolView) (*App, Spawn) {
	t.Helper()
	a, s, q := selFixture(t, "A", wasteRows(), pools, []string{"devin", "claude", "codex"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-luna": "codex"}) // devin-swe2 held
	return a, s
}

func TestSelectWasteUrgency(t *testing.T) {
	cases := []struct {
		name  string
		pools map[string]PoolView
		want  string
	}{
		// claude loses ~68% of its week in 9.4 h; codex has 4 days left.
		{"2026-10-31: sooner reset wins", wastePools20261031(), "sonnet"},
		// After claude's reset it reads 0% used with +168 h to the next one.
		{"after claude reset codex wins", wastePools(0, 0, 0.60, 168, 13, 30, 0.74, 95, true), "codex-luna"},
		// Equal urgency falls back to the projected-at-reset order.
		{"equal urgency uses projected order", wastePools(31, 31.8, 1, 9.4, 13, 30, 1, 95, true), "codex-luna"},
		// Unknown resets rank by projected use at reset.
		{"unknown reset uses projected order", wastePools(31, 31.8, 0, 0, 13, 30, 0, 0, false), "codex-luna"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a, s := wasteFixture(t, tt.pools)
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			if sel.chosen.seatName != tt.want {
				t.Fatalf("want %s, got %s", tt.want, sel.chosen.seatName)
			}
		})
	}
}

// The pool status document's buckets decide urgency by the LONGEST window
// of a day or more: claude's 5h rate window at 3% used, resetting in 1.5 h,
// must not outrank by itself — the 7d window still decides, and the
// 2026-10-31 case still picks claude (≈67% unused in 9.4 h) over codex
// (≈70% unused in 95 h). A pool whose only window is sub-daily, and a pool
// whose longest window is fresh, uses its unused share for urgency.
func TestPoolsWasteUrgencyUsesLongestWindow(t *testing.T) {
	a := testApp(t)
	now := a.Now()
	reset := func(h float64) string { return now.Add(time.Duration(h * float64(time.Hour))).Format(time.RFC3339Nano) }
	doc := fmt.Sprintf(`{"schema":"scopefuel.v1","generated_at":%q,"providers":[
	  {"id":"claude","status":"ok","pool_class":"spend","buckets":[
	    {"window":"5h","used_pct":3,"resets_at":%q,"scope":{"kind":"account"}},
	    {"window":"7d","used_pct":31,"resets_at":%q,"scope":{"kind":"account"}}]},
	  {"id":"short","status":"ok","pool_class":"spend","buckets":[
	    {"window":"5h","used_pct":3,"resets_at":%q,"scope":{"kind":"account"}}]},
	  {"id":"long-unpaced","status":"ok","pool_class":"spend","buckets":[
	    {"window":"7d","used_pct":31,"resets_at":%q,"scope":{"kind":"account"}},
	    {"window":"30d","used_pct":1,"resets_at":%q,"scope":{"kind":"account"}}]},
	  {"id":"codex","status":"ok","pool_class":"spend","buckets":[
	    {"window":"7d","used_pct":13,"resets_at":%q,"scope":{"kind":"account"}}]}]}`,
		now.Format(time.RFC3339Nano), reset(1.5), reset(9.4), reset(1.5), reset(9.4), reset(29*24), reset(95))
	q := CLIQuota{Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: doc} }}, Binary: "pacelever"}
	pools, e := q.Pools(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	// 31% used, 9.4 h of a 7d window left: projected 32.84, urgency ≈ 7.15 —
	// never the 5h bucket's ≈64.7.
	if u := pools["claude"].Urgency; u == nil || math.Abs(*u-7.145) > 0.01 {
		t.Fatalf("claude urgency must come from the 7d window: %v", u)
	}
	if h := pools["claude"].ResetHours; h == nil || *h != 9.4 {
		t.Fatalf("claude reset hours: %v", h)
	}
	if u := pools["codex"].Urgency; u == nil || math.Abs(*u-0.738) > 0.01 {
		t.Fatalf("codex urgency: %v", u)
	}
	if u := pools["short"].Urgency; u != nil {
		t.Fatalf("a sub-daily-only pool must not rank by urgency: %v", *u)
	}
	if u := pools["long-unpaced"].Urgency; u == nil || math.Abs(*u-99.0/(29*24)) > 0.0001 {
		t.Fatalf("fresh longest window uses unused share per hour: %v", u)
	}
	// Blocking still sees the short window: it stays an admission gate.
	if b := pools["short"].Blocking; b == nil || *b != 3 {
		t.Fatalf("short window still reports used: %v", b)
	}

	// Feeding the computed views into selection still picks the claude rung.
	rows := wasteRows()
	a2, s, q2 := selFixture(t, "A", rows, pools, []string{"devin", "claude", "codex"})
	q2.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-luna": "codex"})
	sel, e := a2.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("the 2026-10-31 case picks claude via its 7d window: %s", sel.chosen.seatName)
	}
}

// orderSelCands is a total order: candidates whose pool reset can be paced
// rank ahead of those without urgency, however the projected shares compare.
// A (urgency 5, projected 30), C (urgency 3, projected 10) and B (no
// urgency, projected 20) must sort A, C, B under every input permutation.
func TestOrderSelCandsMixedUrgencyIsTotal(t *testing.T) {
	mk := func(profile string, urg, proj *float64) *selCand {
		return &selCand{row: CatalogRow{Profile: profile, Grade: "A", Pool: "p", Effort: "high", ModelID: "m"}, tier: 1, urgency: urg, projected: proj}
	}
	a := mk("a", ptrF(5), ptrF(30))
	b := mk("b", nil, ptrF(20))
	c := mk("c", ptrF(3), ptrF(10))
	for _, perm := range [][3]*selCand{{a, b, c}, {a, c, b}, {b, a, c}, {b, c, a}, {c, a, b}, {c, b, a}} {
		pending := []*selCand{perm[0], perm[1], perm[2]}
		orderSelCands(pending)
		if pending[0] != a || pending[1] != c || pending[2] != b {
			t.Fatalf("permutation must sort a, c, b: %s %s %s", pending[0].row.Profile, pending[1].row.Profile, pending[2].row.Profile)
		}
	}
}

// A dropped rung is not a ranked candidate: a grade-insufficient rung on a
// paceable pool records none of reset_hours, waste_urgency or waste_order.
func TestSelectRecordDroppedRowHasNoWasteFields(t *testing.T) {
	rows := append(wasteRows(),
		CatalogRow{Profile: "weak", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"})
	pools := wastePools20261031()
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude", "codex"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-luna": "codex"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	dropped := candOutcome(sel.record(), "weak", "high")
	if dropped["outcome"] != "dropped:grade_insufficient" {
		t.Fatalf("the B rung must drop: %v", dropped["outcome"])
	}
	for _, k := range []string{"reset_hours", "waste_urgency", "waste_order"} {
		if _, ok := dropped[k]; ok {
			t.Fatalf("a dropped candidate records no %s: %v", k, dropped)
		}
	}
}

func TestSelectRecordCarriesWasteUrgency(t *testing.T) {
	a, s := wasteFixture(t, wastePools20261031())
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	rec := sel.record()
	if rule := rec["rule"].(string); !strings.Contains(rule, "waste urgency") {
		t.Fatalf("rule must describe the waste-urgency order: %s", rule)
	}
	row := candOutcome(rec, "sonnet", "high")
	if row["waste_urgency"] != 7.26 || row["waste_order"] != "reset" || row["reset_hours"] != 9.4 {
		t.Fatalf("ranked candidate records its urgency and inputs: %v", row)
	}
	if got := candOutcome(rec, "codex-luna", "high")["waste_urgency"]; got != 0.74 {
		t.Fatalf("codex urgency: %v", got)
	}
	chosen := rec["chosen"].(map[string]any)
	if chosen["waste_urgency"] != 7.26 {
		t.Fatalf("chosen records its urgency: %v", chosen)
	}
}

func TestSelectRecordStatesProjectedFallback(t *testing.T) {
	a, s := wasteFixture(t, wastePools(31, 31.8, 0, 0, 13, 30, 0, 0, false))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if got := candOutcome(sel.record(), "sonnet", "high")["waste_order"]; got != "projected_fallback" {
		t.Fatalf("unknown reset must record the projected fallback: %v", got)
	}
}

// A free rung sharing a pool with paid rungs is excluded only by an observed
// free limit, never by the pool's used or projected share.
func TestSelectFreeRungIgnoresPoolProjection(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "devin-ds41", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{
		"devin":  {Status: "ok", Class: "spend", Blocking: ptrF(70), Projected: ptrF(106)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(31), Projected: ptrF(106)},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude"})
	q.GateDo = func(r QuotaRequest) (string, error) {
		if firstNonempty(r.GateProfile, r.Profile) == "devin-swe2" {
			return "pool=devin profile=devin-swe2 gate_basis=free-bucket:devin\n", nil
		}
		return gatePools(map[string]string{"devin-ds41": "devin", "sonnet": "claude"})(r)
	}
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if sel.chosen.seatName != "devin-swe2" {
		t.Fatalf("a projected 106%% pool must not exclude its free rung: %s", sel.chosen.seatName)
	}
	// A paid rung of the same pool is on pace to use up its window: it stays a
	// candidate, marked paced (credit pacing), behind the free rung.
	if got := candOutcome(sel.record(), "devin-ds41", "high"); got["outcome"] != "below_choice" || got["paced"] != true {
		t.Fatalf("a paid rung of a pool projected past reset is paced, not refused: %v", got)
	}
}

func TestSelectFreeRungRefusedByObservedLimit(t *testing.T) {
	rows := wasteRows()
	pools := map[string]PoolView{
		"devin": {Status: "ok", Class: "spend", Blocking: ptrF(70), Projected: ptrF(106)},
		// These paid pools are ranked, so the free rung is tried before them.
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(80)},
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(90)},
	}
	a, s, q := selFixture(t, "A", rows, pools, []string{"devin", "claude", "codex"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-luna": "codex"}) // devin-swe2 refused
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "dropped:gate_refused" {
		t.Fatalf("an observed free limit refuses the free rung: %v", got)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("the next ranked rung is chosen: %s", sel.chosen.seatName)
	}
}
