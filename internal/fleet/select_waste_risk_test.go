package fleet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// riskRows is the 2026-10-31 choice: the free SWE-2 rung (with room) against
// the claude subscription rung.
func riskRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

// riskPool is a paid pool whose longest window projects `projected` percent
// used at a reset `hours` away.
func riskPool(projected, hours float64) PoolView {
	u := (100 - projected) / hours
	return PoolView{Status: "ok", Class: "spend", Blocking: ptrF(projected * 0.9), Projected: ptrF(projected), RankProjected: ptrF(projected), Urgency: &u, ResetHours: ptrF(hours)}
}

func riskFixture(t *testing.T, claude PoolView, policyJSON string) (*App, Spawn) {
	t.Helper()
	pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "claude": claude}
	a, s, q := selFixture(t, "A", riskRows(), pools, []string{"devin", "claude"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude"}) // SWE-2 has room
	if policyJSON != "" {
		path := filepath.Join(t.TempDir(), "pool-order.json")
		if e := os.WriteFile(path, []byte(policyJSON), 0o600); e != nil {
			t.Fatal(e)
		}
		a.Config.Local["pool_policy"] = path
	}
	return a, s
}

func riskPolicy(extra string) string {
	return `{"version": 3, "source_doc": "policy/quota-stop", "order": ["free", "waste", "ranked"],
	  "free": [{"pool": "devin", "models": ["swe-2"]}], "credit_pools": ["anthropic-api"]` + extra + `}`
}

func TestSelectWasteRiskOverridesFree(t *testing.T) {
	// After claude's weekly reset it reads 0% used at the start of a fresh
	// window: it cannot be paced yet, so it has no projection and no risk.
	afterReset := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}
	cases := []struct {
		name     string
		claude   PoolView
		policy   string
		want     string
		wantRisk bool
	}{
		// 2026-10-31: claude 31% used, 8 h to its weekly reset → ~67% of
		// the week projected unused.
		{"2026-10-31 claude at waste risk beats SWE-2", riskPool(32.6, 8), "", "sonnet", true},
		{"after claude's reset SWE-2 is first again", afterReset, "", "devin-swe2", false},
		{"15% projected unused is waste ahead of free", riskPool(85, 8), "", "sonnet", false},
		{"72 h from reset with 60% unused overrides free", riskPool(40, 72), "", "sonnet", true},
		{"72 h reset with projected waste still precedes free", riskPool(90, 72), "", "sonnet", false},
		{"horizon_hours 24 keeps a 72 h reset out", riskPool(40, 72), riskPolicy(`, "waste_risk": {"horizon_hours": 24}`), "devin-swe2", false},
		{"horizon_hours 24 admits an 8 h reset", riskPool(32.6, 8), riskPolicy(`, "waste_risk": {"horizon_hours": 24}`), "sonnet", true},
		{"policy min_unused_pct 80 lifts the 67% pool out of risk", riskPool(32.6, 8), riskPolicy(`, "waste_risk": {"min_unused_pct": 80}`), "devin-swe2", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a, s := riskFixture(t, tt.claude, tt.policy)
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			if sel.chosen.seatName != tt.want {
				t.Fatalf("want %s, got %s", tt.want, sel.chosen.seatName)
			}
			rec := sel.record()
			row := candOutcome(rec, "sonnet", "high")
			if row["waste_risk"] != tt.wantRisk {
				t.Fatalf("claude waste_risk: want %v, got %v (%v)", tt.wantRisk, row["waste_risk"], row)
			}
			if free := candOutcome(rec, "devin-swe2", "high"); free["waste_risk"] != false {
				t.Fatalf("a free seat is never at waste risk: %v", free)
			}
			if tt.claude.Urgency != nil && row["unused_at_reset_pct"] == nil {
				t.Fatalf("the record states the unused-at-reset input: %v", row)
			}
			if _, ok := rec["waste_risk"].(map[string]any); !ok {
				t.Fatalf("the record states the waste_risk thresholds: %v", rec["waste_risk"])
			}
		})
	}
}

func TestPoolPolicyWasteRiskDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.json")
	if e := os.WriteFile(path, []byte(riskPolicy("")), 0o600); e != nil {
		t.Fatal(e)
	}
	p, e := LoadPoolPolicy(path)
	if e != nil {
		t.Fatal(e)
	}
	if *p.WasteRisk.HorizonHours != 0 || *p.WasteRisk.MinUnusedPct != 20 {
		t.Fatalf("defaults: horizon 0 (no limit), min unused 20: %+v", *p.WasteRisk)
	}
	embedded, e := LoadPoolPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	if *embedded.WasteRisk.HorizonHours != 0 || *embedded.WasteRisk.MinUnusedPct != 20 {
		t.Fatalf("embedded policy: %+v", *embedded.WasteRisk)
	}
	for _, bad := range []string{`, "waste_risk": {"horizon_hours": -1}`, `, "waste_risk": {"min_unused_pct": 101}`} {
		if e := os.WriteFile(path, []byte(riskPolicy(bad)), 0o600); e != nil {
			t.Fatal(e)
		}
		if _, e := LoadPoolPolicy(path); e == nil {
			t.Fatalf("%s must be refused", bad)
		}
	}
}

// shortWindowStatus renders a quota status document for claude with a 5h
// window at `short` percent used 1 h into it (a fast pace: 5× the even
// rate) and a 7d window at `week` percent used with 8 h to its reset.
func shortWindowStatus(now time.Time, short, week float64) string {
	at := func(h float64) string { return now.Add(time.Duration(h * float64(time.Hour))).Format(time.RFC3339Nano) }
	return fmt.Sprintf(`{"schema":"scopefuel.v1","generated_at":%q,"providers":[
	  {"id":"devin","status":"ok","pool_class":"spend","buckets":[]},
	  {"id":"claude","status":"ok","pool_class":"spend","buckets":[
	    {"window":"5h","used_pct":%g,"resets_at":%q,"scope":{"kind":"account"}},
	    {"window":"7d","used_pct":%g,"resets_at":%q,"scope":{"kind":"account"}}]}]}`,
		now.Format(time.RFC3339Nano), short, at(4), week, at(8))
}

func TestSelectWasteRiskShortWindowGatesOnlyWhenExhausted(t *testing.T) {
	cases := []struct {
		name       string
		short      float64
		week       float64
		want       string // chosen seat
		wantReason string // claude rung's drop reason, "" when chosen
	}{
		// 7d: 31% used, 8 h left → ~67% unused at reset: waste risk. The 5h
		// window at 90% paces to 450% — it must not refuse a waste-risk pool.
		{"waste-risk pool, 5h at 90% and fast → admitted", 90, 31, "sonnet", ""},
		{"waste-risk pool, 5h exhausted → refused", 100, 31, "devin-swe2", "window_exhausted"},
		// 7d: 85% used → ~11% unused: not at waste risk. Sub-daily windows
		// gate admission only (decision/2026-11-04/pool-order-owner), so the
		// 5h pace no longer refuses it; an exhausted 5h window still does.
		{"pool not at waste risk, 5h at 90% and fast → admitted", 90, 85, "sonnet", ""},
		{"pool not at waste risk, 5h exhausted → refused", 100, 85, "devin-swe2", "window_exhausted"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			a, s, q := selFixture(t, "A", riskRows(), nil, []string{"devin", "claude"})
			pools, e := CLIQuota{Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: shortWindowStatus(a.Now(), tt.short, tt.week)} }}, Binary: "pacelever"}.Pools(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			if p := pools["claude"].Projected; p == nil || *p < 100 {
				t.Fatalf("the 5h window's pace projects past 100%%: %v", p)
			}
			q.PoolViews = pools
			q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude"})
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			if sel.chosen.seatName != tt.want {
				t.Fatalf("want %s, got %s", tt.want, sel.chosen.seatName)
			}
			row := candOutcome(sel.record(), "sonnet", "high")
			if tt.wantReason != "" && row["reason"] != tt.wantReason {
				t.Fatalf("claude reason: want %s, got %v", tt.wantReason, row)
			}
		})
	}
}
