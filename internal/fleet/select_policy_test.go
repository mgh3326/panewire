package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// pace is one pool's 7d account bucket as the quota CLI reports it: the used
// percentage now and the fraction of the window already elapsed. The selector
// projects it to used/elapsed at reset, the way pacelever paces a bucket.
type pace struct{ used, elapsed float64 }

// policyCatalog is a fixed bench catalog over the shipped seat policy
// (policy.json): a free SWE-2 bucket, two subscription pools with B..S+
// rungs and the credit pool.
var policyCatalog = []CatalogRow{
	{Profile: "devin-swe2-medium", Effort: "medium", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2-medium"},
	{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	{Profile: "sonnet", Effort: "medium", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
	{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
	{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
	{Profile: "opus", Effort: "max", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "m"},
	{Profile: "codex-sol", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "m"},
	{Profile: "codex-sol", Effort: "max", Grade: "S+", Pool: "codex", Gate: "default", ModelID: "m"},
	{Profile: "omp-sonnet", Effort: "high", Grade: "A", Pool: "anthropic-api", Gate: "default", ModelID: "m"},
}

// paceStatus renders a quota status document with one 7d account
// bucket per pool, generated at now.
func paceStatus(now time.Time, pools map[string]pace) string {
	var providers []map[string]any
	for id, p := range pools {
		week := 7 * 24 * time.Hour
		reset := now.Add(time.Duration((1 - p.elapsed) * float64(week)))
		providers = append(providers, map[string]any{
			"id": id, "status": "ok", "pool_class": "spend",
			"buckets": []map[string]any{{"window": "7d", "used_pct": p.used, "resets_at": reset.Format(time.RFC3339Nano), "scope": map[string]any{"kind": "account"}}},
		})
	}
	b, _ := json.Marshal(map[string]any{"schema": "scopefuel.v1", "generated_at": now.Format(time.RFC3339Nano), "providers": providers})
	return string(b)
}

// TestSelectionPolicyScenarios is the operator selection policy as a table:
// a paid pool at waste risk (>= 20% projected unused at its reset) first, by
// waste urgency; otherwise projected waste, then free, then ranked, then credit; never a pool projected to run
// out; the lowest grade that fits the task, then the lower projection, then
// the smaller effort.
// Seats come from policy.json and the pool order and projection thresholds
// from the shipped pool-order policy.
func TestSelectionPolicyScenarios(t *testing.T) {
	embedded, err := LoadPoolPolicy("")
	if err != nil || strings.Join(embedded.Order, ",") != "waste,free,ranked" {
		t.Fatalf("embedded operator order=%v err=%v", embedded.Order, err)
	}
	// A credit pool ranks in the free band, with or without a balance
	// reading, so it is spent before the ranked subscription pools.
	roomy := map[string]pace{"devin": {10, 0.5}, "claude": {15, 0.5}, "codex": {20, 0.5}}
	// Paid pools near their projected cap: under 20% unused at reset, so not
	// at waste risk.
	tight := map[string]pace{"devin": {10, 0.5}, "claude": {45, 0.5}, "codex": {42, 0.5}}
	with := func(over map[string]pace) map[string]pace {
		m := map[string]pace{}
		for k, v := range roomy {
			m[k] = v
		}
		for k, v := range over {
			m[k] = v
		}
		return m
	}
	exhausted := pace{100, 0.6}
	cases := []struct {
		situation string
		grade, t  string
		priority  string
		pools     map[string]pace
		want      string // profile@effort
		wantPool  string
		wantTier  string
	}{
		{"B task, projected waste precedes free even below risk override", "B", "T2", "3", tight,
			"sonnet@medium", "claude", "waste"},
		{"urgent B task, projected waste precedes free", "B", "T2", "1", tight,
			"sonnet@medium", "claude", "waste"},
		{"B task, SWE-2 free has room, claude 70% / codex 60% unused at reset → claude (waste risk before free, higher urgency)", "B", "T2", "3", roomy,
			"sonnet@medium", "claude", "waste"},
		{"B task, SWE-2 exhausted, claude 16% / codex 42% projected → claude B fit, not S+", "B", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {8, 0.5}, "codex": {21, 0.5}}),
			"sonnet@medium", "claude", "waste"},
		{"urgent B task, SWE-2 exhausted, claude 16% / codex 42% → claude B fit, not S+", "B", "T2", "1",
			with(map[string]pace{"devin": exhausted, "claude": {8, 0.5}, "codex": {21, 0.5}}),
			"sonnet@medium", "claude", "waste"},
		{"B task, all paid pools at waste risk, codex more unused → codex by waste urgency before grade fit", "B", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {30, 0.5}, "codex": {5, 0.5}}),
			"codex-sol@high", "codex", "waste"},
		{"B task, paid pools not at waste risk, codex lower → lowest sufficient grade before lower projection", "B", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {45, 0.5}, "codex": {41, 0.5}}),
			"sonnet@medium", "claude", "waste"},
		{"A task, all paid pools roomy → A on the lower projection, not S+", "A", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {30, 0.5}, "codex": {5, 0.5}}),
			"codex-sol@high", "codex", "waste"},
		{"claude projected 120% at reset (30% used, quarter of window) → excluded; codex 80% chosen", "B", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {30, 0.25}, "codex": {40, 0.5}}),
			"codex-sol@high", "codex", "waste"},
		{"credit balance recorded, every subscription on pace to use up its week → credit before the paced pools", "B", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {60, 0.5}, "codex": {70, 0.5}, "anthropic-api": {10, 0.5}}),
			"omp-sonnet@high", "anthropic-api", "credit"},
		{"no credit balance recorded, credit still ranks in the free band before the paced pools", "B", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {60, 0.5}, "codex": {70, 0.5}, "anthropic-api": {10, 0.5}}),
			"omp-sonnet@high", "anthropic-api", "credit"},
		{"credit pool readable, claude 16% → waste before credit", "B", "T2", "3",
			with(map[string]pace{"devin": exhausted, "claude": {8, 0.5}, "anthropic-api": {10, 0.5}}),
			"sonnet@medium", "claude", "waste"},
		{"T3 S task, all roomy → S-grade profile, not S+", "S", "T3", "3", roomy,
			"opus@high", "claude", "waste"},
		{"urgent T3 S task, codex more unused → codex S+ by waste urgency", "S", "T3", "1",
			with(map[string]pace{"claude": {30, 0.5}, "codex": {5, 0.5}}),
			"codex-sol@max", "codex", "waste"},
		{"urgent T3 S task, neither paid pool at waste risk, codex lower → the S fit on claude", "S", "T3", "1",
			with(map[string]pace{"devin": exhausted, "claude": {45, 0.5}, "codex": {41, 0.5}}),
			"opus@high", "claude", "waste"},
	}
	policy, e := LoadPoolPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	for _, tt := range cases {
		t.Run(tt.situation, func(t *testing.T) {
			present := []string{"devin", "claude", "codex", "anthropic-api"}
			a, s, q := selFixture(t, tt.grade, policyCatalog, nil, present)
			pools, e := CLIQuota{Now: a.Now, Runner: &fakeRunner{Do: func(c Command) Result {
				if strings.Join(c.Args, " ") == "manual list" {
					if !strings.HasPrefix(tt.situation, "credit balance recorded") {
						return Result{Code: 2, Err: "no manual store"}
					}
					return Result{Out: creditStore(a.Now(), "anthropic-api", 20)}
				}
				return Result{Out: paceStatus(a.Now(), tt.pools)}
			}}, Binary: "quota"}.Pools(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			q.PoolViews = pools
			// The quota gate itself refuses a pool that is used up now.
			q.GateDo = func(r QuotaRequest) (string, error) {
				pool := map[string]string{"devin-swe2": "devin", "devin-swe2-medium": "devin", "sonnet": "claude", "opus": "claude", "codex-sol": "codex", "omp-sonnet": "anthropic-api"}[r.Profile]
				if tt.pools[pool].used >= 100 {
					return "", fail(4, "pool %s exhausted", pool)
				}
				return fmt.Sprintf("pool=%s profile=%s\n", pool, r.Profile), nil
			}
			useFakeHK(t).SetTask("42", fmt.Sprintf(`{"id":42,"title":"work (grade %s, %s)","state":"ready","priority":%s,"refs":{}}`, tt.grade, tt.t, tt.priority))
			s.T = tt.t
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			got := sel.chosen.row.Profile + "@" + sel.chosen.row.Effort
			if got != tt.want || sel.chosen.row.Pool != tt.wantPool || sel.chosen.tierName != tt.wantTier {
				t.Fatalf("want %s on %s (%s), got %s grade %s on %s (%s)", tt.want, tt.wantPool, tt.wantTier, got, sel.chosen.row.Grade, sel.chosen.row.Pool, sel.chosen.tierName)
			}
			rec := sel.record()
			if tiers := strings.Join(rec["tiers"].([]string), ","); tiers != strings.Join(policy.Order, ",") {
				t.Fatalf("tier order must be the policy order %v whatever the urgency: %s", policy.Order, tiers)
			}
		})
	}
}

func TestPoolsProjectAtReset(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := func(d time.Duration) string { return now.Add(d).Format(time.RFC3339Nano) }
	doc := fmt.Sprintf(`{"schema":"scopefuel.v1","generated_at":%q,"providers":[
	  {"id":"half","status":"ok","pool_class":"spend","buckets":[
	    {"window":"7d","used_pct":8,"resets_at":%q,"scope":{"kind":"account"}},
	    {"window":"5h","used_pct":90,"resets_at":%q,"scope":{"kind":"model"}}]},
	  {"id":"hot","status":"ok","pool_class":"spend","buckets":[
	    {"window":"7d","used_pct":10,"resets_at":%q,"scope":{"kind":"account"}},
	    {"window":"5h","used_pct":60,"resets_at":%q,"scope":{"kind":"account"}}]},
	  {"id":"fresh","status":"ok","pool_class":"spend","buckets":[
	    {"window":"7d","used_pct":1,"resets_at":%q,"scope":{"kind":"account"}}]},
	  {"id":"over","status":"ok","pool_class":"spend","buckets":[
	    {"window":"7d","used_pct":104,"resets_at":%q,"scope":{"kind":"account"}}]}
	]}`, at(0), at(84*time.Hour), at(time.Hour), at(84*time.Hour), at(150*time.Minute), at(7*24*time.Hour-time.Hour), at(84*time.Hour))
	pools, e := CLIQuota{Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: doc} }}, Binary: "quota"}.Pools(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if p := pools["half"].Projected; p == nil || *p != 16 {
		t.Fatalf("8%% used half way through 7d projects 16%% (model-scope buckets ignored): %v", p)
	}
	if p := pools["hot"].Projected; p == nil || *p != 120 {
		t.Fatalf("the highest account bucket projection wins: %v", p)
	}
	if p := pools["fresh"].Projected; p == nil || *p != 1 {
		t.Fatalf("a fresh long window projects its used share: %v", p)
	}
	if p := pools["over"].Projected; p == nil || *p != 208 {
		t.Fatalf("an over-consumed reading still projects: %v", p)
	}
}

func TestPoolPolicyProjectionThresholds(t *testing.T) {
	p, e := LoadPoolPolicy("")
	if e != nil || *p.WasteBelow != 100 || *p.ExcludeAt != 100 {
		t.Fatalf("shipped thresholds: %+v %v", p, e)
	}
	dir := t.TempDir()
	for body, ok := range map[string]bool{
		`{"version":3,"order":["free","waste","ranked"]}`:                                                                true,
		`{"version":3,"order":["free","waste","ranked"],"waste_below_projected_pct":80,"exclude_at_projected_pct":100}`:  true,
		`{"version":3,"order":["free","waste","ranked"],"waste_below_projected_pct":110,"exclude_at_projected_pct":100}`: false,
		`{"version":3,"order":["free","waste","ranked"],"waste_below_projected_pct":0}`:                                  true,
		`{"version":3,"order":["free","waste","ranked"],"exclude_at_projected_pct":0}`:                                   false,
	} {
		path := dir + "/p.json"
		if e := os.WriteFile(path, []byte(body), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := LoadPoolPolicy(path); (e == nil) != ok {
			t.Fatalf("%s: %v", body, e)
		}
	}
}

func TestSelectionRecordExplainsChoice(t *testing.T) {
	waste := true
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "medium", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "codex-sol", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "hot", Gate: "default", ModelID: "m"},
		{Profile: "haiku", Effort: "low", Grade: "C", Pool: "hot", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(8), Projected: ptrF(16)},
		"codex":  {Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(21), Projected: ptrF(42)},
		"hot":    {Status: "ok", Class: "spend", Blocking: ptrF(60), Projected: ptrF(120)},
	}
	a, s, q := selFixture(t, "B", rows, pools, []string{"claude", "codex", "hot"})
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "codex", "opus": "hot"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	rec := sel.record()
	chosen := rec["chosen"].(map[string]any)
	if chosen["seat"] != "sonnet" || chosen["tier"] != "waste" || chosen["grade"] != "B" || chosen["projected_pct"] != 16.0 {
		t.Fatalf("chosen: %v", chosen)
	}
	if chosen["why"] != "tier=waste grade=B for task B projected=16%" {
		t.Fatalf("why: %v", chosen["why"])
	}
	if !strings.Contains(rec["rule"].(string), "lowest grade >= B") {
		t.Fatalf("rule: %v", rec["rule"])
	}
	// Credit pacing: a pool on pace to use up its week is not excluded; it is
	// marked paced, and with no credit candidate it keeps its tier.
	if got := candOutcome(rec, "opus", "high"); got["outcome"] != "below_choice" || got["projected_pct"] != 120.0 || got["long_projected_pct"] != 120.0 || got["paced"] != true || got["behind_credit"] != false {
		t.Fatalf("a pool projected past reset must stay a candidate and say it is paced: %v", got)
	}
	if got := candOutcome(rec, "haiku", "low"); got["outcome"] != "dropped:grade_insufficient" || got["account_key"] != "" || got["projected_pct"] != nil {
		t.Fatalf("a static refusal must not borrow another account's projection: %v", got)
	}
	if got := candOutcome(rec, "sonnet", "medium"); got["used_pct"] != 8.0 {
		t.Fatalf("the chosen row records its used percentage: %v", got)
	}
	if got := candOutcome(rec, "codex-sol", "high"); got["outcome"] != "below_choice" || got["tier"] != "waste" {
		t.Fatalf("codex: %v", got)
	}
}

// creditStore is a pacelever manual store recording one credit pool with
// used percent of its balance spent, expiring in thirty days.
func creditStore(now time.Time, pool string, used float64) string {
	b, _ := json.Marshal(map[string]any{
		"schema": "pacelever.manual.v1",
		"latest": map[string]string{pool + ":credit": "m1"},
		"history": []map[string]any{{"manual_observation_id": "m1", "event": "set", "pool": pool, "window": "credit", "used_pct": used,
			"measured_at": now.Add(-time.Hour).Format(time.RFC3339Nano), "expires_at": now.Add(30 * 24 * time.Hour).Format(time.RFC3339Nano)}},
	})
	return string(b)
}
