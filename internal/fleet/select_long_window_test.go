package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

func TestPoolViewLongestWindowRanking(t *testing.T) {
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	buckets := []accountBucket{
		{Scope: "account", Window: "7d", UsedPct: ptrF(60), ResetsAt: now.Add(84 * time.Hour).Format(time.RFC3339)},
		{Scope: "account", Window: "30d", UsedPct: ptrF(10), ResetsAt: now.Add(360 * time.Hour).Format(time.RFC3339)},
	}
	v := rankedPoolView(buckets, now, "account", "")
	if v == nil || v.RankProjected == nil || *v.RankProjected != 20 {
		t.Fatalf("longest projection: %+v", v)
	}
}

func TestPoolViewGroupShortDoesNotRank(t *testing.T) {
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	buckets := []accountBucket{{Scope: "group", Model: "gemini", Window: "5h", UsedPct: ptrF(10), ResetsAt: now.Add(2 * time.Hour).Format(time.RFC3339)}}
	v := rankedPoolView(buckets, now, "group", "gemini")
	if v == nil || v.RankProjected != nil || v.Urgency != nil {
		t.Fatalf("short window ranks: %+v", v)
	}
}

func TestSelectAgyWeeklyAndShortGroup(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "agy-flash38-medium", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "agy-flash38-low", Effort: "low", Grade: "C", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-low"},
		{Profile: "codex-luna", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
	}
	for _, grade := range []string{"B", "C"} {
		t.Run(grade, func(t *testing.T) {
			a, s, q := agyReadingFixture(t, func(now time.Time) []map[string]any {
				reset := now.Add(84 * time.Hour).UTC().Format(time.RFC3339)
				return []map[string]any{
					{"window": "5h", "used_pct": 0, "resets_at": now.Add(4 * time.Hour).UTC().Format(time.RFC3339), "scope": "group", "model": "gemini"},
					{"window": "7d", "used_pct": 10, "resets_at": reset, "scope": "group", "model": "gemini"},
				}
			}, rows, []string{"agy", "codex"})
			s.Grade = grade
			q.GateDo = gatePools(withAgyGates(map[string]string{"codex-luna": "codex"}))
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			want := "agy-flash38-medium"
			if grade == "C" {
				want = "agy-flash38-low"
			}
			if sel.chosen.row.Profile != want || sel.chosen.tierName != "waste" {
				t.Fatalf("got %s/%s, want %s/waste", sel.chosen.row.Profile, sel.chosen.tierName, want)
			}
		})
	}
}

func TestSelectAgyUntouchedWeekly(t *testing.T) {
	rows := []CatalogRow{{Profile: "agy-flash38-medium", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"}}
	a, s, q := agyReadingFixture(t, func(now time.Time) []map[string]any {
		return []map[string]any{{"window": "7d", "used_pct": 0, "resets_at": now.Add(168 * time.Hour).UTC().Format(time.RFC3339), "scope": "group", "model": "gemini"}}
	}, rows, []string{"agy"})
	q.GateDo = gatePools(withAgyGates(nil))
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.tierName != "waste" || !sel.chosen.risk || sel.chosen.urgency == nil || *sel.chosen.urgency < .59 {
		t.Fatalf("untouched week: %+v", sel.chosen)
	}
}

func TestSelectCommandcodeLongestWindowAndExclusion(t *testing.T) {
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	policy, err := LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	row := CatalogRow{Profile: "omp-dsflash41", Pool: "commandcode", Grade: "A", Effort: "low"}
	for _, tc := range []struct {
		name         string
		week, month  float64
		threshold    float64
		tier, reason string
		risk         bool
	}{
		{"longest risk", 60, 10, 150, "waste", "", true},
		{"worst excludes", 60, 10, 100, "", "projected_exhaust", true},
		{"longest paced", 10, 60, 100, "ranked", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buckets := []accountBucket{
				{Scope: "account", Window: "5h", UsedPct: ptrF(5), ResetsAt: now.Add(2 * time.Hour).Format(time.RFC3339)},
				{Scope: "account", Window: "7d", UsedPct: ptrF(tc.week), ResetsAt: now.Add(84 * time.Hour).Format(time.RFC3339)},
				{Scope: "account", Window: "30d", UsedPct: ptrF(tc.month), ResetsAt: now.Add(360 * time.Hour).Format(time.RFC3339)},
			}
			v := rankedPoolView(buckets, now, "account", "")
			p := selPlan{policy: policy, order: []string{"waste", "free", "ranked"}, at: now}
			p.policy.ExcludeAt = ptrF(tc.threshold)
			c := &selCand{row: row}
			p.classify(c, *v)
			if c.tierName != tc.tier || c.reason != tc.reason || c.risk != tc.risk {
				t.Fatalf("tier=%s reason=%s risk=%t", c.tierName, c.reason, c.risk)
			}
		})
	}
}

func TestSelectUntouchedWeeklyAgainstCodexRisk(t *testing.T) {
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	policy, err := LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	mk := func(scope, group string, used float64, left time.Duration) PoolView {
		v := rankedPoolView([]accountBucket{{Scope: scope, Model: group, Window: "7d", UsedPct: ptrF(used), ResetsAt: now.Add(left).Format(time.RFC3339)}}, now, scope, group)
		return *v
	}
	for _, tc := range []struct{ grade, profile string }{{"B", "agy-flash38-medium"}, {"C", "agy-flash38-low"}} {
		t.Run(tc.grade, func(t *testing.T) {
			p := selPlan{policy: policy, order: []string{"waste", "free", "ranked"}, at: now}
			agy := &selCand{row: CatalogRow{Profile: tc.profile, Pool: "agy", Grade: tc.grade}}
			codex := &selCand{row: CatalogRow{Profile: "codex-luna", Pool: "codex", Grade: tc.grade}}
			p.classify(agy, mk("group", "gemini", 0, 168*time.Hour))
			p.classify(codex, mk("account", "", 3, 148*time.Hour+48*time.Minute))
			candidates := []*selCand{codex, agy}
			sortSelCands(candidates)
			if !agy.risk || !codex.risk || candidates[0] != agy || *agy.urgency <= *codex.urgency {
				t.Fatalf("agy=%+v codex=%+v", agy, codex)
			}
		})
	}
}

func TestSelectFreeAndCreditBandsWithFakeQuota(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
		{Profile: "devin-swe2-flash", Effort: "high", Grade: "B", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-high"},
		{Profile: "omp-luna", Effort: "high", Grade: "B", Pool: "openai-api", Gate: "default", ModelID: "openai/gpt-6-luna"},
		{Profile: "haiku", Effort: "medium", Grade: "B", Pool: "claude", Gate: "default", ModelID: "claude-haiku"},
	}
	for _, tc := range []struct {
		grade, want string
		held        bool
	}{{"C", "devin-swe2-flash-medium", false}, {"B", "devin-swe2-flash", false}, {"B", "omp-luna", true}} {
		t.Run(tc.grade+tc.want, func(t *testing.T) {
			pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(10)}, "openai-api": {Status: "ok", Class: "credit", Credit: balance(80, 100*time.Hour)(creditNow)}, "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}
			if tc.held {
				reset := creditNow.Add(2 * time.Hour)
				v := pools["devin"]
				v.Free = &shared.FreeBucket{State: shared.FreeExhausted, ResetsAt: &reset}
				pools["devin"] = v
			}
			a, s, q := selFixture(t, tc.grade, rows, pools, []string{"devin", "openai-api", "claude"})
			serveOMPRouting(t, a)
			q.GateDo = gatePools(map[string]string{"devin-swe2-flash-medium": "devin", "devin-swe2-flash": "devin", "omp-luna": "openai-api", "haiku": "claude"})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.row.Profile != tc.want {
				t.Fatalf("got %s want %s", sel.chosen.row.Profile, tc.want)
			}
		})
	}
}

func TestPoolViewCLILongestAndWorstProjection(t *testing.T) {
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	doc := fmt.Sprintf(`{"schema":"scopefuel.v1","generated_at":%q,"providers":[{"id":"commandcode","status":"ok","pool_class":"spend","buckets":[{"scope":{"kind":"account"},"window":"30d","used_pct":10,"resets_at":%q},{"scope":{"kind":"account"},"window":"7d","used_pct":60,"resets_at":%q}]}]}`, now.Format(time.RFC3339), now.Add(360*time.Hour).Format(time.RFC3339), now.Add(84*time.Hour).Format(time.RFC3339))
	q := CLIQuota{Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: doc} }}, Binary: "quota"}
	pools, err := q.Pools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	v := pools["commandcode"]
	if v.RankProjected == nil || *v.RankProjected != 20 || v.WorstLongProjected == nil || *v.WorstLongProjected != 120 {
		t.Fatalf("rank=%v worst=%v", v.RankProjected, v.WorstLongProjected)
	}
}

func TestSelectCommandcodeRiskPickWithFakeQuota(t *testing.T) {
	now := creditNow
	view := rankedPoolView([]accountBucket{
		{Scope: "account", Window: "5h", UsedPct: ptrF(5), ResetsAt: now.Add(2 * time.Hour).Format(time.RFC3339)},
		{Scope: "account", Window: "7d", UsedPct: ptrF(60), ResetsAt: now.Add(84 * time.Hour).Format(time.RFC3339)},
		{Scope: "account", Window: "30d", UsedPct: ptrF(10), ResetsAt: now.Add(360 * time.Hour).Format(time.RFC3339)},
	}, now, "account", "")
	rows := []CatalogRow{{Profile: "omp-dsflash41", Effort: "low", Grade: "A", Pool: "commandcode", Gate: "default", ModelID: "commandcode/deepseek/deepseek-v4.1-flash"}, {Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"}}
	for _, grade := range []string{"B", "C"} {
		t.Run(grade, func(t *testing.T) {
			a, s, q := selFixture(t, grade, rows, map[string]PoolView{"commandcode": *view, "devin": {Status: "ok", Class: "spend", Blocking: ptrF(10)}}, []string{"commandcode", "devin"})
			serveOMPRouting(t, a)
			a.Config.Local["omp_commandcode"] = true
			var cfg map[string]any
			if err := json.Unmarshal(defaultPoolOrder, &cfg); err != nil {
				t.Fatal(err)
			}
			cfg["exclude_at_projected_pct"] = 150
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "policy.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			a.Config.Local["pool_policy"] = path
			q.GateDo = gatePools(map[string]string{"omp-dsflash41": "commandcode", "devin-swe2-flash-medium": "devin"})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.row.Profile != "omp-dsflash41" || !sel.chosen.risk || sel.chosen.tierName != "waste" {
				t.Fatalf("chosen: %+v", sel.chosen)
			}
		})
	}
}

func TestSelectAgyLiveGroupShapeUntouched(t *testing.T) {
	rows := []CatalogRow{{Profile: "agy-flash38-medium", Effort: "medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"}}
	a, s, q := agyReadingFixture(t, func(now time.Time) []map[string]any {
		return []map[string]any{
			{"window": "5h", "used_pct": 0, "resets_at": now.Add(5 * time.Hour).UTC().Format(time.RFC3339), "scope": "group", "model": "gemini"},
			{"window": "5h", "used_pct": 0, "resets_at": now.Add(5 * time.Hour).UTC().Format(time.RFC3339), "scope": "group", "model": "3p"},
			{"window": "7d", "used_pct": 0, "resets_at": now.Add(168 * time.Hour).UTC().Format(time.RFC3339), "scope": "group", "model": "3p"},
		}
	}, rows, []string{"agy"})
	q.GateDo = gatePools(withAgyGates(nil))
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.tierName != "ranked" || sel.chosen.risk || sel.chosen.wasteGroup != "gemini" {
		t.Fatalf("gemini must not borrow 3p week: %+v", sel.chosen)
	}
}

func TestSelectFreePoolSeatGroupingThroughClassify(t *testing.T) {
	policy, err := LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	policy.Free = []FreeSet{{Pool: "a"}, {Pool: "b"}}
	p := selPlan{policy: policy, order: []string{"waste", "free", "ranked"}, at: creditNow}
	aC := &selCand{row: CatalogRow{Profile: "a-c", Pool: "a", Grade: "C"}}
	aB := &selCand{row: CatalogRow{Profile: "a-b", Pool: "a", Grade: "B"}}
	bC := &selCand{row: CatalogRow{Profile: "b-c", Pool: "b", Grade: "C"}}
	v := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}
	for _, c := range []*selCand{aC, aB, bC} {
		p.classify(c, v)
	}
	cs := []*selCand{bC, aB, aC}
	sortSelCands(cs)
	if cs[0] != aC || cs[1] != aB || cs[2] != bC {
		t.Fatalf("unexpected order %s %s %s", cs[0].row.Profile, cs[1].row.Profile, cs[2].row.Profile)
	}
}
