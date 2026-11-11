package fleet

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestSelectPoolSeatCreditBeforeLowerGradeFree(t *testing.T) {
	free := &selCand{row: CatalogRow{Profile: "devin-swe2-flash-medium", Pool: "devin", Grade: "C", Effort: "medium"}, tier: 1, tierName: "free"}
	credit := &selCand{row: CatalogRow{Profile: "omp-luna", Pool: "openai-api", Grade: "B", Effort: "high"}, tier: 1, tierName: "credit", creditPool: true, usdUrgency: ptrF(1)}
	candidates := []*selCand{free, credit}
	sortSelCands(candidates)
	if candidates[0] != credit {
		t.Fatalf("got %s; want credit pool seat", candidates[0].row.Profile)
	}
}

func TestSelectPoolSeatScenarios(t *testing.T) {
	type row struct {
		name, pool, grade, effort, tier string
		risk, credit                    bool
		urgency, usd                    float64
	}
	devin := []row{{"devin-ds41", "devin", "A+", "low", "ranked", false, false, 0, 0}, {"devin-swe2-medium", "devin", "A", "medium", "free", false, false, 0, 0}, {"devin-swe2-flash", "devin", "B", "high", "free", false, false, 0, 0}, {"devin-swe2-flash-medium", "devin", "C", "medium", "free", false, false, 0, 0}}
	codex := []row{{"codex-terra61", "codex", "A+", "high", "waste", true, false, 5, 0}, {"codex-luna", "codex", "B", "high", "waste", true, false, 5, 0}, {"codex-luna", "codex", "C", "low", "waste", true, false, 5, 0}}
	cases := []struct {
		name, grade, want string
		rows              []row
	}{}
	add := func(name string, rows []row, picks map[string]string) {
		for grade, want := range picks {
			cases = append(cases, struct {
				name, grade, want string
				rows              []row
			}{name, grade, want, rows})
		}
	}
	risky := append([]row(nil), devin...)
	risky[0].risk = true
	risky[0].tier = "waste"
	risky[0].urgency = 9
	add("devin risk", risky, map[string]string{"A+": "devin-ds41@low", "A": "devin-ds41@low", "B": "devin-ds41@low", "C": "devin-ds41@low"})
	add("devin free", devin, map[string]string{"A": "devin-swe2-medium@medium", "B": "devin-swe2-flash@high", "C": "devin-swe2-flash-medium@medium"})
	for _, c := range []row{{"omp-luna", "openai-api", "B", "high", "credit", false, true, 0, 1}, {"omp-haiku", "anthropic-api", "B", "high", "credit", false, true, 0, 1}} {
		add(c.pool, append(append([]row(nil), devin[1:]...), c), map[string]string{"C": c.name + "@" + c.effort})
	}
	live := append(append([]row(nil), devin...), codex...)
	live = append(live, row{"haiku", "claude", "C", "medium", "ranked", false, false, 0, 0}, row{"omp-dsflash41", "commandcode", "A", "low", "ranked", false, false, 0, 0}, row{"omp-luna", "openai-api", "B", "high", "credit", false, true, 0, 1}, row{"omp-haiku", "anthropic-api", "B", "high", "credit", false, true, 0, 1})
	add("live", live, map[string]string{"A": "codex-terra61@high", "B": "codex-luna@high", "C": "codex-luna@low"})
	command := append(append([]row(nil), devin...), row{"omp-dsflash41", "commandcode", "A", "low", "waste", true, false, 9, 0})
	add("commandcode risk", command, map[string]string{"B": "omp-dsflash41@low", "C": "omp-dsflash41@low"})
	add("ranked", []row{{"haiku", "claude", "C", "medium", "ranked", false, false, 0, 0}, {"omp-dsflash41", "commandcode", "A", "low", "ranked", false, false, 0, 0}}, map[string]string{"C": "haiku@medium"})
	for _, tc := range cases {
		t.Run(tc.name+"/"+tc.grade, func(t *testing.T) {
			var candidates []*selCand
			for _, r := range tc.rows {
				if gradeOrder[r.grade] < gradeOrder[tc.grade] {
					continue
				}
				tier := 2
				if r.tier == "free" || r.tier == "credit" {
					tier = 1
				}
				if r.tier == "waste" {
					tier = 0
				}
				c := &selCand{row: CatalogRow{Profile: r.name, Pool: r.pool, Grade: r.grade, Effort: r.effort}, tier: tier, tierName: r.tier, risk: r.risk, creditPool: r.credit}
				if r.urgency != 0 {
					c.urgency = ptrF(r.urgency)
				}
				if r.usd != 0 {
					c.usdUrgency = ptrF(r.usd)
				}
				candidates = append(candidates, c)
			}
			sortSelCands(candidates)
			got := candidates[0].row.Profile + "@" + candidates[0].row.Effort
			if got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestSelectPoolSeatRowsStayTogether(t *testing.T) {
	c := &selCand{row: CatalogRow{Profile: "a-c", Pool: "a", Grade: "C"}, tier: 1, tierName: "free", usdUrgency: ptrF(2)}
	b := &selCand{row: CatalogRow{Profile: "a-b", Pool: "a", Grade: "B"}, tier: 1, tierName: "free", usdUrgency: ptrF(2)}
	other := &selCand{row: CatalogRow{Profile: "b-c", Pool: "b", Grade: "C"}, tier: 1, tierName: "free", usdUrgency: ptrF(1)}
	rows := []*selCand{other, b, c}
	sortSelCands(rows)
	if rows[0] != c || rows[1] != b || rows[2] != other {
		t.Fatalf("order %s %s %s", rows[0].row.Profile, rows[1].row.Profile, rows[2].row.Profile)
	}
}

func TestOrderPoolSeatRandomPermutations(t *testing.T) {
	rng := rand.New(rand.NewSource(4042))
	for trial := 0; trial < 300; trial++ {
		var rows []*selCand
		for pool := 0; pool < 5; pool++ {
			band := rng.Intn(3)
			risk := band == 0
			tierName := []string{"waste", "free", "ranked"}[band]
			credit := band == 1 && rng.Intn(2) == 0
			if credit {
				tierName = "credit"
			}
			var urgency, usd *float64
			if risk || rng.Intn(2) == 0 {
				urgency = ptrF(float64(1 + rng.Intn(5)))
			}
			if credit && rng.Intn(2) == 0 {
				usd = ptrF(float64(1 + rng.Intn(5)))
			}
			for i := 0; i < 1+rng.Intn(3); i++ {
				rows = append(rows, &selCand{row: CatalogRow{Profile: fmt.Sprintf("p%d-r%d", pool, i), Pool: fmt.Sprintf("p%d", pool), Grade: []string{"C", "B", "A", "A+"}[rng.Intn(4)], Effort: []string{"low", "medium", "high"}[rng.Intn(3)]}, tier: band, tierName: tierName, risk: risk, creditPool: credit, urgency: urgency, usdUrgency: usd})
			}
		}
		compare := selCandComparator(rows)
		for _, x := range rows {
			for _, y := range rows {
				for _, z := range rows {
					if compare(x, y) < 0 && compare(y, z) < 0 && compare(z, x) < 0 {
						t.Fatalf("trial %d has a comparison cycle", trial)
					}
				}
			}
		}
		expected := append([]*selCand(nil), rows...)
		sortSelCands(expected)
		for repeat := 0; repeat < 10; repeat++ {
			shuffled := append([]*selCand(nil), rows...)
			rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			sortSelCands(shuffled)
			if !slices.Equal(expected, shuffled) {
				t.Fatalf("trial %d order depends on input", trial)
			}
		}
	}
}

func TestSelectPoolSeatCreditWithFakeQuota(t *testing.T) {
	for _, tc := range []struct{ pool, profile, model string }{{"openai-api", "omp-luna", "openai/gpt-6-luna"}, {"anthropic-api", "omp-haiku", "anthropic/claude-haiku"}} {
		t.Run(tc.pool, func(t *testing.T) {
			rows := []CatalogRow{{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"}, {Profile: tc.profile, Effort: "high", Grade: "B", Pool: tc.pool, Gate: "default", ModelID: tc.model}}
			pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, tc.pool: {Status: "ok", Class: "spend", Credit: balance(80, 100*time.Hour)(creditNow)}}
			a, s, q := selFixture(t, "C", rows, pools, []string{"devin", tc.pool})
			serveOMPRouting(t, a)
			q.GateDo = gatePools(map[string]string{"devin-swe2-flash-medium": "devin", tc.profile: tc.pool})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.row.Profile != tc.profile {
				t.Fatalf("got %s, want %s", sel.chosen.row.Profile, tc.profile)
			}
		})
	}
}

func TestSelectPoolSeatDevinAndCommandRiskWithFakeQuota(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-ds41", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "deepseek-v4-1-flash-high"},
		{Profile: "devin-swe2-medium", Effort: "medium", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2-medium"},
		{Profile: "devin-swe2-flash", Effort: "high", Grade: "B", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-high"},
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
		{Profile: "omp-dsflash41", Effort: "low", Grade: "A", Pool: "commandcode", Gate: "default", ModelID: "commandcode/deepseek/deepseek-v4.1-flash"},
	}
	gates := gatePools(map[string]string{"devin-ds41": "devin", "devin-swe2-medium": "devin", "devin-swe2-flash": "devin", "devin-swe2-flash-medium": "devin", "omp-dsflash41": "commandcode"})
	for _, tc := range []struct {
		name, grade, want string
		devin, command    PoolView
	}{
		{"devin risk", "A+", "devin-ds41", riskPool(10, 4), riskPool(30, 8)}, {"devin risk", "A", "devin-ds41", riskPool(10, 4), riskPool(30, 8)}, {"devin risk", "B", "devin-ds41", riskPool(10, 4), riskPool(30, 8)}, {"devin risk", "C", "devin-ds41", riskPool(10, 4), riskPool(30, 8)},
		{"devin free", "A", "devin-swe2-medium", PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}, PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}}, {"devin free", "B", "devin-swe2-flash", PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}, PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}}, {"devin free", "C", "devin-swe2-flash-medium", PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}, PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}},
		{"command risk", "B", "omp-dsflash41", riskPool(30, 8), riskPool(10, 4)}, {"command risk", "C", "omp-dsflash41", riskPool(30, 8), riskPool(10, 4)},
	} {
		t.Run(tc.name+"/"+tc.grade, func(t *testing.T) {
			a, s, q := selFixture(t, tc.grade, rows, map[string]PoolView{"devin": tc.devin, "commandcode": tc.command}, []string{"devin", "commandcode"})
			serveOMPRouting(t, a)
			a.Config.Local["omp_commandcode"] = true
			q.GateDo = gates
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.row.Profile != tc.want {
				t.Fatalf("got %s want %s candidates=%v", sel.chosen.row.Profile, tc.want, sel.record()["candidates"])
			}
			if tc.name == "devin risk" {
				for _, c := range sel.ranked {
					if strings.HasPrefix(c.row.Profile, "devin-swe2") && (c.risk || c.tierName != "free") {
						t.Errorf("free rung risk=%v tier=%s", c.risk, c.tierName)
					}
				}
			}
		})
	}
}

func TestSelectPoolSeatLiveAndRankedWithFakeQuota(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "codex-terra61", Effort: "high", Grade: "A+", Pool: "codex", Gate: "default", ModelID: "gpt-6.1-terra"},
		{Profile: "codex-luna", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "gpt-6-luna"},
		{Profile: "codex-luna", Effort: "low", Grade: "C", Pool: "codex", Gate: "default", ModelID: "gpt-6-luna"},
		{Profile: "haiku", Effort: "medium", Grade: "C", Pool: "claude", Gate: "default", ModelID: "claude-haiku"},
		{Profile: "omp-dsflash41", Effort: "low", Grade: "A", Pool: "commandcode", Gate: "default", ModelID: "commandcode/deepseek/deepseek-v4.1-flash"},
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
		{Profile: "omp-luna", Effort: "high", Grade: "B", Pool: "openai-api", Gate: "default", ModelID: "openai/gpt-6-luna"},
		{Profile: "omp-haiku", Effort: "high", Grade: "B", Pool: "anthropic-api", Gate: "default", ModelID: "anthropic/claude-haiku"},
	}
	pools := map[string]PoolView{"codex": riskPool(10, 4), "claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}, "commandcode": {Status: "ok", Class: "spend", Blocking: ptrF(10)}, "devin": {Status: "ok", Class: "spend", Blocking: ptrF(10)}, "openai-api": {Status: "ok", Class: "spend", Credit: balance(80, 100*time.Hour)(creditNow)}, "anthropic-api": {Status: "ok", Class: "spend", Credit: balance(80, 100*time.Hour)(creditNow)}}
	gates := gatePools(map[string]string{"codex-terra61": "codex", "codex-luna": "codex", "haiku": "claude", "omp-dsflash41": "commandcode", "devin-swe2-flash-medium": "devin", "omp-luna": "openai-api", "omp-haiku": "anthropic-api"})
	for _, tc := range []struct{ grade, want string }{{"A", "codex-terra61@high"}, {"B", "codex-luna@high"}, {"C", "codex-luna@low"}} {
		t.Run("live/"+tc.grade, func(t *testing.T) {
			a, s, q := selFixture(t, tc.grade, rows, pools, []string{"codex", "claude", "commandcode", "devin", "openai-api", "anthropic-api"})
			serveOMPRouting(t, a)
			a.Config.Local["omp_commandcode"] = true
			q.GateDo = gates
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			got := sel.chosen.row.Profile + "@" + sel.chosen.row.Effort
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
	t.Run("ranked/C", func(t *testing.T) {
		a, s, q := selFixture(t, "C", rows[3:5], pools, []string{"claude", "commandcode"})
		serveOMPRouting(t, a)
		a.Config.Local["omp_commandcode"] = true
		q.GateDo = gates
		sel, err := a.selectProfile(context.Background(), s)
		if err != nil {
			t.Fatal(err)
		}
		if sel.chosen.row.Profile != "haiku" {
			t.Fatalf("got %s", sel.chosen.row.Profile)
		}
	})
}
