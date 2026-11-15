package fleet

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// hk 4529: in the ranked band each pool offers one seat for the task's grade
// and pools are ordered by their seat's room (waste urgency).

func rankedSeatRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "omp-dsflash41", Effort: "low", Grade: "A", Pool: "commandcode", Gate: "default", ModelID: "m"},
		{Profile: "omp-dsflash41", Effort: "high", Grade: "A+", Pool: "commandcode", Gate: "default", ModelID: "m"},
		{Profile: "codex-luna", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "haiku", Effort: "medium", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

// rankedView is a subscription pool over pace (so it ranks) with the given
// waste urgency: the unused share per hour is negative past 100% projected.
func rankedView(urgency float64) PoolView {
	return PoolView{Status: "ok", Class: "spend", Blocking: ptrF(50), Projected: ptrF(100 - urgency*100), Urgency: ptrF(urgency), ResetHours: ptrF(100)}
}

func rankedSeatPick(t *testing.T, grade string, commandcode, codex, claude float64, policyPath string) *selPlan {
	t.Helper()
	pools := map[string]PoolView{"commandcode": rankedView(commandcode), "codex": rankedView(codex), "claude": rankedView(claude)}
	a, s, q := selFixture(t, grade, rankedSeatRows(), pools, []string{"commandcode", "codex", "claude"})
	q.GateDo = gatePools(map[string]string{"omp-dsflash41": "commandcode", "codex-luna": "codex", "haiku": "claude"})
	a.Config.Local["omp_commandcode"] = true
	if policyPath != "" {
		a.Config.Local["pool_policy"] = policyPath
	}
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	return sel
}

func chosenRung(sel *selPlan) string {
	return sel.chosen.row.Profile + "@" + sel.chosen.row.Effort
}

// Command Code takes B and C when it has more room than the other ranked
// pools, through the embedded default policy seat; with less room the codex
// or claude seat keeps the task.
func TestRankedSeatsOrderPoolsByRoom_T4529(t *testing.T) {
	cases := []struct {
		grade                      string
		commandcode, codex, claude float64
		want                       string
	}{
		{"B", -0.083, -0.59, -0.176, "omp-dsflash41@high"},
		{"B", -0.083, -0.013, -0.176, "codex-luna@high"},
		{"C", -0.083, -0.59, -0.176, "omp-dsflash41@high"},
		{"C", -0.2, -0.59, -0.176, "haiku@medium"},
		{"C", -0.2, -0.13, -0.176, "codex-luna@high"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/cc=%g/codex=%g/claude=%g", tc.grade, tc.commandcode, tc.codex, tc.claude), func(t *testing.T) {
			sel := rankedSeatPick(t, tc.grade, tc.commandcode, tc.codex, tc.claude, "")
			if got := chosenRung(sel); got != tc.want {
				t.Fatalf("chosen %s, want %s", got, tc.want)
			}
		})
	}
}

// An A task keeps the lowest-fit seat of every pool unless the policy names an
// A seat for the pool.
func TestRankedSeatsAGradeKeepsLowestFitUnlessPolicy_T4529(t *testing.T) {
	sel := rankedSeatPick(t, "A", -0.01, -0.59, -0.176, "")
	if got := chosenRung(sel); got != "omp-dsflash41@low" {
		t.Fatalf("A task chose %s, want the lowest-fit seat", got)
	}
	rec := sel.record()
	if got := candOutcome(rec, "omp-dsflash41", "low")["seat_source"]; got != "lowest_fit" {
		t.Fatalf("seat_source = %v", got)
	}
	policy := filepath.Join(t.TempDir(), "pool-order.json")
	body := `{"version":3,"source_doc":"t","order":["waste","free","ranked"],"credit_pools":[],"seats":{"commandcode":{"A":"omp-dsflash41@high"}}}`
	if err := os.WriteFile(policy, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	sel = rankedSeatPick(t, "A", -0.01, -0.59, -0.176, policy)
	if got := chosenRung(sel); got != "omp-dsflash41@high" {
		t.Fatalf("A task chose %s with a policy seat for A", got)
	}
	rec = sel.record()
	row := candOutcome(rec, "omp-dsflash41", "high")
	if row["seat_source"] != "policy" || row["pool_waste_urgency"] != -0.01 {
		t.Fatalf("policy seat record: %v", row)
	}
	if got := candOutcome(rec, "omp-dsflash41", "low"); got["seat_source"] != nil || got["outcome"] != "below_choice" {
		t.Fatalf("the pool's other row follows its seat: %v", got)
	}
}

// A policy seat that is not a candidate (the named row does not exist) leaves the pool on its lowest-fit seat.
func TestRankedSeatPolicyNotACandidateFallsBack_T4529(t *testing.T) {
	policy := filepath.Join(t.TempDir(), "pool-order.json")
	body := `{"version":3,"source_doc":"t","order":["waste","free","ranked"],"credit_pools":[],"seats":{"commandcode":{"B":"omp-dsflash41@medium"}}}`
	if err := os.WriteFile(policy, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	sel := rankedSeatPick(t, "B", -0.01, -0.59, -0.176, policy)
	if got := chosenRung(sel); got != "omp-dsflash41@low" {
		t.Fatalf("chosen %s, want the lowest-fit row", got)
	}
	if got := candOutcome(sel.record(), "omp-dsflash41", "low")["seat_source"]; got != "lowest_fit" {
		t.Fatalf("seat_source = %v", got)
	}
}

// Pools with equal room (or none) keep the lowest-grade-first order.
func TestRankedSeatsTieKeepsGradeOrder_T4529(t *testing.T) {
	mk := func(profile, pool, grade string, urgency *float64) *selCand {
		return &selCand{row: CatalogRow{Profile: profile, Pool: pool, Grade: grade, Effort: "high"}, tier: 2, tierName: "ranked", urgency: urgency}
	}
	for _, urgency := range []*float64{nil, ptrF(-0.1)} {
		a := mk("a-a", "pa", "A+", urgency)
		b := mk("b-b", "pb", "B", urgency)
		for _, in := range [][]*selCand{{a, b}, {b, a}} {
			rows := slices.Clone(in)
			sortSelCands(rows)
			if rows[0] != b {
				t.Fatalf("urgency %v: tie must keep grade order, got %s first", urgency, rows[0].row.Profile)
			}
		}
	}
	// Room decides before grade, a paceable pool before an unpaceable one.
	hi, lo, none := mk("hi", "ph", "A+", ptrF(-0.1)), mk("lo", "pl", "B", ptrF(-0.5)), mk("none", "pn", "C", nil)
	rows := []*selCand{none, lo, hi}
	sortSelCands(rows)
	if rows[0] != hi || rows[1] != lo || rows[2] != none {
		t.Fatalf("order %s %s %s", rows[0].row.Profile, rows[1].row.Profile, rows[2].row.Profile)
	}
}

func TestPoolPolicySeatsValidate_T4529(t *testing.T) {
	base := func(seats string) string {
		return `{"version":3,"source_doc":"t","order":["waste","free","ranked"],"credit_pools":[],"seats":` + seats + `}`
	}
	cases := []struct {
		name, seats string
		ok          bool
	}{
		{"named row", `{"commandcode":{"B":"omp-dsflash41@high"}}`, true},
		{"rung-less row", `{"commandcode":{"B":"omp-dsflash41"}}`, true},
		{"unknown grade", `{"commandcode":{"Z":"omp-dsflash41@high"}}`, false},
		{"unknown effort", `{"commandcode":{"B":"omp-dsflash41@loud"}}`, false},
		{"empty effort", `{"commandcode":{"B":"omp-dsflash41@"}}`, false},
		{"empty row", `{"commandcode":{"B":""}}`, false},
		{"bad pool", `{"../x":{"B":"omp-dsflash41@high"}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "p.json")
			if err := os.WriteFile(path, []byte(base(tc.seats)), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadPoolPolicy(path)
			if tc.ok != (err == nil) {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
			if err != nil && !strings.Contains(err.Error(), "seats") {
				t.Fatalf("error does not name seats: %v", err)
			}
		})
	}
}

// Ranked pool groups with policy seats stay one strict weak ordering, whatever
// the input order.
func TestRankedSeatsOrderIsTotal_T4529(t *testing.T) {
	rng := rand.New(rand.NewSource(4529))
	for trial := 0; trial < 300; trial++ {
		var rows []*selCand
		for pool := 0; pool < 5; pool++ {
			var urgency *float64
			if rng.Intn(3) > 0 {
				urgency = ptrF(float64(rng.Intn(4)) / 10)
			}
			behind := rng.Intn(4) == 0
			for i := 0; i < 1+rng.Intn(3); i++ {
				rows = append(rows, &selCand{
					row:  CatalogRow{Profile: fmt.Sprintf("p%d-r%d", pool, i), Pool: fmt.Sprintf("p%d", pool), Grade: []string{"C", "B", "A", "A+"}[rng.Intn(4)], Effort: []string{"low", "medium", "high"}[rng.Intn(3)]},
					tier: 2, tierName: "ranked", urgency: urgency, behindCredit: behind, policySeat: rng.Intn(4) == 0,
				})
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
		expected := slices.Clone(rows)
		sortSelCands(expected)
		for repeat := 0; repeat < 10; repeat++ {
			shuffled := slices.Clone(rows)
			rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			sortSelCands(shuffled)
			if !slices.Equal(expected, shuffled) {
				t.Fatalf("trial %d order depends on input", trial)
			}
		}
	}
}
