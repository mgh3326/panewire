package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 4699, decision 279: Command Code is tried right after the free band and
// before the ranked pools, for every grade it seats; when its window is used up
// or its reading is missing it drops as any subscription pool does.

func afterFreeRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "omp-dsflash41", Effort: "low", Grade: "A", Pool: "commandcode", Gate: "default", ModelID: "m"},
		{Profile: "omp-dsflash41", Effort: "high", Grade: "A+", Pool: "commandcode", Gate: "default", ModelID: "m"},
		{Profile: "codex-sol", Effort: "high", Grade: "A+", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "codex-terra61", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "codex-luna", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "high", Grade: "A+", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "haiku", Effort: "medium", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
	}
}

var afterFreeGates = map[string]string{
	"omp-dsflash41": "commandcode", "codex-sol": "codex", "codex-terra61": "codex", "codex-luna": "codex",
	"opus": "claude", "sonnet": "claude", "haiku": "claude", "devin-swe2": "devin",
}

// afterFreePick selects with commandcode as the worst-roomed pool (urgency
// -0.9, against -0.01 for codex and claude), so only its band can put it first.
func afterFreePick(t *testing.T, grade string, pools map[string]PoolView, policyPath, exclude string) *selPlan {
	t.Helper()
	if pools == nil {
		pools = map[string]PoolView{"commandcode": rankedView(-0.9), "codex": rankedView(-0.01), "claude": rankedView(-0.01), "devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}}
	}
	present := []string{}
	for pool := range pools {
		present = append(present, pool)
	}
	a, s, q := selFixture(t, grade, afterFreeRows(), pools, present)
	q.GateDo = gatePools(afterFreeGates)
	a.Config.Local["omp_commandcode"] = true
	if policyPath != "" {
		a.Config.Local["pool_policy"] = policyPath
	}
	s.ExcludePools = exclude
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	return sel
}

// withoutAfterFree is the embedded policy minus the new key: the behaviour
// before decision 279.
func withoutAfterFree(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pool-order.json")
	body := `{"version":3,"source_doc":"t","order":["waste","free","ranked"],"free":[{"pool":"devin","models":["swe-2"]}],"credit_pools":[],"seats":{"commandcode":{"B":"omp-dsflash41@high","C":"omp-dsflash41@high"}}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Command Code ranks right after the free band for every grade it seats, and
// without the key the same readings pick a ranked pool (red/green).
func TestAfterFreePoolRanksCommandcodeFirstForEveryGrade_T4699(t *testing.T) {
	// The free-band row devin-swe2 serves A only; C and B take the policy seat.
	for _, tc := range []struct{ grade, want, before string }{
		{"C", "omp-dsflash41@high", "haiku@medium"},
		{"B", "omp-dsflash41@high", "codex-luna@high"},
		{"A+", "omp-dsflash41@high", "opus@high"},
	} {
		t.Run(tc.grade, func(t *testing.T) {
			sel := afterFreePick(t, tc.grade, nil, "", "devin-free")
			if got := chosenRung(sel); got != tc.want {
				t.Fatalf("chosen %s, want %s", got, tc.want)
			}
			if sel.chosen.tierName != afterFreeTier {
				t.Fatalf("tier %q, want %q", sel.chosen.tierName, afterFreeTier)
			}
			if old := afterFreePick(t, tc.grade, nil, withoutAfterFree(t), "devin-free"); chosenRung(old) == tc.want {
				t.Fatalf("without after_free_pools the worst-roomed pool must not win, got %s", chosenRung(old))
			}
		})
	}
	// A: the free devin-swe2 rung is the free band, so it still goes first;
	// with the free band excluded Command Code takes the task.
	if got := chosenRung(afterFreePick(t, "A", nil, "", "")); got != "devin-swe2@high" {
		t.Fatalf("A with the free band open chose %s, want the free rung", got)
	}
	if got := chosenRung(afterFreePick(t, "A", nil, "", "devin-free")); got != "omp-dsflash41@low" {
		t.Fatalf("A after the free band chose %s, want omp-dsflash41@low", got)
	}
}

// A used-up window drops Command Code as before, and a missing reading fails
// closed; the ranked pools then take the task.
func TestAfterFreePoolDropsWhenUsedUpOrUnknown_T4699(t *testing.T) {
	healthy := func() map[string]PoolView {
		return map[string]PoolView{"commandcode": rankedView(-0.9), "codex": rankedView(-0.01), "claude": rankedView(-0.01)}
	}
	t.Run("window used up", func(t *testing.T) {
		pools := healthy()
		pools["commandcode"] = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(100), Projected: ptrF(100), Urgency: ptrF(-1), ResetHours: ptrF(100)}
		sel := afterFreePick(t, "B", pools, "", "")
		if got := chosenRung(sel); got != "codex-luna@high" {
			t.Fatalf("chosen %s, want the ranked codex seat", got)
		}
		if got := candOutcome(sel.record(), "omp-dsflash41", "high")["outcome"]; !strings.HasPrefix(got.(string), "dropped:") {
			t.Fatalf("commandcode outcome %v, want dropped", got)
		}
	})
	t.Run("reading unknown", func(t *testing.T) {
		pools := healthy()
		delete(pools, "commandcode")
		sel := afterFreePick(t, "B", pools, "", "")
		if got := chosenRung(sel); got != "codex-luna@high" {
			t.Fatalf("chosen %s, want the ranked codex seat", got)
		}
		if got := candOutcome(sel.record(), "omp-dsflash41", "high")["outcome"]; !strings.HasPrefix(got.(string), "dropped:") {
			t.Fatalf("commandcode outcome %v, want dropped", got)
		}
	})
}

// bandRank puts the after-free pools behind the credit band and ahead of ranked.
func TestBandRankOrdersAfterFreeBetweenCreditAndRanked_T4699(t *testing.T) {
	waste := &selCand{tier: 0, tierName: "waste"}
	free := &selCand{tier: 1, tierName: "free"}
	credit := &selCand{tier: 1, tierName: creditTier}
	after := &selCand{tier: 1, tierName: afterFreeTier}
	ranked := &selCand{tier: 2, tierName: "ranked"}
	got := []int{bandRank(waste), bandRank(free), bandRank(credit), bandRank(after), bandRank(ranked)}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("band ranks %v are not strictly increasing", got)
		}
	}
}

func TestAfterFreePoolsValidation_T4699(t *testing.T) {
	for name, tc := range map[string]struct {
		body string
		ok   bool
	}{
		"plain":        {`"after_free_pools":["commandcode"]`, true},
		"absent":       {``, true},
		"also credit":  {`"credit_pools":["commandcode"],"after_free_pools":["commandcode"]`, false},
		"also free":    {`"free":[{"pool":"commandcode","models":["m"]}],"after_free_pools":["commandcode"]`, false},
		"invalid name": {`"after_free_pools":["bad name"]`, false},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pool-order.json")
			body := `{"version":3,"source_doc":"t","order":["waste","free","ranked"]`
			if tc.body != "" {
				body += "," + tc.body
			}
			if err := os.WriteFile(path, []byte(body+"}"), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadPoolPolicy(path)
			if (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok=%t", err, tc.ok)
			}
		})
	}
}

// `quota status` names Command Code's band with the rules selection uses.
func TestStatusBandNamesAfterFreeCommandcode_T4699(t *testing.T) {
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	policy, err := LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	windows := []StatusWindow{
		{Window: "5h", UsedPct: ptrF(5), ResetsAt: now.Add(2 * time.Hour).Format(time.RFC3339), Scope: "account"},
		{Window: "7d", UsedPct: ptrF(10), ResetsAt: now.Add(84 * time.Hour).Format(time.RFC3339), Scope: "account"},
		{Window: "30d", UsedPct: ptrF(60), ResetsAt: now.Add(360 * time.Hour).Format(time.RFC3339), Scope: "account"},
	}
	if got := StatusBand(policy, "commandcode", "spend", false, windows, now); got != afterFreeTier {
		t.Fatalf("commandcode band = %q, want %q", got, afterFreeTier)
	}
	if got := StatusBand(policy, "commandcode", "spend", true, windows, now); got != "unknown" {
		t.Fatalf("stale commandcode band = %q, want unknown", got)
	}
}

// Credit pacing demotes a subscription pool on pace to use up its longest
// window behind credit, and host-first admission walks such a pool in pass 2,
// after every unpaced ranked candidate. An after-free pool is exempt: it stays
// ahead of the ranked pools after credit is tried (mergewake finding on #579).
func TestAfterFreePoolIsNotPacedBehindCredit_T4699(t *testing.T) {
	policy, err := LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	// Longest window projected to 120%: on pace to be used up.
	over := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), Projected: ptrF(120), RankProjected: ptrF(120), Urgency: ptrF(-0.2), ResetHours: ptrF(100)}
	if policy.paced(CatalogRow{Pool: "commandcode"}, false, false, over) {
		t.Fatal("an after-free pool must not be paced behind credit")
	}
	if !policy.paced(CatalogRow{Pool: "claude"}, false, false, over) {
		t.Fatal("a ranked subscription pool on pace to use up its window is still paced behind credit")
	}

	// Through orderSelCands: a credit pool with a balance is a candidate, and
	// the ranked pool has only a readable short window (not paced).
	mk := func(pool, tier string, idx int, paced bool) *selCand {
		return &selCand{row: CatalogRow{Profile: pool, Pool: pool, Grade: "B"}, tierName: tier, tier: idx, paced: paced}
	}
	credit := mk("anthropic-api", creditTier, 1, false)
	credit.creditPool, credit.creditLeft = true, true
	cc := mk("commandcode", afterFreeTier, 1, policy.paced(CatalogRow{Pool: "commandcode"}, false, false, over))
	ranked := mk("codex", "ranked", 2, false)
	pending := []*selCand{ranked, cc, credit}
	orderSelCands(pending)
	if cc.behindCredit {
		t.Fatal("commandcode was marked behindCredit, so pass 1 would try the ranked pool first")
	}
	if got := []string{pending[0].row.Pool, pending[1].row.Pool, pending[2].row.Pool}; got[0] != "anthropic-api" || got[1] != "commandcode" || got[2] != "codex" {
		t.Fatalf("order %v, want credit, commandcode, ranked", got)
	}
}
