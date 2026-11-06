package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

const testCreditA = "anthropic-api:00000000000000d4"

// creditBandFixture seats a credit account and a ranked codex account on
// hosts mac-b (rank 1) and cloud-small (rank 2). place maps host to account.
func creditBandFixture(t *testing.T, rows []CatalogRow, place map[string]string, credit func(time.Time) *CreditBalance) (*App, Spawn, *fakeQuota, *fakePlacementSource) {
	t.Helper()
	rows = append([]CatalogRow{}, rows...)
	a, s, q, src := hostFirstFixture(t, "worker", "A", rows, place, map[string]float64{"mac-b": 0.1, "cloud-small": 0.5})
	serveOMPRouting(t, a)
	path := filepath.Join(t.TempDir(), "pool-order.json")
	if err := os.WriteFile(path, []byte(riskPolicy("")), 0o600); err != nil {
		t.Fatal(err)
	}
	a.Config.Local["pool_policy"] = path
	view := PoolView{Status: "ok", Class: "spend", Credit: credit(a.Now())}
	// A credit candidate reads its balance from the pool, not the account.
	q.PoolViews["anthropic-api"] = view
	src.accounts[testCreditA] = SharedAccount{Key: testCreditA, Observed: true, Fresh: true, MeasuredAt: a.Now()}
	codex := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(30)}
	src.accounts[testCodexA] = SharedAccount{Key: testCodexA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &codex}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "codex", "omp-opus": "anthropic-api"})
	return a, s, q, src
}

var (
	creditRow = ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")
	codexRow  = CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "sol-m"}
	sonnetRow = CatalogRow{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"}
)

func outcomeOf(t *testing.T, sel *selPlan, profile string) string {
	t.Helper()
	for _, c := range sel.cands {
		if c.row.Profile == profile {
			return c.outcome
		}
	}
	t.Fatalf("no candidate %s", profile)
	return ""
}

// hostPasses lists the pass of every ranked host row in trace order.
func hostPasses(sel *selPlan) []string {
	var out []string
	for _, t := range sel.hosts {
		if t.rank > 0 {
			out = append(out, t.host.name+"/"+string(rune('0'+t.pass)))
		}
	}
	return out
}

// Plain credit is a fleet-wide last band: a ranked subscription on a later
// host is chosen before credit on the first host.
func TestHostFirstCreditIsLastBandAcrossHosts(t *testing.T) {
	a, s, _, _ := creditBandFixture(t, []CatalogRow{creditRow, codexRow}, map[string]string{"mac-b": testCreditA, "cloud-small": testCodexA}, noBalance)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "codex-sol" || sel.chosen.host != "cloud-small" {
		t.Fatalf("ranked subscription on rank 2 must beat credit on rank 1: %s on %s", sel.chosen.row.Profile, sel.chosen.host)
	}
	if got := outcomeOf(t, sel, "omp-opus"); got != "below_choice" {
		t.Fatalf("credit outcome = %q, want below_choice", got)
	}
	for _, got := range hostPasses(sel) {
		if got[len(got)-1] != '1' {
			t.Fatalf("a chosen pass 1 leaves pass 2 untried: %v", hostPasses(sel))
		}
	}
	if got := hostRow(t, sel, "mac-b")["outcome"]; got != "no_candidate_present" {
		t.Fatalf("rank 1 holds only credit, which is not a pass 1 candidate: outcome %v", got)
	}
}

// With nothing else admissible, credit is chosen on the first host that
// admits it, in a second pass over the same ranked hosts.
func TestHostFirstCreditOnlyAdmissibleChosenOnFirstHost(t *testing.T) {
	for _, tc := range []struct {
		name     string
		place    map[string]string
		wantHost string
	}{
		{"both hosts hold it", map[string]string{"mac-b": testCreditA, "cloud-small": testCreditA}, "mac-b"},
		{"only the second host holds it", map[string]string{"cloud-small": testCreditA}, "cloud-small"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, _ := creditBandFixture(t, []CatalogRow{creditRow}, tc.place, noBalance)
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.seatName != "omp-opus" || sel.chosen.host != tc.wantHost {
				t.Fatalf("credit chosen on %s, want %s", sel.chosen.host, tc.wantHost)
			}
			if !slices.Contains(hostPasses(sel), tc.wantHost+"/2") {
				t.Fatalf("credit is admitted in pass 2: %v", hostPasses(sel))
			}
			for _, h := range sel.record()["hosts"].([]any) {
				if row := h.(map[string]any); row["host"] == tc.wantHost && row["pass"] == 2 && row["outcome"] != "chosen" {
					t.Fatalf("pass 2 row must be the chosen one: %v", row)
				}
			}
		})
	}
}

// Expiring credit is waste-risk: it keeps its place ahead of ranked
// subscriptions, so it is a pass 1 candidate and wins on rank 1.
func TestHostFirstExpiringCreditStaysInPassOne(t *testing.T) {
	a, s, _, _ := creditBandFixture(t, []CatalogRow{creditRow, codexRow}, map[string]string{"mac-b": testCreditA, "cloud-small": testCodexA}, balance(80, 100*time.Hour))
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "omp-opus" || sel.chosen.host != "mac-b" || !sel.chosen.risk {
		t.Fatalf("waste-risk credit must be chosen on rank 1: %s on %s risk=%v", sel.chosen.row.Profile, sel.chosen.host, sel.chosen.risk)
	}
	if got := hostPasses(sel); got[0] != "mac-b/1" {
		t.Fatalf("waste-risk credit is judged in pass 1: %v", got)
	}
}

// A subscription paced to use up its window ranks behind credit that is
// worth spending (expiring). When that credit is refused, the paced pool is
// still in the last band: a ranked subscription on a later host wins first.
func TestHostFirstPacedPoolIsInLastBand(t *testing.T) {
	paced := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), LongProjected: ptrF(120), Projected: ptrF(120), ShortUsed: ptrF(40)}
	setup := func(refuseCredit bool) (*selPlan, error) {
		a, s, q, src := creditBandFixture(t, []CatalogRow{creditRow, sonnetRow, codexRow}, map[string]string{"mac-b": testClaudeA, "cloud-small": testCodexA}, balance(80, 100*time.Hour))
		src.presence = append(src.presence, PresenceRecord{HostID: "mac-b", AccountKey: testCreditA, LastSeen: a.Now()})
		src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &paced}
		if refuseCredit {
			gate := q.GateDo
			q.GateDo = func(r QuotaRequest) (string, error) {
				if r.AccountKey == testCreditA {
					return "", fail(3, "quota exhausted")
				}
				return gate(r)
			}
		}
		return a.selectProfile(context.Background(), s)
	}
	sel, err := setup(false)
	if err != nil || sel.chosen.seatName != "omp-opus" || !sel.chosen.risk {
		t.Fatalf("credit on the host is chosen before its paced pool: %v %+v", err, sel)
	}
	if got := outcomeOf(t, sel, "sonnet"); got != "below_choice" {
		t.Fatalf("paced pool outcome = %q, want below_choice", got)
	}
	sel, err = setup(true)
	if err != nil || sel.chosen.seatName != "codex-sol" || sel.chosen.host != "cloud-small" {
		t.Fatalf("ranked subscription on rank 2 must precede the paced pool on rank 1: %v %+v", err, sel)
	}
	if got := outcomeOf(t, sel, "sonnet"); got != "below_choice" {
		t.Fatalf("paced pool outcome = %q, want below_choice", got)
	}
}
