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
	a, s, q, src := hostFirstFixture(t, "implementer", "A", rows, place, map[string]float64{"mac-b": 0.1, "cloud-small": 0.5})
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

// Credit ranks in the free band, so credit on the first host beats a ranked
// subscription on a later host: host-first applies inside the band.
func TestHostFirstCreditRanksWithFreeAcrossHosts(t *testing.T) {
	a, s, _, _ := creditBandFixture(t, []CatalogRow{creditRow, codexRow}, map[string]string{"mac-b": testCreditA, "cloud-small": testCodexA}, noBalance)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "omp-opus" || sel.chosen.host != "mac-b" {
		t.Fatalf("credit on rank 1 must beat a ranked subscription on rank 2: %s on %s", sel.chosen.row.Profile, sel.chosen.host)
	}
	if got := outcomeOf(t, sel, "codex-sol"); got != "below_choice" {
		t.Fatalf("ranked outcome = %q, want below_choice", got)
	}
	for _, got := range hostPasses(sel) {
		if got[len(got)-1] != '1' {
			t.Fatalf("a chosen pass 1 leaves pass 2 untried: %v", hostPasses(sel))
		}
	}
	if got := hostRow(t, sel, "mac-b")["outcome"]; got != "chosen" {
		t.Fatalf("rank 1 holds the chosen credit rung: outcome %v", got)
	}
}

// With nothing else admissible, credit is chosen on the first host that
// admits it, in pass 1.
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
			if !slices.Contains(hostPasses(sel), tc.wantHost+"/1") {
				t.Fatalf("credit is admitted in pass 1: %v", hostPasses(sel))
			}
			for _, h := range sel.record()["hosts"].([]any) {
				if row := h.(map[string]any); row["host"] == tc.wantHost && row["pass"] == 1 && row["outcome"] != "chosen" {
					t.Fatalf("pass 1 row must be the chosen one: %v", row)
				}
			}
		})
	}
}

// Expiring credit ranks in the free band (never the waste band), so it is a
// pass 1 candidate and wins on rank 1.
func TestHostFirstExpiringCreditStaysInPassOne(t *testing.T) {
	a, s, _, _ := creditBandFixture(t, []CatalogRow{creditRow, codexRow}, map[string]string{"mac-b": testCreditA, "cloud-small": testCodexA}, balance(80, 100*time.Hour))
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "omp-opus" || sel.chosen.host != "mac-b" || sel.chosen.risk || sel.chosen.tierName != "credit" {
		t.Fatalf("expiring credit must be chosen on rank 1 in the credit band: %s on %s tier=%s risk=%v", sel.chosen.row.Profile, sel.chosen.host, sel.chosen.tierName, sel.chosen.risk)
	}
	if got := hostPasses(sel); got[0] != "mac-b/1" {
		t.Fatalf("credit is judged in pass 1: %v", got)
	}
}

// A subscription paced to use up its window ranks behind credit that is
// worth spending (expiring). When that credit is refused, the paced pool is
// still in the last band: a ranked subscription on a later host wins first.
func TestHostFirstPacedPoolIsInLastBand(t *testing.T) {
	paced := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), RankProjected: ptrF(120), Projected: ptrF(120), ShortUsed: ptrF(40)}
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
	if err != nil || sel.chosen.seatName != "omp-opus" || sel.chosen.tierName != "credit" {
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

// T2997 stands: a pool is paced behind credit only when that credit is on its
// own host. Credit on another host never demotes a paced pool, so the paced
// pool on rank 1 is chosen in pass 1.
func TestHostFirstPacedPoolOnAWithCreditOnlyOnBChosenInPassOne(t *testing.T) {
	paced := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), RankProjected: ptrF(120), Projected: ptrF(120), ShortUsed: ptrF(40)}
	for _, tc := range []struct {
		name   string
		credit func(time.Time) *CreditBalance
	}{
		{"plain credit", noBalance},
		{"expiring credit", balance(80, 100*time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, src := creditBandFixture(t, []CatalogRow{creditRow, sonnetRow}, map[string]string{"mac-b": testClaudeA, "cloud-small": testCreditA}, tc.credit)
			src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &paced}
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.seatName != "sonnet" || sel.chosen.host != "mac-b" || sel.chosen.behindCredit {
				t.Fatalf("paced pool on A must beat credit that is only on B: %s on %s behindCredit=%v", sel.chosen.row.Profile, sel.chosen.host, sel.chosen.behindCredit)
			}
			if got := outcomeOf(t, sel, "omp-opus"); got != "below_choice" {
				t.Fatalf("credit outcome = %q, want below_choice", got)
			}
			for _, got := range hostPasses(sel) {
				if got[len(got)-1] != '1' {
					t.Fatalf("the paced pool is chosen in pass 1, pass 2 untried: %v", hostPasses(sel))
				}
			}
		})
	}
}

// A credit-only host whose gate says quota exhausted keeps that refusal in the
// placement record.
func TestPlacementRecordKeepsCreditRefusal(t *testing.T) {
	a, s, q, _ := creditBandFixture(t, []CatalogRow{creditRow}, map[string]string{"mac-b": testCreditA}, noBalance)
	q.GateDo = func(QuotaRequest) (string, error) { return "", fail(3, "quota exhausted") }
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil {
		t.Fatal("the only credit account is exhausted: selection must refuse")
	}
	var seen int
	for _, h := range selectionTerms(sel.record()) {
		if h.Host != "mac-b" {
			continue
		}
		seen++
		if h.Outcome != "quota_exhausted" && h.Outcome != "quota_unknown" {
			t.Fatalf("mac-b outcome = %q, want the credit rung's refusal", h.Outcome)
		}
		if len(h.Tried) != 1 || (h.Tried[0].Outcome != "quota_exhausted" && h.Tried[0].Outcome != "quota_unknown") {
			t.Fatalf("mac-b tried rung = %+v, want the credit refusal", h.Tried)
		}
	}
	if seen != 1 {
		t.Fatalf("mac-b must appear once in the record, got %d", seen)
	}
}

// A two-pass selection lists each host once in the job.placement event, so a
// repeated host does not use up MaxCandidates. Pass 1 refuses the credit rung;
// pass 2 admits the paced subscription pool on the same host.
func TestSelectionTermsListEachHostOnce(t *testing.T) {
	paced := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), RankProjected: ptrF(120), Projected: ptrF(120), ShortUsed: ptrF(40)}
	a, s, q, src := creditBandFixture(t, []CatalogRow{creditRow, sonnetRow}, map[string]string{"mac-b": testCreditA}, balance(80, 100*time.Hour))
	src.presence = append(src.presence, PresenceRecord{HostID: "mac-b", AccountKey: testClaudeA, LastSeen: a.Now()})
	src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &paced}
	gate := q.GateDo
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey == testCreditA {
			return "", fail(3, "quota exhausted")
		}
		return gate(r)
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if got := hostPasses(sel); !slices.Contains(got, "mac-b/2") || !slices.Contains(got, "mac-b/1") {
		t.Fatalf("fixture must run two passes: %v", got)
	}
	terms := selectionTerms(sel.record())
	seen := map[string]int{}
	for _, h := range terms {
		seen[h.Host]++
	}
	if len(terms) != len(seen) || seen["mac-b"] != 1 || seen["cloud-small"] != 1 {
		t.Fatalf("each host once, got %+v", terms)
	}
	for _, h := range terms {
		if h.Host == "mac-b" && h.Outcome != "admitted" {
			t.Fatalf("the pass 2 row replaces pass 1: mac-b outcome = %q", h.Outcome)
		}
	}
}

// A credit-band row that placed carries no refusal: the record keeps the
// pass 1 reason for the host selection chose, even when execution never
// started, so it is never reported as skipped for the reason "chosen".
func TestPlacementRecordDoesNotMergeAdmittedCreditBandRow(t *testing.T) {
	a, s, _, _ := creditBandFixture(t, []CatalogRow{creditRow}, map[string]string{"mac-b": testCreditA}, noBalance)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.host != "mac-b" {
		t.Fatalf("credit chosen on mac-b: %v %+v", err, sel)
	}
	for _, r := range selectionCandidates(sel.record(), "mac-b") {
		if r.Host == "mac-b" && (r.Reason == "chosen" || r.Reason == "admitted") {
			t.Fatalf("an admitted pass 2 row must not become a skip reason: %+v", r)
		}
	}
}

// A host the walk leaves not_tried (an earlier host admitted) does not erase
// the refusal recorded for the host that was tried first.
func TestSelectionTermsKeepHostRefusal(t *testing.T) {
	a, s, q, _ := creditBandFixture(t, []CatalogRow{creditRow, codexRow}, map[string]string{"mac-b": testCodexA, "cloud-small": testCreditA}, noBalance)
	gate := q.GateDo
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey == testCodexA {
			return "", fail(3, "quota exhausted")
		}
		return gate(r)
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.seatName != "omp-opus" || sel.chosen.host != "cloud-small" {
		t.Fatalf("credit admits on cloud-small: %v %+v", err, sel)
	}
	for _, h := range selectionTerms(sel.record()) {
		if h.Host == "mac-b" && (h.Outcome != "quota_exhausted" || len(h.Tried) == 0) {
			t.Fatalf("mac-b keeps its refusal, got %+v", h)
		}
	}
}
