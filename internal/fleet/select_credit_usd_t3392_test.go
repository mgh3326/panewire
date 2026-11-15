package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// usdCreditEntry renders one pacelever manual credit observation. remaining
// nil records the entry without dollars.
func usdCreditEntry(id, pool string, usedPct float64, remaining *float64, measured time.Time, expiresOn string) map[string]any {
	m := map[string]any{
		"manual_observation_id": id,
		"event":                 "set",
		"pool":                  pool,
		"window":                "credit",
		"used_pct":              usedPct,
		"measured_at":           measured.Format(time.RFC3339Nano),
		"expires_at":            expiresOn + "T00:00:00Z",
	}
	if remaining != nil {
		m["credit"] = map[string]any{"remaining_usd": *remaining, "total_usd": 400.0, "expires_on": expiresOn}
	}
	return m
}

// usdCreditStore renders a manual store whose latest credit entry per pool is
// the last one listed.
func usdCreditStore(entries []map[string]any) string {
	latest := map[string]string{}
	for _, e := range entries {
		latest[e["pool"].(string)+":credit"] = e["manual_observation_id"].(string)
	}
	b, _ := json.Marshal(map[string]any{"schema": "pacelever.manual.v1", "latest": latest, "history": entries})
	return string(b)
}

func usdCreditStatus(now time.Time, pools ...string) string {
	var providers []map[string]any
	for _, id := range pools {
		providers = append(providers, map[string]any{"id": id, "status": "ok", "pool_class": "spend"})
	}
	b, _ := json.Marshal(map[string]any{"schema": "scopefuel.v1", "generated_at": now.Format(time.RFC3339Nano), "providers": providers})
	return string(b)
}

// usdCreditFixture builds the selection from a fake pacelever manual store and
// status document, timestamped by the app's clock.
func usdCreditFixture(t *testing.T, entries func(now time.Time) []map[string]any, claude *PoolView, openai bool) (*App, Spawn, *fakeQuota) {
	t.Helper()
	rows := []CatalogRow{ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")}
	present := []string{"anthropic-api"}
	if openai {
		rows = append(rows, ompRow("omp-sol", "openai/gpt-6.1-sol", "openai-api"))
		present = append(present, "openai-api")
	}
	if claude != nil {
		rows = append(rows, CatalogRow{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"})
		present = append(present, "claude")
	}
	a, s, q := selFixture(t, "A", rows, nil, present)
	serveOMPRouting(t, a)
	policy := filepath.Join(t.TempDir(), "pool-order.json")
	const doc = `{"version": 3, "source_doc": "policy/quota-stop", "order": ["free", "waste", "ranked"],
	  "free": [], "credit_pools": ["anthropic-api", "openai-api"]}`
	if e := os.WriteFile(policy, []byte(doc), 0o600); e != nil {
		t.Fatal(e)
	}
	a.Config.Local["pool_policy"] = policy
	store, status := usdCreditStore(entries(a.Now())), usdCreditStatus(a.Now(), "anthropic-api", "openai-api")
	pools, e := CLIQuota{Binary: "pacelever", Now: a.Now, Runner: &fakeRunner{Do: func(c Command) Result {
		switch strings.Join(c.Args, " ") {
		case "manual list":
			return Result{Out: store}
		case "--json":
			return Result{Out: status}
		}
		return Result{}
	}}}.Pools(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if claude != nil {
		pools["claude"] = *claude
	}
	q.PoolViews = pools
	q.GateDo = gatePools(map[string]string{"omp-opus": "anthropic-api", "omp-sol": "openai-api", "sonnet": "claude"})
	return a, s, q
}

// creditTrace is a candidate row's nested `credit` trace object.
func creditTrace(t *testing.T, rec map[string]any, profile string) map[string]any {
	t.Helper()
	row := candOutcome(rec, profile, "high")
	if row == nil {
		t.Fatalf("no candidate row for %s", profile)
	}
	c, _ := row["credit"].(map[string]any)
	return c
}

func onDay(now time.Time, in time.Duration) string {
	return now.Add(in).UTC().Format("2006-01-02")
}

// (a) With no burn the pool with more dollars at risk ranks first, even though
// its remaining percentage is lower: anthropic-api $300 / 9 days beats
// openai-api $95 / 11 days (77% vs 95% left: the percent rule picks
// openai-api).
func TestCreditUSDDollarsAtRiskRankAbovePercent(t *testing.T) {
	a, s, _ := usdCreditFixture(t, func(now time.Time) []map[string]any {
		return []map[string]any{
			usdCreditEntry("a1", "anthropic-api", 23, ptrF(300), now.Add(-time.Hour), onDay(now, 9*24*time.Hour)),
			usdCreditEntry("o1", "openai-api", 5, ptrF(95), now.Add(-time.Hour), onDay(now, 11*24*time.Hour)),
		}
	}, nil, true)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "omp-opus" {
		t.Fatalf("dollars at risk must beat percent: chose %s, ranked %v", sel.chosen.seatName, rankedSeats(sel))
	}
	if got := rankedSeats(sel); len(got) < 2 || got[0] != "omp-opus" || got[1] != "omp-sol" {
		t.Fatalf("ranked order %v, want anthropic-api first", got)
	}
	credit := creditTrace(t, sel.record(), "omp-opus")
	if credit["balance_usd"] != 300.0 || credit["burn_usd_per_h"] != 0.0 || credit["at_risk_usd"] != 300.0 {
		t.Fatalf("anthropic-api dollars trace: %v", credit)
	}
	if u, _ := credit["usd_urgency"].(float64); u < 1.38 || u > 1.39 {
		t.Fatalf("usd_urgency = %v, want 300/216h", credit["usd_urgency"])
	}
	row := candOutcome(sel.record(), "omp-opus", "high")
	if row["waste_risk"] != false || row["tier"] != "credit" || row["waste_order"] != "usd" || row["usd_urgency"] == nil {
		t.Fatalf("anthropic-api must be ordered by dollars inside the credit band: %v", row)
	}
}

// (b) Burn that projects spend at or above the balance leaves no money at
// risk: the credit drops out of the waste band into the plain credit tier.
func TestCreditUSDBurnConsumingBalanceIsNotWasteRisk(t *testing.T) {
	a, s, _ := usdCreditFixture(t, func(now time.Time) []map[string]any {
		expires := onDay(now, 240*time.Hour)
		return []map[string]any{
			usdCreditEntry("a1", "anthropic-api", 50, ptrF(200), now.Add(-100*time.Hour), expires),
			usdCreditEntry("a2", "anthropic-api", 75, ptrF(100), now, expires),
		}
	}, ptrPool(riskPool(20, 8)), false)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	credit := creditTrace(t, sel.record(), "omp-opus")
	if credit["at_risk_usd"] != 0.0 || credit["burn_usd_per_h"] != 1.0 {
		t.Fatalf("projected spend covers the balance: %v", credit)
	}
	row := candOutcome(sel.record(), "omp-opus", "high")
	if row["waste_risk"] != false || row["tier"] != "credit" {
		t.Fatalf("a balance the pace uses up is not waste risk: %v", row)
	}
	if got := rankedSeats(sel); got[len(got)-1] != "omp-opus" {
		t.Fatalf("plain credit ranks last: %v", got)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("the waste-risk subscription takes the work: %s", sel.chosen.seatName)
	}
}

// (c) A credit entry with dollars outranks one without, and the missing
// dollars are reported once per plan.
func TestCreditUSDOutranksPercentOnlyAndWarns(t *testing.T) {
	a, s, _ := usdCreditFixture(t, func(now time.Time) []map[string]any {
		return []map[string]any{
			usdCreditEntry("a1", "anthropic-api", 23, ptrF(300), now.Add(-time.Hour), onDay(now, 9*24*time.Hour)),
			usdCreditEntry("o1", "openai-api", 5, nil, now.Add(-time.Hour), onDay(now, 11*24*time.Hour)),
		}
	}, nil, true)
	var warnings bytes.Buffer
	a.Err = &warnings
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.seatName != "omp-opus" {
		t.Fatalf("credit with dollars must outrank percent-only credit: %s", sel.chosen.seatName)
	}
	if got := creditTrace(t, sel.record(), "omp-sol"); got["balance_usd"] != nil || got["at_risk_usd"] != nil {
		t.Fatalf("openai-api records no dollars: %v", got)
	}
	row := candOutcome(sel.record(), "omp-sol", "high")
	if row["waste_order"] != "reset" || row["waste_risk"] != false || row["tier"] != "credit" {
		t.Fatalf("percent-only credit keeps the percentage rule: %v", row)
	}
	if n := strings.Count(warnings.String(), "credit_usd_missing pool=openai-api"); n != 1 {
		t.Fatalf("credit_usd_missing logged %d times: %q", n, warnings.String())
	}
}

// (d) A waste-risk subscription ranks ahead of credit in the free band, with
// or without money at risk.
func TestCreditUSDWasteBandOrdering(t *testing.T) {
	atRisk := func(now time.Time) []map[string]any {
		return []map[string]any{usdCreditEntry("a1", "anthropic-api", 75, ptrF(100), now.Add(-time.Hour), onDay(now, 240*time.Hour))}
	}
	burned := func(now time.Time) []map[string]any {
		expires := onDay(now, 240*time.Hour)
		return []map[string]any{
			usdCreditEntry("a1", "anthropic-api", 50, ptrF(200), now.Add(-100*time.Hour), expires),
			usdCreditEntry("a2", "anthropic-api", 75, ptrF(100), now, expires),
		}
	}
	for _, tc := range []struct {
		name    string
		entries func(time.Time) []map[string]any
		want    string
	}{
		{"a waste-risk subscription beats credit at risk", atRisk, "sonnet"},
		{"a subscription beats credit with nothing at risk", burned, "sonnet"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := usdCreditFixture(t, tc.entries, ptrPool(riskPool(20, 8)), false)
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if sel.chosen.seatName != tc.want {
				t.Fatalf("chose %s, want %s (ranked %v)", sel.chosen.seatName, tc.want, rankedSeats(sel))
			}
		})
	}
}

// (e) A balance that rose records no burn, so nothing is projected spent.
func TestCreditUSDBalanceRoseMeansNoBurn(t *testing.T) {
	a, s, _ := usdCreditFixture(t, func(now time.Time) []map[string]any {
		expires := onDay(now, 100*time.Hour)
		return []map[string]any{
			usdCreditEntry("a1", "anthropic-api", 75, ptrF(100), now.Add(-100*time.Hour), expires),
			usdCreditEntry("a2", "anthropic-api", 62.5, ptrF(150), now, expires),
		}
	}, nil, false)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	credit := creditTrace(t, sel.record(), "omp-opus")
	if credit["burn_usd_per_h"] != 0.0 || credit["at_risk_usd"] != 150.0 || credit["balance_usd"] != 150.0 {
		t.Fatalf("a rise must not project a spend: %v", credit)
	}

	// The burn rate itself comes from the two latest entries, not all of them.
	now := a.Now()
	expires := onDay(now, 100*time.Hour)
	entries := []map[string]any{
		usdCreditEntry("o0", "openai-api", 25, ptrF(300), now.Add(-300*time.Hour), expires),
		usdCreditEntry("o1", "openai-api", 50, ptrF(200), now.Add(-100*time.Hour), expires),
		usdCreditEntry("o2", "openai-api", 62.5, ptrF(150), now, expires),
	}
	q := CLIQuota{Binary: "pacelever", Now: a.Now, Runner: &fakeRunner{Do: func(Command) Result {
		return Result{Out: usdCreditStore(entries)}
	}}}
	balances, err := q.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := balances["openai-api"]; got == nil || got.BurnUSDPerH != 0.5 || *got.BalanceUSD != 150 {
		t.Fatalf("burn from the two latest entries: %+v", got)
	}
}

func ptrPool(v PoolView) *PoolView { return &v }
