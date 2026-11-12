package fleet

import (
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// creditTotalFixture builds an automatic selection whose credit pool reads a
// fake pacelever manual store, with [quota].credit_total_usd configured from
// totals (nil for no table). The status document and store are timestamped by
// the app's clock.
func creditTotalFixture(t *testing.T, totals map[string]any, entries func(now time.Time) []map[string]any) (*App, Spawn) {
	t.Helper()
	rows := []CatalogRow{ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")}
	a, s, q := selFixture(t, "A", rows, nil, []string{"anthropic-api"})
	serveOMPRouting(t, a)
	policy := filepath.Join(t.TempDir(), "pool-order.json")
	const doc = `{"version": 3, "source_doc": "policy/quota-stop", "order": ["free", "waste", "ranked"],
	  "free": [], "credit_pools": ["anthropic-api", "openai-api"]}`
	if e := os.WriteFile(policy, []byte(doc), 0o600); e != nil {
		t.Fatal(e)
	}
	a.Config.Local["pool_policy"] = policy
	if totals != nil {
		a.Config.Quota = map[string]any{"credit_total_usd": totals}
	}
	now := a.Now()
	store, status := usdCreditStore(entries(now)), usdCreditStatus(now, "anthropic-api")
	pools, e := CLIQuota{Binary: "pacelever", Now: a.Now, CreditTotalUSD: creditTotals(a.Config.Quota), Runner: &fakeRunner{Do: func(c Command) Result {
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
	q.PoolViews = pools
	q.GateDo = gatePools(map[string]string{"omp-opus": "anthropic-api"})
	return a, s
}

// (a) A shared entry (no credit object) with a configured total derives its
// dollar balance, and selection ranks on the derived dollars.
func TestCreditTotalDerivesBalanceAndUSDUrgency(t *testing.T) {
	a, s := creditTotalFixture(t, map[string]any{"anthropic-api": 65.0}, func(now time.Time) []map[string]any {
		return []map[string]any{usdCreditEntry("a1", "anthropic-api", 42.35, nil, now.Add(-time.Hour), onDay(now, 9*24*time.Hour))}
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	credit := creditTrace(t, sel.record(), "omp-opus")
	balance, ok := credit["balance_usd"].(float64)
	if !ok || math.Abs(balance-37.47) > 0.01 {
		t.Fatalf("balance_usd = %v, want 65 × (100 − 42.35) / 100 = 37.47", credit["balance_usd"])
	}
	if credit["usd_urgency"] == nil {
		t.Fatalf("derived dollars must rank by usd_urgency: %v", credit)
	}
}

// (b) An entry's own remaining_usd wins over the configured total.
func TestCreditTotalEntryRemainingWins(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := usdCreditStore([]map[string]any{usdCreditEntry("a1", "anthropic-api", 42.35, ptrF(50), now.Add(-time.Hour), onDay(now, 9*24*time.Hour))})
	balances, err := CLIQuota{Binary: "pacelever", Now: func() time.Time { return now }, CreditTotalUSD: map[string]float64{"anthropic-api": 65}, Runner: &fakeRunner{Do: func(Command) Result {
		return Result{Out: store}
	}}}.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if c := balances["anthropic-api"]; c == nil || c.BalanceUSD == nil || *c.BalanceUSD != 50 {
		t.Fatalf("the entry's remaining_usd must win: %+v", c)
	}
}

// (c) Without a total and without a credit object nothing records dollars, so
// the plan keeps today's credit_usd_missing warning.
func TestCreditTotalMissingWithoutEitherSource(t *testing.T) {
	a, s := creditTotalFixture(t, nil, func(now time.Time) []map[string]any {
		return []map[string]any{usdCreditEntry("a1", "anthropic-api", 42.35, nil, now.Add(-time.Hour), onDay(now, 9*24*time.Hour))}
	})
	var warnings bytes.Buffer
	a.Err = &warnings
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if credit := creditTrace(t, sel.record(), "omp-opus"); credit["balance_usd"] != nil {
		t.Fatalf("no source records dollars: %v", credit)
	}
	if n := strings.Count(warnings.String(), "credit_usd_missing pool=anthropic-api"); n != 1 {
		t.Fatalf("credit_usd_missing logged %d times: %q", n, warnings.String())
	}
}

// (c) A configured total gives both entries dollars, so the warning is gone.
func TestCreditTotalSuppressesMissingWarning(t *testing.T) {
	a, s := creditTotalFixture(t, map[string]any{"anthropic-api": 65.0}, func(now time.Time) []map[string]any {
		return []map[string]any{usdCreditEntry("a1", "anthropic-api", 42.35, nil, now.Add(-time.Hour), onDay(now, 9*24*time.Hour))}
	})
	var warnings bytes.Buffer
	a.Err = &warnings
	if _, err := a.selectProfile(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(warnings.String(), "credit_usd_missing") {
		t.Fatalf("derived dollars must clear credit_usd_missing: %q", warnings.String())
	}
}

// (d) Two shared entries 24 h apart give (older − newer) / hours from the
// derived dollars: 65×(100−30)/100 − 65×(100−42.35)/100 = 8.0275 over 24 h.
func TestCreditTotalBurnFromSharedEntries(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	expires := onDay(now, 240*time.Hour)
	store := usdCreditStore([]map[string]any{
		usdCreditEntry("a1", "anthropic-api", 30, nil, now.Add(-25*time.Hour), expires),
		usdCreditEntry("a2", "anthropic-api", 42.35, nil, now.Add(-time.Hour), expires),
	})
	balances, err := CLIQuota{Binary: "pacelever", Now: func() time.Time { return now }, CreditTotalUSD: map[string]float64{"anthropic-api": 65}, Runner: &fakeRunner{Do: func(Command) Result {
		return Result{Out: store}
	}}}.creditBalances(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c := balances["anthropic-api"]
	if c == nil || c.BalanceUSD == nil {
		t.Fatalf("shared entries must derive dollars: %+v", c)
	}
	if math.Abs(c.BurnUSDPerH-0.334) > 0.001 {
		t.Fatalf("burn = %v, want ≈ 0.334 USD/h", c.BurnUSDPerH)
	}
}

// (e) [quota].credit_total_usd accepts a pool → positive dollars table and
// refuses anything else at load.
func TestCreditTotalUSDConfigValidated(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		ok         bool
	}{
		{name: "valid", body: "[quota]\n[quota.credit_total_usd]\nopenai-api = 65.0\nanthropic-api = 200\n", ok: true},
		{name: "empty table", body: "[quota]\n[quota.credit_total_usd]\n", ok: true},
		{name: "zero", body: "[quota]\n[quota.credit_total_usd]\nopenai-api = 0\n"},
		{name: "negative", body: "[quota]\n[quota.credit_total_usd]\nopenai-api = -1\n"},
		{name: "string", body: "[quota]\n[quota.credit_total_usd]\nopenai-api = \"65\"\n"},
		{name: "not a table", body: "[quota]\ncredit_total_usd = 65\n"},
		{name: "invalid pool", body: "[quota]\n[quota.credit_total_usd]\n\"bad pool\" = 5\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "hosts.toml")
			if err := os.WriteFile(p, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfig(p)
			if (err == nil) != tc.ok {
				t.Fatalf("LoadConfig err=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}
