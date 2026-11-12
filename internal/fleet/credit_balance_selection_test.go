package fleet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

const creditBalanceAccount = "0123456789abcdef"

type nativeCreditPoolQuota struct {
	*fakeQuota
	production admissionQuota
}

func (q *nativeCreditPoolQuota) Pools(ctx context.Context) (map[string]PoolView, error) {
	return q.production.Pools(ctx)
}

func creditBalanceSelectionApp(t *testing.T, readErr error) (*App, Spawn) {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
		{Profile: "omp-luna", Effort: "high", Grade: "B", Pool: "openai-api", Gate: "default", ModelID: "openai/gpt-6-luna"},
	}
	a, s, q := selFixture(t, "C", rows, nil, []string{"devin", "openai-api"})
	serveOMPRouting(t, a)
	q.PoolViews = map[string]PoolView{}
	q.PoolViews["devin"] = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(4), Urgency: ptrF(-0.0385), ResetHours: ptrF(24)}
	q.GateDo = gatePools(map[string]string{"devin-swe2-flash-medium": "devin", "omp-luna": "openai-api"})
	poolOrderPath := filepath.Join(t.TempDir(), "pool-order.json")
	if err := os.WriteFile(poolOrderPath, []byte(`{"version":3,"order":["waste","free","ranked"],"free":[{"pool":"devin","models":["swe-2-flash-medium"]}],"credit_pools":["openai-api"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a.Config.Local["pool_policy"] = poolOrderPath
	paceleverConfig := filepath.Join(t.TempDir(), "pacelever.toml")
	if err := os.WriteFile(paceleverConfig, []byte("[quota.accounts]\nopenai-api = \""+creditBalanceAccount+"\"\n[pools.openai-api]\nclass = \"spend\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", paceleverConfig)
	now := a.Now()
	expires := now.Add(6 * 24 * time.Hour)
	older := shared.Observation{ID: 1, Kind: "manual", Window: "credit", MeasuredAt: now.Add(-5 * 24 * time.Hour), ExpiresAt: &expires, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 40, Scope: "account"}}}
	current := shared.Observation{ID: 2, Kind: "manual", Window: "credit", MeasuredAt: now.Add(-time.Hour), ExpiresAt: &expires, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 49.2, Scope: "account"}}}
	a.SharedLatest = func(_ context.Context, key string) (shared.Latest, error) {
		if key != "openai-api:"+creditBalanceAccount {
			return shared.Latest{}, fmt.Errorf("unexpected account key %q", key)
		}
		if readErr != nil {
			return shared.Latest{}, readErr
		}
		return shared.Latest{Manual: []shared.Observation{older, current}}, nil
	}
	cli := CLIQuota{
		Runner:         a.Runner,
		Binary:         "pacelever",
		CreditTotalUSD: map[string]float64{"openai-api": 65},
		SharedLatest:   a.sharedLatest,
	}
	production := admissionQuota{CLIQuota: cli, app: a}
	a.Quota = &nativeCreditPoolQuota{fakeQuota: q, production: production}
	return a, s
}

func TestProductionSelectionPoolsAttachCreditBalance(t *testing.T) {
	a, s := creditBalanceSelectionApp(t, nil)
	views, err := a.Quota.Pools(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	credit := views["openai-api"].Credit
	if credit == nil {
		t.Fatal("production-wired credit pool view has nil Credit")
	}
	if credit.LeftPct != 50.8 || credit.BalanceUSD == nil || *credit.BalanceUSD != 33.02 {
		t.Fatalf("credit balance = %+v, want 50.8%% and $33.02", credit)
	}
	if credit.BurnUSDPerH <= 0 {
		t.Fatalf("credit burn = %v, want a positive burn rate", credit.BurnUSDPerH)
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(sel.record(), "omp-luna", "high")
	if row == nil || row["usd_urgency"] == nil {
		t.Fatalf("OpenAI credit candidate lacks usd_urgency: %v", row)
	}
	if sel.chosen.row.Profile != "devin-swe2-flash-medium" || sel.chosen.row.Effort != "medium" {
		t.Fatalf("chosen %s@%s, want devin-swe2-flash-medium@medium", sel.chosen.row.Profile, sel.chosen.row.Effort)
	}
}

func TestProductionSelectionCreditDollarUrgencyBeatsFreeWasteUrgency(t *testing.T) {
	a, s := creditBalanceSelectionApp(t, nil)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.row.Profile != "devin-swe2-flash-medium" || sel.chosen.row.Effort != "medium" {
		t.Fatalf("chosen %s@%s, want devin-swe2-flash-medium@medium", sel.chosen.row.Profile, sel.chosen.row.Effort)
	}
	free := candOutcome(sel.record(), "devin-swe2-flash-medium", "medium")
	credit := candOutcome(sel.record(), "omp-luna", "high")
	if free == nil || free["waste_urgency"] != -0.0385 {
		t.Fatalf("free candidate lost its waste urgency: %v", free)
	}
	if credit == nil {
		t.Fatal("credit candidate missing")
	}
	creditDetails, _ := credit["credit"].(map[string]any)
	if credit["usd_urgency"] == nil || creditDetails["balance_usd"] != 33.02 {
		t.Fatalf("credit candidate lacks dollar urgency/balance: %v", credit)
	}
}

func TestProductionSelectionMarksCreditBalanceReadFailure(t *testing.T) {
	a, s := creditBalanceSelectionApp(t, errors.New("shared reader unavailable"))
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(sel.degraded, "credit_balance_unavailable") {
		t.Fatalf("degraded = %v, want credit_balance_unavailable", sel.degraded)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
