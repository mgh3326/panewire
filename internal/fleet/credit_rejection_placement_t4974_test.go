package fleet

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// creditRejectedKey is the anthropic-api account the rejection is about. One
// key names it everywhere in these tests: the presence record and placement
// candidate, the quota config's account, and the store's account path.
const creditRejectedKey = "anthropic-api:" + creditBalanceAccount

// anthropicCreditPlacementApp is a placement fixture whose quota is the
// production credit path: the anthropic-api pool's view is built from the hk
// store's latest reading (creditStoreViews, CLIQuota.Pools) and nothing else.
// The devin free rung is the other candidate, so an excluded omp-opus seat
// leaves a chosen seat rather than an empty selection.
func anthropicCreditPlacementApp(t *testing.T, srv *httptest.Server) (*App, Spawn) {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
		ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api"),
	}
	a, s, q := selFixture(t, "C", rows, nil, []string{"devin", "anthropic-api"})
	rekeyAnthropicPresence(t, a)
	serveOMPRouting(t, a)
	q.PoolViews = map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4), Urgency: ptrF(-0.0385), ResetHours: ptrF(24)}}
	q.GateDo = gatePools(map[string]string{"devin-swe2-flash-medium": "devin", "omp-opus": "anthropic-api"})
	poolOrderPath := filepath.Join(t.TempDir(), "pool-order.json")
	if err := os.WriteFile(poolOrderPath, []byte(`{"version":3,"order":["waste","free","ranked"],"free":[{"pool":"devin","models":["swe-2-flash-medium"]}],"credit_pools":["anthropic-api"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a.Config.Local["pool_policy"] = poolOrderPath
	paceleverConfig := filepath.Join(t.TempDir(), "pacelever.toml")
	if err := os.WriteFile(paceleverConfig, []byte("[quota.accounts]\nanthropic-api = \""+creditBalanceAccount+"\"\n[pools.anthropic-api]\nclass = \"spend\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", paceleverConfig)
	// selFixture's fake hk serves the task precheck and carries the svc token;
	// the shared quota store is this test's server, named by Config.Quota.
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": creditBalanceAccount}}
	cli := CLIQuota{Runner: a.Runner, Binary: "pacelever", SharedLatest: a.sharedLatest}
	a.Quota = &nativeCreditPoolQuota{fakeQuota: q, production: admissionQuota{CLIQuota: cli, app: a}}
	return a, s
}

// rekeyAnthropicPresence moves selFixture's anthropic-api presence key (its
// index-derived fingerprint) onto creditRejectedKey, so the placement candidate
// names the account the rejection is about.
func rekeyAnthropicPresence(t *testing.T, a *App) {
	t.Helper()
	src, ok := a.Source.(*fakePlacementSource)
	if !ok {
		t.Fatalf("placement source is %T, want *fakePlacementSource", a.Source)
	}
	moved := false
	for i := range src.presence {
		if strings.HasPrefix(src.presence[i].AccountKey, "anthropic-api:") {
			src.presence[i].AccountKey = creditRejectedKey
			moved = true
		}
	}
	for key, acc := range src.accounts {
		if strings.HasPrefix(key, "anthropic-api:") {
			delete(src.accounts, key)
			acc.Key = creditRejectedKey
			src.accounts[creditRejectedKey] = acc
		}
	}
	if !moved {
		t.Fatal("selFixture has no anthropic-api presence record to re-key")
	}
}

// A reviewer's omp session ends on Anthropic's "credit balance is too low"
// reply. The watcher reads that stop from the session, publishes it to the hk
// store as the pool's exhausted credit, and the next automatic placement
// refuses the pool, with no manual exclude and no manual credit entry set.
func TestCreditTooLowReplyExcludesAnthropicPoolFromAutoPlacement(t *testing.T) {
	store := &orderedCreditStore{account: creditRejectedKey}
	srv := httptest.NewServer(store)
	defer srv.Close()
	a, s := anthropicCreditPlacementApp(t, srv)
	ctx := context.Background()

	// An idle reviewer on the pane; the watcher, not the test, publishes its stop.
	_, h, _ := quotaStopFixtureOn(t, a, "reviewer", "idle", "")
	dir := filepath.Join(a.Home, ".omp", "agent", "sessions", "test")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "session.jsonl")
	rejection := `{"type":"session","cwd":"/tmp/project"}` + "\n" +
		fmt.Sprintf(`{"type":"message","message":{"role":"assistant","stopReason":"error","errorStatus":400,"provider":"anthropic","completedAt":%d,"content":[],"errorMessage":"400 {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low to access the Anthropic API\"}}"}}`, a.Now().UnixMilli()) + "\n"
	if err := os.WriteFile(path, []byte(rejection), 0o600); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "role": "reviewer", "pool": "anthropic-api", "cwd": "/tmp/project", "harness_session": map[string]any{"kind": "omp", "id": path}})
	if err := a.watch(ctx, []string{"j"}); err != nil {
		t.Fatal(err)
	}

	// The rejection went through the watcher's own event path, not a direct call.
	evs, err := a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, ev := range evs {
		seen = seen || ev.Kind == "job.credit_rejection"
	}
	if !seen {
		t.Fatalf("job events hold no job.credit_rejection: watcher did not detect the stop; hk=%v", h.record(t, "j"))
	}
	if store.latest == nil || store.latest.Window != "credit" || len(store.latest.Buckets) != 1 || store.latest.Buckets[0].UsedPct != 100 {
		t.Fatalf("published observation = %+v; want an exhausted credit entry", store.latest)
	}
	if !store.latest.MeasuredAt.Equal(time.UnixMilli(a.Now().UnixMilli())) {
		t.Fatalf("published at %v; want the rejection time %v", store.latest.MeasuredAt, a.Now())
	}

	// The pool view placement reads must carry the rejection: creditStoreViews
	// builds it from the store's latest reading on the rejected account.
	views, err := a.Quota.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if b := views["anthropic-api"].Blocking; b == nil || *b != 100 {
		t.Fatalf("anthropic-api pool view blocking = %v; want the rejection's 100%% used", b)
	}

	sel, err := a.selectProfile(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(sel.record(), "omp-opus", "high")
	if row == nil || row["outcome"] != "dropped:credit_exhausted" {
		t.Fatalf("omp-opus outcome = %v; want dropped:credit_exhausted", row)
	}
	if row["account_key"] != creditRejectedKey {
		t.Fatalf("omp-opus candidate account = %v; want the rejected account %s", row["account_key"], creditRejectedKey)
	}
	if sel.chosen.row.Profile != "devin-swe2-flash-medium" {
		t.Fatalf("chosen %s, want devin-swe2-flash-medium", sel.chosen.row.Profile)
	}
}
