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

// creditReceiptFP is a second anthropic-api account. The receipt-key case names
// it as the rejected account while the local fallback still names
// creditBalanceAccount, so only a watcher that reads the receipt lands it here.
const creditReceiptFP = "fedcba9876543210"

// anthropicCreditPlacementApp is a placement fixture whose quota is the
// production credit path: the anthropic-api pool's view is built from the hk
// store's latest reading (creditStoreViews, CLIQuota.Pools) and nothing else.
// The devin free rung is the other candidate, so an excluded omp-opus seat
// leaves a chosen seat rather than an empty selection.
func anthropicCreditPlacementApp(t *testing.T, srv *httptest.Server) (*App, Spawn) {
	t.Helper()
	return anthropicCreditPlacementAppOn(t, srv, creditBalanceAccount, creditBalanceAccount)
}

// anthropicCreditPlacementAppOn is anthropicCreditPlacementApp with the two
// account fingerprints split: placementFP is the account placement reads and
// the presence record names (pacelever's [quota.accounts]), and localFP is the
// host's own pinned account, which the watcher falls back to when a receipt
// names none (Config.Quota accounts).
func anthropicCreditPlacementAppOn(t *testing.T, srv *httptest.Server, placementFP, localFP string) (*App, Spawn) {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "devin-swe2-flash-medium", Effort: "medium", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2-flash-medium"},
		ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api"),
	}
	a, s, q := selFixture(t, "C", rows, nil, []string{"devin", "anthropic-api"})
	rekeyAnthropicPresence(t, a, "anthropic-api:"+placementFP)
	serveOMPRouting(t, a)
	q.PoolViews = map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4), Urgency: ptrF(-0.0385), ResetHours: ptrF(24)}}
	q.GateDo = gatePools(map[string]string{"devin-swe2-flash-medium": "devin", "omp-opus": "anthropic-api"})
	poolOrderPath := filepath.Join(t.TempDir(), "pool-order.json")
	if err := os.WriteFile(poolOrderPath, []byte(`{"version":3,"order":["waste","free","ranked"],"free":[{"pool":"devin","models":["swe-2-flash-medium"]}],"credit_pools":["anthropic-api"]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a.Config.Local["pool_policy"] = poolOrderPath
	paceleverConfig := filepath.Join(t.TempDir(), "pacelever.toml")
	if err := os.WriteFile(paceleverConfig, []byte("[quota.accounts]\nanthropic-api = \""+placementFP+"\"\n[pools.anthropic-api]\nclass = \"spend\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", paceleverConfig)
	// selFixture's fake hk serves the task precheck and carries the svc token;
	// the shared quota store is this test's server, named by Config.Quota.
	a.Config.Quota = map[string]any{"url": srv.URL, "accounts": map[string]any{"anthropic-api": localFP}}
	cli := CLIQuota{Runner: a.Runner, Binary: "pacelever", SharedLatest: a.sharedLatest}
	a.Quota = &nativeCreditPoolQuota{fakeQuota: q, production: admissionQuota{CLIQuota: cli, app: a}}
	return a, s
}

// rekeyAnthropicPresence moves selFixture's anthropic-api presence key (its
// index-derived fingerprint) onto key, so the placement candidate names the
// account the rejection is about.
func rekeyAnthropicPresence(t *testing.T, a *App, key string) {
	t.Helper()
	src, ok := a.Source.(*fakePlacementSource)
	if !ok {
		t.Fatalf("placement source is %T, want *fakePlacementSource", a.Source)
	}
	moved := false
	for i := range src.presence {
		if strings.HasPrefix(src.presence[i].AccountKey, "anthropic-api:") {
			src.presence[i].AccountKey = key
			moved = true
		}
	}
	for old, acc := range src.accounts {
		if strings.HasPrefix(old, "anthropic-api:") {
			delete(src.accounts, old)
			acc.Key = key
			src.accounts[key] = acc
		}
	}
	if !moved {
		t.Fatal("selFixture has no anthropic-api presence record to re-key")
	}
}

// seedCreditRejection writes an omp session whose last assistant turn is
// Anthropic's "credit balance is too low" reply, then records the job that ran
// it. receiptAccount, when set, is the account_key the spawn receipt names; an
// empty one leaves the receipt without an account.
func seedCreditRejection(t *testing.T, a *App, receiptAccount string) {
	t.Helper()
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
	payload := map[string]any{"pane_id": "w:p1", "role": "reviewer", "pool": "anthropic-api", "cwd": "/tmp/project", "harness_session": map[string]any{"kind": "omp", "id": path}}
	if receiptAccount != "" {
		payload["account_key"] = receiptAccount
	}
	putEvent(t, a, "j", "job.spawned", payload)
}

// assertCreditRejectionPlaced checks what the watcher and placement did with a
// rejection published for wantKey: the watcher's own event, the published
// observation on that account, the pool view placement reads, and the omp-opus
// candidate dropped on that same account.
func assertCreditRejectionPlaced(t *testing.T, a *App, s Spawn, store *orderedCreditStore, h *fakeHK, wantKey string) {
	t.Helper()
	ctx := context.Background()
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
	// orderedCreditStore serves only wantKey's path, so a non-nil latest here
	// means the observation landed on the account the receipt named.
	if store.latest == nil || store.latest.Window != "credit" || len(store.latest.Buckets) != 1 || store.latest.Buckets[0].UsedPct != 100 {
		t.Fatalf("published observation = %+v; want an exhausted credit entry on %s", store.latest, wantKey)
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
	if row["account_key"] != wantKey {
		t.Fatalf("omp-opus candidate account = %v; want the rejected account %s", row["account_key"], wantKey)
	}
	if sel.chosen.row.Profile != "devin-swe2-flash-medium" {
		t.Fatalf("chosen %s, want devin-swe2-flash-medium", sel.chosen.row.Profile)
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
	seedCreditRejection(t, a, "")
	if err := a.watch(ctx, []string{"j"}); err != nil {
		t.Fatal(err)
	}
	assertCreditRejectionPlaced(t, a, s, store, h, creditRejectedKey)
}

// The spawn receipt names the account the rejected session ran on. The local
// fallback names a different account, so the rejection must be published for
// the receipt's account and placement must skip that one. A watcher that always
// used the fallback publishes to a path the store refuses, and this test fails.
func TestCreditTooLowReceiptAccountKeyWinsOverLocalFallback(t *testing.T) {
	receiptKey := "anthropic-api:" + creditReceiptFP
	store := &orderedCreditStore{account: receiptKey}
	srv := httptest.NewServer(store)
	defer srv.Close()
	a, s := anthropicCreditPlacementAppOn(t, srv, creditReceiptFP, creditBalanceAccount)
	ctx := context.Background()

	_, h, _ := quotaStopFixtureOn(t, a, "reviewer", "idle", "")
	seedCreditRejection(t, a, receiptKey)
	if err := a.watch(ctx, []string{"j"}); err != nil {
		t.Fatal(err)
	}
	assertCreditRejectionPlaced(t, a, s, store, h, receiptKey)
}
