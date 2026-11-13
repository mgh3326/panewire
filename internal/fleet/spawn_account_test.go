package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// selectedAccountApp is an executing host ("local") asked to launch opus on
// the selected account testClaudeB.
func selectedAccountApp(t *testing.T) (*App, Spawn) {
	t.Helper()
	a, s, q, _ := accountConsultFixture(t)
	a.Config.Local["spawn"] = true
	s.Role, s.RoutingRole, s.AccountKey = "implementer", "consult", testClaudeB
	q.LaunchValue = Launch{Profile: "opus", ModelID: "opus-m", Effort: "high", Grade: "C", Gate: "default", Pool: "claude"}
	q.GateDo = func(QuotaRequest) (string, error) {
		return `{"schema":"scopefuel.gate.v1","provider_id":"claude","profile":"opus","account_key":"` + testClaudeB + `","ok":true,"exit_code":0}`, nil
	}
	return a, s
}

func admitSelected(a *App, s Spawn) error {
	_, _, err := a.admitLaunch(context.Background(), s, a.Policy.Profiles["opus"], "high", "high", "S+", "", true)
	return err
}

// The spawn-time recheck admits the host that holds the selected account and
// refuses every other case with the reason, never a bare "no longer holds".
func TestSpawnRecheckOfSelectedAccount(t *testing.T) {
	quota := riskPool(30, 8)
	for _, tc := range []struct {
		name     string
		presence string // account the executing host's presence names; "" = none
		acc      SharedAccount
		readErr  bool
		reason   string // "" = admitted
	}{
		{name: "holder with fresh evidence", presence: testClaudeB, acc: SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, Quota: &quota}},
		{name: "holder with stale evidence", presence: testClaudeB, acc: SharedAccount{Key: testClaudeB, Observed: true, Quota: &quota}},
		{name: "mismatched observation", presence: testClaudeB, acc: SharedAccount{Key: testClaudeA, Observed: true}, reason: ": account_mismatch"},
		{name: "holder never observed", presence: testClaudeB, acc: SharedAccount{Key: testClaudeB}, reason: ": not_observed"},
		{name: "holder with unreadable evidence", presence: testClaudeB, readErr: true, reason: ": degraded"},
		{name: "non-holder", presence: testClaudeA, acc: SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, Quota: &quota}, reason: ": account_mismatch (host holds " + testClaudeA + ")"},
		{name: "unknown account fails closed", acc: SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, Quota: &quota}, reason: ": no_usable_presence"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s := selectedAccountApp(t)
			src := &fakePlacementSource{accounts: map[string]SharedAccount{testClaudeB: tc.acc}}
			if tc.presence != "" {
				src.presence = []PresenceRecord{{HostID: a.machine(), AccountKey: tc.presence, LastSeen: a.Now()}}
			}
			if tc.readErr {
				src.accountErr = fmt.Errorf("hk down")
			}
			a.Source = src
			err := admitSelected(a, s)
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("holder refused: %v", err)
				}
				return
			}
			if exitCode(err) != exitAuthUnavailable || !strings.HasSuffix(err.Error(), tc.reason) {
				t.Fatalf("want AUTH_UNAVAILABLE ending %q, got %v", tc.reason, err)
			}
		})
	}
}

// hkServer answers presence and the latest shared observation for one key the
// way handoffkeep does, with the observation measured age ago.
func hkServer(t *testing.T, a *App, host, key string, age time.Duration) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/quota/presence":
			_ = json.NewEncoder(w).Encode([]PresenceRecord{{HostID: host, AccountKey: key, LastSeen: a.Now()}})
		case "/v1/quota/accounts/" + key + "/latest":
			_ = json.NewEncoder(w).Encode(map[string]any{"observation": map[string]any{
				"kind": "auto", "measured_at": a.Now().Add(-age),
				"buckets": []map[string]any{{"window": "7d", "used_pct": 6, "resets_at": a.Now().Add(72 * time.Hour).Format(time.RFC3339), "scope": "account"}},
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	// hosts.toml [quota] url overrides this endpoint.
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	if a.Config.Quota == nil {
		a.Config.Quota = map[string]any{}
	}
	a.Config.Quota["url"] = srv.URL
	a.Source = fleetSource{a}
}

// The incident: the planner (fresh_ttl_s 1800) chose a holder whose shared
// reading was 20 minutes old; the executing host, with no fresh_ttl_s of its
// own, judged the same reading by the 300 s default and refused. A delegated
// login confirmation must not refuse that account before the quota gate can
// refresh or refuse it, regardless of the planner's freshness window.
func TestDelegatedSpawnStaleLoginReachesQuotaGate(t *testing.T) {
	for _, tc := range []struct {
		name     string
		plannerS int64
		age      time.Duration
	}{
		{name: "own default window", plannerS: 0, age: 20 * time.Minute},
		{name: "planner window", plannerS: 1800, age: 20 * time.Minute},
		{name: "older than planner window", plannerS: 1800, age: 40 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s := selectedAccountApp(t)
			hkServer(t, a, a.machine(), testClaudeB, tc.age)
			if err := a.adoptFreshTTL(tc.plannerS); err != nil {
				t.Fatal(err)
			}
			q := a.Quota.(*fakeQuota)
			gate := q.GateDo
			called := false
			q.GateDo = func(r QuotaRequest) (string, error) {
				called = true
				if r.AccountKey != testClaudeB {
					t.Fatalf("gate lost account binding: %+v", r)
				}
				return gate(r)
			}
			if err := admitSelected(a, s); err != nil {
				t.Fatalf("holder refused: %v", err)
			}
			if !called {
				t.Fatal("stale login skipped the quota gate")
			}
		})
	}
}

func TestStaleLoginDoesNotBypassQuotaRefusal(t *testing.T) {
	a, s := selectedAccountApp(t)
	hkServer(t, a, a.machine(), testClaudeB, 40*time.Minute)
	a.Quota.(*fakeQuota).GateDo = func(r QuotaRequest) (string, error) {
		return "", fail(exitQuotaUnknown, "quota remains stale after refresh")
	}
	if err := admitSelected(a, s); code(err) != exitQuotaUnknown || !strings.Contains(err.Error(), "quota remains stale") {
		t.Fatalf("quota gate refusal was bypassed: %v", err)
	}
}

func TestAdoptFreshTTLRejectsOutOfRange(t *testing.T) {
	a := testApp(t)
	for _, v := range []int64{-1, 86401} {
		if err := a.adoptFreshTTL(v); exitCode(err) != exitUsage {
			t.Fatalf("fresh_ttl_s %d must be refused: %v", v, err)
		}
	}
	if a.placementTTL("fresh_ttl_s", 300) != 300*time.Second {
		t.Fatal("a refused window must not change this host's own")
	}
}

// The placing host puts its freshness window in the delegation envelope of a
// spawn bound to a selected account.
func TestRemoteSpawnEnvelopeCarriesPlannerFreshness(t *testing.T) {
	src := &fakePlacementSource{hosts: []HostView{{Name: "h1", Connected: true, LoadRatio: ptrF(0.1)}}}
	a, s, f := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(string) Result {
		return Result{Out: "OK pane=w:p1\n"}
	})
	a.Config.Hosts = map[string]map[string]any{"h1": {}}
	a.Config.HostOrder = []string{"h1"}
	a.Config.Quota = map[string]any{"fresh_ttl_s": int64(1800)}
	s.AccountKey = "claude:0123456789abcdef"
	if _, _, err := a.placeAuto(context.Background(), s, "brief"); err != nil {
		t.Fatal(err)
	}
	var env struct{ Spawn Spawn }
	if err := json.Unmarshal([]byte(spawnInput(t, f)), &env); err != nil {
		t.Fatal(err)
	}
	if env.Spawn.FreshTTLS != 1800 || env.Spawn.AccountKey != s.AccountKey {
		t.Fatalf("envelope must carry the planner window with the account: %+v", env.Spawn)
	}
}
