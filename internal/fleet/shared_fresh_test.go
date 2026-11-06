package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// sharedHK answers one account's latest shared observation the way
// handoffkeep does: a re-measurement that publishes the same values keeps
// measured_at and only advances confirmed_at (confirmed < 0 omits it).
func sharedHK(t *testing.T, a *App, key string, measured, confirmed time.Duration) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/quota/accounts/"+key+"/latest" {
			http.NotFound(w, r)
			return
		}
		obs := map[string]any{
			"kind": "auto", "measured_at": a.Now().Add(-measured),
			"buckets": []map[string]any{{"window": "7d", "used_pct": 8, "resets_at": a.Now().Add(72 * time.Hour).Format(time.RFC3339), "scope": "account"}},
		}
		if confirmed >= 0 {
			obs["confirmed_at"] = a.Now().Add(-confirmed)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"observation": obs})
	}))
	t.Cleanup(srv.Close)
	t.Setenv("HANDOFFKEEP_TOKEN", "test-token")
	if a.Config.Quota == nil {
		a.Config.Quota = map[string]any{}
	}
	a.Config.Quota["url"] = srv.URL
	a.Source = fleetSource{a}
}

// The m1b incident (pw-ci-pr-fast, 2026-11-05): codex measured 14 minutes
// earlier, re-confirmed a minute ago with the same values, was judged stale
// by measured_at alone, so the gate dropped codex-terra61 as quota_unknown.
func TestSharedReadingFreshByConfirmedAt(t *testing.T) {
	for _, tc := range []struct {
		name                string
		key                 string
		measured, confirmed time.Duration
		fresh               bool
	}{
		{name: "re-confirmed after an old measurement", key: testCodexA, measured: 14 * time.Minute, confirmed: time.Minute, fresh: true},
		{name: "measured and confirmed long ago", key: testCodexA, measured: 14 * time.Minute, confirmed: 10 * time.Minute},
		{name: "no confirmed_at, old measurement", key: testCodexA, measured: 14 * time.Minute, confirmed: -1},
		{name: "confirmed_at before measured_at is ignored", key: testCodexA, measured: 2 * time.Minute, confirmed: 30 * time.Minute, fresh: true},
		{name: "claude is fresh for 15 minutes", key: testClaudeB, measured: 10 * time.Minute, confirmed: -1, fresh: true},
		{name: "claude past 15 minutes", key: testClaudeB, measured: 16 * time.Minute, confirmed: -1},
		{name: "codex past 5 minutes", key: testCodexA, measured: 6 * time.Minute, confirmed: -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			sharedHK(t, a, tc.key, tc.measured, tc.confirmed)
			acc, err := a.Source.SharedAccount(context.Background(), tc.key)
			if err != nil {
				t.Fatal(err)
			}
			if !acc.Observed || acc.Fresh != tc.fresh {
				t.Fatalf("observed=%v fresh=%v, want fresh=%v (measured_at %s)", acc.Observed, acc.Fresh, tc.fresh, acc.MeasuredAt)
			}
		})
	}
}

func TestSharedFreshTTLPrecedence(t *testing.T) {
	a := testApp(t)
	if got := a.sharedFreshTTL("claude"); got != 900*time.Second {
		t.Fatalf("claude default %s", got)
	}
	if got := a.sharedFreshTTL("codex"); got != 300*time.Second {
		t.Fatalf("codex default %s", got)
	}
	a.Config.Quota = map[string]any{"fresh_ttl_s": int64(600), "pool_fresh_ttl_s": map[string]any{"codex": int64(1200)}}
	if got := a.sharedFreshTTL("codex"); got != 1200*time.Second {
		t.Fatalf("pool override %s", got)
	}
	if got := a.sharedFreshTTL("claude"); got != 600*time.Second {
		t.Fatalf("fresh_ttl_s before the claude default %s", got)
	}
	// A delegated spawn judges with the placing host's resolved window.
	if err := a.adoptFreshTTL(450); err != nil {
		t.Fatal(err)
	}
	if got := a.sharedFreshTTL("codex"); got != 450*time.Second {
		t.Fatalf("delegated window lost to a local pool override: %s", got)
	}
}

func TestPoolFreshTTLConfigValidated(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		ok         bool
	}{
		{name: "valid", body: "[quota]\n[quota.pool_fresh_ttl_s]\ncodex = 900\n", ok: true},
		{name: "negative", body: "[quota]\n[quota.pool_fresh_ttl_s]\ncodex = -1\n"},
		{name: "not a table", body: "[quota]\npool_fresh_ttl_s = 900\n"},
		{name: "string value", body: "[quota]\n[quota.pool_fresh_ttl_s]\ncodex = \"900\"\n"},
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
