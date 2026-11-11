package fleet

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota/shared"
)

// creditPresenceConfig gives this host pacelever's config with the credit
// accounts the pacelever era published, and a fleet config with none of its
// own.
func creditPresenceConfig(t *testing.T, pools string) {
	t.Helper()
	dir := t.TempDir()
	hosts := filepath.Join(dir, "hosts.toml")
	if err := os.WriteFile(hosts, []byte("[quota]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", hosts)
	pacelever := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(pacelever, []byte(pools), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", pacelever)
}

// liveCreditLatest is the store's answer for an account whose operator credit
// entry was recorded an hour ago and expires in two days.
func liveCreditLatest(now time.Time, expires time.Time) shared.Latest {
	return shared.Latest{Manual: []shared.Observation{{
		ID: 7, Kind: "manual", Window: "credit", MeasuredAt: now.Add(-time.Hour), ExpiresAt: &expires,
		Buckets: []shared.Bucket{{Window: "credit", UsedPct: 25, Scope: "account"}},
	}}}
}

// hk 3998: the key and liveness rule the node announces a credit pool's
// presence with are the pacelever era's, and a plan binds that pool's omp rung
// on the host that announces it.
func TestCreditPresenceBindsOMPRungOnThePublishingHost(t *testing.T) {
	creditPresenceConfig(t, "[quota.accounts]\nanthropic-api = \"5984150754efdf68\"\n")
	key, ok := CreditAccountKey("anthropic-api")
	if !ok || key != "anthropic-api:5984150754efdf68" {
		t.Fatalf("key = %q ok=%v, want the pacelever era's anthropic-api account", key, ok)
	}
	rows := []CatalogRow{ompRow("omp-sonnet", "anthropic/claude-sonnet-5-5", "anthropic-api")}
	a, s, q := selFixture(t, "A", rows, map[string]PoolView{"anthropic-api": {Class: "credit"}}, nil)
	now := a.Now()
	if CreditPoolExcluded("anthropic-api", now) {
		t.Fatal("a pool with no class table is not excluded")
	}
	expires := now.Add(48 * time.Hour)
	if state := CreditStateOf(liveCreditLatest(now, expires), now); state != CreditLive {
		t.Fatalf("state = %q, want live", state)
	}
	a.Config.Local["spawn"] = false
	a.Config.Hosts = map[string]map[string]any{"mac-work": {"presence_id": "mac-work"}}
	a.Config.HostOrder = []string{"mac-work"}
	src := a.Source.(*fakePlacementSource)
	src.hosts = []HostView{{Name: "mac-work", Connected: true}}
	src.presence = []PresenceRecord{{HostID: "mac-work", AccountKey: key, LastSeen: now}}
	q.GateDo = gatePools(map[string]string{"omp-sonnet": "anthropic-api"})
	serveOMPRouting(t, a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	chosen := sel.record()["chosen"].(map[string]any)
	if chosen["host"] != "mac-work" || chosen["canon"] != "omp-sonnet" {
		t.Fatalf("chosen = %v, want omp-sonnet on the announcing host", chosen)
	}
}

// The account key is panewire's own `[quota].accounts` fingerprint when it has
// one (the key `panewire manual set` writes), else pacelever's.
func TestCreditAccountKeyPrefersPanewireAccounts(t *testing.T) {
	dir := t.TempDir()
	hosts := filepath.Join(dir, "hosts.toml")
	if err := os.WriteFile(hosts, []byte("[quota]\naccounts = { anthropic-api = \"0123456789abcdef\" }\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", hosts)
	pacelever := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(pacelever, []byte("[quota.accounts]\nanthropic-api = \"5984150754efdf68\"\nopenai-api = \"ac91f07b57208a36\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", pacelever)
	if key, ok := CreditAccountKey("anthropic-api"); !ok || key != "anthropic-api:0123456789abcdef" {
		t.Fatalf("anthropic-api key = %q ok=%v, want the panewire fingerprint", key, ok)
	}
	if key, ok := CreditAccountKey("openai-api"); !ok || key != "openai-api:ac91f07b57208a36" {
		t.Fatalf("openai-api key = %q ok=%v, want pacelever's fallback", key, ok)
	}
	if _, ok := CreditAccountKey("claude"); ok {
		t.Fatal("a pool with no configured fingerprint has no key")
	}
}

// CreditStateOf is pacelever's liveCredit: any unexpired credit window is
// live; a credit entry whose expiry passed is expired; none is absent. An
// entry for another window, or one without an expiry, is not a credit entry.
func TestCreditStateOfFollowsLiveCredit(t *testing.T) {
	now := time.Date(2026, 11, 11, 15, 0, 0, 0, time.UTC)
	expired, future := now.Add(-time.Hour), now.Add(time.Hour)
	for _, tc := range []struct {
		name   string
		latest shared.Latest
		want   CreditState
	}{
		{"live", liveCreditLatest(now, future), CreditLive},
		{"expired", liveCreditLatest(now, expired), CreditExpired},
		{"absent", shared.Latest{}, CreditAbsent},
		{"other window", shared.Latest{Manual: []shared.Observation{{Kind: "manual", Window: "7d", MeasuredAt: now, ExpiresAt: &future, Buckets: []shared.Bucket{{Window: "7d", UsedPct: 10, Scope: "account"}}}}}, CreditAbsent},
		{"no expiry", shared.Latest{Manual: []shared.Observation{{Kind: "manual", Window: "credit", MeasuredAt: now, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 10, Scope: "account"}}}}}, CreditAbsent},
		{"two buckets", shared.Latest{Manual: []shared.Observation{{Kind: "manual", Window: "credit", MeasuredAt: now, ExpiresAt: &future, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 10, Scope: "account"}, {Window: "credit", UsedPct: 20, Scope: "account"}}}}}, CreditAbsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if state := CreditStateOf(tc.latest, now); state != tc.want {
				t.Fatalf("state = %q, want %q", state, tc.want)
			}
		})
	}
	old, live := now.Add(-3*time.Hour), liveCreditLatest(now, future)
	live.Manual = append(live.Manual, shared.Observation{ID: 8, Kind: "manual", Window: "credit", MeasuredAt: old, ExpiresAt: &expired, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 90, Scope: "account"}}})
	state, recorded := CreditEntryOf(live, now)
	if state != CreditLive || !recorded.Equal(now.Add(-time.Hour)) {
		t.Fatalf("state=%q recorded=%v, want live at the newest entry", state, recorded)
	}
}

// The operator's exclude class refuses the pool at now, and an `until` date
// that has passed lifts it, as the gate reads it.
func TestCreditPoolExcludedFollowsThePoolPolicy(t *testing.T) {
	now := time.Date(2026, 11, 11, 15, 0, 0, 0, time.UTC)
	creditPresenceConfig(t, "[pools.anthropic-api]\nclass = \"exclude\"\nuntil = \"2026-12-01\"\n")
	if !CreditPoolExcluded("anthropic-api", now) {
		t.Fatal("an excluded pool must not be announced")
	}
	if CreditPoolExcluded("openai-api", now) {
		t.Fatal("a pool with no class table is not excluded")
	}
	creditPresenceConfig(t, "[pools.anthropic-api]\nclass = \"exclude\"\nuntil = \"2026-11-01\"\n")
	if CreditPoolExcluded("anthropic-api", now) {
		t.Fatal("an exclusion whose until date passed is lifted")
	}
	creditPresenceConfig(t, "[pools.anthropic-api]\nclass = \"preserve\"\n")
	if class := CreditPoolClass("anthropic-api", now); class != "preserve" {
		t.Fatalf("class = %q, want the operator's own class on the status line", class)
	}
	if class := CreditPoolClass("openai-api", now); class != "spend" {
		t.Fatalf("class = %q, want spend for a pool with no table", class)
	}
}

// A credit pool with no panewire fingerprint of its own is recorded under
// pacelever's, the key presence publishes and the pacelever era wrote.
func TestManualCreditUsesPaceleversAccountKey(t *testing.T) {
	creditPresenceConfig(t, "[quota.accounts]\nanthropic-api = \"5984150754efdf68\"\n")
	a := testApp(t)
	a.Config.Quota = map[string]any{}
	key, err := manualAccountKey(a, "anthropic-api")
	if err != nil || key != "anthropic-api:5984150754efdf68" {
		t.Fatalf("manual key = %q err=%v, want pacelever's account", key, err)
	}
	if _, err := manualAccountKey(a, "claude"); err == nil {
		t.Fatal("a pool with no fingerprint in either config still refuses")
	}
}

// The presence id and TTL a status line reads are placement's: the local
// host's id from the fleet config and `[quota].presence_ttl_s`.
func TestHostPresenceIDAndTTL(t *testing.T) {
	dir := t.TempDir()
	hosts := filepath.Join(dir, "hosts.toml")
	if err := os.WriteFile(hosts, []byte("[quota]\npresence_ttl_s = 3600\n[hub]\nmachine_id = \"mac-work\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", hosts)
	if id := HostPresenceID(); id != "mac-work" {
		t.Fatalf("presence id = %q, want the hub machine id placement reads", id)
	}
	if ttl := HostPresenceTTL(); ttl != time.Hour {
		t.Fatalf("presence ttl = %v, want the configured hour", ttl)
	}
	if err := os.WriteFile(hosts, []byte("[local]\npresence_id = \"node-a\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if id := HostPresenceID(); id != "node-a" {
		t.Fatalf("presence id = %q, want [local].presence_id first", id)
	}
	if ttl := HostPresenceTTL(); ttl != 24*time.Hour {
		t.Fatalf("presence ttl = %v, want the 24 h default", ttl)
	}
}
