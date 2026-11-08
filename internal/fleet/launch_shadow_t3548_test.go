package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

// The shadow comparisons ride on the default spawn path, so they must be
// offline and leave every shared file as they found it (docs/quota.md
// §Launch, §Credit pool views): they read pacelever's config.toml and
// panewire's catalog cache, never handoffkeep, never write or refresh the
// cache, and never spend the caller's deadline.

// countingHandoffkeep is a handoffkeep endpoint that counts every request and
// answers each after delay (zero answers at once). It serves body as the bench
// catalog.
type countingHandoffkeep struct {
	hits  atomic.Int64
	delay time.Duration
	body  string
	done  chan struct{}
}

func (h *countingHandoffkeep) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.hits.Add(1)
	if h.delay > 0 {
		select {
		case <-time.After(h.delay):
		case <-r.Context().Done():
			return
		case <-h.done:
			return
		}
	}
	_, _ = w.Write([]byte(h.body))
}

// shadowCatalogEnv points the catalog client at h and a private cache, and
// returns the client a lookup builds from the same environment.
func shadowCatalogEnv(t *testing.T, h *countingHandoffkeep) *CatalogClient {
	t.Helper()
	h.done = make(chan struct{})
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(h.done) })
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test-token")
	t.Setenv("HANDOFFKEEP_CONFIG", filepath.Join(t.TempDir(), "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	client, err := NewCatalogClient(CatalogClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// writeCatalogCacheAt stores rows as the client's cache entry, fetched age
// before launchNow, with a file time an hour in the past so any rewrite is
// visible in the mtime.
func writeCatalogCacheAt(t *testing.T, client *CatalogClient, rows []CatalogRow, age time.Duration) {
	t.Helper()
	writeCatalogCacheFetched(t, client, rows, launchNow.Add(-age))
}

// writeCatalogCacheFetched stores rows as the client's cache entry fetched
// at fetched.
func writeCatalogCacheFetched(t *testing.T, client *CatalogClient, rows []CatalogRow, fetched time.Time) {
	t.Helper()
	raw := make([]json.RawMessage, len(rows))
	for i, r := range rows {
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		raw[i] = b
	}
	data, err := json.Marshal(catalogCache{FetchedAt: fetched.Format(time.RFC3339Nano), EndpointID: client.endpointID(), Rows: raw})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(client.cachePath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(client.cachePath(), data, 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(client.cachePath(), old, old); err != nil {
		t.Fatal(err)
	}
}

// cacheDirState is every file under the catalog cache directory with its
// bytes and modification time.
func cacheDirState(t *testing.T, client *CatalogClient) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, err := os.ReadDir(filepath.Dir(client.cachePath()))
	if os.IsNotExist(err) {
		return out
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		path := filepath.Join(filepath.Dir(client.cachePath()), e.Name())
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = info.ModTime().String() + "\x00" + string(data)
	}
	return out
}

// cacheLaunchRows makes rows panewire's catalog cache, fetched ten minutes
// before now, for a handoffkeep that fails the test if it is asked: what a
// shadow comparison resolves against.
func cacheLaunchRows(t *testing.T, rows []CatalogRow, now time.Time) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("handoffkeep was asked: %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN", "test-token")
	t.Setenv("HANDOFFKEEP_CONFIG", filepath.Join(t.TempDir(), "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	client, err := NewCatalogClient(CatalogClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	writeCatalogCacheFetched(t, client, rows, now.Add(-10*time.Minute))
}

// newRungLaunch is pacelever's admit of a rung only handoffkeep's live
// catalog has: panewire's cached catalog does not list it.
var newRungLaunch = Launch{Profile: "test-new", ModelID: "model-new", Effort: "high", Grade: "A", Gate: "default", Pool: "codex"}

const newRungCatalog = `{"catalog":[{"profile":"test-new","effort":"high","model_id":"model-new","pool":"codex","grade":"A","gate":"default"}]}`

// TestLaunchShadowReadsNoHandoffkeep: in the default (shadow) mode the native
// comparison reads only the local catalog cache. A cached refusal is not
// refetched, a cache past panewire's one-hour TTL or a missing one is
// skipped (outcome skipped, reason no_fresh_cache), handoffkeep is never
// asked, and no cache file is created, rewritten or touched. pacelever's
// answer is returned as is.
func TestLaunchShadowReadsNoHandoffkeep(t *testing.T) {
	for _, tc := range []struct {
		name    string
		age     time.Duration // the cache entry's age; zero writes no cache
		outcome string
		reason  string
	}{
		{name: "fresh cache refuses", age: 10 * time.Minute, outcome: "disagree", reason: "codex"},
		{name: "cache past the TTL", age: 2 * time.Hour, outcome: "skipped", reason: "no_fresh_cache"},
		{name: "no cache", outcome: "skipped", reason: "no_fresh_cache"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hk := &countingHandoffkeep{body: newRungCatalog}
			client := shadowCatalogEnv(t, hk)
			a, runner, events := launchApp(t, "", nil, paceleverLaunchJSON(newRungLaunch))
			a.BenchCatalog = nil
			if tc.age > 0 {
				writeCatalogCacheAt(t, client, launchRulesCatalog(), tc.age)
			}
			before := cacheDirState(t, client)
			got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "test-new", Effort: "high", Role: "worker"})
			if err != nil || got != newRungLaunch {
				t.Fatalf("shadow must return pacelever's launch: %+v %v", got, err)
			}
			if policyLaunchRuns(runner) != 1 {
				t.Fatalf("pacelever policy launch ran %d times, want 1", policyLaunchRuns(runner))
			}
			if n := hk.hits.Load(); n != 0 {
				t.Fatalf("shadow read handoffkeep %d times, want 0", n)
			}
			if after := cacheDirState(t, client); !equalStringMaps(before, after) {
				t.Fatalf("shadow changed the catalog cache:\nbefore %q\nafter  %q", before, after)
			}
			if len(*events) != 1 {
				t.Fatalf("want one shadow event, got %+v", *events)
			}
			if e := (*events)[0]; e.Kind != "quota_launch_shadow" || e.Outcome != tc.outcome || e.Reason != tc.reason || !strings.Contains(e.Detail, "profile=test-new") {
				t.Fatalf("shadow event = %+v", e)
			}
		})
	}
}

func equalStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// TestLaunchShadowLeavesCallerDeadline: with handoffkeep slow (it answers
// after a minute), a shadow-mode lookup returns as fast as pacelever's lookup
// alone, within a small margin, and the caller's context is still live with
// its deadline unchanged — the shadow cannot spend the deadline the next gate
// call needs.
func TestLaunchShadowLeavesCallerDeadline(t *testing.T) {
	hk := &countingHandoffkeep{body: newRungCatalog, delay: time.Minute}
	shadowCatalogEnv(t, hk)
	a, _, events := launchApp(t, "", nil, paceleverLaunchJSON(newRungLaunch))
	a.BenchCatalog = nil
	req := QuotaRequest{Profile: "test-new", Effort: "high", Role: "worker"}

	start := time.Now()
	if _, err := a.Quota.(admissionQuota).CLIQuota.Launch(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	alone := time.Since(start)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	start = time.Now()
	got, err := a.Quota.Launch(ctx, req)
	took := time.Since(start)
	if err != nil || got != newRungLaunch {
		t.Fatalf("shadow must return pacelever's launch: %+v %v", got, err)
	}
	if margin := 500 * time.Millisecond; took > alone+margin {
		t.Fatalf("shadow lookup took %v, pacelever alone %v: more than %v added", took, alone, margin)
	}
	if ctx.Err() != nil {
		t.Fatalf("the caller's context ended inside the lookup: %v", ctx.Err())
	}
	if d, _ := ctx.Deadline(); !d.Equal(deadline) {
		t.Fatalf("caller deadline changed: %v -> %v", deadline, d)
	}
	if n := hk.hits.Load(); n != 0 {
		t.Fatalf("shadow read handoffkeep %d times, want 0", n)
	}
	if len(*events) != 1 || (*events)[0].Outcome != "skipped" || (*events)[0].Reason != "no_fresh_cache" {
		t.Fatalf("shadow event = %+v", *events)
	}
}

// TestLaunchShadowIgnoresCallerCancellation: the caller's context ends the
// moment pacelever answers (its deadline is spent). The comparison still runs
// on its own context and records its answer: the shadow never receives the
// caller's context.
func TestLaunchShadowIgnoresCallerCancellation(t *testing.T) {
	hk := &countingHandoffkeep{body: newRungCatalog}
	client := shadowCatalogEnv(t, hk)
	writeCatalogCacheAt(t, client, launchRulesCatalog(), 10*time.Minute)
	a, runner, events := launchApp(t, "", nil, nil)
	a.BenchCatalog = nil
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	answer := paceleverLaunchJSON(opusLaunch)
	runner.Do = func(Command) Result {
		cancel()
		return *answer
	}
	got, err := a.Quota.Launch(ctx, QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
	if err != nil || got != opusLaunch {
		t.Fatalf("shadow must return pacelever's launch: %+v %v", got, err)
	}
	if len(*events) != 1 || (*events)[0].Outcome != "agree" {
		t.Fatalf("the comparison must run past the caller's cancellation: %+v", *events)
	}
	if n := hk.hits.Load(); n != 0 {
		t.Fatalf("shadow read handoffkeep %d times, want 0", n)
	}
}

// TestCreditPoolShadowReadsNoStore: in the default mode the credit pool
// comparison reads only pacelever's pool policy: it makes no shared store
// read beyond the ones pacelever's own path makes, and still records its
// comparison when the caller's context ends as pacelever answers.
func TestCreditPoolShadowReadsNoStore(t *testing.T) {
	var reads atomic.Int64
	latest := func(key string) shared.Latest {
		reads.Add(1)
		return anthropicCreditLatest(key)
	}
	a, runner := creditPoolApp(t, excludedAnthropicPolicy, latest)
	if _, err := a.Quota.(admissionQuota).CLIQuota.Pools(context.Background()); err != nil {
		t.Fatal(err)
	}
	alone := reads.Swap(0)

	var events []fleetevent.Event
	a.EventSink = func(e fleetevent.Event) { events = append(events, e) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner.Do = func(Command) Result {
		cancel()
		return Result{Out: creditStatusDoc}
	}
	views, err := a.Quota.Pools(ctx)
	if err != nil || views["anthropic-api"].Class != "spend" {
		t.Fatalf("shadow must return pacelever's views: %+v %v", views, err)
	}
	if n := reads.Load(); n != alone {
		t.Fatalf("shadow mode read the shared store %d times, pacelever's path alone %d", n, alone)
	}
	if len(events) != 1 || events[0].Kind != "quota_pools_shadow" || events[0].Outcome != "disagree" || !strings.Contains(events[0].Detail, "diff=anthropic-api:class") {
		t.Fatalf("shadow event = %+v", events)
	}
}

// TestLaunchNativeHonoursCatalogFreshness: in native mode the catalog's
// freshness follows pacelever's [bench] settings in its config.toml, as
// pacelever's policy launch does (bench.go ReadRows): catalog_stale_max_s
// bounds how old a last-good cache may be when handoffkeep is down, and
// catalog_ttl_s how long a cache is used without asking handoffkeep. Unset,
// they are pacelever's defaults, one day and one hour.
func TestLaunchNativeHonoursCatalogFreshness(t *testing.T) {
	opus := QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}
	setConfig := func(t *testing.T, toml string) {
		path := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(path, []byte(toml), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PACELEVER_CONFIG", path)
	}
	t.Run("catalog_stale_max_s", func(t *testing.T) {
		state := &catalogServer{fail: true}
		client := catalogEnv(t, state)
		a, _, _ := launchApp(t, "native", nil, nil)
		writeCatalogCacheAt(t, client, launchRulesCatalog(), 2*time.Hour)
		setConfig(t, "[bench]\ncatalog_stale_max_s = 3600\n")
		got, err := a.Quota.Launch(context.Background(), opus)
		if exitCode(err) != exitConfig || err.Error() != "quota catalog is stale" || !got.Catalog.Stale {
			t.Fatalf("a 2 h cache past a 1 h stale maximum must be refused stale: %+v %v", got, err)
		}
		setConfig(t, "")
		if got, err := a.Quota.Launch(context.Background(), opus); err != nil || got != opusLaunch {
			t.Fatalf("unset, the stale maximum is pacelever's day: %+v %v", got, err)
		}
	})
	t.Run("catalog_ttl_s", func(t *testing.T) {
		state := &catalogServer{body: `{"catalog":[{"profile":"opus","effort":"high","model_id":"claude-opus-5-1","pool":"claude","grade":"S","gate":"default"}]}`}
		client := catalogEnv(t, state)
		a, _, _ := launchApp(t, "native", nil, nil)
		writeCatalogCacheAt(t, client, launchRulesCatalog(), 90*time.Minute)
		setConfig(t, "[bench]\ncatalog_ttl_s = 7200\n")
		if got, err := a.Quota.Launch(context.Background(), opus); err != nil || got != opusLaunch || state.count() != 0 {
			t.Fatalf("a 90 min cache inside a 2 h TTL is used without a read: %+v %v reads=%d", got, err, state.count())
		}
		setConfig(t, "")
		if got, err := a.Quota.Launch(context.Background(), opus); err != nil || got.ModelID != "claude-opus-5-1" || state.count() != 1 {
			t.Fatalf("unset, the TTL is pacelever's hour: %+v %v reads=%d", got, err, state.count())
		}
	})
}

// TestLaunchNativeValidatesPaceleverConfig: native launch refuses (exit 2)
// every config.toml pacelever v0.3.28's LoadConfig refuses before it
// resolves a launch, and admits the ones it loads. Each row names the
// pacelever rule.
func TestLaunchNativeValidatesPaceleverConfig(t *testing.T) {
	for _, tc := range []struct {
		name, toml string
		ok         bool
	}{
		// config.go:131 the file must be TOML.
		{"not toml", "[pools\n", false},
		// config.go:145 typed decode of [quota], [profiles], [bench], [settings].
		{"quota url type", "[quota]\nurl = 1\n", false},
		{"quota lease_ttl_s type", "[quota]\nlease_ttl_s = 1.5\n", false},
		{"profile subscribed type", "[profiles.opus]\nsubscribed = \"no\"\n", false},
		{"bench catalog_stale_max_s type", "[bench]\ncatalog_stale_max_s = \"1h\"\n", false},
		{"bench allow_plaintext_url type", "[bench]\nallow_plaintext_url = \"yes\"\n", false},
		{"settings type", "[settings]\naltlane_pace_multiple = \"fast\"\n", false},
		{"pools not a table", "pools = 1\n", false},
		// shared_quota.go:33 shared quota TTL ranges.
		{"quota fresh_ttl_s negative", "[quota]\nfresh_ttl_s = -1\n", false},
		{"quota fresh_ttl_s over a day", "[quota]\nfresh_ttl_s = 86401\n", false},
		{"quota fresh_ttl_s nan", "[quota]\nfresh_ttl_s = nan\n", false},
		{"quota lease_ttl_s over 120", "[quota]\nlease_ttl_s = 121\n", false},
		{"quota wait_s over 10", "[quota]\nwait_s = 11\n", false},
		{"quota presence_every_s negative", "[quota]\npresence_every_s = -1\n", false},
		{"quota presence_every_s inf", "[quota]\npresence_every_s = inf\n", false},
		// shared_quota.go:36 host_id.
		{"quota host_id unsafe", "[quota]\nhost_id = \"m1 b\"\n", false},
		{"quota host_id too long", "[quota]\nhost_id = \"" + strings.Repeat("h", 65) + "\"\n", false},
		// shared_quota.go:39 per-pool fresh TTLs.
		{"quota pool_fresh_ttl_s zero", "[quota.pool_fresh_ttl_s]\nclaude = 0\n", false},
		{"quota pool_fresh_ttl_s unknown pool", "[quota.pool_fresh_ttl_s]\nnowhere = 60\n", false},
		// shared_quota.go:44 account fingerprints.
		{"quota account fingerprint", "[quota.accounts]\nclaude = \"XYZ\"\n", false},
		{"quota account unknown pool", "[quota.accounts]\nnowhere = \"0123456789abcdef\"\n", false},
		// shared_quota.go:49 measurable account keys.
		{"quota measurable account key", "[quota]\nmeasurable_accounts = [\"claude\"]\n", false},
		// config.go:166 typed decode of a known pool.
		{"pool boost type", "[pools.claude]\nboost = \"high\"\n", false},
		{"pool credit_max_age_s type", "[pools.claude]\ncredit_max_age_s = \"3d\"\n", false},
		{"pool exploration type", "[pools.claude]\nexploration = 1\n", false},
		// config.go:170 class.
		{"pool class", "[pools.claude]\nclass = \"hoard\"\n", false},
		// config.go:173 exploration (exploration.go:23).
		{"exploration fraction over 1", "[pools.claude.exploration]\nfraction = 1.5\nexperiment_id = \"e1\"\n", false},
		{"exploration fraction negative", "[pools.claude.exploration]\nfraction = -0.1\n", false},
		{"exploration fraction nan", "[pools.claude.exploration]\nfraction = nan\n", false},
		{"exploration active without id", "[pools.claude.exploration]\nfraction = 0.5\n", false},
		{"exploration id with a space", "[pools.claude.exploration]\nfraction = 0.5\nexperiment_id = \"e 1\"\n", false},
		{"exploration id too long", "[pools.claude.exploration]\nfraction = 0.5\nexperiment_id = \"" + strings.Repeat("e", 129) + "\"\n", false},
		{"exploration seed too long", "[pools.claude.exploration]\nseed = \"" + strings.Repeat("s", 257) + "\"\n", false},
		{"exploration model_order repeat", "[pools.claude.exploration]\nmodel_order = [\"opus\", \"opus\"]\n", false},
		{"exploration model_order empty name", "[pools.claude.exploration]\nmodel_order = [\"\"]\n", false},
		// config.go:176 cutoff.
		{"pool cutoff over 100", "[pools.claude]\ncutoff = 101\n", false},
		// config.go:179 in-flight reserve.
		{"pool inflight_reserve_pct", "[pools.claude.inflight_reserve_pct]\nweek = 101\n", false},
		// config.go:184 credit balance age.
		{"pool credit_max_age_s negative", "[pools.claude]\ncredit_max_age_s = -1\n", false},
		{"pool credit_max_age_s nan", "[pools.claude]\ncredit_max_age_s = nan\n", false},
		{"pool credit_max_age_s over 1e9", "[pools.claude]\ncredit_max_age_s = 1e10\n", false},

		// What pacelever loads, the lookup admits.
		{"empty", "", true},
		{"every rule at its edge", `[quota]
url = "https://hk.example"
credential_env = "HK_TOKEN"
host_id = "m1-b.local_2"
node_env = "NODE"
fresh_ttl_s = 86400
lease_ttl_s = 120
wait_s = 10
presence_every_s = 86400
measurable_accounts = ["claude:0123456789abcdef"]
[quota.pool_fresh_ttl_s]
claude = 86400
[quota.accounts]
claude = "0123456789abcdef"
[bench]
backend = "handoffkeep"
cache_ttl_s = 60
catalog_ttl_s = 3600
catalog_stale_max_s = 86400
allow_plaintext_catalog = true
[settings]
altlane_pace_multiple = 1.5
[profiles.opus]
subscribed = true
[pools.claude]
class = "preserve"
until = 2026-12-01
note = "n"
boost = 1
cutoff = 100
capacity_weight = 2
price_usd = 200
plan = "max"
fusion_billing_fp = "f"
credit_max_age_s = 1e9
on_exhaust = "wait"
bin = "claude"
org_id = "o"
[pools.claude.inflight_reserve_pct]
week = 100
[pools.claude.exploration]
fraction = 1
enabled = true
experiment_id = "exp-1"
seed = "s"
model_order = ["opus", "haiku"]
`, true},
		{"inactive exploration needs no id", "[pools.claude.exploration]\nfraction = 0.5\nenabled = false\n", true},
		{"unknown pool ignored", "[pools.nowhere]\nclass = \"hoard\"\ncredit_max_age_s = -1\n", true},
		{"unknown sections kept", "[elsewhere]\nx = \"y\"\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, _ := launchApp(t, "native", launchRulesCatalog(), nil)
			path := filepath.Join(t.TempDir(), "config.toml")
			if err := os.WriteFile(path, []byte(tc.toml), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PACELEVER_CONFIG", path)
			got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
			if tc.ok {
				if err != nil || got != opusLaunch {
					t.Fatalf("pacelever loads this config; native must admit: %+v %v", got, err)
				}
				return
			}
			if exitCode(err) != gateInvalid {
				t.Fatalf("pacelever refuses this config with exit 2; native: %+v %v", got, err)
			}
		})
	}
}
