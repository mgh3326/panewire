package fleet

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

var launchNow = time.Date(2026, 11, 8, 3, 0, 0, 0, time.UTC)

func awaitShadowIdle(t *testing.T) { t.Helper() }

func isolateConfigHome(t *testing.T) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
}

func cacheLaunchRows(t *testing.T, rows []CatalogRow, now time.Time) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("handoffkeep was asked: %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	client, err := NewCatalogClient(CatalogClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	writeUndatedCatalogCache(t, client, rows, now.Add(-10*time.Minute).Format(time.RFC3339Nano))
}

func launchTestModelFamilies() ModelFamiliesPolicy {
	return ModelFamiliesPolicy{Version: 1, CountedBy: "pool", Families: map[string]ModelFamily{
		"claude":        {Harness: "claude", Billing: "subscription", Models: []string{"haiku", "sonnet", "opus", "fable"}, EffortVia: "flag", Efforts: []string{"low", "medium", "high", "xhigh", "max"}, LadderCap: "high", EntryEffort: "medium"},
		"codex":         {Harness: "codex", Billing: "subscription", Models: []string{"codex-luna", "codex-terra61", "codex-sol", "codex-astra"}, EffortVia: "flag", Efforts: []string{"low", "medium", "high", "xhigh", "max"}, LadderCap: "max", EntryEffort: "medium"},
		"anthropic-api": {Harness: "omp", Billing: "credit", Models: []string{"omp-haiku", "omp-sonnet", "omp-opus"}, EffortVia: "flag", Efforts: []string{"low", "medium", "high", "xhigh", "max"}, LadderCap: "high", EntryEffort: "medium"},
		"openai-api":    {Harness: "omp", Billing: "credit", Models: []string{"omp-luna", "omp-terra61", "omp-sol"}, EffortVia: "flag", Efforts: []string{"low", "medium", "high", "xhigh", "max"}, LadderCap: "max", EntryEffort: "medium"},
	}}
}

// launchApp is an App whose quota is the production admissionQuota over a fake
// pacelever. paceleverLaunch answers `pacelever policy launch`; nil means the
// test expects none, so any run of it fails the test. rows, when set, is the
// bench catalog: the native lookup's fixture, and the catalog cache the
// shadow comparison reads, for a handoffkeep that must not be asked.
func launchApp(t *testing.T, mode string, rows []CatalogRow, paceleverLaunch *Result) (*App, *fakeRunner, *[]fleetevent.Event) {
	t.Helper()
	isolateConfigHome(t)
	t.Setenv("PACELEVER_CONFIG", "")
	t.Setenv("PACELEVER_E6_ARM", "")
	runner := &fakeRunner{Do: func(c Command) Result {
		if len(c.Args) >= 2 && c.Args[0] == "policy" && c.Args[1] == "launch" {
			if paceleverLaunch == nil {
				t.Errorf("pacelever policy launch ran: %v", c.Args)
				return Result{Code: 1, Err: "error: pacelever must not run"}
			}
			return *paceleverLaunch
		}
		t.Errorf("unexpected command %s %v", c.Name, c.Args)
		return Result{Code: 1}
	}}
	a := &App{Home: t.TempDir(), Now: func() time.Time { return launchNow }, Runner: runner, Err: io.Discard, Config: Config{Quota: map[string]any{}}}
	a.modelFamilies = func(context.Context) (ModelFamiliesPolicy, error) { return launchTestModelFamilies(), nil }
	// A shadow comparison may outlive the call; it ends before the test's
	// directories are removed.
	t.Cleanup(func() { awaitShadowIdle(t) })
	if mode != "" {
		a.Config.Quota["launch"] = mode
	}
	a.Quota = admissionQuota{CLIQuota{Runner: runner, Binary: "pacelever"}, a}
	if rows != nil {
		a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return rows, nil }
		cacheLaunchRows(t, rows, launchNow)
	}
	var events []fleetevent.Event
	a.EventSink = func(e fleetevent.Event) { events = append(events, e) }
	return a, runner, &events
}

func policyLaunchRuns(r *fakeRunner) int {
	n := 0
	for _, c := range r.Calls {
		if len(c.Args) >= 2 && c.Args[0] == "policy" && c.Args[1] == "launch" {
			n++
		}
	}
	return n
}

// snapshotRows is pacelever v0.3.28's bundled catalog served as handoffkeep's
// rows: the catalog its policy launch goldens were recorded against.
// launchRulesCatalog is the live handoffkeep catalog the refusal rules are judged on.
func launchRulesCatalog() []CatalogRow {
	retired := "2026-11-01T00:00:00Z"
	row := func(profile, effort, model, pool, grade, gate string) CatalogRow {
		return CatalogRow{Profile: profile, Effort: effort, ModelID: model, Pool: pool, Grade: grade, Gate: gate}
	}
	gone := row("test-gone", "high", "gpt-x", "codex", "A", "default")
	gone.RetiredAt = &retired
	retiredRung := row("test-retired", "high", "gpt-x", "codex", "A", "default")
	retiredRung.RetiredAt = &retired
	return []CatalogRow{
		row("opus", "low", "claude-opus-5-5", "claude", "S", "default"),
		row("opus", "high", "claude-opus-5-5", "claude", "S", "default"),
		row("fable", "high", "claude-fable-5-2", "claude", "S+", "consult_only"),
		row("codex-astra", "xhigh", "gpt-6-astra", "codex", "S+", "consult_only"),
		row("test-unrated", "high", "model-u", "claude", "unrated", "default"),
		row("test-mixed", "high", "model-m", "claude", "A", "default"),
		row("test-mixed", "max", "model-m", "claude", "unrated", "default"),
		row("test-esc", "high", "gpt-e", "codex", "A", "escalation"),
		row("test-consult", "high", "gpt-c", "codex", "A", "consult_only"),
		retiredRung,
		row("test-retired", "low", "gpt-x", "codex", "B", "default"),
		gone,
		row("pi-test", "low", "anthropic/claude-sonnet-5-5", "anthropic-api", "A", "default"),
		row("test-terra", "high", "gpt-5.6-terra", "codex", "A", "default"),
		row("test-astra", "high", "gpt-6-astra", "codex", "A", "default"),
		row("haiku", "xhigh", "claude-haiku-5-5", "claude", "C", "default"),
	}
}

// TestLaunchNativeRefusalRules covers each rule of pacelever's policy launch
// (catalog.go resolveLaunch, resolveRow, roleCheck, defaultEffort; bench.go's
// snapshot overlay). A refusal is exit 3 with pacelever's reason after
// "quota catalog: ", as `pacelever policy launch` exits 3 with that error.
func TestLaunchNativeRefusalRules(t *testing.T) {
	for _, tc := range []struct {
		name   string
		req    QuotaRequest
		reason string // a refusal's reason, or "" for an admit
		want   Launch // an admit's fields (Catalog.Stale false)
	}{
		{name: "profile required", req: QuotaRequest{}, reason: "profile required"},
		{name: "credit profile needs an effort", req: QuotaRequest{Profile: "pi-test", Role: "worker"}, reason: "explicit --effort required for credit profile"},
		{name: "unknown effort rung", req: QuotaRequest{Profile: "opus", Effort: "turbo"}, reason: "unknown effort rung"},
		{name: "credit effort not placed", req: QuotaRequest{Profile: "pi-test", Effort: "ultra", Role: "worker"}, reason: "credit effort is not placed"},
		{name: "barred profile", req: QuotaRequest{Profile: "codex-med", Effort: "high"}, reason: "model_refused: retired model identity"},
		{name: "retired model id", req: QuotaRequest{Profile: "test-terra", Effort: "high"}, reason: "model_refused: retired model identity"},
		{name: "profile unknown", req: QuotaRequest{Profile: "test-nowhere", Effort: "high"}, reason: `profile "test-nowhere" is unknown or retired`},
		{name: "profile retired", req: QuotaRequest{Profile: "test-gone", Effort: "high"}, reason: `profile "test-gone" is unknown or retired`},
		{name: "unrated rung without marker", req: QuotaRequest{Profile: "test-mixed", Effort: "max", Role: "worker"}, reason: "e6_arm_required: exact measurement marker required"},
		{name: "unplaced profile without marker", req: QuotaRequest{Profile: "test-unrated", Role: "worker"}, reason: "e6_arm_required: unplaced profile requires exact measurement marker"},
		{name: "rung retired", req: QuotaRequest{Profile: "test-retired", Effort: "high", Role: "worker"}, reason: "rung is retired in the catalog"},
		{name: "credit rung not placed", req: QuotaRequest{Profile: "pi-test", Effort: "medium", Role: "worker"}, reason: "e6_arm_required: rung is not placed"},
		{name: "fallback effort comes from the marked live row", req: QuotaRequest{Profile: "test-unrated", Effort: "medium", Role: "worker", E6Arm: "test-unrated@high"}, want: Launch{Profile: "test-unrated", ModelID: "model-u", Effort: "high", Grade: "unrated", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "credit role", req: QuotaRequest{Profile: "pi-test", Effort: "low", Role: "reviewer"}, reason: "role_restricted: credit rung requires worker/tester; placed default rungs also allow builder"},
		{name: "consult model seat refused by its gate", req: QuotaRequest{Profile: "fable", Role: "worker"}, reason: "profile is consult_only; pass --operator-request"},
		{name: "consult model builder refused by its gate", req: QuotaRequest{Profile: "fable", Role: "builder"}, reason: "profile is consult_only; pass --operator-request"},
		{name: "removed astra alias", req: QuotaRequest{Profile: "test-astra", Effort: "high"}, reason: "role_restricted: removed astra seat alias"},
		{name: "astra admitted without a purpose", req: QuotaRequest{Profile: "codex-astra", Role: "worker"}, want: Launch{Profile: "codex-astra", ModelID: "gpt-6-astra", Effort: "xhigh", Grade: "S+", Gate: "consult_only", Pool: "codex", Family: "codex"}},
		{name: "operator request ref", req: QuotaRequest{Profile: "opus", Effort: "high", OperatorRequest: "ticket 12"}, reason: "operator_request_ref_invalid"},
		{name: "consult only", req: QuotaRequest{Profile: "test-consult", Role: "worker"}, reason: "profile is consult_only; pass --operator-request"},
		{name: "placement follows catalog grade", req: QuotaRequest{Profile: "haiku", Effort: "xhigh", Role: "worker"}, want: Launch{Profile: "haiku", ModelID: "claude-haiku-5-5", Effort: "xhigh", Grade: "C", Gate: "default", Pool: "claude", Family: "claude"}},

		{name: "default effort resolves to a live row", req: QuotaRequest{Profile: "opus", Role: "worker"}, want: Launch{Profile: "opus", ModelID: "claude-opus-5-5", Effort: "low", Grade: "S", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "missing rung falls back to live effort", req: QuotaRequest{Profile: "opus", Effort: "medium", Role: "worker"}, want: Launch{Profile: "opus", ModelID: "claude-opus-5-5", Effort: "low", Grade: "S", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "legacy alias is not retained", req: QuotaRequest{Profile: "codex-max", Effort: "max", Role: "worker"}, reason: `profile "codex-max" is unknown or retired`},
		{name: "snapshot-only profile is not retained", req: QuotaRequest{Profile: "kimi-k3-low", Role: "worker"}, reason: `profile "kimi-k3-low" is unknown or retired`},
		{name: "marker admits an unrated rung", req: QuotaRequest{Profile: "test-unrated", Effort: "high", Role: "worker", E6Arm: "test-unrated@high"}, want: Launch{Profile: "test-unrated", ModelID: "model-u", Effort: "high", Grade: "unrated", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "marker names the effort", req: QuotaRequest{Profile: "test-unrated", Role: "worker", E6Arm: "test-unrated@high"}, want: Launch{Profile: "test-unrated", ModelID: "model-u", Effort: "high", Grade: "unrated", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "unrated role needs no marker", req: QuotaRequest{Profile: "test-mixed", Effort: "max", Role: "builder"}, want: Launch{Profile: "test-mixed", ModelID: "model-m", Effort: "max", Grade: "unrated", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "credit rung seats a builder in pacelever", req: QuotaRequest{Profile: "pi-test", Effort: "low", Role: "builder"}, want: Launch{Profile: "pi-test", ModelID: "anthropic/claude-sonnet-5-5", Effort: "low", Grade: "A", Gate: "default", Pool: "anthropic-api", Family: "anthropic-api"}},
		{name: "escalation rung", req: QuotaRequest{Profile: "test-esc", Effort: "high", Role: "worker"}, want: Launch{Profile: "test-esc", ModelID: "gpt-e", Effort: "high", Grade: "A", Gate: "escalation", Pool: "codex", Family: "codex"}},
		{name: "consult only with a request", req: QuotaRequest{Profile: "test-consult", Role: "worker", OperatorRequest: "hk:doc/consult"}, want: Launch{Profile: "test-consult", ModelID: "gpt-c", Effort: "high", Grade: "A", Gate: "consult_only", Pool: "codex", Family: "codex"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, runner, _ := launchApp(t, "native", launchRulesCatalog(), nil)
			got, err := a.Quota.Launch(context.Background(), tc.req)
			if policyLaunchRuns(runner) != 0 {
				t.Fatal("native lookup ran pacelever policy launch")
			}
			if tc.reason != "" {
				want := 3
				if tc.reason == "profile required" {
					want = 2
				}
				if exitCode(err) != want || err.Error() != "quota catalog: "+tc.reason {
					t.Fatalf("got %+v, %v (exit %d); want exit %d %q", got, err, exitCode(err), want, tc.reason)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

// TestLaunchNativeReadsPoolPolicy: an unsubscribed profile is refused from
// pacelever's config.toml, which the lookup reads without running pacelever.
func TestLaunchNativeReadsPoolPolicy(t *testing.T) {
	a, _, _ := launchApp(t, "native", launchRulesCatalog(), nil)
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte("[profiles.opus]\nsubscribed = false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", path)
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}); exitCode(err) != 3 || err.Error() != "quota catalog: profile is unsubscribed" {
		t.Fatalf("unsubscribed profile: %v", err)
	}
	if err := os.WriteFile(path, []byte("[pools.claude]\nclass = \"hoard\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}); exitCode(err) != 2 {
		t.Fatalf("an invalid pool policy refuses as pacelever's config load does (exit 2): %v", err)
	}
}

// catalogEnv points the catalog client at a fixture handoffkeep and a private
// cache, and returns the client the lookup will build.
func catalogEnv(t *testing.T, state *catalogServer) *CatalogClient {
	t.Helper()
	server := httptest.NewServer(state)
	t.Cleanup(server.Close)
	t.Setenv("HANDOFFKEEP_URL", server.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	client, err := NewCatalogClient(CatalogClientConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// TestLaunchNativeSeesNewCatalogRowWithoutCacheExpiry is the 2026-11-08
// incident: a rung added in handoffkeep after the catalog was cached is found
// by the next lookup, with no cache expired by hand.
func TestLaunchNativeSeesNewCatalogRowWithoutCacheExpiry(t *testing.T) {
	before := `{"catalog":[{"profile":"omp-dsflash41","effort":"high","model_id":"deepseek/deepseek-v4.1-flash","pool":"commandcode","grade":"unrated","gate":"default"}]}`
	after := `{"catalog":[{"profile":"omp-dsflash41","effort":"high","model_id":"deepseek/deepseek-v4.1-flash","pool":"commandcode","grade":"unrated","gate":"default"},
 {"profile":"omp-dsflash41","effort":"max","model_id":"deepseek/deepseek-v4.1-flash","pool":"commandcode","grade":"A+","gate":"default"}]}`
	state := &catalogServer{body: before}
	catalogEnv(t, state)
	a, runner, _ := launchApp(t, "native", nil, nil)
	req := QuotaRequest{Profile: "omp-dsflash41", Effort: "max", Role: "worker"}
	if _, err := a.Quota.Launch(context.Background(), req); exitCode(err) != 3 || !strings.Contains(err.Error(), "e6_arm_required: unplaced profile requires exact measurement marker") {
		t.Fatalf("before the row exists the rung is unplaced: %v", err)
	}
	state.mu.Lock()
	state.body = after
	state.mu.Unlock()
	got, err := a.Quota.Launch(context.Background(), req)
	if err != nil || got.Effort != "max" || got.Grade != "A+" || got.Pool != "commandcode" {
		t.Fatalf("the new row must be found on the next lookup: %+v %v", got, err)
	}
	if state.count() != 2 {
		t.Fatalf("handoffkeep reads = %d, want 2 (the first read, then one past the cached refusal)", state.count())
	}
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "omp-dsflash41", Effort: "max", Role: "worker"}); err != nil || state.count() != 2 {
		t.Fatalf("an admit from a fresh cache reads handoffkeep again: reads=%d err=%v", state.count(), err)
	}
	if policyLaunchRuns(runner) != 0 {
		t.Fatal("native lookup ran pacelever policy launch")
	}
}

// TestLaunchNativeStaleCatalog: staleness is panewire's catalog rule (a cache
// a day old with handoffkeep down). A stale ordinary rung is refused as the
// pacelever path refuses it (exit 70); a stale escalation rung is pacelever's
// own refusal (exit 3).
func TestLaunchNativeStaleCatalog(t *testing.T) {
	state := &catalogServer{fail: true}
	client := catalogEnv(t, state)
	rows := []json.RawMessage{
		json.RawMessage(`{"profile":"opus","effort":"high","model_id":"claude-opus-5-5","pool":"claude","grade":"S","gate":"default"}`),
		json.RawMessage(`{"profile":"test-esc","effort":"high","model_id":"gpt-e","pool":"codex","grade":"A","gate":"escalation"}`),
	}
	cache, _ := json.Marshal(catalogCache{FetchedAt: launchNow.Add(-25 * time.Hour).Format(time.RFC3339Nano), EndpointID: client.endpointID(), Rows: rows})
	if err := os.MkdirAll(filepath.Dir(client.cachePath()), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(client.cachePath(), cache, 0o600); err != nil {
		t.Fatal(err)
	}
	a, _, _ := launchApp(t, "native", nil, nil)
	got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
	if exitCode(err) != exitConfig || err.Error() != "quota catalog is stale" || !got.Catalog.Stale {
		t.Fatalf("stale ordinary rung: %+v %v", got, err)
	}
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "test-esc", Effort: "high", Role: "worker"}); exitCode(err) != 3 || err.Error() != "quota catalog: catalog is stale; non-default gate requires --operator-request" {
		t.Fatalf("stale escalation rung: %v", err)
	}
}

// TestLaunchNativeCatalogUnavailable: without a live or cached catalog no
// snapshot rows are supplied, so both a formerly known and unknown profile
// are refused as unknown.
func TestLaunchNativeCatalogUnavailable(t *testing.T) {
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	a, _, _ := launchApp(t, "native", nil, nil)
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}); exitCode(err) != 3 || err.Error() != `quota catalog: profile "opus" is unknown or retired` {
		t.Fatalf("profile without a catalog: %v", err)
	}
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "test-nowhere", Effort: "high"}); exitCode(err) != 3 {
		t.Fatalf("unknown profile without a catalog: %v", err)
	}
}

func TestLaunchDefaultsToNativeWithoutExecutingQuotaCLI(t *testing.T) {
	a, runner, events := launchApp(t, "", launchRulesCatalog(), nil)
	got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
	if err != nil || got != opusLaunch {
		t.Fatalf("native launch = %+v, %v", got, err)
	}
	if len(runner.Calls) != 0 || len(*events) != 0 {
		t.Fatalf("default launch used external quota CLI or comparison events: calls=%+v events=%+v", runner.Calls, *events)
	}
}

func paceleverLaunchJSON(l Launch) *Result {
	b, _ := json.Marshal(l)
	return &Result{Out: string(b)}
}

var opusLaunch = Launch{Profile: "opus", ModelID: "claude-opus-5-5", Effort: "high", Grade: "S", Gate: "default", Pool: "claude", Family: "claude"}

// TestLaunchShadowRecordsAgreement: by default pacelever decides and the
// native lookup is recorded beside it.
