package fleet

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

var launchNow = time.Date(2026, 11, 8, 3, 0, 0, 0, time.UTC)

// launchApp is an App whose quota is the production admissionQuota over a fake
// pacelever. paceleverLaunch answers `pacelever policy launch`; nil means the
// test expects none, so any run of it fails the test. rows, when set, is the
// bench catalog: the native lookup's fixture, and the catalog cache the
// shadow comparison reads, for a handoffkeep that must not be asked.
func launchApp(t *testing.T, mode string, rows []CatalogRow, paceleverLaunch *Result) (*App, *fakeRunner, *[]fleetevent.Event) {
	t.Helper()
	t.Setenv("PACELEVER_CONFIG", filepath.Join(t.TempDir(), "absent.toml"))
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
func snapshotRows(t *testing.T) []CatalogRow {
	t.Helper()
	data, err := os.ReadFile("catalog-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Rows []CatalogRow `json:"rows"`
	}
	if err := json.Unmarshal(data, &doc); err != nil || len(doc.Rows) == 0 {
		t.Fatalf("snapshot: %v", err)
	}
	return doc.Rows
}

// TestLaunchNativeMatchesPaceleverGoldens runs pacelever's own policy launch
// goldens through the native lookup over the catalog they were recorded
// against: the same exit status and, on an admit, the same launch fields.
func TestLaunchNativeMatchesPaceleverGoldens(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "launch_golden", "policy_launch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Request struct {
				Profile, Effort, Role, Purpose string
			} `json:"request"`
			LaunchCode int     `json:"launch_code"`
			Launch     *Launch `json:"launch"`
			Change     string  `json:"intentional_change"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil || len(fixture.Cases) == 0 {
		t.Fatalf("goldens: %v", err)
	}
	a, runner, _ := launchApp(t, "native", snapshotRows(t), nil)
	for _, tc := range fixture.Cases {
		r := tc.Request
		name := r.Profile + "/" + r.Effort + "/" + r.Role + "/" + r.Purpose
		got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: r.Profile, Effort: r.Effort, Role: r.Role, Purpose: r.Purpose})
		switch {
		case tc.Change != "":
			if err != nil {
				t.Errorf("%s (%s): want an admit, got %v", name, tc.Change, err)
			}
		case tc.LaunchCode != 0:
			if exitCode(err) != tc.LaunchCode {
				t.Errorf("%s: exit %d (%v), want %d", name, exitCode(err), err, tc.LaunchCode)
			}
		case err != nil:
			t.Errorf("%s: %v", name, err)
		case got != *tc.Launch:
			t.Errorf("%s: launch %+v, want %+v", name, got, *tc.Launch)
		}
	}
	if n := policyLaunchRuns(runner); n != 0 {
		t.Fatalf("native lookup ran pacelever policy launch %d times", n)
	}
}

// launchRulesCatalog is the handoffkeep catalog the refusal rules are judged
// on. Profiles it does not name come from pacelever's bundled snapshot, as in
// pacelever.
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
		row("test-unrated", "high", "model-u", "claude", "unrated", "default"),
		row("test-mixed", "high", "model-m", "claude", "A", "default"),
		row("test-mixed", "max", "model-m", "claude", "unrated", "default"),
		row("test-esc", "high", "gpt-e", "codex", "A", "escalation"),
		row("test-consult", "", "gpt-c", "codex", "A", "consult_only"),
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
		{name: "fallback rung needs its own marker", req: QuotaRequest{Profile: "test-unrated", Effort: "medium", Role: "worker", E6Arm: "test-unrated@high"}, reason: "e6_arm_required"},
		{name: "credit role", req: QuotaRequest{Profile: "pi-test", Effort: "low", Role: "reviewer"}, reason: "role_restricted: credit rung requires worker/tester; placed default rungs also allow builder"},
		{name: "consult model seat", req: QuotaRequest{Profile: "fable", Role: "worker"}, reason: "role_restricted: consult model cannot hold this seat"},
		{name: "round extension", req: QuotaRequest{Profile: "fable", Role: "builder"}, reason: "role_restricted: round-extension and a round-ext document required"},
		{name: "removed astra alias", req: QuotaRequest{Profile: "test-astra", Effort: "high"}, reason: "role_restricted: removed astra seat alias"},
		{name: "astra purpose", req: QuotaRequest{Profile: "codex-astra"}, reason: "role_restricted: astra requires director, architect, or operator-request purpose"},
		{name: "operator request ref", req: QuotaRequest{Profile: "opus", Effort: "high", OperatorRequest: "ticket 12"}, reason: "operator_request_ref_invalid"},
		{name: "consult only", req: QuotaRequest{Profile: "test-consult", Role: "worker"}, reason: "profile is consult_only; pass --operator-request"},
		{name: "snapshot keeps an equally graded rung unplaced", req: QuotaRequest{Profile: "haiku", Effort: "xhigh", Role: "worker"}, reason: "e6_arm_required: unplaced profile requires exact measurement marker"},

		{name: "default effort from pacelever's defaults", req: QuotaRequest{Profile: "opus", Role: "worker"}, want: Launch{Profile: "opus", ModelID: "claude-opus-5-5", Effort: "high", Grade: "S", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "missing rung falls back, effort kept", req: QuotaRequest{Profile: "opus", Effort: "medium", Role: "worker"}, want: Launch{Profile: "opus", ModelID: "claude-opus-5-5", Effort: "medium", Grade: "S", Gate: "default", Pool: "claude", Family: "claude"}},
		{name: "alias resolves to canon", req: QuotaRequest{Profile: "codex-max", Effort: "max", Role: "worker"}, want: Launch{Profile: "codex-sol", ModelID: "gpt-6.1-sol", Effort: "max", Grade: "S+", Gate: "default", Pool: "codex", Family: "codex"}},
		{name: "snapshot-only profile", req: QuotaRequest{Profile: "kimi-k3-low", Role: "worker"}, want: Launch{Profile: "kimi-k3-low", ModelID: "kimi-k3", Grade: "A", Gate: "default", Pool: "kimi", Family: "kimi"}},
		{name: "marker admits an unrated rung", req: QuotaRequest{Profile: "test-unrated", Effort: "high", Role: "worker", E6Arm: "test-unrated@high"}, want: Launch{Profile: "test-unrated", ModelID: "model-u", Effort: "high", Grade: "unrated", Gate: "default", Pool: "claude"}},
		{name: "marker names the effort", req: QuotaRequest{Profile: "test-unrated", Role: "worker", E6Arm: "test-unrated@high"}, want: Launch{Profile: "test-unrated", ModelID: "model-u", Effort: "high", Grade: "unrated", Gate: "default", Pool: "claude"}},
		{name: "unrated role needs no marker", req: QuotaRequest{Profile: "test-mixed", Effort: "max", Role: "builder"}, want: Launch{Profile: "test-mixed", ModelID: "model-m", Effort: "max", Grade: "unrated", Gate: "default", Pool: "claude"}},
		{name: "credit rung seats a builder in pacelever", req: QuotaRequest{Profile: "pi-test", Effort: "low", Role: "builder"}, want: Launch{Profile: "pi-test", ModelID: "anthropic/claude-sonnet-5-5", Effort: "low", Grade: "A", Gate: "default", Pool: "anthropic-api"}},
		{name: "escalation rung", req: QuotaRequest{Profile: "test-esc", Effort: "high", Role: "worker"}, want: Launch{Profile: "test-esc", ModelID: "gpt-e", Effort: "high", Grade: "A", Gate: "escalation", Pool: "codex"}},
		{name: "consult only with a request", req: QuotaRequest{Profile: "test-consult", Role: "worker", OperatorRequest: "hk:doc/consult"}, want: Launch{Profile: "test-consult", ModelID: "gpt-c", Grade: "A", Gate: "consult_only", Pool: "codex"}},
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

// TestLaunchNativeCatalogUnavailable: with no catalog at all pacelever judges
// its bundled snapshot as stale, so a known rung is refused stale (exit 70)
// and an unknown profile is unknown (exit 3).
func TestLaunchNativeCatalogUnavailable(t *testing.T) {
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(t.TempDir(), "absent.env"))
	a, _, _ := launchApp(t, "native", nil, nil)
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}); exitCode(err) != exitConfig || err.Error() != "quota catalog is stale" {
		t.Fatalf("known rung without a catalog: %v", err)
	}
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "test-nowhere", Effort: "high"}); exitCode(err) != 3 {
		t.Fatalf("unknown profile without a catalog: %v", err)
	}
}

func paceleverLaunchJSON(l Launch) *Result {
	b, _ := json.Marshal(l)
	return &Result{Out: string(b)}
}

var opusLaunch = Launch{Profile: "opus", ModelID: "claude-opus-5-5", Effort: "high", Grade: "S", Gate: "default", Pool: "claude", Family: "claude"}

// TestLaunchShadowRecordsAgreement: by default pacelever decides and the
// native lookup is recorded beside it.
func TestLaunchShadowRecordsAgreement(t *testing.T) {
	a, runner, events := launchApp(t, "", launchRulesCatalog(), paceleverLaunchJSON(opusLaunch))
	got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
	if err != nil || got != opusLaunch {
		t.Fatalf("shadow returns pacelever's launch: %+v %v", got, err)
	}
	if policyLaunchRuns(runner) != 1 {
		t.Fatalf("shadow runs pacelever policy launch once, got %d", policyLaunchRuns(runner))
	}
	awaitShadowIdle(t)
	if len(*events) != 1 {
		t.Fatalf("want one shadow event, got %+v", *events)
	}
	if e := (*events)[0]; e.Kind != "quota_launch_shadow" || e.Outcome != "agree" || e.Reason != "claude" || !strings.Contains(e.Detail, "profile=opus") {
		t.Fatalf("shadow event = %+v", e)
	}
}

// TestLaunchShadowRecordsDisagreement: a native answer that differs is
// recorded with both answers and the differing fields, and never changes the
// spawn's outcome — pacelever's admit or refusal stands.
func TestLaunchShadowRecordsDisagreement(t *testing.T) {
	other := opusLaunch
	other.ModelID, other.Grade = "claude-opus-5-1", "S+"
	a, _, events := launchApp(t, launchModeShadowForTest, launchRulesCatalog(), paceleverLaunchJSON(other))
	got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
	if err != nil || got != other {
		t.Fatalf("shadow must return pacelever's launch: %+v %v", got, err)
	}
	awaitShadowIdle(t)
	if len(*events) != 1 {
		t.Fatalf("want one shadow event, got %+v", *events)
	}
	e := (*events)[0]
	if e.Kind != "quota_launch_shadow" || e.Outcome != "disagree" {
		t.Fatalf("shadow event = %+v", e)
	}
	for _, want := range []string{"pacelever{exit=0 profile=opus model=claude-opus-5-1", "native{exit=0 profile=opus model=claude-opus-5-5", "diff=model_id,grade"} {
		if !strings.Contains(e.Detail, want) {
			t.Errorf("shadow detail lacks %q: %s", want, e.Detail)
		}
	}

	// The incident: pacelever's stale cache refuses a rung handoffkeep has.
	refused := &Result{Code: 3, Err: "error: e6_arm_required: unplaced profile requires exact measurement marker\n"}
	a, _, events = launchApp(t, "", launchRulesCatalog(), refused)
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"}); exitCode(err) != 3 {
		t.Fatalf("shadow must keep pacelever's refusal: %v", err)
	}
	awaitShadowIdle(t)
	if len(*events) != 1 || (*events)[0].Outcome != "disagree" {
		t.Fatalf("shadow event = %+v", *events)
	}
	for _, want := range []string{`pacelever{exit=3`, `reason="e6_arm_required: unplaced profile requires exact measurement marker"`, "native{exit=0", "diff=exit"} {
		if !strings.Contains((*events)[0].Detail, want) {
			t.Errorf("shadow detail lacks %q: %s", want, (*events)[0].Detail)
		}
	}

	// Both refuse alike: agreement, with pacelever's stderr reason read.
	a, _, events = launchApp(t, "", launchRulesCatalog(), &Result{Code: 3, Err: "warning: cache old\nerror: rung is retired in the catalog\n"})
	if _, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "test-retired", Effort: "high", Role: "worker"}); exitCode(err) != 3 {
		t.Fatalf("shadow must keep pacelever's refusal: %v", err)
	}
	awaitShadowIdle(t)
	if len(*events) != 1 || (*events)[0].Outcome != "agree" {
		t.Fatalf("matching refusals must agree: %+v", *events)
	}
}

const launchModeShadowForTest = "shadow"

// TestLaunchNativeNeverRunsPacelever: in native mode `pacelever policy
// launch` is never run and nothing is recorded as a comparison.
func TestLaunchNativeNeverRunsPacelever(t *testing.T) {
	a, runner, events := launchApp(t, "native", launchRulesCatalog(), nil)
	got, err := a.Quota.Launch(context.Background(), QuotaRequest{Profile: "opus", Effort: "high", Role: "worker"})
	if err != nil || got != opusLaunch {
		t.Fatalf("native launch: %+v %v", got, err)
	}
	awaitShadowIdle(t)
	if len(runner.Calls) != 0 || len(*events) != 0 {
		t.Fatalf("native mode ran %v and recorded %+v", runner.Calls, *events)
	}
}

// TestLaunchModeConfig: `[quota].launch` takes shadow or native, like gate.
func TestLaunchModeConfig(t *testing.T) {
	for _, tc := range []struct {
		value string
		ok    bool
	}{{`"shadow"`, true}, {`"native"`, true}, {`"pacelever"`, false}, {`true`, false}} {
		path := filepath.Join(t.TempDir(), "hosts.toml")
		if err := os.WriteFile(path, []byte("[quota]\nlaunch = "+tc.value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadConfig(path); (err == nil) != tc.ok {
			t.Errorf("quota.launch=%s: err=%v want ok=%v", tc.value, err, tc.ok)
		}
	}
}

// TestSpawnAdmissionDefaultRunsPaceleverLaunch is the default on every host
// until the switch is flipped: a spawn's admission resolves the rung with
// `pacelever policy launch`, and the native lookup is only recorded.
func TestSpawnAdmissionDefaultRunsPaceleverLaunch(t *testing.T) {
	a := testApp(t)
	t.Setenv("PACELEVER_CONFIG", filepath.Join(t.TempDir(), "absent.toml"))
	pacelever := opusLaunch
	pacelever.Grade = "S+"
	launchOut, _ := json.Marshal(pacelever)
	receipt := `{"schema":"scopefuel.gate.v1","provider_id":"claude","profile":"opus","ok":true,"exit_code":0,"reason":"opus pool=claude used=10% class=spend"}`
	runner := &fakeRunner{Do: func(c Command) Result {
		switch {
		case len(c.Args) >= 2 && c.Args[0] == "policy" && c.Args[1] == "launch":
			return Result{Out: string(launchOut)}
		case len(c.Args) > 0 && c.Args[0] == "gate":
			return Result{Out: receipt}
		}
		return Result{Code: 1, Err: "unexpected command"}
	}}
	a.Runner = runner
	a.Quota = admissionQuota{CLIQuota{Runner: runner, Binary: "pacelever"}, a}
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return launchRulesCatalog(), nil }
	cacheLaunchRows(t, launchRulesCatalog(), a.Now())
	// The gate's events arrive on this goroutine and the launch shadow's on
	// its own.
	var mu sync.Mutex
	var events []fleetevent.Event
	a.EventSink = func(e fleetevent.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	s := Spawn{CWD: "/work/project", Profile: "opus", Role: "worker", Effort: "high"}
	launch, _, err := a.admitLaunch(context.Background(), s, a.Policy.Profiles["opus"], "high", "", "", "", false)
	if err != nil {
		t.Fatalf("admission: %v", err)
	}
	if launch.Grade != "S+" {
		t.Fatalf("the spawn must take pacelever's launch, got %+v", launch)
	}
	if n := policyLaunchRuns(runner); n != 1 {
		t.Fatalf("pacelever policy launch ran %d times, want 1", n)
	}
	awaitShadowIdle(t)
	mu.Lock()
	defer mu.Unlock()
	shadow := slices.IndexFunc(events, func(e fleetevent.Event) bool { return e.Kind == "quota_launch_shadow" })
	if shadow < 0 || events[shadow].Outcome != "disagree" || !strings.Contains(events[shadow].Detail, "diff=grade") {
		t.Fatalf("native lookup not recorded as a disagreement: %+v", events)
	}
}
