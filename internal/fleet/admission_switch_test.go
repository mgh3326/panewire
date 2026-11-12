package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
	"github.com/mgh3326/panewire/internal/quota/shared"
)

var switchNow = time.Date(2026, 11, 7, 3, 0, 0, 0, time.UTC)

const switchKey = "claude:0123456789abcdef"

// switchCatalog is the bench catalog the gate judges with.
const switchCatalog = `{"catalog":[
 {"profile":"opus","effort":"high","model_id":"claude-opus-5-5","pool":"claude","grade":"S+","gate":"default"},
 {"profile":"codex-sol","effort":"max","model_id":"gpt-6.1-sol","pool":"codex","grade":"S+","gate":"default"},
 {"profile":"cmd-dsflash41","effort":"high","model_id":"deepseek/deepseek-v4.1-flash","pool":"commandcode","grade":"A+","gate":"default"},
 {"profile":"kiro-sonnet","effort":"high","model_id":"claude-sonnet-5-5","pool":"kiro","grade":"A","gate":"default"}
],"source":{"stale":false}}`

func switchRows(t *testing.T) []CatalogRow {
	t.Helper()
	var doc struct {
		Catalog []CatalogRow `json:"catalog"`
	}
	if err := json.Unmarshal([]byte(switchCatalog), &doc); err != nil || len(doc.Catalog) == 0 {
		t.Fatalf("switch catalog: %v", err)
	}
	return doc.Catalog
}

// switchApp is an App whose quota CLI is a fake pacelever that answers `gate`
// with paceleverGate, and whose catalog is handoffkeep's. The shared store
// holds a reading for switchKey.
func switchApp(t *testing.T, mode string, reading *shared.Observation, paceleverGate Result) (*App, *fakeRunner, *[]fleetevent.Event) {
	t.Helper()
	t.Setenv("PACELEVER_CONFIG", "")
	runner := &fakeRunner{Do: func(c Command) Result {
		switch {
		case c.Name == "pacelever" && len(c.Args) > 0 && c.Args[0] == "gate":
			return paceleverGate
		case c.Name == "panewire" && len(c.Args) >= 2 && slices.Equal(c.Args[:2], []string{"quota", "refresh"}):
			return Result{}
		}
		return Result{Code: 1, Err: "unexpected command"}
	}}
	a := &App{Home: t.TempDir(), Now: func() time.Time { return switchNow }, Runner: runner, Err: io.Discard, Config: Config{Quota: map[string]any{}}}
	if mode != "" {
		a.Config.Quota["gate"] = mode
	}
	a.Quota = admissionQuota{CLIQuota{Runner: runner, Binary: "pacelever"}, a}
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return switchRows(t), nil }
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) {
		return shared.Latest{Observation: reading}, nil
	}
	var events []fleetevent.Event
	a.EventSink = func(e fleetevent.Event) { events = append(events, e) }
	return a, runner, &events
}

func switchReading(fivehour, weekly float64) *shared.Observation {
	reset5h, reset7d := switchNow.Add(3*time.Hour), switchNow.Add(5*24*time.Hour)
	return &shared.Observation{Kind: shared.KindAuto, MeasuredAt: switchNow.Add(-time.Minute), Buckets: []shared.Bucket{
		{Window: "5h", UsedPct: fivehour, ResetsAt: &reset5h, Scope: "account"},
		{Window: "7d", UsedPct: weekly, ResetsAt: &reset7d, Scope: "account"},
	}}
}

func paceleverReceipt(ok bool, code int, reason string) Result {
	b, _ := json.Marshal(map[string]any{"schema": "scopefuel.gate.v1", "account_key": switchKey, "provider_id": "claude", "profile": "opus", "ok": ok, "exit_code": code, "reason": reason})
	return Result{Code: code, Out: string(b)}
}

// paceleverRuns lists every pacelever execution.
func paceleverRuns(r *fakeRunner) []Command {
	var runs []Command
	for _, c := range r.Calls {
		if c.Name == "pacelever" {
			runs = append(runs, c)
		}
	}
	return runs
}

func gateRuns(r *fakeRunner) int {
	n := 0
	for _, c := range r.Calls {
		if len(c.Args) > 0 && c.Args[0] == "gate" {
			n++
		}
	}
	return n
}

var opusRequest = QuotaRequest{Profile: "opus", Effort: "high", Role: "worker", AccountKey: switchKey}

func TestAdmissionDefaultsToNativeWithoutExecutingQuotaCLI(t *testing.T) {
	a, runner, _ := switchApp(t, "", switchReading(20, 40), Result{Code: 99})
	if _, err := a.Quota.Gate(context.Background(), opusRequest); err != nil {
		t.Fatalf("native gate refused: %v", err)
	}
	if len(runner.Calls) != 0 {
		t.Fatalf("default gate executed an external quota CLI: %+v", runner.Calls)
	}
}

func TestRetiredQuotaKeysWarnOnceAndModesValidate(t *testing.T) {
	path := writeHostsConfig(t, `[quota]
gate = "native"
launch = "native"
native_pools = ["commandcode"]
`)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("FLEET_HOSTS_CONFIG", path)
	t.Setenv("PANEWIRE_QUOTA_BIN", "/ignored")
	var warnings strings.Builder
	if _, err := New(io.Discard, &warnings); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"gate", "launch", "native_pools", "PANEWIRE_QUOTA_BIN"} {
		if n := strings.Count(warnings.String(), key); n != 1 {
			t.Errorf("%s warning count=%d output=%q", key, n, warnings.String())
		}
	}
	for _, key := range []string{"gate", "launch"} {
		bad := writeHostsConfig(t, "[quota]\n"+key+" = \"shadow\"\n")
		if _, err := LoadConfig(bad); err == nil {
			t.Errorf("retired %s=shadow loaded", key)
		}
	}
}

func TestLoadConfigRejectsRetiredQuotaSwitchValues(t *testing.T) {
	for _, key := range []string{"gate", "launch"} {
		t.Run(key, func(t *testing.T) {
			path := writeHostsConfig(t, "[quota]\n"+key+" = \"shadow\"\n")
			if _, err := LoadConfig(path); err == nil {
				t.Fatalf("LoadConfig accepted retired %s=shadow", key)
			}
		})
	}
}

// TestAdmissionShadowLogsDisagreement: in shadow mode pacelever's verdict
// decides the spawn, and a native verdict that differs is logged with the
// profile, pool and both verdicts and reasons.
func TestAdmissionNativeNeverRunsPaceleverGate(t *testing.T) {
	a, runner, events := switchApp(t, gateModeNative, switchReading(20, 40), Result{Code: 99})
	j, err := a.judgeGate(context.Background(), opusRequest, "claude", quotaJudgment{Host: "local", Source: "shared", AccountKey: switchKey})
	if err != nil || j.Outcome != "admitted" {
		t.Fatalf("native admit: %+v %v", j, err)
	}
	if len(*events) != 0 {
		t.Fatalf("native mode logs no shadow comparison: %+v", *events)
	}
	out, err := a.Quota.Gate(context.Background(), opusRequest)
	if err != nil {
		t.Fatal(err)
	}
	granted, err := admissionGate("opus", switchKey, "claude", out)
	if err != nil || granted.Pool != "claude" {
		t.Fatalf("native receipt must admit through admissionGate: %+v %v\n%s", granted, err, out)
	}
	if runs := paceleverRuns(runner); len(runs) != 0 {
		t.Fatalf("native mode ran pacelever: %v", runs)
	}
}

// TestAdmissionNativeStaleUnmeasuredPoolRunsNoPacelever: a stale reading of
// a held account in a pool panewire does not measure (kiro) is unmeasurable;
// the native gate never refreshes it through pacelever.
func TestAdmissionNativeStaleUnmeasuredPoolRunsNoPacelever(t *testing.T) {
	stale := switchReading(20, 40)
	stale.MeasuredAt = switchNow.Add(-time.Hour)
	a, runner, _ := switchApp(t, gateModeNative, stale, Result{Code: 99})
	a.Config.Quota["accounts"] = map[string]any{"kiro": "0123456789abcdef"}
	for _, key := range []string{"kiro:0123456789abcdef", ""} {
		_, err := a.Quota.Gate(context.Background(), QuotaRequest{Profile: "kiro-sonnet", Effort: "high", Role: "worker", AccountKey: key})
		if exitCode(err) != gateUnmeasurable || !strings.Contains(err.Error(), msgAccountStale) {
			t.Fatalf("account %q: stale kiro reading must be unmeasurable: %v", key, err)
		}
	}
	if runs := paceleverRuns(runner); len(runs) != 0 || slices.ContainsFunc(runner.Calls, func(c Command) bool { return slices.Contains(c.Args, "refresh") }) {
		t.Fatalf("native gate measured or ran pacelever: %v", runner.Calls)
	}
}

// TestAdmissionNativeRefusalsKeepGateOutcomes: a native refusal surfaces as
// the typed gate refusal pacelever's exit gave.
func TestAdmissionNativeRefusalsKeepGateOutcomes(t *testing.T) {
	a, runner, _ := switchApp(t, gateModeNative, switchReading(99.5, 40), Result{Code: 99})
	_, err := a.Quota.Gate(context.Background(), opusRequest)
	_, paceleverErr := gateResult(paceleverReceipt(false, quotaGateBlocked, "quota exhausted window=5h cutoff=99"), opusRequest)
	var refusal, paceleverRefusal *gateRefusal
	if !errors.As(err, &refusal) || !errors.As(paceleverErr, &paceleverRefusal) || refusal.outcome != paceleverRefusal.outcome || exitCode(err) != quotaGateBlocked || err.Error() != paceleverErr.Error() {
		t.Fatalf("exhausted account must refuse as pacelever's receipt does: %v vs %v", err, paceleverErr)
	}
	stale := switchReading(20, 40)
	stale.MeasuredAt = switchNow.Add(-time.Hour)
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) { return shared.Latest{Observation: stale}, nil }
	_, err = a.Quota.Gate(context.Background(), opusRequest)
	if exitCode(err) != gateUnmeasurable || !strings.Contains(err.Error(), msgAccountStale) {
		t.Fatalf("stale reading must refuse unmeasurable: %v", err)
	}
	for _, role := range []string{"reviewer", "builder"} {
		_, err = a.Quota.Gate(context.Background(), QuotaRequest{Profile: "cmd-dsflash41", Effort: "high", Role: role})
		if exitCode(err) != exitSeatRefused {
			t.Fatalf("cmd-dsflash41 %s must be role-denied (gate exit 5 → %d): %v", role, exitSeatRefused, err)
		}
	}
	if runs := paceleverRuns(runner); len(runs) != 0 {
		t.Fatalf("native refusals ran pacelever: %v", runs)
	}
}

// TestAdmissionNativeAlternativesReadOtherHeldPools: a refusal lists the
// same-grade rungs that the stored readings of the other pools this host
// holds would admit — read without a lease or a measurement (hk 3211).
func TestAdmissionNativeAlternativesReadOtherHeldPools(t *testing.T) {
	a, runner, _ := switchApp(t, gateModeNative, nil, Result{Code: 99})
	a.Config.Quota["accounts"] = map[string]any{"codex": "fedcba9876543210"}
	a.SharedLatest = func(_ context.Context, key string) (shared.Latest, error) {
		if key == "codex:fedcba9876543210" {
			return shared.Latest{Observation: switchReading(10, 20)}, nil
		}
		return shared.Latest{Observation: switchReading(99.5, 40)}, nil
	}
	x := a.nativeGate(context.Background(), opusRequest, true)
	var receipt gateReceipt
	if err := json.Unmarshal([]byte(x.Out), &receipt); err != nil || x.Code != quotaGateBlocked {
		t.Fatalf("opus must be refused: %d %s", x.Code, x.Out)
	}
	if !slices.Equal(receipt.Alternatives, []string{"codex-sol --effort max"}) {
		t.Fatalf("alternatives = %v, want the codex rung the held codex account admits", receipt.Alternatives)
	}
	if runs := paceleverRuns(runner); len(runs) != 0 || slices.ContainsFunc(runner.Calls, func(c Command) bool { return slices.Contains(c.Args, "refresh") }) {
		t.Fatalf("alternatives must not run pacelever or measure: %v", runner.Calls)
	}
}

func TestAdmissionNativeSameGradeEscalationIncludesGateCode(t *testing.T) {
	a, _, _ := switchApp(t, gateModeNative, switchReading(20, 40), Result{Code: 99})
	rows := switchRows(t)
	rows[0].Gate = escalationGate
	rows = append(rows, CatalogRow{Profile: "sonnet", Effort: "high", ModelID: "claude-sonnet-5-5", Pool: "claude", Grade: "S+", Gate: defaultGate})
	a.BenchCatalog = func(context.Context) ([]CatalogRow, error) { return rows, nil }
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) {
		return shared.Latest{Observation: switchReading(20, 40)}, nil
	}

	r := opusRequest
	r.Effort = ""
	x := a.nativeGate(context.Background(), r, true)
	var receipt gateReceipt
	if err := json.Unmarshal([]byte(x.Out), &receipt); err != nil || x.Code != quotaGateBlocked {
		t.Fatalf("escalation must be refused with a gate receipt: %d %s (%v)", x.Code, x.Out, err)
	}
	if receipt.Reason != "escalation: same-grade ordinary alternative available" || receipt.GateCode != "escalation" {
		t.Fatalf("same-grade escalation refusal lost its gate code: %+v", receipt)
	}
	_, err := gateResult(x, r)
	if got := hostGateNote(quotaJudgment{Outcome: outcomeQuotaExhausted}, err); got != "gate_blocked:escalation" {
		t.Fatalf("remote host note = %q, want gate code", got)
	}
}

// TestAdmissionNativeRefreshesHeldStaleAccount: like pacelever's gate, a stale
// reading of an account this host holds is measured once and read again.
func TestAdmissionNativeRefreshesHeldStaleAccount(t *testing.T) {
	stale := switchReading(20, 40)
	stale.MeasuredAt = switchNow.Add(-time.Hour)
	a, runner, _ := switchApp(t, gateModeNative, stale, Result{Code: 99})
	a.Config.Quota["accounts"] = map[string]any{"claude": "0123456789abcdef"}
	reads := 0
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) {
		reads++
		if reads == 1 {
			return shared.Latest{Observation: stale}, nil
		}
		return shared.Latest{Observation: switchReading(20, 40)}, nil
	}
	if _, err := a.Quota.Gate(context.Background(), opusRequest); err != nil {
		t.Fatalf("refreshed reading must admit: %v", err)
	}
	refreshed := slices.ContainsFunc(runner.Calls, func(c Command) bool {
		return c.Name == "panewire" && slices.Equal(c.Args, []string{"quota", "refresh", "claude"})
	})
	if !refreshed || reads != 2 || len(paceleverRuns(runner)) != 0 {
		t.Fatalf("want one native refresh, a second read and no pacelever: reads=%d calls=%v", reads, runner.Calls)
	}
}

// TestAdmissionLocalRequestReadsHeldAccount: without an account key the gate
// judges the account this host holds, as pacelever's local gate does.
func TestAdmissionLocalRequestReadsHeldAccount(t *testing.T) {
	a, _, _ := switchApp(t, gateModeNative, switchReading(20, 40), Result{Code: 99})
	local := opusRequest
	local.AccountKey = ""
	if _, err := a.Quota.Gate(context.Background(), local); exitCode(err) != gateUnmeasurable {
		t.Fatalf("a host holding no claude account cannot be judged: %v", err)
	}
	config := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(config, []byte("[quota.accounts]\nclaude = \"0123456789abcdef\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PACELEVER_CONFIG", config)
	out, err := a.Quota.Gate(context.Background(), local)
	if err != nil || !strings.Contains(out, `"reading_account_key":"`+switchKey+`"`) {
		t.Fatalf("local request must judge the account pacelever's config pins: %v\n%s", err, out)
	}
	t.Setenv("PACELEVER_CONFIG", "")
	a.Config.Quota["accounts"] = map[string]any{"claude": "0123456789abcdef"}
	out, err = a.Quota.Gate(context.Background(), local)
	if err != nil || !strings.Contains(out, `"reading_account_key":"`+switchKey+`"`) {
		t.Fatalf("local request must judge the held account: %v\n%s", err, out)
	}
}

// TestAdmissionLocalRequestFallsBackToManualWindows: a local request with no
// usable automatic reading is judged on the account's operator entries, as
// pacelever's local gate is; a named account stays strict.
func TestAdmissionLocalRequestFallsBackToManualWindows(t *testing.T) {
	a, _, _ := switchApp(t, gateModeNative, nil, Result{Code: 99})
	a.Config.Quota["accounts"] = map[string]any{"commandcode": "0123456789abcdef"}
	expires := switchNow.Add(48 * time.Hour)
	entry := shared.Observation{ID: 41, Kind: "manual", Window: "credit", MeasuredAt: switchNow.Add(-time.Hour), ExpiresAt: &expires, Buckets: []shared.Bucket{{Window: "credit", UsedPct: 20, Scope: "account"}}}
	a.SharedLatest = func(context.Context, string) (shared.Latest, error) {
		return shared.Latest{Manual: []shared.Observation{entry}}, nil
	}
	local := QuotaRequest{Profile: "cmd-dsflash41", Effort: "high", Role: "worker"}
	x := a.nativeGate(context.Background(), local, true)
	var receipt gateReceipt
	if err := json.Unmarshal([]byte(x.Out), &receipt); err != nil || x.Code != 0 || receipt.Source == nil || *receipt.Source != sourceOperator || !slices.Equal(receipt.ManualIDs, []string{"hk-41"}) {
		t.Fatalf("local request must admit on the manual credit window: %d %s", x.Code, x.Out)
	}
	named := local
	named.AccountKey = "commandcode:0123456789abcdef"
	if x = a.nativeGate(context.Background(), named, true); x.Code != gateUnmeasurable || !strings.Contains(x.Out, msgAccountStale) {
		t.Fatalf("a named account without a fresh automatic reading stays unmeasurable: %d %s", x.Code, x.Out)
	}
}

// TestAdmissionKeepsConfiguredQuotaBinary: wrapping the quota CLI keeps
// PANEWIRE_QUOTA_BIN for the refreshes panewire runs through pacelever.
func TestAdmissionNativeReadsStoredReadingOverHandoffkeep(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/v1/quota/accounts/" + switchKey + "/latest":
			_ = json.NewEncoder(w).Encode(shared.Latest{Observation: switchReading(20, 40)})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	// hosts.toml [quota] url overrides this endpoint.
	t.Setenv("HANDOFFKEEP_URL", "https://hk.test.invalid")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-token")
	a, runner, _ := switchApp(t, gateModeNative, nil, Result{Code: 99})
	a.SharedLatest = nil
	a.Config.Quota["url"] = srv.URL
	out, err := a.Quota.Gate(context.Background(), opusRequest)
	if err != nil || gatePool(out) != "claude" {
		t.Fatalf("native gate over handoffkeep: %v\n%s", err, out)
	}
	if !slices.Equal(paths, []string{"/v1/quota/accounts/" + switchKey + "/latest"}) {
		t.Fatalf("handoffkeep reads = %v", paths)
	}
	if len(runner.Calls) != 0 {
		t.Fatalf("native gate executed %v", runner.Calls)
	}
}

// TestCommandcodeRequires30dWindow pins the commandcode 30-day rule
// (decision/2026-11-06/commandcode-30d-required-in-panewire-gate).
func TestCommandcodeRequires30dWindow(t *testing.T) {
	in := admissionInputs{Now: switchNow, Rows: []admissionRow{{Profile: "cmd-dsflash41", Effort: "high", Pool: "commandcode", Grade: "A+", Gate: "default", Placed: true}}, AccountScoped: true}
	week := func(used float64) *float64 { return &used }
	in.Readings = map[string]admissionReading{"commandcode": {Buckets: []admissionBucket{{Window: "5h", Used: week(10), ScopeKind: "account"}, {Window: "7d", Used: week(20), ScopeKind: "account"}}}}
	d := in.admit(admissionRequest{Profile: "cmd-dsflash41", Effort: "high", Role: "worker"})
	if d.OK || d.Exit != gateUnmeasurable || d.Reason != "commandcode_required_window_missing:30d" {
		t.Fatalf("commandcode without 30d must be unmeasurable: %+v", d)
	}
	r := in.Readings["commandcode"]
	r.Buckets = append(r.Buckets, admissionBucket{Window: "30d", Used: week(30), ScopeKind: "account"})
	in.Readings["commandcode"] = r
	if d = in.admit(admissionRequest{Profile: "cmd-dsflash41", Effort: "high", Role: "worker"}); !d.OK {
		t.Fatalf("commandcode with 30d must admit: %+v", d)
	}
}

func TestAdmissionGateModeConfig(t *testing.T) {
	for _, tc := range []struct {
		value any
		ok    bool
	}{{"shadow", false}, {"native", true}, {"pacelever", false}, {true, false}} {
		if err := validateGateMode(map[string]any{"gate": tc.value}); (err == nil) != tc.ok {
			t.Errorf("quota.gate=%v: err=%v want ok=%v", tc.value, err, tc.ok)
		}
	}
	a := &App{Config: Config{Quota: map[string]any{}}}
	if a.gateMode() != gateModeNative {
		t.Fatal("an unset switch is native")
	}
}

// TestAdmissionPolicyReadsPaceleverConfig: the verdict judges with the
// operator's pacelever pool policy; an unreadable file is refused, never
// replaced by defaults.
func TestAdmissionPolicyReadsPaceleverConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte("[pools.claude]\nclass = \"preserve\"\nuntil = 2026-12-01\ncutoff = 0\n[pools.unknown]\nclass = \"x\"\n[profiles.opus]\nsubscribed = false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	getenv := func(k string) string {
		if k == "PACELEVER_CONFIG" {
			return path
		}
		return ""
	}
	p, err := loadAdmissionPolicy(getenv, dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.pool("claude", switchNow); got.Class != "preserve" || got.Cutoff != 99 {
		t.Fatalf("claude policy = %+v", got)
	}
	if p.subscribed("opus", "claude") {
		t.Fatal("profile subscription must win")
	}
	if err := os.WriteFile(path, []byte("[pools.claude]\nclass = \"hoard\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadAdmissionPolicy(getenv, dir); err == nil {
		t.Fatal("an invalid class must refuse the policy")
	}
	if p, err := loadAdmissionPolicy(func(string) string { return "" }, t.TempDir()); err != nil || len(p.Pools) != 0 {
		t.Fatalf("a missing config is the default policy: %+v %v", p, err)
	}
}
