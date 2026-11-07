package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

// switchCatalog is the bench catalog JSON pacelever lists.
const switchCatalog = `{"catalog":[
 {"profile":"opus","effort":"high","model_id":"claude-opus-5-5","pool":"claude","grade":"S+","gate":"default"},
 {"profile":"codex-sol","effort":"max","model_id":"gpt-6.1-sol","pool":"codex","grade":"S+","gate":"default"},
 {"profile":"cmd-dsflash41","effort":"high","model_id":"deepseek/deepseek-v4.1-flash","pool":"commandcode","grade":"A+","gate":"default"}
],"source":{"stale":false}}`

// switchApp is an App whose quota CLI is a fake pacelever: it lists the
// catalog and answers `gate` with paceleverGate. The shared store holds
// reading for switchKey.
func switchApp(t *testing.T, mode string, reading *shared.Observation, paceleverGate Result) (*App, *fakeRunner, *[]fleetevent.Event) {
	t.Helper()
	t.Setenv("PACELEVER_CONFIG", filepath.Join(t.TempDir(), "absent.toml"))
	runner := &fakeRunner{Do: func(c Command) Result {
		switch {
		case c.Name == "pacelever" && slices.Equal(c.Args, []string{"bench", "catalog", "list", "--json"}):
			return Result{Out: switchCatalog}
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
	a.Quota = admissionQuota{CLIQuota{runner, "pacelever"}, a}
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

// TestAdmissionShadowLogsDisagreement: in shadow mode pacelever's verdict
// decides the spawn, and a native verdict that differs is logged with the
// profile, pool and both verdicts and reasons.
func TestAdmissionShadowLogsDisagreement(t *testing.T) {
	a, runner, events := switchApp(t, "", switchReading(99.5, 40), paceleverReceipt(true, 0, "opus pool=claude used=99.5% class=spend"))
	out, err := a.Quota.Gate(context.Background(), opusRequest)
	if err != nil || gatePool(out) != "claude" {
		t.Fatalf("shadow must return pacelever's admit: %q %v", out, err)
	}
	if gateRuns(runner) != 1 {
		t.Fatalf("shadow runs pacelever's gate once, got %d", gateRuns(runner))
	}
	if len(*events) != 1 {
		t.Fatalf("want one shadow event, got %+v", *events)
	}
	e := (*events)[0]
	if e.Kind != "quota_gate_shadow" || e.Outcome != "disagree" || e.Reason != "claude" {
		t.Fatalf("shadow event = %+v", e)
	}
	for _, want := range []string{"profile=opus", "pacelever{exit=0 pool=claude", "native{exit=3 pool=claude", `reason="quota exhausted window=5h cutoff=99"`, `reason="opus pool=claude used=99.5% class=spend"`} {
		if !strings.Contains(e.Detail, want) {
			t.Errorf("shadow detail lacks %q: %s", want, e.Detail)
		}
	}
}

func TestAdmissionShadowRecordsAgreement(t *testing.T) {
	native := "opus pool=claude used=40% class=spend [rung opus@high]\npace-warning pool=claude window=7d multiple=1.40"
	a, _, events := switchApp(t, gateModeShadow, switchReading(20, 40), paceleverReceipt(true, 0, native))
	if _, err := a.Quota.Gate(context.Background(), opusRequest); err != nil {
		t.Fatal(err)
	}
	if len(*events) != 1 || (*events)[0].Outcome != "agree" {
		t.Fatalf("shadow agreement event = %+v", *events)
	}
}

// TestAdmissionShadowNeverMeasures: the shadow verdict reads only; a stale
// reading of an account this host holds is not refreshed beside pacelever.
func TestAdmissionShadowNeverMeasures(t *testing.T) {
	stale := switchReading(20, 40)
	stale.MeasuredAt = switchNow.Add(-time.Hour)
	a, runner, events := switchApp(t, gateModeShadow, stale, paceleverReceipt(true, 0, "opus pool=claude used=40% class=spend"))
	a.Config.Quota["accounts"] = map[string]any{"claude": "0123456789abcdef"}
	if _, err := a.Quota.Gate(context.Background(), opusRequest); err != nil {
		t.Fatal(err)
	}
	for _, c := range runner.Calls {
		if slices.Contains(c.Args, "refresh") {
			t.Fatalf("shadow verdict measured: %v", c)
		}
	}
	if len(*events) != 1 || (*events)[0].Outcome != "disagree" || !strings.Contains((*events)[0].Detail, msgAccountStale) {
		t.Fatalf("stale shadow reading must show as a disagreement: %+v", *events)
	}
}

// TestAdmissionNativeNeverRunsPaceleverGate: native mode decides from the
// stored reading and executes `pacelever gate` zero times; the catalog read
// (hk 3249's to move) is the only pacelever call left.
func TestAdmissionNativeNeverRunsPaceleverGate(t *testing.T) {
	a, runner, events := switchApp(t, gateModeNative, switchReading(20, 40), Result{Code: 99})
	j, err := a.judgeGate(context.Background(), opusRequest, "claude", quotaJudgment{Host: "local", Source: "shared", AccountKey: switchKey})
	if err != nil || j.Outcome != "admitted" {
		t.Fatalf("native admit: %+v %v", j, err)
	}
	if n := gateRuns(runner); n != 0 {
		t.Fatalf("native mode ran pacelever gate %d times", n)
	}
	for _, c := range runner.Calls {
		if !slices.Equal(c.Args, []string{"bench", "catalog", "list", "--json"}) {
			t.Fatalf("native mode ran %s %v", c.Name, c.Args)
		}
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
	_, err = a.Quota.Gate(context.Background(), QuotaRequest{Profile: "cmd-dsflash41", Effort: "high", Role: "reviewer"})
	if exitCode(err) != exitSeatRefused {
		t.Fatalf("cmd-dsflash41 reviewer must be role-denied (gate exit 5 → %d): %v", exitSeatRefused, err)
	}
	if gateRuns(runner) != 0 {
		t.Fatal("native refusals ran pacelever gate")
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
	x := a.nativeGate(context.Background(), a.Quota.(admissionQuota).CLIQuota, opusRequest, true)
	var receipt gateReceipt
	if err := json.Unmarshal([]byte(x.Out), &receipt); err != nil || x.Code != quotaGateBlocked {
		t.Fatalf("opus must be refused: %d %s", x.Code, x.Out)
	}
	if !slices.Equal(receipt.Alternatives, []string{"codex-sol --effort max"}) {
		t.Fatalf("alternatives = %v, want the codex rung the held codex account admits", receipt.Alternatives)
	}
	if gateRuns(runner) != 0 || slices.ContainsFunc(runner.Calls, func(c Command) bool { return slices.Contains(c.Args, "refresh") }) {
		t.Fatalf("alternatives must not run pacelever gate or measure: %v", runner.Calls)
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
	if !refreshed || reads != 2 {
		t.Fatalf("want one native refresh and a second read: reads=%d calls=%v", reads, runner.Calls)
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
	a.Config.Quota["accounts"] = map[string]any{"claude": "0123456789abcdef"}
	out, err := a.Quota.Gate(context.Background(), local)
	if err != nil || !strings.Contains(out, `"reading_account_key":"`+switchKey+`"`) {
		t.Fatalf("local request must judge the held account: %v\n%s", err, out)
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
	}{{"shadow", true}, {"native", true}, {"pacelever", false}, {true, false}} {
		if err := validateGateMode(map[string]any{"gate": tc.value}); (err == nil) != tc.ok {
			t.Errorf("quota.gate=%v: err=%v want ok=%v", tc.value, err, tc.ok)
		}
	}
	a := &App{Config: Config{Quota: map[string]any{}}}
	if a.gateMode() != gateModeShadow {
		t.Fatal("an unset switch is shadow")
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
