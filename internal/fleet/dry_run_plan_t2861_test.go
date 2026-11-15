package fleet

// hk 2861: the client's plan request. A 200 is the hub's placement; a 404 or
// 405 means the hub predates the route; a refusal keeps the hub's reason.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func planClientFor(t *testing.T, status int, body any) hubHTTP {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/spawns/plan" {
			t.Errorf("plan request went to %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(server.Close)
	return hubHTTP{base: server.URL, token: "fixture", client: server.Client()}
}

func TestHubPlanReturnsTheHubsPlacement_t2861(t *testing.T) {
	c := planClientFor(t, http.StatusOK, SpawnPlan{Host: "hub-host", Profile: "codex-terra61", Role: "implementer", Job: "job-a"})
	plan, err := c.Plan(context.Background(), Submission{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Host != "hub-host" || plan.Profile != "codex-terra61" {
		t.Fatalf("plan chose %s/%s, want the hub's hub-host/codex-terra61", plan.Host, plan.Profile)
	}
}

func TestHubPlanFromOldHubIsUnsupported_t2861(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusMethodNotAllowed} {
		c := planClientFor(t, status, map[string]string{"error": "not_found"})
		if _, err := c.Plan(context.Background(), Submission{}); !errors.Is(err, errHubPlanUnsupported) {
			t.Fatalf("status %d: err=%v, want errHubPlanUnsupported", status, err)
		}
	}
}

// hubPlanSpawn gives a dry run the brief and label a submission needs, and
// marks it as a dry run.
func hubPlanSpawn(t *testing.T, a *App, s Spawn) Spawn {
	t.Helper()
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	brief := filepath.Join(a.Home, "plan-brief.md")
	if err := os.WriteFile(brief, []byte("brief"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Prompt, s.Label, s.T, s.Owner, s.DryRun = brief, "job-a", "T1", "explicit-lane", true
	s.OverrideReason, s.Containment = "fixture", "hk:12"
	return s
}

// planningHub is a dry run's hub transport: a fake hub whose plan answer the
// test scripts.
type planningHub struct {
	*fakeHub
	plan func(Submission) (SpawnPlan, error)
}

func (h planningHub) Plan(_ context.Context, request Submission) (SpawnPlan, error) {
	return h.plan(request)
}

// With a hub configured, --dry-run prints the hub's plan verbatim and does not
// print this machine's choice.
func TestDryRunPrintsTheHubPlan_t2861(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Hub = map[string]any{"hub_url": "https://hub.test"}
	hubOutput := "hub plan: the hub's own placement; nothing reserved, claimed or dispatched\ndry-run host=hub-host profile=codex-terra61 role=implementer job=job-a\n"
	a.Hub = planningHub{hub, func(Submission) (SpawnPlan, error) {
		return SpawnPlan{Host: "hub-host", Profile: "codex-terra61", Role: "implementer", Job: "job-a", Output: hubOutput}, nil
	}}
	s = hubPlanSpawn(t, a, s)
	if err := a.previewSpawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if out := a.Out.(interface{ String() string }).String(); out != hubOutput {
		t.Fatalf("dry run printed %q, want the hub's plan %q", out, hubOutput)
	}
	if len(hub.spawns) != 0 {
		t.Fatalf("dry run must not spawn: %v", hub.spawns)
	}
}

// A hub that predates the plan route gets a warning and this machine's plan,
// labelled as local, so a local choice is never read as the hub's.
func TestDryRunFromOldHubShowsTheLocalPlan_t2861(t *testing.T) {
	a, s, _, hub, _ := hubFixture(t, gitState{pushed: true}, "wk/feat", hubHosts())
	a.Config.Hub = map[string]any{"hub_url": "https://hub.test"}
	a.Hub = planningHub{hub, func(Submission) (SpawnPlan, error) { return SpawnPlan{}, errHubPlanUnsupported }}
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "sonnet", Effort: "high", Grade: "A", ModelID: "sonnet-m", Gate: "default", Pool: "claude"}, GateDo: gatePools(map[string]string{"sonnet": "claude"})}
	s = hubPlanSpawn(t, a, s)
	if err := a.previewSpawn(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	errText := a.Err.(interface{ String() string }).String()
	out := a.Out.(interface{ String() string }).String()
	if !strings.Contains(errText, "hub does not support spawn plans; showing the local plan") {
		t.Fatalf("old hub must warn before the local plan: %q", errText)
	}
	if !strings.Contains(out, "local plan:") || strings.Contains(out, "hub plan:") {
		t.Fatalf("old hub dry run must show only the local plan: %q", out)
	}
	if len(hub.spawns) != 0 {
		t.Fatalf("dry run must not spawn: %v", hub.spawns)
	}
	if !a.planOnly || len(hub.refreshes) != 0 {
		t.Fatalf("the local fallback must keep the plan's no-measure rule: planOnly=%v refreshes=%v", a.planOnly, hub.refreshes)
	}
}

// A plan refuses a stale account of a host it does not hold without measuring
// it: no refresh reaches the hub or an ssh target, and the plan says so.
func TestDryRunPlanDoesNotMeasureAStaleAccount_t2861(t *testing.T) {
	key := "claude:0123456789abcdef"
	a := placementApp(t, &fakePlacementSource{})
	a.Config.Hosts = map[string]map[string]any{"host-b": {"via": "hub"}}
	hub := &fakeHub{t: t, events: &[]string{}}
	a.Hub = hub
	sshMeasures := 0
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name == "ssh" && isQuotaRefresh(strings.Join(c.Args, " ")) {
			sshMeasures++
		}
		return Result{}
	}}
	a.planOnly = true
	acc := SharedAccount{Key: key, Observed: true, Fresh: false, MeasuredAt: a.Now().Add(-75 * time.Minute)}
	_, err := a.judgeHost(context.Background(), QuotaRequest{}, "claude", "host-b", false, key, acc)
	var refusal *quotaHostRefusal
	if !errors.As(err, &refusal) || refusal.reason != "would_measure" {
		t.Fatalf("plan judged the stale account: %v", err)
	}
	if len(hub.refreshes) != 0 || sshMeasures != 0 {
		t.Fatalf("a plan measured the account: hub=%v ssh=%d", hub.refreshes, sshMeasures)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "would measure host=host-b") {
		t.Fatalf("plan did not say the host would be measured: %q", out)
	}
}

// A plan's profile selection measures nothing: a stale via=hub account is
// dropped as would_measure, no refresh reaches the hub or an ssh target, and
// the refusal names the measurement the spawn would run.
func TestSelectInAPlanDoesNotMeasure_t2861(t *testing.T) {
	a, s, q, src := remoteFreeFixture(t, freeReceipt)
	const key = "devin:fedcba9876543210"
	src.accounts[key] = SharedAccount{Key: key, Observed: true}
	a.Config.Hosts["host-b"]["via"] = "hub"
	events := &[]string{}
	hub := &fakeHub{t: t, events: events}
	a.Hub = hub
	var ssh []string
	a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
		if c.Name == "ssh" {
			ssh = append(ssh, strings.Join(c.Args, " "))
		}
		return Result{}, c.Name == "ssh"
	})
	a.planOnly = true
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || sel.chosen != nil {
		t.Fatalf("a plan must not place on an unmeasured account: %v", err)
	}
	if !strings.Contains(err.Error(), "would measure") {
		t.Fatalf("the refusal must name the skipped measurement: %v", err)
	}
	if len(hub.refreshes) != 0 || len(ssh) != 0 {
		t.Fatalf("a plan measured the account: hub=%v ssh=%v", hub.refreshes, ssh)
	}
	if len(q.Calls) != 0 {
		t.Fatalf("the plan judged without fresh evidence: %+v", q.Calls)
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "would measure host=host-b pool=devin") {
		t.Fatalf("the plan did not say what it would measure: %q", out)
	}
}

// A plan's admission gate measures nothing either: a stale reading of an
// account this machine holds is refused unmeasured, with no quota refresh.
func TestPlanGateDoesNotRefreshAStaleLocalAccount_t2861(t *testing.T) {
	stale := switchReading(20, 40)
	stale.MeasuredAt = switchNow.Add(-time.Hour)
	a, runner, _ := switchApp(t, gateModeNative, stale, Result{Code: 99})
	a.Out = &bytes.Buffer{}
	a.Config.Quota["accounts"] = map[string]any{"claude": "0123456789abcdef"}
	a.planOnly = true
	_, err := a.Quota.Gate(context.Background(), opusRequest)
	if exitCode(err) != gateUnmeasurable || !strings.Contains(err.Error(), msgAccountStale) {
		t.Fatalf("a plan must refuse the stale account unmeasured: %v", err)
	}
	for _, c := range runner.Calls {
		if c.Name == "panewire" && slices.Contains(c.Args, "refresh") {
			t.Fatalf("a plan ran a quota refresh: %v", c.Args)
		}
	}
	if out := a.Out.(interface{ String() string }).String(); !strings.Contains(out, "would measure") || !strings.Contains(out, "pool=claude") {
		t.Fatalf("the plan did not say it would measure the account: %q", out)
	}
}

// A hub refusal without a reason still names the hub's error and fix.
func TestHubPlanRefusalKeepsTheHubsFix_t2861(t *testing.T) {
	c := planClientFor(t, http.StatusServiceUnavailable, map[string]string{"error": "spawn_config_unavailable", "fix": "configure hub fleet policy and trust before retrying this request"})
	_, err := c.Plan(context.Background(), Submission{})
	if err == nil || !strings.Contains(err.Error(), "spawn_config_unavailable") || !strings.Contains(err.Error(), "configure hub fleet policy") {
		t.Fatalf("refusal dropped the hub's error or fix: %v", err)
	}
}
