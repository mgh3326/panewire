package fleet

// hk 2861: the client's plan request. A 200 is the hub's placement; a 404 or
// 405 means the hub predates the route; a refusal keeps the hub's reason.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	c := planClientFor(t, http.StatusOK, SpawnPlan{Host: "hub-host", Profile: "codex-terra61", Role: "worker", Job: "job-a"})
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
	hubOutput := "hub plan: the hub's own placement; nothing reserved, claimed or dispatched\ndry-run host=hub-host profile=codex-terra61 role=worker job=job-a\n"
	a.Hub = planningHub{hub, func(Submission) (SpawnPlan, error) {
		return SpawnPlan{Host: "hub-host", Profile: "codex-terra61", Role: "worker", Job: "job-a", Output: hubOutput}, nil
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
}
