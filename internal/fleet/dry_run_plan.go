package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// A dry run prints one receipt per placement, labelled with the source that
// planned it. A local plan is this machine's choice; the hub plans with its own
// state and may choose another host or profile. See docs/hub-spawn-operations.md.
const (
	localPlanLabel = "local plan: this machine's placement; the hub may choose another host or profile"
	hubPlanLabel   = "hub plan: the hub's own placement; nothing reserved, claimed or dispatched"
	hubPlanTimeout = 15 * time.Second
)

// SpawnPlan is one dry-run placement: the host, profile, role and job it chose,
// and the receipt text it printed.
type SpawnPlan struct {
	Host    string `json:"host"`
	Profile string `json:"profile"`
	Role    string `json:"role"`
	Job     string `json:"job"`
	Output  string `json:"output"`
}

// errHubPlanUnsupported means the hub predates POST /v1/spawns/plan. The CLI
// then shows the local plan instead of failing the dry run.
var errHubPlanUnsupported = errors.New("hub does not support spawn plans")

// SpawnPlanner is implemented by hub transports that can answer a dry run with
// the hub's own placement.
type SpawnPlanner interface {
	Plan(ctx context.Context, request Submission) (SpawnPlan, error)
}

// dryRunLine prints one placement's dry-run receipt, labelled with the source
// that planned it, and records the plan for PlanSubmission.
func (a *App) dryRunLine(host string, s Spawn, payload string) {
	label := localPlanLabel
	if a.hubPlan {
		label = hubPlanLabel
	}
	fmt.Fprintf(a.Out, "%s\ndry-run host=%s profile=%s role=%s job=%s\n%s\n", label, host, s.Profile, s.Role, s.Job, payload)
	a.lastPlan = &SpawnPlan{Host: host, Profile: s.Profile, Role: s.Role, Job: s.Job}
}

// PlanSubmission runs the placement the hub would run for request, as a dry
// run. It reserves no host or slot, writes no hk record and dispatches to no
// node: the transport this App holds for the plan refuses any spawn.
func (a *App) PlanSubmission(ctx context.Context, request Submission) (SpawnPlan, error) {
	s, cleanup, err := a.hubSpawn(request)
	if err != nil {
		return SpawnPlan{}, err
	}
	defer cleanup()
	s.DryRun = true
	a.hubPlan = true
	a.lastPlan = nil
	if err := a.Spawn(ctx, s); err != nil {
		return SpawnPlan{}, err
	}
	if a.lastPlan == nil {
		return SpawnPlan{}, fail(exitPlacement, "hub plan chose no host")
	}
	return *a.lastPlan, nil
}

// previewSpawn answers `--dry-run` for the CLI. With a hub configured, the hub
// plans the spawn with its own placement and state. Without one, or against a
// hub that predates plan requests, this machine plans and its receipt says so.
func (a *App) previewSpawn(ctx context.Context, s Spawn) error {
	if str(a.Config.Hub, "hub_url") == "" {
		return a.Spawn(ctx, s)
	}
	defer a.reportHubRetries()
	request, err := a.PrepareSubmission(ctx, s)
	if err != nil {
		return err
	}
	hub, err := a.hub()
	if err != nil {
		return err
	}
	planner, ok := hub.(SpawnPlanner)
	if !ok {
		return fail(exitConfig, "hub transport cannot plan spawns")
	}
	plan, err := planner.Plan(ctx, request)
	if errors.Is(err, errHubPlanUnsupported) {
		a.warn("hub does not support spawn plans; showing the local plan")
		return a.Spawn(ctx, s)
	}
	if err != nil {
		return err
	}
	fmt.Fprint(a.Out, plan.Output)
	return nil
}

// Plan asks the hub for its placement of request as a dry run. A hub that
// predates the route answers 404 or 405, which becomes errHubPlanUnsupported.
func (c *hubHTTP) Plan(ctx context.Context, request Submission) (SpawnPlan, error) {
	status, raw, err := c.do(ctx, http.MethodPost, "/v1/spawns/plan", request, hubPlanTimeout)
	if err != nil {
		return SpawnPlan{}, err
	}
	switch status {
	case http.StatusOK:
		var plan SpawnPlan
		if err := json.Unmarshal(raw, &plan); err != nil || plan.Host == "" {
			return SpawnPlan{}, fail(exitConfig, "hub plan reply unreadable")
		}
		return plan, nil
	case http.StatusNotFound, http.StatusMethodNotAllowed:
		return SpawnPlan{}, errHubPlanUnsupported
	}
	var refusal struct {
		Reason string `json:"reason"`
		RC     int    `json:"rc"`
	}
	_ = json.Unmarshal(raw, &refusal)
	code := refusal.RC
	if code == 0 {
		code = exitConfig
	}
	return SpawnPlan{}, fail(code, "hub plan refused (HTTP %d): %s", status, refusal.Reason)
}
