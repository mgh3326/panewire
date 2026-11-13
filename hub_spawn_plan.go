package panewire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// planOnlyTransport is the transport a dry-run plan places with. It refuses
// every spawn, so a plan that reaches dispatch fails instead of dispatching.
// A plan never measures an account: the quota judgment refuses a stale one
// before it calls Refresh, so Refresh refuses too.
type planOnlyTransport struct{}

func (planOnlyTransport) Spawn(context.Context, fleet.HubSpawnRequest) (fleet.HubSpawnReply, error) {
	return fleet.HubSpawnReply{}, errors.New("spawn plan must not dispatch")
}

func (planOnlyTransport) Refresh(context.Context, string, string) error {
	return errors.New("spawn plan must not measure pools")
}

// handleSpawnPlan answers a dry run with the hub's own placement of the request
// a submitter would send. It takes the submit route's auth, validation and
// read-only pre-placement refusals, then stops before anything is recorded: no
// operation row, no reservation, no hk write and no dispatch.
func (h *HubServer) handleSpawnPlan(w http.ResponseWriter, r *http.Request) {
	request, spawn, ok := h.decodeSpawnRequest(w, r)
	if !ok {
		return
	}
	var out bytes.Buffer
	app, err := h.newSpawnApp(&out, io.Discard)
	if err != nil || app.CheckTrust() != nil {
		writeJSON(w, 503, map[string]string{"error": "spawn_config_unavailable", "fix": "configure hub fleet policy and trust before retrying this request"})
		return
	}
	// The plan answers what a submission would do, so it gives the same
	// refusals the submit route gives before placement; the checks below read
	// only.
	if !validLaneReference(spawn.Owner) {
		writeJSON(w, 400, map[string]string{"error": "invalid_owner", "fix": "supply an existing lane name"})
		return
	}
	if !fleet.ValidAgentName(spawn.Label) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "reason": "label_invalid", "fix": "pass -l LABEL as a herdr agent name: a lowercase letter first, then lowercase letters, digits, - or _, at most 32 characters"})
		return
	}
	if err := app.SubmitHostRefusal(spawn); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_request", "reason": "no_eligible_host", "fix": spawnRefusalLine(err.Error())})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if existing, found, err := app.ExistingJob(ctx, spawn.Job); err != nil {
		writeJSON(w, 503, map[string]string{"error": "job_store_unavailable", "fix": "restore handoffkeep access and retry this same request"})
		return
	} else if found && !h.retryableClaim(ctx, spawn.Job, existing) {
		writeJSON(w, 409, map[string]string{"error": "job_already_claimed", "fix": "inspect the existing job; use a new job id for independent work"})
		return
	}
	h.bindPlacement(app, planOnlyTransport{})
	plan, err := app.PlanSubmission(r.Context(), request)
	if err != nil {
		writeJSON(w, 409, map[string]any{"error": "plan_refused", "reason": err.Error(), "rc": fleet.ErrorCode(err)})
		return
	}
	plan.Output = out.String()
	writeJSON(w, 200, plan)
}

// retryableClaim reports whether the job hk already holds is one the submit
// route accepts as a retry: a claimed record whose newest hub operation is
// retryable. Without the inbox that state is unreadable, so the plan refuses
// the job id rather than promise a placement a submission may not make.
func (h *HubServer) retryableClaim(ctx context.Context, job string, existing map[string]any) bool {
	if h.inbox == nil || existing["state"] != "claimed" {
		return false
	}
	latest, err := h.inbox.store.latestSpawnForJob(ctx, job)
	return err == nil && latest.Status == "retryable"
}
