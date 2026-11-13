package panewire

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/mgh3326/panewire/internal/fleet"
)

// planOnlyTransport is the transport a dry-run plan places with. It refuses
// every spawn, so a plan that reaches dispatch fails instead of dispatching.
type planOnlyTransport struct{}

func (planOnlyTransport) Spawn(context.Context, fleet.HubSpawnRequest) (fleet.HubSpawnReply, error) {
	return fleet.HubSpawnReply{}, errors.New("spawn plan must not dispatch")
}

func (planOnlyTransport) Refresh(context.Context, string, string) error {
	return errors.New("spawn plan must not measure pools")
}

// handleSpawnPlan answers a dry run with the hub's own placement of the request
// a submitter would send. It takes the submit route's auth and validation, then
// stops before anything is recorded: no operation row, no reservation, no hk
// write and no dispatch.
func (h *HubServer) handleSpawnPlan(w http.ResponseWriter, r *http.Request) {
	request, _, ok := h.decodeSpawnRequest(w, r)
	if !ok {
		return
	}
	var out bytes.Buffer
	app, err := h.newSpawnApp(&out, io.Discard)
	if err != nil || app.CheckTrust() != nil {
		writeJSON(w, 503, map[string]string{"error": "spawn_config_unavailable", "fix": "configure hub fleet policy and trust before retrying this request"})
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
