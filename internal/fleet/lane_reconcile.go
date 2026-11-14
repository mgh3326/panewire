package fleet

import (
	"context"
	"io"
)

// ReconcileNodeLane uses the same registration and event ledger as landing.
func ReconcileNodeLane(ctx context.Context, jobsRoot, session, jobID, machine string) error {
	a, err := New(io.Discard, io.Discard)
	if err != nil {
		return err
	}
	a.Jobs, a.HerdrSession = jobsRoot, session
	return a.reconcileNodeLane(ctx, jobID, machine)
}

func (a *App) reconcileNodeLane(ctx context.Context, jobID, machine string) error {
	j, err := a.job(jobID)
	if err != nil {
		return err
	}
	if j.Terminal || j.Reaped || j.Pane == "" || j.Receipt == nil {
		return nil
	}
	if session := str(j.Receipt, "herdr_session"); session != "" {
		a.HerdrSession = session
	}
	lane, parent := j.Label, j.Owner
	// A takeover may have moved a predecessor's row, so the lane this pane
	// holds is the one its registration recorded, not necessarily its label.
	if j.HubLane != "" {
		lane = j.HubLane
	}
	if j.Role == "builder" {
		lane, parent = j.Owner, j.Parent
	}
	if boolean(j.Receipt, "lane_deferred", false) {
		return nil
	}
	registration, err := a.registerLane(ctx, lanePutRequest{Lane: lane, Parent: parent, Pane: j.Pane, Role: j.Role, Machine: machine})
	if err != nil {
		_ = a.event(ctx, jobID, "job.lane_registration_failed", map[string]any{"lane": lane, "lane_error": laneFailureReason(err)})
		return err
	}
	return a.event(ctx, jobID, "job.lane_registered", map[string]any{"hub_lane": registration.Lane, "hub_lane_machine": registration.Machine, "pane_id": j.Pane})
}
