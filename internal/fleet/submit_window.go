package fleet

import (
	"context"
	"time"
)

// briefSubmitMinWindow is the least time a node leaves itself to confirm a
// brief after sending it: one submit-key retry period plus one wait slice, so a
// pane that starts its turn late still gets the Return retry and a
// confirmation read. The node's hub_wait budget (docs/config.md) is split into
// the steps before the brief (fetch, checkout, launch), this window and the
// briefSubmitReserve for capture and cleanup. The steps before the brief may
// not take the window (hk 5174).
const briefSubmitMinWindow = defaultBriefSubmitRetryMS*time.Millisecond + briefSubmitWaitSlice

// submitFloor is the window the node keeps for confirming a brief: the
// minimum, or the configured submit window when a host sets it lower.
func (a *App) submitFloor() time.Duration {
	timeout, _ := a.briefSubmitWindow()
	return min(briefSubmitMinWindow, timeout)
}

// submitBudgetLeft is the time the node budget still allows for the submit
// window, after the reserve. bounded is false when ctx has no deadline.
func (a *App) submitBudgetLeft(ctx context.Context) (left time.Duration, bounded bool) {
	until, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(until) - briefSubmitReserve, true
}

// requireSubmitWindow refuses, before the brief is sent, a node budget that can
// no longer hold the submit window. The refusal is exitConfig, not 76: nothing
// was submitted, so there is no unconfirmed brief to explain.
func (a *App) requireSubmitWindow(ctx context.Context) error {
	if left, bounded := a.submitBudgetLeft(ctx); bounded && left < a.submitFloor() {
		return fail(exitConfig, "SUBMIT_WINDOW_UNAVAILABLE: %ds of the node budget remain for the brief after the %ds reserve, below the %ds submit window; brief withheld, raise hub_wait for this host", int(max(left, 0)/time.Second), int(briefSubmitReserve/time.Second), int(a.submitFloor()/time.Second))
	}
	return nil
}

// reserveSubmitWindow bounds the launch steps so they cannot spend the submit
// window. The returned context ends the launch where the window (and the
// reserve) begin; the caller keeps ctx for everything after. When the budget is
// already too short it refuses before any pane exists.
func (a *App) reserveSubmitWindow(ctx context.Context) (context.Context, context.CancelFunc, error) {
	until, ok := ctx.Deadline()
	if !ok {
		return ctx, func() {}, nil
	}
	launchBy := until.Add(-briefSubmitReserve - a.submitFloor())
	if time.Until(launchBy) <= 0 {
		return nil, nil, fail(exitConfig, "SUBMIT_WINDOW_UNAVAILABLE: the node budget has %ds left, less than the %ds reserve plus the %ds submit window; nothing launched, raise hub_wait for this host", int(max(time.Until(until), 0)/time.Second), int(briefSubmitReserve/time.Second), int(a.submitFloor()/time.Second))
	}
	launch, cancel := context.WithDeadline(ctx, launchBy)
	return launch, cancel, nil
}

// launchBudgetSpent turns a launch failure into the refusal that names the
// cause when the reserved launch deadline, not the node budget, ended it.
func (a *App) launchBudgetSpent(launch, node context.Context, e error) error {
	if e != nil && launch.Err() != nil && node.Err() == nil {
		return fail(exitConfig, "SUBMIT_WINDOW_UNAVAILABLE: launch used the node budget up to the %ds submit window and the %ds reserve; no brief was sent, raise hub_wait for this host (launch error: %v)", int(a.submitFloor()/time.Second), int(briefSubmitReserve/time.Second), e)
	}
	return e
}
