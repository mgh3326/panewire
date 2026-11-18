package panewire

import (
	"context"

	"github.com/mgh3326/panewire/internal/fleet"
)

// In-flight commandcode occupancy (hk 5124): the bind, not the terminal
// outcome, owns the seat. commandcodePoolName is the pool whose seats draw on
// the shared omp accounts; commandcodeBusyWeeklyPct is the weekly gate from
// hk 5113 (at or above it, one seat per account). accountBusyUnknown is the
// fail-closed refusal when the occupancy lookup itself fails.
const (
	commandcodePoolName        = "commandcode"
	commandcodeBusyWeeklyPct   = 75.0
	accountBusyUnknown         = "account_busy_unknown"
	accountBusyNearWeeklyLimit = "account_busy_near_weekly_limit"
)

// refuseBusyCommandcodeSeat re-checks the seat this pass chose after taking
// the account's lock: a nearly spent account admits a second seat only when
// no other seat runs on it. A lookup failure refuses fail-closed. Nil means
// the bind may proceed.
func (t *directSpawnTransport) refuseBusyCommandcodeSeat(ctx context.Context) error {
	jobs, err := t.h.liveAccountJobs(ctx, t.task.Request.RequestID)
	if err != nil {
		return &fleet.HubError{Kind: accountBusyUnknown}
	}
	if t.snapshotWeeklyUsed == nil || *t.snapshotWeeklyUsed < commandcodeBusyWeeklyPct {
		return nil
	}
	if job := jobs[t.snapshotAccountKey]; job != "" {
		return &fleet.HubError{Kind: accountBusyNearWeeklyLimit + " account=" + t.snapshotAccountKey + " job=" + job}
	}
	return nil
}
