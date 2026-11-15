package fleet

import (
	"context"

	"github.com/mgh3326/panewire/internal/arbiter"
)

// ReconcileLedger runs the arbiter's identity-checked cleanup against an
// authoritative snapshot of this node's Herdr agents.
func (a *App) ReconcileLedger(ctx context.Context, agents arbiter.AgentSource) (arbiter.GCResult, error) {
	return a.ledger().GC(ctx, agents)
}

// ReconcileLedgerForJobs passes the leased and quota-held local jobs to the
// source so it can read each job's recorded Herdr session.
func (a *App) ReconcileLedgerForJobs(ctx context.Context, agents arbiter.AgentSourceForJobs) (arbiter.GCResult, error) {
	return a.ledger().GCWithJobs(ctx, agents)
}
