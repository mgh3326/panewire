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
