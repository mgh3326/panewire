package panewire

import (
	"context"
	"time"
)

// The checkout stage reports three sub-stage timing keys in stage_ms beside its
// total (hk 6075). lock_wait is the wait for the in-process spawn lock plus the
// per-clone git-op lock; fetch and worktree_add are the git fetch and the
// worktree add a prepare runs. The steps are disjoint, so their sum is at most
// the checkout total. A key is present only when its step ran.
const (
	checkoutStageLockWait    = "lock_wait"
	checkoutStageFetch       = "fetch"
	checkoutStageWorktreeAdd = "worktree_add"
)

// checkoutStageKeys are the sub-stage keys a node sends only to a hub that
// lists hubFeatureSpawnStageCheckout (spawnCheckoutStagesForHub).
var checkoutStageKeys = []string{checkoutStageLockWait, checkoutStageFetch, checkoutStageWorktreeAdd}

// checkoutStageTimer accumulates the sub-stage milliseconds of one checkout.
// It travels in the context so the git runner and the add path can record into
// it without a new parameter on every prepare function.
type checkoutStageTimer struct {
	ms map[string]int64
}

type checkoutStageTimerKey struct{}

// withCheckoutStageTimer returns a context that carries a fresh timer.
func withCheckoutStageTimer(ctx context.Context) (context.Context, *checkoutStageTimer) {
	timer := &checkoutStageTimer{ms: map[string]int64{}}
	return context.WithValue(ctx, checkoutStageTimerKey{}, timer), timer
}

// addCheckoutStage adds one step's elapsed time to key. A context without a
// timer records nothing, so a worktree.prepare run outside a spawn is unchanged.
func addCheckoutStage(ctx context.Context, key string, elapsed time.Duration) {
	if timer, ok := ctx.Value(checkoutStageTimerKey{}).(*checkoutStageTimer); ok {
		timer.ms[key] += elapsed.Milliseconds()
	}
}
