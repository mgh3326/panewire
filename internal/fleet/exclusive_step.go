package fleet

import "context"

// Exclusive spawn steps. A node runs several hub spawns at once (hk 4388);
// only the steps that change state shared by every spawn on the host run one
// at a time:
//
//   - StepWorktreePrepare: the git worktree add/checkout in the work root.
//   - StepPaneCreate: herdr workspace resolution and tab (pane) creation in
//     the node's session.
//
// The node puts the lock in the spawn's context; without one (a local
// spawn) ExclusiveStep is a no-op.
const (
	StepWorktreePrepare = "worktree_prepare"
	StepPaneCreate      = "pane_create"
)

type exclusiveStepKey struct{}

// WithExclusiveSteps returns ctx carrying lock, which takes the named step's
// lock and returns its release.
func WithExclusiveSteps(ctx context.Context, lock func(step string) (release func())) context.Context {
	return context.WithValue(ctx, exclusiveStepKey{}, lock)
}

// ExclusiveStep takes the lock ctx carries for step and returns its release.
func ExclusiveStep(ctx context.Context, step string) (release func()) {
	if lock, ok := ctx.Value(exclusiveStepKey{}).(func(string) func()); ok && lock != nil {
		return lock(step)
	}
	return func() {}
}
