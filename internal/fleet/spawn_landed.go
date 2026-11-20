package fleet

import "context"

type spawnLandedKey struct{}

// WithSpawnLanded returns a context whose spawn calls fn once the job's
// durable landing is committed: the job.spawned record is written, the point at
// which this execution holds the job claims and can no longer be cleaned up and
// returned (hk 5776). The node reports its start receipt to the hub from it.
func WithSpawnLanded(ctx context.Context, fn func()) context.Context {
	return context.WithValue(ctx, spawnLandedKey{}, fn)
}

// notifySpawnLanded runs the observer WithSpawnLanded set, if any.
func notifySpawnLanded(ctx context.Context) {
	if fn, ok := ctx.Value(spawnLandedKey{}).(func()); ok && fn != nil {
		fn()
	}
}

// NotifySpawnLanded is notifySpawnLanded for an engine that stands in for the
// spawn (a node test double).
func NotifySpawnLanded(ctx context.Context) { notifySpawnLanded(ctx) }
