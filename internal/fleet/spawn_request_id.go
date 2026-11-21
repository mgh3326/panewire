package fleet

import "context"

type spawnNodeRequestIDKey struct{}

// WithSpawnNodeRequestID returns a context whose spawn stamps id on the
// job.spawned receipt as node_request_id, binding the record to the node
// spawn request that asked for it (hk 5829). The node's restart settle
// accepts a landed record only when the id is the executing row's own; a
// record without it, or with another request's, never proves the landing.
func WithSpawnNodeRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, spawnNodeRequestIDKey{}, id)
}

// spawnNodeRequestID is the id WithSpawnNodeRequestID set, if any.
func spawnNodeRequestID(ctx context.Context) string {
	id, _ := ctx.Value(spawnNodeRequestIDKey{}).(string)
	return id
}
