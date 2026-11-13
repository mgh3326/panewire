package panewire

import (
	"context"
	"sync"
)

// jobGuard serializes the hub's operations that end a stopped job: a
// screen-triggered quota-stop replacement and the exhausted-pool sweep. Both
// close the old pane and write its hk record, which hk never rewrites, so the
// one that runs second must see what the first one wrote (a reviewer's
// replaced_by pointer cannot be added after a plain lost record).
type jobGuard struct {
	mu   sync.Mutex
	held map[string]chan struct{}
}

// acquire waits for job's guard and returns its release. ok is false when ctx
// ended first. The caller holds the guard through the terminal record write
// and the successor decision.
func (g *jobGuard) acquire(ctx context.Context, job string) (release func(), ok bool) {
	for {
		g.mu.Lock()
		wait, busy := g.held[job]
		if !busy {
			done := make(chan struct{})
			if g.held == nil {
				g.held = map[string]chan struct{}{}
			}
			g.held[job] = done
			g.mu.Unlock()
			return func() {
				g.mu.Lock()
				delete(g.held, job)
				g.mu.Unlock()
				close(done)
			}, true
		}
		g.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, false
		}
	}
}
