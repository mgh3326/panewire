package panewire

import (
	"context"
	"sync"
	"time"
)

// defaultStopDrain bounds how long Daemon.Stop waits for work already in
// flight (a spawn launch with its brief, a relay injection, a node command
// that uploads a result) before it cancels the node's loops. The unit and the
// plist in deploy/ allow more than this before they kill the process. What a
// stop cuts off at the bound is settled after the restart
// (hub_spawn_settle.go).
const defaultStopDrain = 30 * time.Second

// workDrain counts the node's operations that must not be cut in half by a
// restart. It is the one place Stop looks: every site that starts such an
// operation brackets it with begin and the returned end.
type workDrain struct {
	mu       sync.Mutex
	inflight int
	draining bool
	changed  chan struct{} // closed and replaced whenever inflight drops
	stopping chan struct{} // closed once a drain begins
}

// begin registers one operation. It reports false once a drain has started
// and refuse is set, so a new spawn is left for the hub to replay after the
// restart instead of starting a launch the stop would cut off.
func (d *workDrain) begin(refuse bool) (end func(), ok bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.draining && refuse {
		return nil, false
	}
	d.inflight++
	var once sync.Once
	return func() {
		once.Do(func() {
			d.mu.Lock()
			d.inflight--
			if d.changed != nil {
				close(d.changed)
				d.changed = nil
			}
			d.mu.Unlock()
		})
	}, true
}

// stopSignal wakes admission waiters without waiting for the active spawn.
func (d *workDrain) stopSignal() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopping == nil {
		d.stopping = make(chan struct{})
		if d.draining {
			close(d.stopping)
		}
	}
	return d.stopping
}

// admitSpawn linearizes a gate-owning waiter's admission against drain. The
// handler is already counted by begin, so a winning admission is drained.
func (d *workDrain) admitSpawn() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.draining
}

// drain stops new spawns and waits until nothing is in flight and the event
// queue has been written, or until the bound passes. It returns the number of
// operations still running.
func (d *workDrain) drain(ctx context.Context, bound time.Duration, queued func() int) int {
	d.mu.Lock()
	if !d.draining {
		d.draining = true
		if d.stopping != nil {
			close(d.stopping)
		}
	}
	d.mu.Unlock()
	timeout := time.NewTimer(bound)
	defer timeout.Stop()
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for {
		d.mu.Lock()
		running := d.inflight
		if running == 0 && (queued == nil || queued() == 0) {
			d.mu.Unlock()
			return 0
		}
		if d.changed == nil {
			d.changed = make(chan struct{})
		}
		changed := d.changed
		d.mu.Unlock()
		select {
		case <-changed:
		case <-poll.C:
		case <-timeout.C:
			d.mu.Lock()
			defer d.mu.Unlock()
			return d.inflight
		case <-ctx.Done():
			d.mu.Lock()
			defer d.mu.Unlock()
			return d.inflight
		}
	}
}

// beginWork brackets one operation a restart must not cut in half.
func (client *HubClient) beginWork(refuse bool) (func(), bool) {
	return client.work.begin(refuse)
}

// Drain is the node's graceful-stop step: it refuses new spawns, then waits
// up to the stop bound for the launches, injections and result uploads in
// flight. It returns how many were still running at the bound.
func (client *HubClient) Drain(ctx context.Context) int {
	bound := client.stopDrain
	if bound <= 0 {
		bound = defaultStopDrain
	}
	left := client.work.drain(ctx, bound, func() int {
		if client.serving.Load() == 0 {
			return 0
		}
		return len(client.events)
	})
	if left > 0 {
		client.warnMessage("stop drain bound passed with work in flight; settled after restart")
	}
	return left
}
