package panewire

import (
	"context"
	"sync"
)

// hubSpawnActions excludes an operator abandon while this hub is dispatching
// or reconciling the same request, and excludes dispatch while abandon probes
// the node. The durable row, not this process-local fence, is the proof that a
// request was sent. There is one hub per database (docs/hub-postgres.md).
type hubSpawnActions struct {
	mu     sync.Mutex
	active map[string]bool
}

func (a *hubSpawnActions) begin(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active[id] {
		return false
	}
	if a.active == nil {
		a.active = make(map[string]bool)
	}
	a.active[id] = true
	return true
}

func (a *hubSpawnActions) end(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.active, id)
}

// hubSpawnAdmissions serializes a fresh send with accepting-override changes
// for that node without holding the hub-wide lock during a store operation.
// Entries disappear after their last holder or waiter leaves.
type hubSpawnAdmissions struct {
	mu    sync.Mutex
	nodes map[string]*hubSpawnAdmission
}

type hubSpawnAdmission struct {
	gate  chan struct{}
	users int
}

func (a *hubSpawnAdmissions) lock(ctx context.Context, machine string) (func(), error) {
	a.mu.Lock()
	if a.nodes == nil {
		a.nodes = make(map[string]*hubSpawnAdmission)
	}
	node := a.nodes[machine]
	if node == nil {
		node = &hubSpawnAdmission{gate: make(chan struct{}, 1)}
		a.nodes[machine] = node
	}
	node.users++
	a.mu.Unlock()
	releaseRef := func() {
		a.mu.Lock()
		node.users--
		if node.users == 0 {
			delete(a.nodes, machine)
		}
		a.mu.Unlock()
	}
	select {
	case node.gate <- struct{}{}:
		return func() {
			<-node.gate
			releaseRef()
		}, nil
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	}
}
