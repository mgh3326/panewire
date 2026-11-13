package panewire

import "sync"

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
