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

// hubKeyedLocks serializes operations on one key without holding a hub-wide
// lock during I/O. Entries disappear after their last holder or waiter leaves.
type hubKeyedLocks struct {
	mu   sync.Mutex
	keys map[string]*hubKeyedLock
}

type hubKeyedLock struct {
	gate  chan struct{}
	users int
}

// enter registers a user of key's lock and returns the entry with the function
// that drops that registration.
func (a *hubKeyedLocks) enter(key string) (*hubKeyedLock, func()) {
	a.mu.Lock()
	if a.keys == nil {
		a.keys = make(map[string]*hubKeyedLock)
	}
	entry := a.keys[key]
	if entry == nil {
		entry = &hubKeyedLock{gate: make(chan struct{}, 1)}
		a.keys[key] = entry
	}
	entry.users++
	a.mu.Unlock()
	return entry, func() {
		a.mu.Lock()
		entry.users--
		if entry.users == 0 {
			delete(a.keys, key)
		}
		a.mu.Unlock()
	}
}

// tryLock takes key's lock only if nobody holds it, and never waits: ok is false
// while another holder has it.
func (a *hubKeyedLocks) tryLock(key string) (unlock func(), ok bool) {
	entry, releaseRef := a.enter(key)
	select {
	case entry.gate <- struct{}{}:
		return func() {
			<-entry.gate
			releaseRef()
		}, true
	default:
		releaseRef()
		return nil, false
	}
}

func (a *hubKeyedLocks) lock(ctx context.Context, key string) (func(), error) {
	entry, releaseRef := a.enter(key)
	select {
	case entry.gate <- struct{}{}:
		return func() {
			<-entry.gate
			releaseRef()
		}, nil
	case <-ctx.Done():
		releaseRef()
		return nil, ctx.Err()
	}
}
