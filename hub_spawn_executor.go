package panewire

import (
	"cmp"
	"context"
	"slices"
	"sync"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The hub runs an accepted spawn in two stages (hk 4388, design
// design/2026-11-13/spawn-per-node):
//
//	pending row ──► placement pool (spawn_placement_workers, 2)
//	                 picks host+account, binds the row (node_id), never waits on a node
//	            ──► node executor, one per node (spawn_in_flight, 3; per host spawn_in_flight_hosts)
//	                 queue ordered by task priority, then age; sends job.spawn, waits for the result
//	host refusal / not started ──► unbind + exclusion ──► placement again at once
//	node disconnect ──► queued rows not yet sent are unbound ──► placement again
//
// The durable record stays spawn_operations: a bound row is "dispatching"
// with its node binding, so a hub restart finds it and queues it on its node
// again (a replay of the same node request id).

const (
	defaultSpawnPlacementWorkers = 2
	defaultSpawnInFlight         = 3
	maxSpawnConcurrency          = 32
)

// spawnQueueItem is one bound request on a node's queue. fresh means this
// process bound it and has not sent it: the node cannot have it, so a node
// disconnect may return it to placement. A row read back from the store may
// have been sent before a restart and is only ever replayed on its node.
type spawnQueueItem struct {
	task  hubSpawnTask
	fresh bool
}

type nodeSpawnQueue struct {
	queued   []spawnQueueItem
	inFlight int
}

// hubSpawnQueue is the in-memory side of the spawn stages. active holds every
// request a placement worker or a node executor owns, so the store scan never
// hands one out twice.
type hubSpawnQueue struct {
	mu     sync.Mutex
	active map[string]bool
	nodes  map[string]*nodeSpawnQueue
	place  chan hubSpawnTask
}

// claim marks id active and reports whether it was free.
func (q *hubSpawnQueue) claim(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.active == nil {
		q.active = map[string]bool{}
	}
	if q.active[id] {
		return false
	}
	q.active[id] = true
	return true
}

func (q *hubSpawnQueue) release(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.active, id)
}

// depth is a node's queued plus in-flight spawns, its load in placement.
func (q *hubSpawnQueue) depth(machine string) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	if node := q.nodes[machine]; node != nil {
		return len(node.queued) + node.inFlight
	}
	return 0
}

func (q *hubSpawnQueue) nodeLocked(machine string) *nodeSpawnQueue {
	if q.nodes == nil {
		q.nodes = map[string]*nodeSpawnQueue{}
	}
	node := q.nodes[machine]
	if node == nil {
		node = &nodeSpawnQueue{}
		q.nodes[machine] = node
	}
	return node
}

// spawnQueueBefore orders a queue: task priority first (lower rank), then age.
func spawnQueueBefore(a, b hubSpawnTask) int {
	return cmp.Or(cmp.Compare(a.Rank, b.Rank), a.Created.Compare(b.Created), cmp.Compare(a.Request.RequestID, b.Request.RequestID))
}

// spawnPolicy is the current placement policy, or the default one.
func (h *HubServer) spawnPolicy() PlacementPolicy {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reloadPlacementPolicyLocked()
	if h.placementPolicy.valid() {
		return h.placementPolicy
	}
	return DefaultPlacementPolicy()
}

func (p PlacementPolicy) spawnPlacementWorkers() int {
	if p.SpawnPlacementWorkers > 0 {
		return p.SpawnPlacementWorkers
	}
	return defaultSpawnPlacementWorkers
}

func (p PlacementPolicy) spawnInFlight(machine string) int {
	if n := p.SpawnInFlightHosts[machine]; n > 0 {
		return n
	}
	if p.SpawnInFlight > 0 {
		return p.SpawnInFlight
	}
	return defaultSpawnInFlight
}

// enqueueBoundSpawn puts a bound request on its node's queue and starts what
// the node has room for. The caller has claimed the request.
func (h *HubServer) enqueueBoundSpawn(ctx context.Context, task hubSpawnTask, fresh bool) {
	q := &h.spawnQueue
	machine := task.Node.Machine
	q.mu.Lock()
	node := q.nodeLocked(machine)
	at, _ := slices.BinarySearchFunc(node.queued, task, func(item spawnQueueItem, t hubSpawnTask) int { return spawnQueueBefore(item.task, t) })
	node.queued = slices.Insert(node.queued, at, spawnQueueItem{task: task, fresh: fresh})
	q.mu.Unlock()
	h.pumpNodeSpawns(ctx, machine)
}

// pumpNodeSpawns starts queued requests on machine while it has fewer than
// its spawn_in_flight in flight. The caller must be counted in spawnWorkers.
func (h *HubServer) pumpNodeSpawns(ctx context.Context, machine string) {
	limit := h.spawnPolicy().spawnInFlight(machine)
	q := &h.spawnQueue
	q.mu.Lock()
	defer q.mu.Unlock()
	node := q.nodeLocked(machine)
	for node.inFlight < limit && len(node.queued) > 0 && ctx.Err() == nil {
		item := node.queued[0]
		node.queued = node.queued[1:]
		node.inFlight++
		h.spawnWorkers.Add(1)
		go func() {
			defer h.spawnWorkers.Done()
			h.runNodeSpawn(ctx, item)
		}()
	}
}

// runNodeSpawn sends one bound request to its node and waits for the answer.
// A host refusal or a not-started answer has unbound it with an exclusion;
// it then goes back to placement at once.
func (h *HubServer) runNodeSpawn(ctx context.Context, item spawnQueueItem) {
	q := &h.spawnQueue
	machine := item.task.Node.Machine
	replace := false
	defer func() {
		q.mu.Lock()
		q.nodeLocked(machine).inFlight--
		q.mu.Unlock()
		q.release(item.task.Request.RequestID)
		if replace {
			h.kickSpawns()
		}
		h.pumpNodeSpawns(ctx, machine)
	}()
	// A draining hub sends nothing new; the row stays bound for the next hub
	// (hub_drain.go).
	if !h.drain.enter() {
		return
	}
	defer h.drain.leave()
	if item.fresh && (!h.nodeAcceptsSpawn(machine) || !h.now().Before(item.task.Deadline)) {
		// Never sent, so the node cannot have it: place it again. Past its
		// deadline, placement settles it as a retryable queue timeout.
		_, err := h.inbox.store.unbindRefusedSpawn(ctx, item.task.Request.RequestID, item.task.Attempt, item.task.Node.RequestID, nil)
		replace = err == nil
		return
	}
	h.inbox.store.touchSpawn(ctx, item.task.Request.RequestID, h.now().UTC())
	replace = h.dispatchBoundSpawn(ctx, item.task)
}

// nodeAcceptsSpawn reports whether machine is connected with spawn_ops_v1.
func (h *HubServer) nodeAcceptsSpawn(machine string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	record := h.nodes[machine]
	return record != nil && record.agent != nil && record.state == "connected" && record.capabilities["spawn_ops_v1"]
}

func (h *HubServer) kickSpawns() {
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
}

// unbindQueuedSpawns returns the requests queued on a node that went away,
// and that this process never sent, to placement.
func (h *HubServer) unbindQueuedSpawns(machine string) {
	if h.inbox == nil {
		return
	}
	q := &h.spawnQueue
	q.mu.Lock()
	node := q.nodes[machine]
	var unsent, kept []spawnQueueItem
	if node != nil {
		for _, item := range node.queued {
			if item.fresh {
				unsent = append(unsent, item)
			} else {
				kept = append(kept, item)
			}
		}
		node.queued = kept
	}
	q.mu.Unlock()
	for _, item := range unsent {
		_, _ = h.inbox.store.unbindRefusedSpawn(h.closeCtx, item.task.Request.RequestID, item.task.Attempt, item.task.Node.RequestID, nil)
		q.release(item.task.Request.RequestID)
	}
	if len(unsent) > 0 {
		h.kickSpawns()
	}
}

// spawnPlacementView is the hub placement answer the spawn coordinator
// places with: each node's queued and in-flight spawns count as live jobs and
// cost what a bridged claim costs, so a burst spreads across nodes instead of
// queueing behind one.
func (h *HubServer) spawnPlacementView(ctx context.Context, cwd string) PlacementResult {
	result := h.placementFresh(ctx, "worker", cwd)
	claimCost := h.spawnPolicy().scoreK().claimCost
	candidates := make([]PlacementCandidate, len(result.Candidates))
	for i, candidate := range result.Candidates {
		if depth := h.spawnQueue.depth(candidate.Machine); depth > 0 {
			candidate.ActiveJobs += depth
			candidate.Score -= float64(depth) * claimCost
		}
		candidates[i] = candidate
	}
	result.Candidates = candidates
	return result
}

// spawnTaskPriority reads an hk task's priority for the queue rank; the
// spawnPriority seam replaces it in tests.
func (h *HubServer) spawnTaskPriority(ctx context.Context, task string) (int, bool) {
	if h.spawnPriority != nil {
		return h.spawnPriority(ctx, task)
	}
	return fleet.TaskPriority(ctx, task)
}

// spawnExcluded reports whether a request's persisted exclusions rule out
// machine with profile.
func spawnExcluded(task hubSpawnTask, machine, profile string) bool {
	return slices.ContainsFunc(task.Excluded, func(e spawnExclusion) bool {
		return e.Host == machine && (e.Profile == "" || e.Profile == profile)
	})
}
