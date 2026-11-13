package panewire

import (
	"context"
	"sync"
	"time"
)

// A job counts against its host from the placement decision, not from the
// node's claim. The claim reaches the hub at a heartbeat after the node has
// checked out, started and landed the job, and in the 2026-11-13 oci burst
// that took longer than a minute: six jobs in a row saw oci with 0 claims and
// all went there (hk 4448, docs/placement.md "Spawn bridge").
//
// placementCounts holds the jobs decided but not yet claimed, keyed by job id.
// placementCandidates adds the ones on a host to its bridged count, so a
// reservation costs what a claim inside the bridge costs and nothing more.

// placementCount is one decided job.
type placementCount struct {
	machine string
	at      time.Time
}

// decisionGate lets one spawn decision at a time read the hub's view and
// count its job. Without it, jobs submitted together all read before any has
// counted, and pile onto the best host as in the 2026-11-13 burst. The zero
// value is ready.
type decisionGate struct {
	once sync.Once
	slot chan struct{}
}

// acquire waits for the gate, or for ctx, and returns the release.
func (g *decisionGate) acquire(ctx context.Context) (func(), error) {
	g.once.Do(func() { g.slot = make(chan struct{}, 1) })
	select {
	case g.slot <- struct{}{}:
		return func() { <-g.slot }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// countPlacement counts job against machine from now. A job counts on one
// host at a time: a later decision for the same job (the next candidate after
// a refusal) moves its count.
func (h *HubServer) countPlacement(job, machine string) {
	if job == "" || machine == "" {
		return
	}
	now := h.now().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.placementCounts == nil {
		h.placementCounts = map[string]placementCount{}
	}
	bridge := h.placementCountWindowLocked()
	for id, held := range h.placementCounts {
		if now.Sub(held.at) >= bridge {
			delete(h.placementCounts, id)
		}
	}
	h.placementCounts[job] = placementCount{machine: machine, at: now}
	h.placementCache = placementCache{}
}

// releasePlacementCount drops job's count: the request ended without a launch
// (refused, not started, cancelled) or went back to placement.
func (h *HubServer) releasePlacementCount(job string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, held := h.placementCounts[job]; held {
		delete(h.placementCounts, job)
		h.placementCache = placementCache{}
	}
}

// restorePlacementCounts counts again the bound spawns that may still hold an
// unclaimed launch (docs/placement.md, "Spawn bridge"). The counts are hub
// memory, so without this a restart blinds placement to those launches until
// their claims show (hk 4460). Each row counts from its creation, which is no
// later than its decision, so it ends no later than a live count would. A claim,
// a definitive failure or the bridge window ends it as before. Called once, from
// NewHubServer, before the hub takes any request.
func (h *HubServer) restorePlacementCounts() {
	if h.inbox == nil {
		return
	}
	bridge := h.placementCountWindowLocked()
	tasks, err := h.inbox.store.boundSpawnsSince(h.closeCtx, h.now().UTC().Add(-bridge))
	if err != nil {
		h.logger.Warn("hub could not restore placement counts", "error", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.placementCounts == nil {
		h.placementCounts = map[string]placementCount{}
	}
	for _, task := range tasks {
		if task.Node == nil || task.Node.Machine == "" {
			continue
		}
		h.placementCounts[task.Job] = placementCount{machine: task.Node.Machine, at: task.Created}
	}
}

// placementCountWindowLocked is how long a count lasts without a claim to
// confirm it: the spawn bridge, so a job whose claim is never seen cannot
// weigh on its host longer than a claim would.
func (h *HubServer) placementCountWindowLocked() time.Duration {
	if h.placementPolicy.valid() {
		return h.placementPolicy.spawnBridge()
	}
	return DefaultPlacementPolicy().spawnBridge()
}

// countedPlacementsLocked is how many decided jobs on machine the claims do
// not cover yet. A job already in the node's active set is counted there, by
// countBridgedJobs, and not twice.
func (h *HubServer) countedPlacementsLocked(machine string, record *hubNodeRecord, now time.Time, bridge time.Duration) int {
	count := 0
	for job, held := range h.placementCounts {
		if held.machine != machine || now.Sub(held.at) >= bridge {
			continue
		}
		if record != nil {
			if _, claimed := record.activeJobs[job]; claimed {
				continue
			}
		}
		count++
	}
	return count
}

// refreshFleetLoad republishes the hosts' load values now. It takes the same
// lock as the sweep's publish, so two computations never race to publish and
// an older one never lands over a newer one.
func (h *HubServer) refreshFleetLoad(ctx context.Context) {
	h.fleetLoadMu.Lock()
	defer h.fleetLoadMu.Unlock()
	now := h.now().UTC()
	h.fleetLoadAt = now
	h.publishFleetLoad(ctx, now)
}

// completeSpawn records a request's outcome and ends its placement count when
// it says no job was launched: refused, or never started. A launched job
// (succeeded) keeps its count until its claim shows. A reconciling one, and a
// bound one that ran out of time (deadline_exceeded: the store keeps its node
// binding and accepts a later node result), may have started and keep it too;
// the claim, a later definitive failure, or the bridge window ends it.
func (h *HubServer) completeSpawn(ctx context.Context, task hubSpawnTask, outcome hubSpawnOutcome) (bool, error) {
	changed, err := h.inbox.store.completeSpawn(ctx, task, outcome, h.now().UTC())
	if changed {
		switch spawnOutcomeStatus(outcome) {
		case "failed", "retryable":
			h.releasePlacementCount(task.Job)
		case "deadline_exceeded":
			// Never bound: no node was asked, so nothing launched.
			if task.Node == nil {
				h.releasePlacementCount(task.Job)
			}
		}
	}
	return changed, err
}
