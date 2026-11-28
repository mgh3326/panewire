package panewire

import (
	"context"
	"time"
)

// A completed job keeps its lane route only while the job's own agent holds the
// route's pane (hk 6900). Completion alone does not keep it: a worker's pane stays
// up until its PR merges, and reviews and orders still reach it. The contract is
// in docs/r21-lane-event.md.
//
// The verdict is read from the node's latest heartbeat pane snapshot, under the
// lock that decides the event. No node round trip is made on the relay path, so a
// lane.event relayed on a node's own reader cannot wait for a reply that reader
// must carry.

// jobPaneEndedLocked reports whether a lane event on route to job can never reach
// the job's pane. A completed job's pane is ended when a reap, close, lost pane or
// pane.close recorded it gone, or when the node's latest snapshot does not show
// the job's own agent on the route's pane. A live job is never ended here. The
// caller holds h.mu.
func (h *HubServer) jobPaneEndedLocked(job *hubJobRecord, route reportRelayRoute) bool {
	if job == nil || !job.Completed {
		return false
	}
	return job.PaneGone || !h.routePaneHeldLocked(route, job.AgentLabel)
}

// routePaneHeldLocked reports whether the route's node's latest fresh, complete
// snapshot lists label as the occupant of the route's pane. Absence, a truncated
// snapshot, a stale or missing snapshot, a reused pane id (another occupant), or
// the label listed only on another pane all count as not held. The caller holds
// h.mu.
func (h *HubServer) routePaneHeldLocked(route reportRelayRoute, label string) bool {
	if route.Machine == "" || route.Pane == "" || label == "" {
		return false
	}
	snap := h.freshSessionSnapshotLocked(route.Machine)
	if snap == nil {
		return false
	}
	for _, session := range snap.sessions {
		if session.PaneID == route.Pane {
			return session.Label == label || session.AgentName == label
		}
	}
	return false
}

// markJobPaneGone records that a reap of job on node closed its pane or found it
// gone. A job that is not on node is not touched.
func (h *HubServer) markJobPaneGone(node, job string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rec := h.jobs[job]; rec != nil && rec.Node == node {
		rec.PaneGone = true
	}
}

// markPaneJobsGone records that node closed pane, or found it gone, on a pane.close
// the hub issued at commandStart. A pane id can be reused: a job placed on it
// after the close began must not inherit a close that was not its own. So a
// completed job is marked only when its spawn started before commandStart, whenever
// its completion arrived. The start is the earliest across the job's spawn rows:
// each row's bind time (the pane is made then), or its created time when never
// bound. A job with no spawn record is marked as before.
func (h *HubServer) markPaneJobsGone(node, pane string, commandStart time.Time) {
	if pane == "" {
		return
	}
	h.mu.Lock()
	var candidates []string
	for id, rec := range h.jobs {
		if rec.Completed && rec.Node == node && rec.Pane == pane {
			candidates = append(candidates, id)
		}
	}
	h.mu.Unlock()
	// The spawn lookups read the store, so they run outside h.mu.
	var marks []string
	for _, id := range candidates {
		if h.jobStartedBefore(id, commandStart) {
			marks = append(marks, id)
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, id := range marks {
		if rec := h.jobs[id]; rec != nil && rec.Completed && rec.Node == node && rec.Pane == pane {
			rec.PaneGone = true
		}
	}
}

// jobStartedBefore reports whether the job's earliest spawn start is before at.
// A job with no readable spawn record is treated as before, as the base code did.
func (h *HubServer) jobStartedBefore(job string, at time.Time) bool {
	if h.inbox == nil || h.inbox.store == nil {
		return true
	}
	start, found, err := h.inbox.store.earliestSpawnStartForJob(context.Background(), job)
	if err != nil || !found {
		return true
	}
	return start.Before(at)
}
