package panewire

import (
	"context"
	"time"
)

// A completed job keeps its pane until something ends the pane: a reap or
// close, a pane.close the hub issued, a lost pane, or a pane the node no
// longer lists. Completion alone does not end it. A worker's pane stays up
// until its PR merges, and reviews, findings and design orders still reach it
// (hk 6900). The contract is in docs/r21-lane-event.md.

// completedPaneProbeTTL bounds how often the relay path asks a node whether a
// completed job's pane is still listed. A probe that finds the pane present, or
// cannot be answered, is reused for this long.
const completedPaneProbeTTL = time.Minute

// jobPaneEndedLocked reports whether a completed job's pane is proven gone, so a
// lane event on its route can never reach a pane. The caller holds h.mu.
func jobPaneEndedLocked(job *hubJobRecord) bool {
	return job != nil && job.Completed && job.PaneGone
}

// laneRegistered reports whether the lanes file has a row for lane. A row whose
// node has no live agent resolves to no route, but its pane may still live, so
// the route-less drop (laneJobEnded) does not apply to it.
func (h *HubServer) laneRegistered(lane string) bool {
	_, ok := loadReportRelayRoutes(h.reportRelayPath)[lane]
	return ok
}

// refreshCompletedRoutePane starts a pane probe for each completed job that a
// lane.event on routes would be delivered to, while the job's pane is not yet
// proven gone. The probe runs off the caller's goroutine: a node round trip can
// wait seconds, and the caller may be the node's own read loop, which carries
// the probe's reply. The caller must not hold h.mu. The event is decided on the
// state already recorded; the next event after a probe finds the pane gone
// gets job_ended.
func (h *HubServer) refreshCompletedRoutePane(kind string, event hubJobEventPayload, routes map[string]reportRelayRoute) {
	if kind != "lane.event" {
		return
	}
	route, ok := routes[event.OwnerLane]
	if !ok || route.Sink || route.Machine == "" || route.Pane == "" {
		return
	}
	now := h.now()
	type candidate struct{ job, label string }
	var todo []candidate
	h.mu.Lock()
	for id, job := range h.jobs {
		if route.Job != "" && id != route.Job {
			continue
		}
		if route.Job == "" && (job.AgentLabel != event.OwnerLane || job.Node != route.Machine || job.Pane != route.Pane) {
			continue
		}
		if !job.Completed || job.PaneGone || job.paneProbing {
			continue
		}
		if !job.PaneProbedAt.IsZero() && now.Sub(job.PaneProbedAt) < completedPaneProbeTTL {
			continue
		}
		job.paneProbing = true
		todo = append(todo, candidate{job: id, label: job.AgentLabel})
	}
	h.mu.Unlock()
	for _, c := range todo {
		go h.probeCompletedRoutePane(c.job, route.Machine, route.Pane, c.label)
	}
}

// probeCompletedRoutePane asks machine's node whether pane (or label) is still
// listed and records the answer on the job. Only an answer that the pane is
// absent proves it gone. Anything else keeps the route, and the probe is retried
// after completedPaneProbeTTL.
func (h *HubServer) probeCompletedRoutePane(job, machine, pane, label string) {
	var probe heldProbe
	if h.routePaneProbe != nil {
		probe = h.routePaneProbe(context.Background(), machine, pane, label)
	} else {
		probe = h.probeNodePanes(context.Background(), machine, pane, label)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	rec := h.jobs[job]
	if rec == nil {
		return
	}
	rec.paneProbing = false
	rec.PaneProbedAt = h.now()
	if probe.state == heldPaneAbsent && rec.Completed {
		rec.PaneGone = true
	}
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
// the hub issued. Only completed jobs are marked. A pane id can be reused by a
// live job that is recorded later on the same node, and its record must not
// inherit a close that was not its own.
func (h *HubServer) markPaneJobsGone(node, pane string) {
	if pane == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, rec := range h.jobs {
		if rec.Completed && rec.Node == node && rec.Pane == pane {
			rec.PaneGone = true
		}
	}
}
