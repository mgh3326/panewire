package panewire

import "context"

// A completed job keeps its lane route only while its pane is proven to be held
// by the job's own agent (hk 6900). Completion alone does not keep it: a worker's
// pane stays up until its PR merges, and reviews and orders still reach it. The
// contract is in docs/r21-lane-event.md.

// jobPaneEndedLocked reports whether a lane event on a route to job can never
// reach the job's pane. A completed job's pane is ended when a reap, close, lost
// pane or pane.close recorded it gone, or when the latest probe did not find the
// job's own agent on the route's pane. The caller holds h.mu.
func jobPaneEndedLocked(job *hubJobRecord) bool {
	return job != nil && job.Completed && (job.PaneGone || !job.paneAlive)
}

// paneHeldBy reports whether a probe found label as the occupant of pane. A
// reused pane id, or the label listed on another pane, does not count.
func paneHeldBy(probe heldProbe, pane, label string) bool {
	return probe.state == heldPanePresent && pane != "" && label != "" && probe.row.PaneID == pane && probe.row.Name == label
}

// refreshCompletedRoutePane probes the route's pane for each completed job a
// lane.event on routes would be delivered to, and records whether the job's
// agent still holds it. It runs before the caller takes h.mu, so the node round
// trip never holds the lock. The probe is synchronous because its verdict decides
// the event: a pane the job no longer holds must not receive it. The probe waits
// on the node, so a lane.event relayed on that node's own read loop waits up to
// spawnHeldPaneProbeWait and is then dropped (RISKS in the PR).
func (h *HubServer) refreshCompletedRoutePane(kind string, event hubJobEventPayload, routes map[string]reportRelayRoute) {
	if kind != "lane.event" {
		return
	}
	route, ok := routes[event.OwnerLane]
	if !ok || route.Sink {
		return
	}
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
		if !job.Completed || job.PaneGone {
			continue
		}
		// A verdict from an earlier event is never reused: until this probe answers,
		// the route is ended.
		job.paneAlive = false
		todo = append(todo, candidate{job: id, label: job.AgentLabel})
	}
	h.mu.Unlock()
	if route.Machine == "" || route.Pane == "" {
		return
	}
	for _, c := range todo {
		probe := h.probeRoutePane(route.Machine, route.Pane, c.label)
		h.mu.Lock()
		if rec := h.jobs[c.job]; rec != nil {
			rec.paneAlive = paneHeldBy(probe, route.Pane, c.label) && rec.Completed && !rec.PaneGone
		}
		h.mu.Unlock()
	}
}

// probeRoutePane asks machine's node for the pane the route names.
func (h *HubServer) probeRoutePane(machine, pane, label string) heldProbe {
	if h.routePaneProbe != nil {
		return h.routePaneProbe(context.Background(), machine, pane, label)
	}
	return h.probeNodePanes(context.Background(), machine, pane, label)
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
