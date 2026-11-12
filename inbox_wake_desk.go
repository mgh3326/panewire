package panewire

import "fmt"

// inboxWakeDeskText is the line the operator desk receives when a design
// lane has no live pane. The unread count and urgency still belong to lane.
func inboxWakeDeskText(lane string, unread int, mostUrgent string) string {
	return fmt.Sprintf("[inbox] %s: %d unread (most urgent: %s) | no pane: desk takes it | panewire inbox list --lane %s", lane, unread, mostUrgent, lane)
}

// inboxWakeDestination keeps the normal lane route unless the design lane is
// provably paneless. The desk is resolved independently through its own row.
func (h *HubServer) inboxWakeDestination(lane, class string, route reportRelayRoute, agent *hubAgent, routed bool, unread int, mostUrgent string) (reportRelayRoute, *hubAgent, bool, string, bool) {
	if class != "design" {
		return route, agent, routed, inboxWakeText(lane, unread, mostUrgent), false
	}
	paneless, jobRoute, jobAgent, jobRouted, useJobRoute := h.inboxWakeDesignRoute(lane, route, routed)
	if useJobRoute {
		return jobRoute, jobAgent, jobRouted, inboxWakeText(lane, unread, mostUrgent), false
	}
	if !paneless {
		return route, agent, routed, inboxWakeText(lane, unread, mostUrgent), false
	}
	deskRoute, deskAgent, deskRouted := h.resolveRelayRoute("lane.event", hubJobEventPayload{OwnerLane: deskPolicyLane})
	return deskRoute, deskAgent, deskRouted, inboxWakeDeskText(lane, unread, mostUrgent), true
}

func (h *HubServer) inboxWakeDesignRoute(lane string, route reportRelayRoute, routed bool) (paneless bool, jobRoute reportRelayRoute, jobAgent *hubAgent, jobRouted bool, useJobRoute bool) {
	routes := loadReportRelayRoutes(h.reportRelayPath)
	row, hasRow := routes[lane]
	if !hasRow {
		if route.jobRecord && routed {
			return false, reportRelayRoute{}, nil, false, false
		}
		return !routed && !h.hasLiveInboxWakeJobRecord(lane), reportRelayRoute{}, nil, false, false
	}
	if row.Sink || row.Pane == "" {
		return true, reportRelayRoute{}, nil, false, false
	}
	obs, ok := h.observeNodePanes(row.Machine)
	if !ok || !h.laneProvablyGone(lane, row, obs) {
		return false, reportRelayRoute{}, nil, false, false
	}
	jobRoute, jobAgent, jobRouted, hasLiveJob := h.inboxWakeLiveJobRoute(lane)
	if hasLiveJob {
		return false, jobRoute, jobAgent, jobRouted, true
	}
	return true, reportRelayRoute{}, nil, false, false
}

// inboxWakeLiveJobRoute matches resolveRelayRoute's live-job filter and keeps
// a proven-gone lane row from shadowing the job's own pane.
func (h *HubServer) inboxWakeLiveJobRoute(lane string) (reportRelayRoute, *hubAgent, bool, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	var match *hubJobRecord
	for _, job := range h.jobs {
		if job.Completed || job.Orphaned || h.jobRecordStaleLocked(job, now) || job.AgentLabel != lane {
			continue
		}
		if match != nil {
			return reportRelayRoute{}, nil, false, true
		}
		match = job
	}
	if match == nil {
		return reportRelayRoute{}, nil, false, false
	}
	if match.Pane == "" || h.nodes[match.Node] == nil || h.nodes[match.Node].agent == nil {
		return reportRelayRoute{}, nil, false, true
	}
	return reportRelayRoute{Machine: match.Node, Pane: match.Pane, jobRecord: true}, h.nodes[match.Node].agent, true, true
}

// hasLiveInboxWakeJobRecord includes records that cannot currently be routed.
func (h *HubServer) hasLiveInboxWakeJobRecord(lane string) bool {
	_, _, _, found := h.inboxWakeLiveJobRoute(lane)
	return found
}
