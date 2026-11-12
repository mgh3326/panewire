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
	if class != "design" || !h.inboxWakeDesignPaneless(lane, route, routed) {
		return route, agent, routed, inboxWakeText(lane, unread, mostUrgent), false
	}
	deskRoute, deskAgent, deskRouted := h.resolveRelayRoute("lane.event", hubJobEventPayload{OwnerLane: deskPolicyLane})
	return deskRoute, deskAgent, deskRouted, inboxWakeDeskText(lane, unread, mostUrgent), true
}

func (h *HubServer) inboxWakeDesignPaneless(lane string, route reportRelayRoute, routed bool) bool {
	routes := loadReportRelayRoutes(h.reportRelayPath)
	row, hasRow := routes[lane]
	if !hasRow {
		return !route.jobRecord && !routed
	}
	if row.Sink || row.Pane == "" {
		return true
	}
	obs, ok := h.observeNodePanes(row.Machine)
	return ok && h.laneProvablyGone(lane, row, obs)
}
