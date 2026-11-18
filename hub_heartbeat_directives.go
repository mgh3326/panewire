package panewire

// sendHeartbeatDirectives answers a node's heartbeat with what the node must
// hold: the hub's feature set, once per connection (queueFeatures), and every
// job the hub has assigned to this machine that has not completed. HK work is
// scheduled separately so it cannot block the node's websocket reader.
func (h *HubServer) sendHeartbeatDirectives(machineID string, agent *hubAgent) {
	if agent == nil {
		return
	}
	agent.queueFeatures()
	h.queueHeartbeatAssignments(machineID, agent)
	h.scheduleOwnerRecovery(machineID, agent)
}

func (h *HubServer) queueHeartbeatAssignments(machineID string, agent *hubAgent) {
	h.mu.Lock()
	assignments := make([]hubJobAssignedEvent, 0)
	for _, job := range h.jobs {
		if job.Node == machineID && !job.Completed {
			assignments = append(assignments, hubJobAssignedEvent{Type: "job.assigned", JobID: job.JobID, Epoch: job.Epoch})
		}
	}
	h.mu.Unlock()
	for _, assignment := range assignments {
		h.scheduleOwnerSeed(hubOwnerAssignment{Job: assignment.JobID, Node: machineID, Epoch: assignment.Epoch})
		agent.queueAssignment(assignment)
	}
}
