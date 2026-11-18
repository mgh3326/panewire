package panewire

import "context"

// sendHeartbeatDirectives answers a node's heartbeat with what the node must
// hold: the hub's feature set, once per connection (queueFeatures), and every
// job the hub has assigned to this machine that has not completed.
func (h *HubServer) sendHeartbeatDirectives(machineID string, agent *hubAgent) {
	if agent == nil {
		return
	}
	h.recoverOwnerTransfers(machineID)
	h.mu.Lock()
	assignments := make([]hubJobAssignedEvent, 0)
	for _, job := range h.jobs {
		if job.Node == machineID && !job.Completed {
			assignments = append(assignments, hubJobAssignedEvent{Type: "job.assigned", JobID: job.JobID, Epoch: job.Epoch})
		}
	}
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(h.closeCtx, nodeLoopHKTimeout)
	defer cancel()
	for _, assignment := range assignments {
		if held, found, err := h.ownerAssignment(assignment.JobID); err == nil && found && held.Node == machineID && held.Epoch == assignment.Epoch {
			h.seedOwnerAssignment(ctx, held)
		}
	}
	agent.queueFeatures()
	for _, assignment := range assignments {
		agent.queueAssignment(assignment)
	}
}
