package panewire

import (
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	hubJobIDPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	hubAgentLabelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	hubPushSHAPattern    = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)
)

func validHubActiveJob(job HubActiveJob) bool {
	return hubJobIDPattern.MatchString(job.JobID) && hubAgentLabelPattern.MatchString(job.AgentLabel) && job.Epoch > 0 && (job.PushSHA == "" || hubPushSHAPattern.MatchString(job.PushSHA))
}

func decodeHubJobCompletionPayload(payload []byte) (hubJobEventPayload, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || len(fields) != 2 {
		return hubJobEventPayload{}, false
	}
	for name := range fields {
		if name != "job_id" && name != "epoch" {
			return hubJobEventPayload{}, false
		}
	}
	var completion hubJobEventPayload
	if json.Unmarshal(fields["job_id"], &completion.JobID) != nil || json.Unmarshal(fields["epoch"], &completion.Epoch) != nil || !hubJobIDPattern.MatchString(completion.JobID) || completion.Epoch == 0 {
		return hubJobEventPayload{}, false
	}
	return completion, true
}

func (h *HubServer) observeActiveJobs(machineID string, active []HubActiveJob, received time.Time) {
	seen := make(map[string]struct{}, len(active))
	revocations := make([]hubJobRevokedEvent, 0)
	h.mu.Lock()
	record := h.nodes[machineID]
	if record == nil {
		h.mu.Unlock()
		return
	}
	record.activeJobs = make(map[string]HubActiveJob, len(active))
	for _, job := range active {
		seen[job.JobID] = struct{}{}
		record.activeJobs[job.JobID] = job
		current := h.jobs[job.JobID]
		if current != nil && current.Epoch > job.Epoch && current.ReassignedFrom == machineID {
			if current.RevokedEpoch < current.Epoch {
				current.RevokedEpoch = current.Epoch
				revocations = append(revocations, hubJobRevokedEvent{Type: "job.revoked", JobID: job.JobID, Epoch: current.Epoch})
				h.queueJobEventLocked("job.revoked", hubJobEventPayload{JobID: job.JobID, Node: machineID, Epoch: current.Epoch, LastSeen: received})
			}
			continue
		}
		if current == nil || current.Completed || job.Epoch >= current.Epoch {
			h.jobs[job.JobID] = &hubJobRecord{HubActiveJob: job, Node: machineID, LastSeen: received}
		}
	}
	// A job absent after the node has reconnected is a local terminal-file
	// observation. Inspect the durable-in-process job view rather than just the
	// replaced websocket record, because connect deliberately starts a fresh
	// presence record. It is never a remote command.
	for id, current := range h.jobs {
		if _, stillActive := seen[id]; stillActive {
			continue
		}
		if current.Node == machineID && current.Orphaned && current.ReassignedFrom == "" {
			current.Orphaned, current.Completed = false, true
			h.queueJobEventLocked("job.recovered", hubJobEventPayload{JobID: id, Node: machineID, Epoch: current.Epoch, LastSeen: received})
		}
	}
	h.mu.Unlock()
	for _, revoked := range revocations {
		h.queueRevocation(machineID, revoked)
	}
}

func (h *HubServer) queueRevocation(machineID string, event hubJobRevokedEvent) {
	h.mu.Lock()
	record := h.nodes[machineID]
	var agent *hubAgent
	if record != nil {
		agent = record.agent
	}
	h.mu.Unlock()
	if agent != nil {
		agent.queueRevocation(event)
	}
}

func (agent *hubAgent) queueRevocation(event hubJobRevokedEvent) {
	if agent == nil || agent.revocations == nil {
		return
	}
	select {
	case agent.revocations <- event:
	default:
	}
}

func (h *HubServer) observeJobCompletion(machineID string, completion hubJobEventPayload, received time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	job := h.jobs[completion.JobID]
	if job == nil || job.Epoch != completion.Epoch || job.Node != machineID {
		return false // epoch fencing: stale completion is deliberately invisible.
	}
	wasOrphaned := job.Orphaned
	job.Completed, job.Orphaned, job.LastSeen = true, false, received
	if wasOrphaned {
		h.queueJobEventLocked("job.recovered", hubJobEventPayload{JobID: job.JobID, Node: machineID, Epoch: job.Epoch, LastSeen: received})
	}
	return true
}

func (h *HubServer) sweepOrphanedJobs(now time.Time) {
	h.mu.Lock()
	for _, node := range h.nodes {
		if node.state == "connected" || node.stateSince.IsZero() || now.Sub(node.stateSince) < h.orphanGrace {
			continue
		}
		for id, active := range node.activeJobs {
			job := h.jobs[id]
			if job == nil || job.Completed || job.Orphaned || job.Node != node.machineID || job.Epoch != active.Epoch {
				continue
			}
			job.Orphaned = true
			h.queueJobEventLocked("job.orphaned", hubJobEventPayload{JobID: job.JobID, Node: node.machineID, Epoch: job.Epoch, LastSeen: job.LastSeen, ResumeHint: "local events retained"})
		}
	}
	h.mu.Unlock()
}

func (h *HubServer) orphanedJobs() []hubJobEventPayload {
	h.mu.Lock()
	defer h.mu.Unlock()
	jobs := make([]hubJobEventPayload, 0)
	for _, job := range h.jobs {
		if job.Orphaned && !job.Completed {
			jobs = append(jobs, hubJobEventPayload{JobID: job.JobID, Node: job.Node, Epoch: job.Epoch, LastSeen: job.LastSeen, ResumeHint: "local events retained"})
		}
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].JobID < jobs[j].JobID })
	return jobs
}

func (h *HubServer) reassignJob(jobID, to string) (hubJobEventPayload, bool) {
	if !hubJobIDPattern.MatchString(jobID) || !machineIDPattern.MatchString(to) || to == hubOperatorMachineID {
		return hubJobEventPayload{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	job := h.jobs[jobID]
	if job == nil || !job.Orphaned || job.Completed || job.Node == to {
		return hubJobEventPayload{}, false
	}
	if _, knownNode := h.tokens[to]; !knownNode {
		return hubJobEventPayload{}, false
	}
	from := job.Node
	job.Epoch++
	job.ReassignedFrom, job.Node, job.Orphaned = from, to, false
	payload := hubJobEventPayload{JobID: jobID, From: from, To: to, Epoch: job.Epoch, LastSeen: job.LastSeen}
	h.queueJobEventLocked("job.reassigned", payload)
	return payload, true
}

// queueJobEventLocked records only fixed job metadata for UI/event listeners.
// The caller owns h.mu.
func (h *HubServer) queueJobEventLocked(kind string, payload hubJobEventPayload) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	node := payload.Node
	if node == "" {
		node = payload.From
	}
	h.recordUIEventWithJobLocked("job", strings.TrimPrefix(kind, "job."), node, payload.JobID, h.now().UTC())
	go h.broadcast(hubEvent{MachineID: node, Kind: kind, Payload: encoded, Received: h.now().UTC()})
	if kind != "job.orphaned" {
		return
	}
	if notifier, ok := h.notifier.(interface {
		SendJob(context.Context, string) error
	}); ok {
		text := "job " + strings.TrimPrefix(kind, "job.") + " id=" + payload.JobID
		if node != "" {
			text += " node=" + node
		}
		go func() { _ = notifier.SendJob(context.Background(), text) }()
	}
}
