package panewire

import (
	"context"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	hubJobIDPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	hubAgentLabelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	hubPushSHAPattern    = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)
)

func validHubActiveJob(job HubActiveJob) bool {
	return hubJobIDPattern.MatchString(job.JobID) && hubAgentLabelPattern.MatchString(job.AgentLabel) && job.Epoch > 0 && (job.PushSHA == "" || hubPushSHAPattern.MatchString(job.PushSHA))
}

const hubRelayPayloadTextLimit = 240

// truncateHubRelayPayloadText applies the character (rather than byte) bound
// used by the relay record contract. Newlines in a question are display text,
// not a record separator, so callers may opt into normalization for it.
func truncateHubRelayPayloadText(value string, normalizeNewlines bool) (string, bool) {
	if normalizeNewlines {
		value = strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ").Replace(value)
	}
	if utf8.RuneCountInString(value) <= hubRelayPayloadTextLimit {
		return value, false
	}
	return string([]rune(value)[:hubRelayPayloadTextLimit]), true
}

func decodeHubJobCompletionPayloadDetailed(payload []byte) (hubJobEventPayload, []string, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || len(fields) < 2 || len(fields) > 13 {
		return hubJobEventPayload{}, nil, false
	}
	for name := range fields {
		if name != "job_id" && name != "epoch" && name != "agent_label" && name != "owner_lane" && name != "label" && name != "host" && name != "report_path" && name != "report_last_line" && name != "question" && name != "pr" && name != "head" && name != "pane_id" && name != "replay" {
			return hubJobEventPayload{}, nil, false
		}
	}
	var completion hubJobEventPayload
	if json.Unmarshal(fields["job_id"], &completion.JobID) != nil || json.Unmarshal(fields["epoch"], &completion.Epoch) != nil || !hubJobIDPattern.MatchString(completion.JobID) || completion.Epoch == 0 {
		return hubJobEventPayload{}, nil, false
	}
	// agent_label is the claim metadata a node carries on the terminal record so
	// the hub can late-register a job it never saw active. It is descriptive
	// only; it never grants ownership on its own.
	if raw, present := fields["agent_label"]; present {
		if json.Unmarshal(raw, &completion.AgentLabel) != nil || !hubAgentLabelPattern.MatchString(completion.AgentLabel) {
			return hubJobEventPayload{}, nil, false
		}
	}
	// replay marks a record the node had already sent before it restarted. It
	// is log and event metadata only; routing never consults it.
	if raw, present := fields["replay"]; present {
		if json.Unmarshal(raw, &completion.Replay) != nil {
			return hubJobEventPayload{}, nil, false
		}
	}
	var truncated []string
	for key, destination := range map[string]*string{"owner_lane": &completion.OwnerLane, "label": &completion.Label, "host": &completion.Host, "report_path": &completion.ReportPath, "report_last_line": &completion.ReportLastLine, "question": &completion.Question, "pr": &completion.PR, "head": &completion.Head, "pane_id": &completion.PaneID} {
		if raw, ok := fields[key]; ok {
			if json.Unmarshal(raw, destination) != nil || strings.Contains(*destination, "\x00") {
				return hubJobEventPayload{}, nil, false
			}
			if key != "question" && strings.ContainsAny(*destination, "\r\n") {
				return hubJobEventPayload{}, nil, false
			}
			var wasTruncated bool
			*destination, wasTruncated = truncateHubRelayPayloadText(*destination, key == "question")
			if wasTruncated {
				truncated = append(truncated, key)
			}
		}
	}
	return completion, truncated, true
}

func decodeHubJobCompletionPayload(payload []byte) (hubJobEventPayload, bool) {
	event, _, valid := decodeHubJobCompletionPayloadDetailed(payload)
	return event, valid
}

// decodeHubJobEscalationPayload is the flat record contract written by wrk
// and captains. It deliberately reuses completion metadata and adds a compact
// operator-readable reason; it is not a command channel.
func decodeHubJobEscalationPayloadDetailed(payload []byte) (hubJobEventPayload, []string, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || len(fields) < 3 || len(fields) > 14 {
		return hubJobEventPayload{}, nil, false
	}
	if _, hasReason := fields["reason"]; !hasReason {
		return hubJobEventPayload{}, nil, false
	}
	copyFields := make(map[string]json.RawMessage, len(fields)-1)
	for key, value := range fields {
		if key != "reason" {
			copyFields[key] = value
		}
	}
	base, _ := json.Marshal(copyFields)
	event, truncated, valid := decodeHubJobCompletionPayloadDetailed(base)
	if !valid || json.Unmarshal(fields["reason"], &event.Reason) != nil || event.Reason == "" || strings.ContainsAny(event.Reason, "\r\n\x00") {
		return hubJobEventPayload{}, nil, false
	}
	var wasTruncated bool
	event.Reason, wasTruncated = truncateHubRelayPayloadText(event.Reason, false)
	if wasTruncated {
		truncated = append(truncated, "reason")
	}
	return event, truncated, true
}

func decodeHubJobEscalationPayload(payload []byte) (hubJobEventPayload, bool) {
	event, _, valid := decodeHubJobEscalationPayloadDetailed(payload)
	return event, valid
}

func decodeRelayAckPayload(payload []byte) (relayAckPayload, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil || len(fields) < 2 || len(fields) > 3 {
		return relayAckPayload{}, false
	}
	for name := range fields {
		if name != "job_id" && name != "pane" && name != "reason" {
			return relayAckPayload{}, false
		}
	}
	var ack relayAckPayload
	if json.Unmarshal(fields["job_id"], &ack.JobID) != nil || json.Unmarshal(fields["pane"], &ack.Pane) != nil || !hubJobIDPattern.MatchString(ack.JobID) || ack.Pane == "" || len(ack.Pane) > 128 {
		return relayAckPayload{}, false
	}
	if raw, ok := fields["reason"]; ok && (json.Unmarshal(raw, &ack.Reason) != nil || len(ack.Reason) > 240 || strings.ContainsAny(ack.Reason, "\r\n\x00")) {
		return relayAckPayload{}, false
	}
	return ack, true
}

func (h *HubServer) observeActiveJobs(machineID string, active []HubActiveJob, received time.Time) {
	seen := make(map[string]struct{}, len(active))
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
		if current == nil {
			// Epoch 1 is issued by the hub for a first-seen job. A node cannot
			// establish authority by presenting an arbitrary higher epoch.
			if job.Epoch == 1 {
				h.jobs[job.JobID] = &hubJobRecord{HubActiveJob: job, Node: machineID, LastSeen: received, FencedNodes: make(map[string]uint64)}
			}
			continue
		}
		if current.Completed || current.Node != machineID || current.Epoch != job.Epoch {
			// A stale or self-promoted heartbeat is presence-only. It must not
			// modify ownership, epoch, fencing, or liveness of the true owner.
			continue
		}
		// A returned current owner can prove it is still working. This is distinct
		// from a local terminal-file observation below: clear sticky orphan state
		// and make it unavailable for redispatch again.
		if current.Orphaned {
			current.Orphaned = false
			h.queueJobEventLocked("job.recovered", hubJobEventPayload{JobID: job.JobID, Node: machineID, Epoch: job.Epoch, LastSeen: received})
		}
		// Same owner and hub-issued epoch: only local metadata/liveness advances.
		current.AgentLabel, current.LastEventSeq, current.PushSHA, current.LastSeen = job.AgentLabel, job.LastEventSeq, job.PushSHA, received
	}
	// A job absent after the node has reconnected is a local terminal-file
	// observation. Inspect the durable-in-process job view rather than just the
	// replaced websocket record, because connect deliberately starts a fresh
	// presence record. It is never a remote command.
	for id, current := range h.jobs {
		if _, stillActive := seen[id]; stillActive {
			continue
		}
		if current.Node == machineID && current.Orphaned {
			current.Orphaned, current.Completed = false, true
			h.queueJobEventLocked("job.recovered", hubJobEventPayload{JobID: id, Node: machineID, Epoch: current.Epoch, LastSeen: received})
		}
	}
	h.mu.Unlock()
	h.tryPendingRevocations(machineID)
}

// tryPendingRevocations deliberately retains every event until the node
// acknowledges its local marker write. A missing agent or full channel is a
// retry condition, never a delivery acknowledgement.
func (h *HubServer) tryPendingRevocations(machineID string) {
	h.mu.Lock()
	record := h.nodes[machineID]
	pending := h.pendingRevocations[machineID]
	events := make([]hubJobRevokedEvent, 0, len(pending))
	for _, event := range pending {
		events = append(events, event)
	}
	var agent *hubAgent
	if record != nil {
		agent = record.agent
	}
	h.mu.Unlock()
	if agent == nil {
		return
	}
	for _, event := range events {
		agent.queueRevocation(event)
	}
}

func (agent *hubAgent) queueRevocation(event hubJobRevokedEvent) bool {
	if agent == nil || agent.revocations == nil {
		return false
	}
	select {
	case agent.revocations <- event:
		return true
	default:
		return false
	}
}

func (h *HubServer) acknowledgeRevocation(machineID string, ack hubJobEventPayload) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	pending := h.pendingRevocations[machineID]
	event, exists := pending[ack.JobID]
	if !exists || event.Epoch != ack.Epoch {
		return false
	}
	delete(pending, ack.JobID)
	if len(pending) == 0 {
		delete(h.pendingRevocations, machineID)
	}
	return true
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

// lateRegisterJobCompletion admits a terminal record for a job that never
// reached h.jobs, so the operator job view is not silently missing work that
// finished before a heartbeat could carry it. The record is created with
// Completed set, which keeps it out of the orphan sweep and out of
// reassignJob: it is a receipt, never a redispatch candidate.
//
// Registration is restricted to epoch 1 for the same reason observeActiveJobs
// is: every epoch above 1 was issued by the hub, so a first-seen job claiming
// one cannot be proving anything. A higher-epoch record is still relayed; only
// its bookkeeping entry is declined.
func (h *HubServer) lateRegisterJobCompletion(machineID string, completion hubJobEventPayload, received time.Time) bool {
	if completion.Epoch != 1 || !hubAgentLabelPattern.MatchString(completion.AgentLabel) {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.jobs[completion.JobID] != nil {
		return false
	}
	h.jobs[completion.JobID] = &hubJobRecord{
		HubActiveJob: HubActiveJob{JobID: completion.JobID, AgentLabel: completion.AgentLabel, Epoch: completion.Epoch},
		Node:         machineID,
		LastSeen:     received,
		Completed:    true,
		FencedNodes:  make(map[string]uint64),
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
	job := h.jobs[jobID]
	if job == nil || !job.Orphaned || job.Completed || job.Node == to {
		h.mu.Unlock()
		return hubJobEventPayload{}, false
	}
	if _, knownNode := h.tokens[to]; !knownNode {
		h.mu.Unlock()
		return hubJobEventPayload{}, false
	}
	from := job.Node
	job.Epoch++
	if job.FencedNodes == nil {
		job.FencedNodes = make(map[string]uint64)
	}
	// A node may become the current owner again (A→B→A). Its old local epoch
	// is still fenced by the hub epoch comparison, but this node-level channel
	// must not deliver a revocation for the assignment it is about to own.
	delete(job.FencedNodes, to)
	if pending := h.pendingRevocations[to]; pending != nil {
		delete(pending, jobID)
		if len(pending) == 0 {
			delete(h.pendingRevocations, to)
		}
	}
	for node := range job.FencedNodes {
		job.FencedNodes[node] = job.Epoch
	}
	job.FencedNodes[from] = job.Epoch
	job.Reassignments = append(job.Reassignments, hubJobReassignment{From: from, To: to, Epoch: job.Epoch, At: h.now().UTC()})
	job.Node, job.Orphaned = to, false
	for node, epoch := range job.FencedNodes {
		h.enqueueRevocationLocked(node, hubJobRevokedEvent{Type: "job.revoked", JobID: jobID, Epoch: epoch})
	}
	payload := hubJobEventPayload{JobID: jobID, From: from, To: to, Epoch: job.Epoch, LastSeen: job.LastSeen}
	h.queueJobEventLocked("job.reassigned", payload)
	h.mu.Unlock()
	// The recipient also gets this on its next heartbeat/connect. Immediate
	// delivery narrows the window for an already-online receiving node.
	h.mu.Lock()
	newOwner := h.nodes[to]
	h.mu.Unlock()
	if newOwner != nil {
		newOwner.agent.queueAssignment(hubJobAssignedEvent{Type: "job.assigned", JobID: jobID, Epoch: payload.Epoch})
	}
	// The queue is also retried on reconnect and heartbeat; attempting now only
	// reduces the time to a local stop marker for an already-online predecessor.
	h.tryPendingRevocations(from)
	return payload, true
}

func (h *HubServer) enqueueRevocationLocked(machineID string, event hubJobRevokedEvent) {
	pending := h.pendingRevocations[machineID]
	if pending == nil {
		pending = make(map[string]hubJobRevokedEvent)
		h.pendingRevocations[machineID] = pending
	}
	if prior, exists := pending[event.JobID]; !exists || prior.Epoch < event.Epoch {
		pending[event.JobID] = event
		h.queueJobEventLocked("job.revoked", hubJobEventPayload{JobID: event.JobID, Node: machineID, Epoch: event.Epoch})
	}
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
