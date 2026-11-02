package panewire

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

func asyncSpawnAccepted(record *hubSpawnRecord) map[string]string {
	return map[string]string{"request_id": record.request.RequestID, "job_id": record.request.JobID, "status": "accepted"}
}

func (h *HubServer) handleAsyncSpawn(w http.ResponseWriter, _ *http.Request, body hubSpawnRequest) {
	h.mu.Lock()
	for _, existing := range h.spawnRecords {
		if existing.request.Async && existing.request.JobID == body.JobID {
			// A job id cannot redirect a prior request to a different owner or host.
			if existing.request.OwnerLane != body.OwnerLane || existing.request.Machine != body.Machine {
				h.mu.Unlock()
				writeJSON(w, http.StatusConflict, map[string]string{"error": "job_conflict"})
				return
			}
			response := asyncSpawnAccepted(existing)
			h.mu.Unlock()
			writeJSON(w, http.StatusAccepted, response)
			return
		}
	}
	if h.spawnRecords[body.RequestID] != nil {
		h.mu.Unlock()
		writeJSON(w, http.StatusConflict, map[string]string{"error": "request_conflict"})
		return
	}
	node := h.nodes[body.Machine]
	if node == nil || node.agent == nil || node.state != "connected" || !h.acceptingEffectiveLocked(body.Machine, node.accepting) {
		h.mu.Unlock()
		writeJSON(w, http.StatusServiceUnavailable, h.spawnUnavailableResponse(body.Machine))
		return
	}
	// Older nodes would ignore the deadline and stop early.
	if !node.capabilities["spawn_async"] {
		h.mu.Unlock()
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "spawn_capability_required", "capability": "spawn_async"})
		return
	}
	if body.DeadlineSeconds == 0 {
		body.DeadlineSeconds = 600
	}
	entry := &hubSpawnRecord{request: body, status: "pending", created: h.now().UTC(), waiter: make(chan hubSpawnResult, 1)}
	h.spawnRecords[body.RequestID] = entry
	agent := node.agent
	h.mu.Unlock()
	// Dispatch belongs to the hub lifetime, independent of the submitting HTTP request.
	go func() {
		if err := agent.writeJSON(struct {
			Type            string            `json:"type"`
			RequestID       string            `json:"request_id"`
			CWDKey          string            `json:"cwd_key"`
			Brief           hubSpawnBrief     `json:"brief"`
			Checkout        *hubSpawnCheckout `json:"checkout,omitempty"`
			Args            []string          `json:"args"`
			WaitSeconds     int               `json:"wait_seconds"`
			DeadlineSeconds int               `json:"deadline_seconds"`
		}{"job.spawn", body.RequestID, body.CWDKey, body.Brief, body.Checkout, body.Args, body.WaitSeconds, body.DeadlineSeconds}); err != nil {
			h.resolveSpawn(body.Machine, hubSpawnResult{RequestID: body.RequestID, RC: 1, Error: "spawn_failed"})
			return
		}
		h.broadcastSpawnRequested(body)
		h.awaitAsyncSpawn(entry, time.Duration(body.DeadlineSeconds)*time.Second)
	}()
	writeJSON(w, http.StatusAccepted, asyncSpawnAccepted(entry))
}

func (h *HubServer) awaitAsyncSpawn(entry *hubSpawnRecord, deadline time.Duration) {
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	reason := "timeout"
	select {
	case result := <-entry.waiter:
		if result.CompletedAt != nil {
			return
		}
		reason = "spawn_failed"
	case <-timer.C:
	case <-h.closeCtx.Done():
		return
	}
	h.resolveSpawn(entry.request.Machine, hubSpawnResult{RequestID: entry.request.RequestID, RC: 1, Error: reason})
}

func (h *HubServer) notifyAsyncSpawn(request hubSpawnRequest, result hubSpawnResult) {
	h.traceAsyncSpawn(request, result)
	text := ""
	if result.RC == 0 && result.JobID == request.JobID && result.Pane != "" {
		model := "unknown"
		for _, line := range strings.Split(result.StdoutTail, "\n") {
			if !strings.HasPrefix(line, "OK pane=") {
				continue
			}
			for _, field := range strings.Fields(line) {
				if value, ok := strings.CutPrefix(field, "model="); ok {
					model = value
				}
			}
		}
		text = fmt.Sprintf("[runtug] SPAWNED %s host=%s pane=%s model=%s", request.JobID, request.Machine, result.Pane, model)
	} else {
		reason := result.Error
		if reason == "" {
			reason = "invalid_spawn_receipt"
		}
		text = fmt.Sprintf("[runtug] SPAWN_FAILED %s rc=%d reason=%s", request.JobID, result.RC, reason)
	}
	id := "spawn-" + request.RequestID
	h.relayLaneEvent(hubJobEventPayload{JobID: laneEventTransportID(request.OwnerLane, id), Epoch: 1, OwnerLane: request.OwnerLane, EventID: id, Text: text, Label: "runtug", Host: request.Machine, NotifyType: "report"}, nil)
}
