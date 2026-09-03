package panewire

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// hubBurstHold is in-memory by design, like the existing burst decision state.
// A lost target never silently regains a lease after reconnecting.
type hubBurstHold struct {
	ID        string    `json:"id"`
	Target    string    `json:"target"`
	Reason    string    `json:"reason"`
	ExpiresAt time.Time `json:"expires_at"`
	Status    string    `json:"status"`
}

func validBurstHold(reason string, hold time.Duration) bool {
	return hold > 0 && hold <= 24*time.Hour && len(reason) > 0 && len(reason) <= 256 && !strings.ContainsAny(reason, "\r\n\x00")
}

func newBurstHoldID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "hold-" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return "hold-" + hex.EncodeToString(buf)
}

func (h *HubServer) sweepBurstHoldsLocked(now time.Time) {
	for _, hold := range h.holds {
		if hold.Status == "active" && !now.Before(hold.ExpiresAt) {
			hold.Status = "expired"
		}
	}
}

func (h *HubServer) markBurstHoldsLostLocked(target string) {
	for _, hold := range h.holds {
		if hold.Target == target && hold.Status == "active" {
			hold.Status = "lost"
		}
	}
}

func (h *HubServer) holdsActiveLocked(target string, now time.Time) bool {
	h.sweepBurstHoldsLocked(now)
	for _, hold := range h.holds {
		if hold.Target == target && hold.Status == "active" {
			return true
		}
	}
	return false
}

func (h *HubServer) burstHolds() []hubBurstHold {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sweepBurstHoldsLocked(h.now().UTC())
	result := make([]hubBurstHold, 0, len(h.holds))
	for _, hold := range h.holds {
		result = append(result, *hold)
	}
	// Small stable ordering avoids operator/CLI output flapping.
	for i := range result {
		for j := i + 1; j < len(result); j++ {
			if result[j].ID < result[i].ID {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result
}

func (h *HubServer) sendHeartbeatDirectives(machineID string, agent *hubAgent) {
	if agent == nil {
		return
	}
	now := h.now().UTC()
	h.mu.Lock()
	active := h.holdsActiveLocked(machineID, now)
	assignments := make([]hubJobAssignedEvent, 0)
	for _, job := range h.jobs {
		if job.Node == machineID && !job.Completed {
			assignments = append(assignments, hubJobAssignedEvent{Type: "job.assigned", JobID: job.JobID, Epoch: job.Epoch})
		}
	}
	h.mu.Unlock()
	agent.queueHolds(hubBurstHoldsEvent{Type: "burst.holds", HoldsActive: active})
	for _, assignment := range assignments {
		agent.queueAssignment(assignment)
	}
}

func (h *HubServer) requestBurst(ctx context.Context, target, reason string, hold time.Duration) (hubBurstHold, string) {
	if !machineIDPattern.MatchString(target) || !validBurstHold(reason, hold) {
		return hubBurstHold{}, "invalid_request"
	}
	now := h.now().UTC()
	h.mu.Lock()
	h.reloadBurstPolicyLocked()
	if h.burstPolicyPath == "" || h.burstPolicy.TargetMachine != target {
		h.mu.Unlock()
		return hubBurstHold{}, "target_unavailable"
	}
	wake := h.nodes[h.burstPolicy.WakeVia]
	event := hubBurstEvent{Type: "burst", Machine: target, Phase: hubFailoverPhaseUp, EmittedAt: now, WakeMAC: h.burstPolicy.WakeMAC}
	h.mu.Unlock()
	if wake == nil || wake.agent == nil {
		return hubBurstHold{}, "wake_via_unavailable"
	}
	// Reuse the exact R12 target/wake-via dispatch path; packet send outcome is
	// intentionally confirmed only by the target's authenticated heartbeat.
	h.dispatchBurst(event)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		h.mu.Lock()
		record := h.nodes[target]
		up := record != nil && record.agent != nil && record.state == "connected"
		if up {
			lease := hubBurstHold{ID: newBurstHoldID(), Target: target, Reason: reason, ExpiresAt: h.now().UTC().Add(hold), Status: "active"}
			h.holds[lease.ID] = &lease
			h.mu.Unlock()
			h.sendHeartbeatDirectives(target, record.agent)
			return lease, ""
		}
		h.mu.Unlock()
		select {
		case <-ctx.Done():
			return hubBurstHold{}, "target_timeout"
		case <-tick.C:
		}
	}
}

func (h *HubServer) handleBurstRequest(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	var body struct {
		Target  string `json:"target"`
		Hold    string `json:"hold"`
		Reason  string `json:"reason"`
		Timeout string `json:"timeout,omitempty"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil {
		http.Error(w, "invalid burst request", http.StatusBadRequest)
		return
	}
	duration, err := time.ParseDuration(body.Hold)
	if err != nil || !validBurstHold(body.Reason, duration) {
		http.Error(w, "invalid burst request", http.StatusBadRequest)
		return
	}
	timeout := 120 * time.Second
	if body.Timeout != "" {
		parsed, parseErr := time.ParseDuration(body.Timeout)
		if parseErr != nil || parsed <= 0 || parsed > 10*time.Minute {
			http.Error(w, "invalid burst request", http.StatusBadRequest)
			return
		}
		timeout = parsed
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	lease, reason := h.requestBurst(ctx, body.Target, body.Reason, duration)
	w.Header().Set("Content-Type", "application/json")
	if reason != "" {
		w.WriteHeader(http.StatusGatewayTimeout)
		_ = json.NewEncoder(w).Encode(map[string]string{"reason": reason})
		return
	}
	_ = json.NewEncoder(w).Encode(lease)
}

func (h *HubServer) handleBurstRelease(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	var body struct {
		ID string `json:"id"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&body) != nil || !strings.HasPrefix(body.ID, "hold-") {
		http.Error(w, "invalid burst release", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	hold := h.holds[body.ID]
	if hold == nil {
		h.mu.Unlock()
		http.Error(w, "hold not found", http.StatusNotFound)
		return
	}
	if hold.Status == "active" {
		hold.Status = "released"
	}
	copy := *hold
	record := h.nodes[hold.Target]
	h.mu.Unlock()
	if record != nil {
		h.sendHeartbeatDirectives(copy.Target, record.agent)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(copy)
}

func (h *HubServer) handleBurstHolds(w http.ResponseWriter, r *http.Request) {
	if !h.authorizeOperator(r) {
		hubUnauthorized(w)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Holds []hubBurstHold `json:"holds"`
	}{h.burstHolds()})
}
