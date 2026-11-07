package panewire

// Observability for the hub's relay, held-reservation and job-result paths.
// The fleet-event and Sentry contracts are in docs/observability.md.

import (
	"fmt"
	"strings"
	"time"

	"github.com/getsentry/sentry-go"
)

const (
	// relayOutcomeMaxEntries bounds the per-row outcome memory. A row's
	// delivered, dropped and unconfirmed observations each fit here long
	// enough to absorb a node's retries after a hub restart.
	relayOutcomeMaxEntries = 4096
	// spawnHeldLoopWindow and spawnHeldLoopCount define the repeated-hold
	// signal: one role key held this many times inside the window.
	spawnHeldLoopWindow   = 30 * time.Minute
	spawnHeldLoopCount    = 5
	spawnHeldLoopCooldown = time.Hour
	// spawnHeldLoopMaxKeys bounds the per-role-key memory. Past it the least
	// recently held key is evicted, so a new key is always counted.
	spawnHeldLoopMaxKeys = 4096
)

// spawnHeldLoop is one role key's recent holds and the last Sentry report.
type spawnHeldLoop struct {
	held       []time.Time
	lastSentry time.Time
}

// emitFleet appends one hub fleet event. Tests substitute fleetEvents; the
// configured state directory is the production sink.
func (h *HubServer) emitFleet(ev fleetEvent) bool {
	write := emitFleetEvent
	if h.fleetEvents != nil {
		write = h.fleetEvents
	}
	return write(h.now(), ev)
}

// relayLaneRoles says whether lane is a worker lane (it reports to a parent)
// or a design lane (another lane reports to it). The relay only serves those
// lanes; a lane with no route is matched to a live job's agent label, the same
// fallback resolveRelayRoute uses.
func (h *HubServer) relayLaneRoles(lane string) (worker, design bool) {
	if lane == "" {
		return false, false
	}
	routes := loadReportRelayRoutes(h.reportRelayPath)
	if route, ok := routes[lane]; ok && route.Parent != "" {
		worker = true
	}
	for _, route := range routes {
		if route.Parent == lane {
			design = true
			break
		}
	}
	if worker || design {
		return worker, design
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, job := range h.jobs {
		if !job.Completed && !job.Orphaned && job.AgentLabel == lane {
			return true, false
		}
	}
	return false, false
}

// relayOutcomeFirst reports whether this is the first time a row's outcome
// class is observed. A node reports the same row again on every retry, and the
// fleet event is one per outcome, not per attempt.
func (h *HubServer) relayOutcomeFirst(rowKey, class string) bool {
	key := class + "|" + rowKey
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.relayControl.relayOutcomes == nil {
		h.relayControl.relayOutcomes = make(map[string]struct{})
	}
	if _, seen := h.relayControl.relayOutcomes[key]; seen {
		h.relayControl.relayOutcomeOrder.touch(key, relayOutcomeMaxEntries)
		return false
	}
	_, evicted, overflowed := h.relayControl.relayOutcomeOrder.touch(key, relayOutcomeMaxEntries)
	if overflowed {
		delete(h.relayControl.relayOutcomes, evicted)
	}
	h.relayControl.relayOutcomes[key] = struct{}{}
	return true
}

// observeRelayDelivered records one row that a pane proved it received.
func (h *HubServer) observeRelayDelivered(pending relayPending) {
	lane := pending.event.OwnerLane
	if lane == "" {
		return
	}
	if worker, design := h.relayLaneRoles(lane); !worker && !design {
		return
	}
	if !h.relayOutcomeFirst(relayPendingKey(pending.eventID, pending.event.JobID), "delivered") {
		return
	}
	h.emitFleet(fleetEvent{Kind: "relay", Job: pending.event.JobID, Outcome: "delivered", Reason: pending.kind, Detail: relayDetail(lane, pending.machine, pending.pane, "", "")})
}

// observeRelayUnconfirmed records one node report that it could not prove a
// row was delivered. The row may still be dropped or re-delivered later; this
// observation is that report, not a retry.
func (h *HubServer) observeRelayUnconfirmed(pending relayPending, ack relayAckPayload) {
	lane := pending.event.OwnerLane
	reason := relayOutcomeCode(ack.Reason)
	if lane == "" || reason == "" {
		return
	}
	if worker, design := h.relayLaneRoles(lane); !worker && !design {
		return
	}
	if !h.relayOutcomeFirst(relayPendingKey(pending.eventID, pending.event.JobID), "unconfirmed") {
		return
	}
	h.emitFleet(fleetEvent{Kind: "relay", Job: pending.event.JobID, Outcome: "unconfirmed:" + reason, Reason: reason, Detail: relayDetail(lane, pending.machine, pending.pane, "", ack.Reason)})
}

// observeRelayDropped records one row a node gave up on. A dropped row for a
// worker lane also leaves a warning-level Sentry event, because the worker may
// never have seen the text and the durable row stays undelivered.
func (h *HubServer) observeRelayDropped(machine string, dropped relayDroppedPayload) {
	reason := relayOutcomeCode(dropped.Reason)
	worker, design := h.relayLaneRoles(dropped.Lane)
	if !worker && !design {
		return
	}
	if !h.relayOutcomeFirst(relayPendingKey(dropped.OriginalEventID, dropped.JobID), "dropped") {
		return
	}
	h.emitFleet(fleetEvent{Kind: "relay", Job: dropped.JobID, Outcome: "dropped:" + reason, Reason: reason, Detail: relayDetail(dropped.Lane, machine, dropped.Pane, dropped.Harness, dropped.Reason)})
	if !worker || !relayDropWarns(reason) {
		return
	}
	harness := fleetEventCode(dropped.Harness)
	if harness == "" {
		harness = "unknown"
	}
	h.captureWarning([]string{"relay-dropped", reason, harness}, "RELAY_DROPPED "+reason+"/"+harness, map[string]string{
		"job": dropped.JobID, "lane": dropped.Lane, "machine": machine, "pane": dropped.Pane, "reason": reason, "harness": harness,
	})
}

// relayDropWarns says whether a dropped row's reason is a failure worth a
// warning. job_ended and lane_rerouted are the designed end-of-job and re-route
// drops; every other reason is a failure that would otherwise leave no trace.
func relayDropWarns(reason string) bool {
	switch reason {
	case relayDroppedJobEnded, "lane_rerouted":
		return false
	}
	return true
}

func relayDetail(lane, machine, pane, harness, reason string) string {
	var parts []string
	if lane != "" {
		parts = append(parts, "lane="+lane)
	}
	if machine != "" {
		parts = append(parts, "machine="+machine)
	}
	if pane != "" {
		parts = append(parts, "pane="+pane)
	}
	if harness != "" {
		parts = append(parts, "harness="+harness)
	}
	if reason != "" {
		parts = append(parts, "reason="+reason)
	}
	return strings.Join(parts, " ")
}

// relayOutcomeCode keeps an outcome label to the reason's first token: a
// node's evidence text is free form and would otherwise grow the label.
func relayOutcomeCode(reason string) string {
	if fields := strings.Fields(reason); len(fields) > 0 {
		return fleetEventCode(fields[0])
	}
	return ""
}

// observeSpawnHeld records one held role reservation. Five holds of the same
// role key inside spawnHeldLoopWindow are a loop, reported once per role key
// per hour.
func (h *HubServer) observeSpawnHeld(job, roleKey, branch, reason, details string) {
	h.emitFleet(fleetEvent{Kind: "spawn_held", Job: job, Outcome: branch, Reason: reason, Detail: details})
	if roleKey == "" {
		return
	}
	now := h.now().UTC()
	h.heldLoopMu.Lock()
	if h.heldLoop == nil {
		h.heldLoop = make(map[string]*spawnHeldLoop)
	}
	_, evicted, overflowed := h.heldLoopOrder.touch(roleKey, spawnHeldLoopMaxKeys)
	if overflowed {
		delete(h.heldLoop, evicted)
	}
	state := h.heldLoop[roleKey]
	if state == nil {
		state = &spawnHeldLoop{}
		h.heldLoop[roleKey] = state
	}
	cutoff := now.Add(-spawnHeldLoopWindow)
	kept := state.held[:0]
	for _, at := range state.held {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	state.held = append(kept, now)
	holds := len(state.held)
	send := holds >= spawnHeldLoopCount && (state.lastSentry.IsZero() || now.Sub(state.lastSentry) >= spawnHeldLoopCooldown)
	if send {
		state.lastSentry = now
	}
	h.heldLoopMu.Unlock()
	if send {
		h.captureWarning([]string{"spawn-held-loop", branch}, fmt.Sprintf("SPAWN_HELD_LOOP %s held %d times in %s", roleKey, holds, spawnHeldLoopWindow), map[string]string{"role_key": roleKey, "branch": branch})
	}
}

// captureWarning sends one warning-level Sentry event through the hub's
// private client. Nothing is sent when reporting is not configured.
func (h *HubServer) captureWarning(fingerprint []string, message string, tags map[string]string) {
	client := h.spawnSentryClient()
	if client == nil {
		return
	}
	hub := sentry.NewHub(client, sentry.NewScope())
	hub.CaptureEvent(&sentry.Event{Level: sentry.LevelWarning, Message: message, Fingerprint: fingerprint, Tags: tags})
}
