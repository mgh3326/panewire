package panewire

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// laneStallSweepBudget bounds one background lane-stall run end to end. The
// per-request client timeout still applies; the budget additionally caps the
// number of pages and lanes a degraded handoffkeep can stretch over.
const laneStallSweepBudget = 20 * time.Second

// laneStallObservation is one sweep's view of a single lane: every undelivered
// durable row that still counts against it (not retired, attempts below the
// replay bound), the oldest such row, how many of those rows sit in the held
// projection waiting on a busy pane, and how many active jobs make the lane
// "in use". A lane without active work is never stalled.
type laneStallObservation struct {
	lane             string
	observed         bool
	stalled          bool
	problemSince     time.Time
	oldestEventID    int64
	oldestReceivedAt string
	undelivered      int
	heldCount        int
	activeJobs       int
}

// consider folds one listed row into the observation. Retired rows and rows
// that already spent their attempts do not count: the first was deliberately
// closed, the second already has its own operator signal.
func (o *laneStallObservation) consider(record handoffkeepRelayEvent, now time.Time) {
	if record.DeliveredAt != "" || strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) || record.Attempts >= relayReplayMaxAttempts {
		return
	}
	o.undelivered++
	received, err := time.Parse(time.RFC3339Nano, record.ReceivedAt)
	if err != nil {
		// A row with no provable age counts against the backlog but can never
		// establish a stall, the same gate the replay age check uses.
		return
	}
	if o.problemSince.IsZero() || received.Before(o.problemSince) {
		o.problemSince = received
		o.oldestEventID = record.ID
		o.oldestReceivedAt = record.ReceivedAt
	}
	if now.Sub(received) > relayLaneStallAge {
		o.stalled = true
	}
}

// startLaneStallSweep is Sweep's hook. The observation does handoffkeep HTTP
// (a 10s client timeout per page over 200-row pages), so it never runs on the
// Sweep goroutine: failover broadcasts, alert dispatch and keepalive pings
// must not wait behind a slow or hung durable store — the same reason
// startUpdateOverdueFlush runs off this path. At most one run is in flight;
// a sweep that finds one running skips, leaving the next tick to retry.
func (h *HubServer) startLaneStallSweep(now time.Time) {
	if h.handoffkeep == nil {
		return
	}
	if !h.laneStallMu.TryLock() {
		return
	}
	// The check sits between TryLock and the WaitGroup Add so a launch racing
	// Close either holds the mutex before it (Close waits the run out) or
	// observes laneStallClosing and never counts against Wait.
	if h.laneStallClosing {
		h.laneStallMu.Unlock()
		return
	}
	h.laneStallSweeps.Add(1)
	go func() {
		defer h.laneStallSweeps.Done()
		defer h.laneStallMu.Unlock()
		ctx, cancel := context.WithTimeout(h.laneStallBaseContext(), laneStallSweepBudget)
		defer cancel()
		h.sweepLaneStalls(ctx, now)
	}()
}

// laneStallBaseContext anchors a run's budget to the RunMaintenance context
// so SIGTERM shortens an in-flight observation. A hub that never ran
// maintenance — unit tests driving Sweep directly — falls back to
// context.Background.
func (h *HubServer) laneStallBaseContext() context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.maintenanceCtx != nil {
		return h.maintenanceCtx
	}
	return context.Background()
}

// sweepLaneStalls is the background half of the lane-stall alarm. A non-sink
// lane is stalled when an undelivered relay row for it is older than
// relayLaneStallAge AND the lane has active work — it is an active job's
// owner lane or the parent of one in the nodes' active-jobs view. The alarm
// observes and tells: it never re-injects, reroutes, or retires a row itself.
func (h *HubServer) sweepLaneStalls(ctx context.Context, now time.Time) {
	routes := loadReportRelayRoutes(h.reportRelayPath)
	var notifications []hubNotification
	for _, lane := range h.laneStallCandidates(routes) {
		if ctx.Err() != nil {
			// Budget spent: lanes not yet evaluated are unobserved — the same
			// neutral rule as a read error, so an exhausted run neither opens
			// nor closes an episode.
			break
		}
		jobs := h.laneActiveJobCount(lane, routes)
		// A lane without active work is never stalled — that answer is a
		// definite observation, not a skipped check, so it can end an episode.
		obs := laneStallObservation{lane: lane, observed: true, activeJobs: jobs}
		if jobs > 0 {
			obs = h.observeLaneUndelivered(ctx, lane, now)
			obs.activeJobs = jobs
		}
		if !obs.observed {
			// A failed read is neither a problem nor a clear observation:
			// feeding false here would end a real episode on a transient
			// handoffkeep error.
			continue
		}
		key := "lane-stall:" + lane
		h.mu.Lock()
		state := h.alerts[key]
		wasActive := state != nil && state.active
		notifications = append(notifications, h.observeHubAlertLocked(now, key, obs.stalled, obs.problemSince, hubAlertReasonLaneStalled, "relay.lane_stalled", h.gracePeriod)...)
		state = h.alerts[key]
		activated := state != nil && state.active && !wasActive
		obs.heldCount = h.laneHeldCountLocked(lane)
		if laneStallAlertInactive(state) {
			// Nothing is open, owed, or half-accumulated: drop the entry so
			// an ephemeral lane does not stay a candidate forever. An open
			// episode or an unproduced recovery keeps its entry — the
			// recovery notification's dispatch still needs it.
			delete(h.alerts, key)
		}
		h.mu.Unlock()
		if activated {
			// One Warn and one broadcast per stall episode, at the moment the
			// dampened alert activates.
			h.logger.Warn("relay lane stalled", "lane", obs.lane, "oldest_event_id", obs.oldestEventID, "oldest_received_at", obs.oldestReceivedAt, "undelivered_count", obs.undelivered, "held_count", obs.heldCount, "active_jobs", obs.activeJobs)
			h.broadcastLaneStalled(obs)
		}
	}
	h.dispatchHubNotifications(notifications)
}

// laneHeldCountLocked counts the lane's undelivered rows currently held for a
// busy pane — present in the h.relayHeld projection. Held rows still count
// toward the stall; the number only annotates the alarm so an operator can
// tell "nothing is listening" from "a pane is busy". The caller holds mu.
func (h *HubServer) laneHeldCountLocked(lane string) int {
	count := 0
	for _, held := range h.relayHeld {
		if held.Lane == lane {
			count++
		}
	}
	return count
}

// laneStallAlertInactive reports whether a lane-stall entry carries nothing
// worth keeping: no open episode, no recovery owed or already queued for
// dispatch, and no debounce progress a fresh entry would lose.
func laneStallAlertInactive(state *hubAlertState) bool {
	return state != nil && !state.active && !state.recoveryNeeded && !state.recoveryPending &&
		state.badRuns == 0 && state.badSince.IsZero() && state.clearRuns == 0
}

// laneStallCandidates enumerates the lanes a stall could ever be declared for:
// the non-sink lanes in the current routes file, plus any lane still holding a
// lane-stall alert so a removed or re-sunk lane drains its episode cleanly.
func (h *HubServer) laneStallCandidates(routes map[string]reportRelayRoute) []string {
	lanes := make(map[string]struct{})
	for lane, route := range routes {
		if !route.Sink {
			lanes[lane] = struct{}{}
		}
	}
	h.mu.Lock()
	for key := range h.alerts {
		if lane, found := strings.CutPrefix(key, "lane-stall:"); found {
			lanes[lane] = struct{}{}
		}
	}
	h.mu.Unlock()
	out := make([]string, 0, len(lanes))
	for lane := range lanes {
		out = append(out, lane)
	}
	sort.Strings(out)
	return out
}

// laneActiveJobCount counts the distinct active jobs that make a lane "in
// use": jobs whose owner lane is the lane itself, plus jobs whose owner lane's
// parent is the lane — the view resolveIdleWakeOwner falls back to.
func (h *HubServer) laneActiveJobCount(lane string, routes map[string]reportRelayRoute) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	seen := make(map[string]struct{})
	count := 0
	for _, record := range h.nodes {
		for _, job := range record.activeJobs {
			if _, dup := seen[job.JobID]; job.JobID == "" || dup {
				continue
			}
			if job.OwnerLane != lane {
				owner, routed := routes[job.OwnerLane]
				if !routed || owner.Parent != lane {
					continue
				}
			}
			seen[job.JobID] = struct{}{}
			count++
		}
	}
	return count
}

// observeLaneUndelivered reads the lane's durable backlog. A read error —
// including budget exhaustion mid-page — leaves the observation empty and
// unobserved so a transient failure cannot declare or end an episode.
func (h *HubServer) observeLaneUndelivered(ctx context.Context, lane string, now time.Time) laneStallObservation {
	obs := laneStallObservation{lane: lane}
	var afterID int64
	for {
		pageStart := afterID
		records, err := h.handoffkeep.listUndelivered(ctx, lane, "", afterID, handoffkeepReplayLimit)
		if err != nil {
			h.logger.Warn("lane stall check could not read undelivered relay events", "lane", lane)
			return laneStallObservation{lane: lane}
		}
		obs.observed = true
		for _, record := range records {
			obs.consider(record, now)
			if record.ID > afterID {
				afterID = record.ID
			}
		}
		if len(records) < handoffkeepReplayLimit {
			return obs
		}
		if afterID <= pageStart {
			h.logger.Warn("lane stall check cursor did not advance", "lane", lane, "after_id", pageStart)
			return obs
		}
	}
}

// broadcastLaneStalled puts the activated stall on the operator feed once per
// episode. It announces state only; delivery decisions are untouched.
func (h *HubServer) broadcastLaneStalled(obs laneStallObservation) {
	payload, _ := json.Marshal(struct {
		Lane             string `json:"lane"`
		OldestEventID    int64  `json:"oldest_event_id"`
		OldestReceivedAt string `json:"oldest_received_at"`
		UndeliveredCount int    `json:"undelivered_count"`
		HeldCount        int    `json:"held_count"`
		ActiveJobs       int    `json:"active_jobs"`
	}{Lane: obs.lane, OldestEventID: obs.oldestEventID, OldestReceivedAt: obs.oldestReceivedAt, UndeliveredCount: obs.undelivered, HeldCount: obs.heldCount, ActiveJobs: obs.activeJobs})
	h.broadcast(hubEvent{Kind: "relay.lane_stalled", Payload: payload, Received: h.now().UTC()})
}
