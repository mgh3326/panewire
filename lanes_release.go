package panewire

import (
	"sort"
	"sync"
	"time"
)

// Lane rows outlive their panes: nothing but a hand-run hygiene pass removed a
// row, so a node that spawned a pane per job filled its row cap with rows
// whose panes were long gone and the hub then refused every new lane. The hub
// already holds each node's heartbeat session snapshot, so it releases a
// node's own rows when that snapshot proves the pane is gone:
//
//   - releaseDeadLanes sweeps on a timer: a row whose pane is absent from a
//     fresh snapshot is dropped once the job on that pane is finished, or once
//     the pane has stayed absent for laneReleaseGrace.
//   - putLane at the row cap evicts the caller's rows whose pane is absent from
//     its fresh snapshot (evictableLanes) before it refuses.
//   - a pane.close node command that reports closed or pane-gone releases the
//     rows pointing at that pane (releasePaneLanes).
//
// Every path applies deleteLane's rules (nodeMayDropLane) and acts only on rows
// the node owns. Unknown is never dead: a missing, stale, truncated or
// unavailable snapshot, or a node that is not connected, releases nothing.
const (
	// laneReleaseGrace is how long a pane must stay absent from fresh
	// snapshots before the sweep drops a row whose job is not known to be
	// finished. It outlasts a herdr restart and the settle after a spawn.
	laneReleaseGrace    = 5 * time.Minute
	laneReleaseInterval = 30 * time.Second
	// laneWriteSettle is how much newer than a row's last hub write a snapshot
	// must be before its absence counts. A snapshot is collected shortly
	// before the hub receives it, so one received just after a write can still
	// predate the pane that write registered.
	laneWriteSettle = 10 * time.Second

	unroutedWarnInterval = time.Minute
	unroutedWarnMaxLanes = 256
)

// laneLiveness is the hub's in-memory memory about rows. None of it is
// persisted: a restart only delays a release by one grace period.
type laneLiveness struct {
	mu     sync.Mutex
	wrote  map[string]time.Time
	absent map[laneAbsenceKey]time.Time
	warned map[string]unroutedWarnState
}

type laneAbsenceKey struct{ lane, machine, pane string }

type unroutedWarnState struct {
	last       time.Time
	suppressed int
}

func (l *laneLiveness) noteWrite(lane string, at time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.wrote == nil {
		l.wrote = make(map[string]time.Time)
	}
	l.wrote[lane] = at
}

func (l *laneLiveness) forget(lane string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.wrote, lane)
}

func (l *laneLiveness) lastWrite(lane string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	at, known := l.wrote[lane]
	return at, known
}

// laneObservation is what one node's fresh snapshot proves.
type laneObservation struct {
	panes      map[string]struct{}
	receivedAt time.Time
	// finished holds the panes whose every job record on the node is completed.
	finished map[string]struct{}
}

// freshSessionSnapshotLocked returns the node's latest heartbeat snapshot only
// when it can be trusted as a complete, current list of the node's panes.
// Callers hold h.mu.
func (h *HubServer) freshSessionSnapshotLocked(machine string) *hubSessionSnapshotRecord {
	node := h.nodes[machine]
	if node == nil || node.state != "connected" {
		return nil
	}
	snap := node.sessionSnapshot
	if snap == nil || snap.snapshotStatus != hubSnapshotStatusOK || snap.truncated {
		return nil
	}
	now := h.now()
	if snap.receivedAt.After(now) || now.Sub(snap.receivedAt) > sessionsFindSnapshotMaxAge {
		return nil
	}
	return snap
}

// observeNodePanes reports what machine's fresh snapshot proves; ok is false
// when it proves nothing.
func (h *HubServer) observeNodePanes(machine string) (laneObservation, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	snap := h.freshSessionSnapshotLocked(machine)
	if snap == nil {
		return laneObservation{}, false
	}
	obs := laneObservation{panes: make(map[string]struct{}, len(snap.sessions)), receivedAt: snap.receivedAt, finished: make(map[string]struct{})}
	for _, session := range snap.sessions {
		obs.panes[session.PaneID] = struct{}{}
	}
	running := make(map[string]struct{})
	for _, job := range h.jobs {
		if job == nil || job.Node != machine || job.Pane == "" {
			continue
		}
		if job.Completed {
			obs.finished[job.Pane] = struct{}{}
		} else {
			running[job.Pane] = struct{}{}
		}
	}
	for pane := range running {
		delete(obs.finished, pane)
	}
	return obs, true
}

// nodeMayDropLane applies deleteLane's rules for a node caller: the row is the
// node's own, it is not protected, no foreign machine still routes through the
// name, and the control plane does not own the lane. h.mu must not be held.
func (h *HubServer) nodeMayDropLane(routes map[string]reportRelayRoute, lane string, route reportRelayRoute, machine string) bool {
	if route.Sink || route.Machine != machine || route.Protected || route.Pane == "" {
		return false
	}
	if h.laneHasForeignReference(routes, lane, machine) {
		return false
	}
	return h.controlPlaneAuthorityDecision(lane) == nil
}

// laneProvablyGone is true when the fresh snapshot omits the row's pane and was
// received after the row's last hub write had time to show up in it.
func (h *HubServer) laneProvablyGone(lane string, route reportRelayRoute, obs laneObservation) bool {
	if _, listed := obs.panes[route.Pane]; listed {
		return false
	}
	if wrote, known := h.laneLife.lastWrite(lane); known && !obs.receivedAt.After(wrote.Add(laneWriteSettle)) {
		return false
	}
	return true
}

// evictableLanes lists the caller's own rows whose pane is provably gone, in
// lane order. It never looks at another machine's rows.
func (h *HubServer) evictableLanes(routes map[string]reportRelayRoute, machine string) []string {
	obs, ok := h.observeNodePanes(machine)
	if !ok {
		return nil
	}
	var dead []string
	for lane, route := range routes {
		if h.laneProvablyGone(lane, route, obs) && h.nodeMayDropLane(routes, lane, route, machine) {
			dead = append(dead, lane)
		}
	}
	sort.Strings(dead)
	return dead
}

func (h *HubServer) laneReleaseGraceOrDefault() time.Duration {
	if h.laneReleaseGrace > 0 {
		return h.laneReleaseGrace
	}
	return laneReleaseGrace
}

// releaseDeadLanes is one sweep. It returns the lanes it removed.
func (h *HubServer) releaseDeadLanes() []string {
	return h.rewriteLanes("sweep", func(routes map[string]reportRelayRoute) []string {
		now := h.now().UTC()
		grace := h.laneReleaseGraceOrDefault()
		machines := make(map[string]struct{})
		for _, route := range routes {
			if !route.Sink && route.Machine != "" {
				machines[route.Machine] = struct{}{}
			}
		}
		observed := make(map[string]laneObservation, len(machines))
		for machine := range machines {
			if obs, ok := h.observeNodePanes(machine); ok {
				observed[machine] = obs
			}
		}
		seen := make(map[laneAbsenceKey]struct{})
		var release []string
		for lane, route := range routes {
			obs, ok := observed[route.Machine]
			if !ok || route.Pane == "" {
				continue
			}
			if !h.laneProvablyGone(lane, route, obs) {
				continue
			}
			key := laneAbsenceKey{lane: lane, machine: route.Machine, pane: route.Pane}
			seen[key] = struct{}{}
			h.laneLife.mu.Lock()
			if h.laneLife.absent == nil {
				h.laneLife.absent = make(map[laneAbsenceKey]time.Time)
			}
			since, tracked := h.laneLife.absent[key]
			if !tracked {
				since = now
				h.laneLife.absent[key] = since
			}
			h.laneLife.mu.Unlock()
			_, finished := obs.finished[route.Pane]
			if (finished || now.Sub(since) >= grace) && h.nodeMayDropLane(routes, lane, route, route.Machine) {
				release = append(release, lane)
			}
		}
		h.laneLife.mu.Lock()
		for key := range h.laneLife.absent {
			if _, still := seen[key]; !still {
				delete(h.laneLife.absent, key)
			}
		}
		h.laneLife.mu.Unlock()
		sort.Strings(release)
		return release
	})
}

// releasePaneLanes drops machine's rows that point at a pane a node command just
// closed. Rows written after the command began are kept: they belong to a
// pane that reused the id.
func (h *HubServer) releasePaneLanes(machine, pane string, commandStart time.Time) []string {
	return h.rewriteLanes("pane-close", func(routes map[string]reportRelayRoute) []string {
		var release []string
		for lane, route := range routes {
			if route.Pane != pane {
				continue
			}
			if wrote, known := h.laneLife.lastWrite(lane); known && wrote.After(commandStart) {
				continue
			}
			if h.nodeMayDropLane(routes, lane, route, machine) {
				release = append(release, lane)
			}
		}
		sort.Strings(release)
		return release
	})
}

// rewriteLanes removes the rows pick chooses in one atomic file replacement,
// under the lanes file lock, and announces each removal like deleteLane does.
// A failure is logged and removes nothing: the next pass retries.
func (h *HubServer) rewriteLanes(reason string, pick func(routes map[string]reportRelayRoute) []string) []string {
	if h.reportRelayPath == "" {
		return nil
	}
	var removed []string
	err := withLanesFileLock(h.reportRelayPath, func() error {
		snapshot, err := readLanesFileForWrite(h.reportRelayPath)
		if err != nil {
			return err
		}
		removed = pick(snapshot.Routes)
		if len(removed) == 0 {
			removed = nil
			return nil
		}
		for _, lane := range removed {
			delete(snapshot.Routes, lane)
		}
		contents, err := encodeLanesFile(snapshot)
		if err != nil {
			removed = nil
			return err
		}
		if len(contents) > lanesNodeFileMaxBytes && len(contents) > len(snapshot.Bytes) {
			removed = nil
			return errLanesFileFull
		}
		if err := h.replaceLanesFile(h.reportRelayPath, snapshot, contents, lanesBackupNow(h)); err != nil {
			removed = nil
			return err
		}
		return nil
	})
	if err != nil {
		h.logger.Warn("lane release failed", "reason", reason, "error", err)
		return nil
	}
	for _, lane := range removed {
		h.laneLife.forget(lane)
		h.logger.Info("lane released", "lane", lane, "reason", reason)
		h.broadcastLanesChanged(lane, "remove")
	}
	return removed
}

func (h *HubServer) runLaneRelease(ctxDone <-chan struct{}) {
	interval := h.laneReleaseInterval
	if interval <= 0 {
		interval = laneReleaseInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctxDone:
			return
		case <-ticker.C:
			h.releaseDeadLanes()
		}
	}
}

// warnUnroutedRelay logs one WARN per lane per unroutedWarnInterval, carrying
// the lane, the event id and how many warnings the interval swallowed.
func (h *HubServer) warnUnroutedRelay(lane, eventID string) {
	now := h.now()
	l := &h.laneLife
	l.mu.Lock()
	if l.warned == nil {
		l.warned = make(map[string]unroutedWarnState)
	}
	state, known := l.warned[lane]
	if known && now.Sub(state.last) < unroutedWarnInterval {
		state.suppressed++
		l.warned[lane] = state
		l.mu.Unlock()
		return
	}
	if !known && len(l.warned) >= unroutedWarnMaxLanes {
		for name, other := range l.warned {
			if now.Sub(other.last) >= unroutedWarnInterval {
				delete(l.warned, name)
			}
		}
		if len(l.warned) >= unroutedWarnMaxLanes {
			l.mu.Unlock()
			return
		}
	}
	suppressed := state.suppressed
	l.warned[lane] = unroutedWarnState{last: now}
	l.mu.Unlock()
	h.logger.Warn("relay unrouted: no deliverable route for lane", "lane", lane, "event_id", eventID, "suppressed", suppressed)
}
