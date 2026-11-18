package panewire

import (
	"context"
	"strconv"
	"strings"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The hub's placement-record outbox retries a pending attempt on every flush,
// and its delivery App writes its own warnings to io.Discard, so before hk 4691
// an hk refusal, a spool failure or a missing credential left the record
// pending with nothing in the hub log. Every path that leaves a finished
// attempt's record undelivered now reports through notePlacementStall: one
// line per request, attempt and reason, however often the flush retries it.
// docs/config.md ("Placement record producer") lists the reasons.

// placementStallLimit bounds how many pending attempts the hub remembers a
// reported reason for. Past it the notes reset: a long-stuck record logs again
// rather than the map growing without bound.
const placementStallLimit = 4096

// notePlacementStall logs once that an attempt's record is still undelivered
// and why. The same reason for the same attempt is not logged again; a changed
// reason is.
func (h *HubServer) notePlacementStall(requestID, job string, attempt int, reason string) {
	key := placementTraceBaseKey{RequestID: requestID, Attempt: attempt}
	h.placementStallMu.Lock()
	if h.placementStalls[key] == reason {
		h.placementStallMu.Unlock()
		return
	}
	if h.placementStalls == nil || len(h.placementStalls) >= placementStallLimit {
		h.placementStalls = make(map[placementTraceBaseKey]string)
	}
	h.placementStalls[key] = reason
	h.placementStallMu.Unlock()
	h.logger.Warn("placement record not delivered; snapshot stays pending", "request_id", requestID, "job", job, "attempt", attempt, "reason", reason)
}

// clearPlacementStall forgets an attempt's reported reason once its record is
// delivered, quarantined or sealed.
func (h *HubServer) clearPlacementStall(requestID string, attempt int) {
	h.placementStallMu.Lock()
	delete(h.placementStalls, placementTraceBaseKey{RequestID: requestID, Attempt: attempt})
	h.placementStallMu.Unlock()
}

// reportPlacementDelivery turns one delivery pass into the hub's log lines and
// reports whether the attempt's snapshot may be sealed: the record left the
// spool, delivered or quarantined.
func (h *HubServer) reportPlacementDelivery(requestID, job string, attempt int, d fleet.PlacementDelivery) bool {
	if !d.Delivered {
		h.notePlacementStall(requestID, job, attempt, d.Reason)
		return false
	}
	h.loopNoteDone(loopPlacementRecords, requestID+"/"+strconv.Itoa(attempt))
	h.clearPlacementStall(requestID, attempt)
	switch {
	case d.Quarantined:
		// The attempt is sealed and never retried, so this line is the only
		// trace of the drop besides the spool's placements-rejected sidecar.
		h.logger.Warn("placement record refused by hk and quarantined", "request_id", requestID, "job", job, "attempt", attempt, "reason", d.Reason)
	case d.Duplicate:
		h.logger.Info("placement record already held by hk", "request_id", requestID, "job", job, "attempt", attempt)
	}
	return true
}

// placementEnqueueReason is the stall reason for a record that could not be
// spooled at all (the spool directory cannot be created, is behind a symlink,
// or is not writable). The error names a path, never a credential.
func placementEnqueueReason(err error) string {
	return "spool_enqueue_failed: " + utf8PrefixWithin(strings.Join(strings.Fields(err.Error()), " "), 200)
}

// noteMissingPlacementSnapshot logs a landed attempt that has no placement
// snapshot: its record was never built. completeSpawn calls it on the terminal
// transition, because the asynchronous worker only enumerates existing
// snapshot rows and would never reach such an attempt. A refusal before the
// plan ranked hosts legitimately has none, so only succeeded is reported.
func (h *HubServer) noteMissingPlacementSnapshot(ctx context.Context, task hubSpawnTask) {
	payload, _, _, err := h.inbox.store.spawnPlacementSnapshot(ctx, task.Request.RequestID, task.Attempt)
	if err != nil || len(payload) != 0 {
		return
	}
	h.logger.Warn("placement record not built: a landed attempt has no placement snapshot", "request_id", task.Request.RequestID, "job", task.Job, "attempt", task.Attempt)
}
