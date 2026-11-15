package panewire

import (
	"maps"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// quotaStopReasonFields returns the pool and the reset token of a job.lost
// "quota_stop" reason ("quota_stop pool=<p> reset_at=<t>"); pool is "" when the
// reason names none. The stop's own pool is the only place its band is
// recorded: the job record's pool is the launch pool, the catalog pool for both
// Devin bands (hk 4582).
func quotaStopReasonFields(reason string) (pool, reset string) {
	fields := strings.Fields(reason)
	if len(fields) < 2 || fields[0] != "quota_stop" || !strings.HasPrefix(fields[1], "pool=") {
		return "", ""
	}
	if len(fields) >= 3 && strings.HasPrefix(fields[2], "reset_at=") {
		reset = strings.TrimPrefix(fields[2], "reset_at=")
	}
	return strings.TrimPrefix(fields[1], "pool="), reset
}

// Pool cooling is held in hub memory and is lost on hub restart.
func (h *HubServer) observePoolCooling(reason string, now time.Time) bool {
	pool, reset := quotaStopReasonFields(reason)
	if pool == "" {
		return false
	}
	// A stop of the catalog pool devin is a paid-band stop: the SWE-2 free
	// rungs draw on the free band and cool apart under their own key, so the
	// paid stop holds only the paid band (fleet.BandKey, hk 4582).
	pool = fleet.BandKey(pool)
	if !fleetStateTokenPattern.MatchString(pool) {
		return false
	}
	until := now.Add(5 * time.Minute)
	if reset != "" {
		parsed, err := time.Parse(time.RFC3339Nano, reset)
		if err != nil {
			if h.logger != nil {
				h.logger.Warn("quota stop reset is invalid", "pool", pool)
			}
		} else {
			until = parsed
		}
	}
	if !until.After(now) {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.poolCooling == nil {
		h.poolCooling = make(map[string]time.Time)
	}
	if until.After(h.poolCooling[pool]) {
		h.poolCooling[pool] = until
	}
	return true
}

func (h *HubServer) fleetSnapshot(now time.Time) fleetStateSnapshot {
	snap := h.stateStore().snapshot(now)
	h.mu.Lock()
	defer h.mu.Unlock()
	for pool, until := range h.poolCooling {
		if !until.After(now) {
			delete(h.poolCooling, pool)
		}
	}
	snap.Cooling = maps.Clone(h.poolCooling)
	return snap
}
