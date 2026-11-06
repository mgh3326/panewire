package panewire

import (
	"maps"
	"strings"
	"time"
)

// Pool cooling is held in hub memory and is lost on hub restart.
func (h *HubServer) observePoolCooling(reason string, now time.Time) {
	fields := strings.Fields(reason)
	if len(fields) < 2 || fields[0] != "quota_stop" || !strings.HasPrefix(fields[1], "pool=") {
		return
	}
	pool := strings.TrimPrefix(fields[1], "pool=")
	if !fleetStateTokenPattern.MatchString(pool) {
		return
	}
	until := now.Add(5 * time.Minute)
	if len(fields) >= 3 && strings.HasPrefix(fields[2], "reset_at=") {
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(fields[2], "reset_at="))
		if err != nil {
			return
		}
		until = parsed
	}
	if !until.After(now) {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.poolCooling == nil {
		h.poolCooling = make(map[string]time.Time)
	}
	if until.After(h.poolCooling[pool]) {
		h.poolCooling[pool] = until
	}
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
