package panewire

import (
	"maps"
	"strings"
	"time"
)

// Pool cooling is held in hub memory and is lost on hub restart.
func (h *HubServer) observePoolCooling(reason string, now time.Time) bool {
	fields := strings.Fields(reason)
	if len(fields) < 2 || fields[0] != "quota_stop" || !strings.HasPrefix(fields[1], "pool=") {
		return false
	}
	pool := strings.TrimPrefix(fields[1], "pool=")
	if !fleetStateTokenPattern.MatchString(pool) {
		return false
	}
	for _, field := range fields[2:] {
		if field == "provider_credit_exhausted=true" {
			at := now.UTC()
			for _, detail := range fields[2:] {
				if value, ok := strings.CutPrefix(detail, "exhausted_at="); ok {
					if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
						at = parsed.UTC()
					}
				}
			}
			h.mu.Lock()
			if h.poolCreditExhausted == nil {
				h.poolCreditExhausted = make(map[string]time.Time)
			}
			if at.After(h.poolCreditExhausted[pool]) {
				h.poolCreditExhausted[pool] = at
			}
			h.mu.Unlock()
			return true
		}
	}
	until := now.Add(5 * time.Minute)
	if len(fields) >= 3 && strings.HasPrefix(fields[2], "reset_at=") {
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(fields[2], "reset_at="))
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
	snap.CreditExhausted = maps.Clone(h.poolCreditExhausted)
	return snap
}
