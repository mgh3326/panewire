package panewire

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"
)

// Parked requests and the re-placement back-off of the spawn queue (hk 4661;
// hk design/2026-11-13/hub-owned-spawn-queue §2 parked, §3 rules 3 and 4,
// §9a). See docs/hub-spawn-operations.md#parked.

// The park reasons (design §2). Each names why no candidate can take the
// request now, and which events wake it before its next_check_at.
const (
	spawnParkQuota            = "quota"
	spawnParkLaunchBackoff    = "launch_backoff"
	spawnParkPoolCap          = "pool_cap"
	spawnParkHostFull         = "host_full"
	spawnParkNoEligibleHost   = "no_eligible_host"
	spawnParkNodeDisconnected = "node_disconnected"
)

// The hub's spawn-queue defaults (design §9a, unmeasured). placement.json
// overrides them with spawn_backoff_seconds, spawn_backoff_max_seconds and
// spawn_wake_batch.
const (
	spawnDefaultBackoff    = 30 * time.Second
	spawnDefaultBackoffMax = 10 * time.Minute
	spawnDefaultWakeBatch  = 8
	// spawnBackoffJitter is the ±fraction every back-off and re-check delay
	// is spread by, so requests that failed together do not retry together.
	spawnBackoffJitter = 0.2
	maxSpawnBackoffSec = 24 * 3600
	maxSpawnWakeBatch  = 256
)

func (p PlacementPolicy) spawnBackoffBase() time.Duration {
	if p.SpawnBackoffSeconds > 0 {
		return time.Duration(p.SpawnBackoffSeconds) * time.Second
	}
	return spawnDefaultBackoff
}

func (p PlacementPolicy) spawnBackoffMax() time.Duration {
	if p.SpawnBackoffMaxSeconds > 0 {
		return time.Duration(p.SpawnBackoffMaxSeconds) * time.Second
	}
	return spawnDefaultBackoffMax
}

func (p PlacementPolicy) spawnWakeBatch() int {
	if p.SpawnWakeBatch > 0 {
		return p.SpawnWakeBatch
	}
	return spawnDefaultWakeBatch
}

// spawnRetryBackoff is the wait before re-placement after the request's
// failures-th failed attempt: base × 2^(failures-1), capped at limit, spread
// by ±20 % with r (uniform in [0,1)), and never above limit after the spread.
// failures below 1 counts as 1.
func spawnRetryBackoff(base, limit time.Duration, failures int, r float64) time.Duration {
	delay := base
	for i := 1; i < failures && delay < limit; i++ {
		delay *= 2
	}
	delay = min(delay, limit)
	delay = time.Duration(float64(delay) * (1 + spawnBackoffJitter*(2*r-1)))
	return min(delay, limit)
}

// spawnJitter draws the back-off spread; tests replace HubServer.spawnRand.
func (h *HubServer) spawnJitter() float64 {
	if h.spawnRand != nil {
		return h.spawnRand()
	}
	return rand.Float64()
}

// spawnRetryDelay is the store's back-off after a request's failures-th
// failed attempt, under the current placement policy.
func (h *HubServer) spawnRetryDelay(failures int) time.Duration {
	h.mu.Lock()
	policy := h.placementPolicy
	h.mu.Unlock()
	return spawnRetryBackoff(policy.spawnBackoffBase(), policy.spawnBackoffMax(), failures, h.spawnJitter())
}

// spawnParkRecheck is how long a parked request waits for its timer when no
// event wakes it first: one back-off base, spread like a back-off.
func (h *HubServer) spawnParkRecheck() time.Duration {
	h.mu.Lock()
	policy := h.placementPolicy
	h.mu.Unlock()
	return spawnRetryBackoff(policy.spawnBackoffBase(), policy.spawnBackoffMax(), 1, h.spawnJitter())
}

// spawnParkReason reads a placement refusal (its rc and text) as "no
// candidate can take it now" and names the park reason, or "" for a refusal
// that is not about capacity and ends the request as before.
func spawnParkReason(rc int, text string) string {
	switch {
	case strings.Contains(text, "launch_backoff"):
		return spawnParkLaunchBackoff
	case strings.Contains(text, "concurrency cap"):
		return spawnParkPoolCap
	case rc == 86 || rc == 87 || rc == 89 || strings.Contains(text, "quota_unconfirmed") || strings.Contains(text, "QUOTA_") || strings.Contains(text, "quota_exhausted"):
		return spawnParkQuota
	case strings.Contains(text, "did not connect") || strings.Contains(text, "hub_node_unavailable") || strings.Contains(text, "not connected"):
		return spawnParkNodeDisconnected
	case rc == 81 && (strings.Contains(text, "no awake host admitted") || strings.Contains(text, "no host admitted") || strings.Contains(text, "no placement candidate accepted")):
		return spawnParkHostFull
	case strings.Contains(text, "role_no_candidate") || strings.Contains(text, "NO_ELIGIBLE_HOST") || strings.Contains(text, "no_eligible_host"):
		return spawnParkNoEligibleHost
	}
	return ""
}

// spawnParkUntil caps a next check at the request's limit, so a parked
// request is looked at when it can be given up.
func spawnParkUntil(task hubSpawnTask, next time.Time) time.Time {
	if !task.Expires.IsZero() && task.Expires.Before(next) {
		return task.Expires
	}
	return next
}

// spawnParked reports whether the status API shows the row as parked: an
// accepted, unbound row with a reason whose next check is still ahead.
func spawnParked(task hubSpawnTask, now time.Time) bool {
	return task.Status == "accepted" && task.Node == nil && task.ParkReason != "" && now.Before(task.NextCheck)
}

// parkSpawnTask parks an attempt whose placement found no candidate. It
// reports false when the row moved on meanwhile.
func (h *HubServer) parkSpawnTask(ctx context.Context, task hubSpawnTask, reason, detail string) bool {
	now := h.now().UTC()
	next := spawnParkUntil(task, now.Add(h.spawnParkRecheck()))
	parked, err := h.inbox.store.parkSpawn(ctx, task.Request.RequestID, task.Attempt, reason, next)
	if err != nil || !parked {
		return false
	}
	h.logger.Info("hub spawn parked", "request_id", task.Request.RequestID, "job", task.Job, "reason", reason, "next_check_at", next.Format(time.RFC3339), "refusal", utf8PrefixWithin(detail, 256))
	return true
}

// noteSpawnWake records an event that may let parked requests with these
// reasons place now; the coordinator wakes them on its next tick.
func (h *HubServer) noteSpawnWake(reasons ...string) {
	h.spawnWakeMu.Lock()
	if h.spawnWakes == nil {
		h.spawnWakes = map[string]bool{}
	}
	for _, reason := range reasons {
		h.spawnWakes[reason] = true
	}
	h.spawnWakeMu.Unlock()
	h.kickSpawnPlacement()
}

// wakeParkedSpawns is one tick's wake: it moves the next check of at most
// one wake batch of parked requests, whose reasons an event named, to now.
// When the batch was full the reasons stay noted for the next tick, so a
// flood of parked requests wakes in batches rather than at once.
func (h *HubServer) wakeParkedSpawns(ctx context.Context) {
	h.spawnWakeMu.Lock()
	reasons := make([]string, 0, len(h.spawnWakes))
	for reason := range h.spawnWakes {
		reasons = append(reasons, reason)
	}
	clear(h.spawnWakes)
	h.spawnWakeMu.Unlock()
	if len(reasons) == 0 {
		return
	}
	h.mu.Lock()
	batch := h.placementPolicy.spawnWakeBatch()
	h.mu.Unlock()
	woken, err := h.inbox.store.wakeParkedSpawns(ctx, reasons, batch, h.now().UTC())
	if err != nil || woken >= batch {
		h.spawnWakeMu.Lock()
		if h.spawnWakes == nil {
			h.spawnWakes = map[string]bool{}
		}
		for _, reason := range reasons {
			h.spawnWakes[reason] = true
		}
		h.spawnWakeMu.Unlock()
		h.kickSpawnPlacement()
	}
}

// parkSpawn stores the park of an unbound attempt. A back-off already
// running past next is kept with its reason: a pass that found no other
// candidate never shortens it.
func (s *laneInboxStore) parkSpawn(parent context.Context, id string, attempt int, reason string, next time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET park_reason=CASE WHEN next_check_ms>? AND park_reason<>'' THEN park_reason ELSE ? END,next_check_ms=CASE WHEN next_check_ms>? AND park_reason<>'' THEN next_check_ms ELSE ? END WHERE request_id=? AND attempt=? AND status='accepted' AND node_id IS NULL`), next.UnixMilli(), reason, next.UnixMilli(), next.UnixMilli(), id, attempt)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

// wakeParkedSpawns makes at most limit parked rows with one of reasons due
// at now, earliest next check first, and answers how many it woke.
func (s *laneInboxStore) wakeParkedSpawns(parent context.Context, reasons []string, limit int, now time.Time) (int, error) {
	ctx, done := s.op(parent)
	defer done()
	marks := strings.TrimSuffix(strings.Repeat("?,", len(reasons)), ",")
	args := []any{now.UnixMilli(), now.UnixMilli()}
	for _, reason := range reasons {
		args = append(args, reason)
	}
	args = append(args, limit)
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET next_check_ms=? WHERE request_id IN (SELECT request_id FROM spawn_operations WHERE status='accepted' AND node_id IS NULL AND next_check_ms>? AND park_reason IN (`+marks+`) ORDER BY next_check_ms,request_id LIMIT ?)`), args...)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}

// spawnRetryAt is when a request whose failures-th attempt just failed may
// be placed again; zero without a back-off (a store no hub configured).
func (s *laneInboxStore) spawnRetryAt(task hubSpawnTask, failures int, now time.Time) time.Time {
	if s.spawnRetryDelay == nil {
		return time.Time{}
	}
	return spawnParkUntil(task, now.Add(s.spawnRetryDelay(failures)))
}

// spawnBackoffPark is the stored park of a back-off until retryAt: reason
// launch_backoff and its next check, or none for a zero retryAt.
func spawnBackoffPark(retryAt time.Time) (string, int64) {
	if retryAt.IsZero() {
		return "", 0
	}
	return spawnParkLaunchBackoff, retryAt.UnixMilli()
}
