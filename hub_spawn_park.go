package panewire

import (
	"context"
	"errors"
	"maps"
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
	// spawnParkQuotaReset holds a request until a free limit's end time, which
	// fits inside the request's time limit (hk 5358). Like launch_backoff it is
	// timer-only: an event that wakes quota parks must not start the request on
	// another pool before the wait it chose has run.
	spawnParkQuotaReset = "quota_reset"
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
// that is not about capacity and ends the request as before. A refusal with
// several blockers names each, comma-separated, the first one primary: every
// one of their events wakes the request.
func spawnParkReason(rc int, text string) string {
	switch {
	// An escalation asks the parent lane to act: it ends the request and
	// notifies, never a silent park.
	case rc == 87 || strings.Contains(text, "role_exhausted_escalate"):
		return ""
	// Hub placement never spawns locally: every remote candidate tried and
	// none completed. The tried notes name why; a mock lane is refused by
	// every remote for good.
	case rc == 80 && strings.Contains(text, "no remote candidate completed"):
		if strings.Contains(text, "mock_lane_remote_refused") {
			return ""
		}
		var reasons []string
		for _, c := range []struct {
			reason string
			tokens []string
		}{
			{spawnParkHostFull, []string{"task_slots_full", "active_jobs>=max", "lanes_full"}},
			{spawnParkNodeDisconnected, []string{"disconnected"}},
			{spawnParkQuota, []string{"quota"}},
			{spawnParkLaunchBackoff, []string{"launch_backoff"}},
		} {
			for _, token := range c.tokens {
				if strings.Contains(text, token) {
					reasons = append(reasons, c.reason)
					break
				}
			}
		}
		if len(reasons) == 0 {
			return spawnParkNoEligibleHost
		}
		return strings.Join(reasons, ",")
	case strings.Contains(text, "launch_backoff"):
		return spawnParkLaunchBackoff
	case strings.Contains(text, "concurrency cap"):
		return spawnParkPoolCap
	case rc == 86 || rc == 89 || strings.Contains(text, "quota_unconfirmed") || strings.Contains(text, "QUOTA_") || strings.Contains(text, "quota_exhausted"):
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

// spawnParkPrimary is the reason the status API shows: the first named.
func spawnParkPrimary(reason string) string {
	primary, _, _ := strings.Cut(reason, ",")
	return primary
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
// reports false when the row moved on meanwhile, and the store error when
// the write failed or its result is unknown: the row is then still accepted
// and a later pass parks it again. before is the per-reason wake count read
// before placement began: an event for one of this park's reasons noted
// since then may have been drained before the park landed, so that reason
// is noted again (not as a new event, and with no kick) for the next tick.
func (h *HubServer) parkSpawnTask(ctx context.Context, task hubSpawnTask, reason, detail string, before map[string]uint64) (bool, error) {
	now := h.now().UTC()
	next := spawnParkUntil(task, now.Add(h.spawnParkRecheck()))
	parked, err := h.inbox.store.parkSpawn(ctx, task.Request.RequestID, task.Attempt, reason, next, now)
	if err != nil || !parked {
		return false, err
	}
	h.spawnWakeMu.Lock()
	for _, r := range strings.Split(reason, ",") {
		if h.spawnWakeCounts[r] != before[r] {
			h.markSpawnWakeLocked(false, r)
		}
	}
	h.spawnWakeMu.Unlock()
	h.logger.Info("hub spawn parked", "request_id", task.Request.RequestID, "job", task.Job, "reason", reason, "next_check_at", next.Format(time.RFC3339), "refusal", utf8PrefixWithin(detail, 256))
	return true, nil
}

// spawnWakeSnapshot copies the per-reason wake counts.
func (h *HubServer) spawnWakeSnapshot() map[string]uint64 {
	h.spawnWakeMu.Lock()
	defer h.spawnWakeMu.Unlock()
	return maps.Clone(h.spawnWakeCounts)
}

// noteSpawnWake records an event that may let parked requests with these
// reasons place now, and kicks the coordinator to wake them.
func (h *HubServer) noteSpawnWake(reasons ...string) {
	h.spawnWakeMu.Lock()
	for _, reason := range reasons {
		h.markSpawnWakeLocked(true, reason)
	}
	h.spawnWakeMu.Unlock()
	h.kickSpawnPlacement()
}

// markSpawnWakeLocked notes reason pending under spawnWakeMu, counting it as
// an event when event is set; the pending set and the counts change
// together, so a park can never miss a drained wake. launch_backoff and
// quota_reset are timer-only: no event wakes them.
func (h *HubServer) markSpawnWakeLocked(event bool, reason string) {
	if reason == spawnParkLaunchBackoff || reason == spawnParkQuotaReset {
		return
	}
	if h.spawnWakes == nil {
		h.spawnWakes = map[string]bool{}
		h.spawnWakeCounts = map[string]uint64{}
	}
	h.spawnWakes[reason] = true
	if event {
		h.spawnWakeCounts[reason]++
	}
}

// wakeParkedSpawns is one pass's wake: it moves the next check of at most
// one wake batch of parked requests, whose reasons an event named, to now.
// After a full batch (or a store error) the reasons stay noted and the next
// batch waits for a timer tick (ticked), however often the coordinator runs
// in between, so a flood of parked requests wakes one batch per tick.
func (h *HubServer) wakeParkedSpawns(ctx context.Context, ticked bool) {
	h.spawnWakeMu.Lock()
	if h.spawnWakeHeld && !ticked {
		h.spawnWakeMu.Unlock()
		return
	}
	h.spawnWakeHeld = false
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
	if err != nil {
		h.loopNoteSilentError(loopSpawnCoordinator, "hub_spawn_park.go:wakeParkedSpawns", err)
		h.spawnWakeMu.Lock()
		for _, reason := range reasons {
			h.markSpawnWakeLocked(false, reason)
		}
		h.spawnWakeHeld = true
		h.spawnWakeMu.Unlock()
		return
	}
	h.loopClearSilent("hub_spawn_park.go:wakeParkedSpawns")
	if woken >= batch {
		h.spawnWakeMu.Lock()
		for _, reason := range reasons {
			h.markSpawnWakeLocked(false, reason)
		}
		h.spawnWakeHeld = true
		h.spawnWakeMu.Unlock()
	}
}

// parkSpawn stores the park of an unbound attempt. A back-off already
// running past next is kept with its reason: a pass that found no other
// candidate never shortens it. The first park of a wait starts it at now and
// forgets any priority read; a later park keeps both, a bind-and-refusal
// included, so the wait that is counted is the whole one (the expressions
// read the row's old values).
//
// MySQL assigns a SET list left to right, so a later expression sees the
// columns an earlier one set; Postgres and SQLite read the old row for all of
// them. TestHubUpdatesReadNoEarlierAssignment holds every update to the order
// that gives the same answer on both. Here park_priority, its failure count
// and park_since_ms, which they read, come last; park_reason and next_check_ms
// read each other, so next_check_ms goes first and park_reason reads it after:
// a row that keeps its wake keeps it unchanged, a row that does not has just
// been set to next, which the same test (next_check_ms>next) fails.
func (s *laneInboxStore) parkSpawn(parent context.Context, id string, attempt int, reason string, next, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	tx, err := s.beginTx(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(`UPDATE spawn_operations SET next_check_ms=CASE WHEN next_check_ms>? AND park_reason<>'' THEN next_check_ms ELSE ? END,park_reason=CASE WHEN next_check_ms>? AND park_reason<>'' THEN park_reason ELSE ? END,park_priority=CASE WHEN park_since_ms=0 THEN -1 ELSE park_priority END,park_priority_failures=CASE WHEN park_since_ms=0 THEN 0 ELSE park_priority_failures END,park_since_ms=CASE WHEN park_since_ms=0 THEN `+s.dialect.castInt("?")+` ELSE park_since_ms END WHERE request_id=? AND attempt=? AND status='accepted' AND node_id IS NULL`), next.UnixMilli(), next.UnixMilli(), next.UnixMilli(), reason, now.UnixMilli(), id, attempt)
	if err != nil {
		return false, err
	}
	if n, _ := s.dialect.rowsMatched(result); n != 1 {
		return false, nil
	}
	if err = s.syncSpawnQueueNextCheck(ctx, tx, id); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// wakeParkedSpawns makes at most limit parked rows with one of reasons due
// at now, earliest next check first, and answers how many it woke.
func (s *laneInboxStore) wakeParkedSpawns(parent context.Context, reasons []string, limit int, now time.Time) (int, error) {
	ctx, done := s.op(parent)
	defer done()
	// park_reason may name several reasons, comma-separated: match each.
	marks := strings.TrimSuffix(strings.Repeat("("+s.dialect.concat("','", "park_reason", "','")+") LIKE ? OR ", len(reasons)), " OR ")
	args := []any{now.UnixMilli(), now.UnixMilli()}
	for _, reason := range reasons {
		args = append(args, "%,"+reason+",%")
	}
	args = append(args, limit)
	tx, err := s.beginTx(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, s.q(s.dialect.updateOrdered("spawn_operations", "request_id", "next_check_ms=?", `status='accepted' AND node_id IS NULL AND next_check_ms>? AND (`+marks+`)`, "next_check_ms,request_id")), args...)
	if err != nil {
		return 0, err
	}
	n, _ := s.dialect.rowsMatched(result)
	if n > 0 {
		// The woken rows are the queue rows whose due time now differs from
		// the operation's: the queue is the small table, so this reads it only.
		if err = s.syncSpawnQueueNextChecks(ctx, tx); err != nil {
			return 0, err
		}
	}
	if n > 0 {
		return int(n), s.commitNoting(tx, readSpawnScan)
	}
	return int(n), tx.Commit()
}

// spawnRetryAt is when a request whose failures-th attempt just failed may
// be placed again; zero without a back-off (a store no hub configured, or a
// row with no limit, which keeps the legacy deadline contract).
func (s *laneInboxStore) spawnRetryAt(task hubSpawnTask, failures int, now time.Time) time.Time {
	if s.spawnRetryDelay == nil || task.Expires.IsZero() {
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

// errSpawnRollbackDrain refuses a hub-made spawn under --spawn-rollback-drain.
var errSpawnRollbackDrain = errors.New("spawn_rollback_drain")

// spawnRollbackTerminal are the statuses a rollback past parking may leave
// behind: every other spawn row blocks it (docs/hub-spawn-operations.md,
// "Rolling back past parking").
const spawnRollbackTerminal = `'succeeded','failed','retryable','given_up','refused','cancelled'`

// spawnRollbackBlockingRows counts what blocks a rollback past parking: the
// nonterminal spawn rows (any status, *_grant included) and the open
// consults. The rollback gate is 0.
func (s *laneInboxStore) spawnRollbackBlockingRows(parent context.Context) (int, error) {
	ctx, done := s.op(parent)
	defer done()
	var spawns, consults int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM spawn_operations WHERE status NOT IN (`+spawnRollbackTerminal+`)`).Scan(&spawns); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM consult_requests WHERE state IN ('queued','running')`).Scan(&consults); err != nil {
		return 0, err
	}
	return spawns + consults, nil
}

// logSpawnRollbackRows prints the rollback gate count under
// --spawn-rollback-drain, so a rollout script can wait for rollback_rows=0.
func (h *HubServer) logSpawnRollbackRows(ctx context.Context) {
	if !h.spawnRollbackDrain || h.inbox == nil {
		return
	}
	n, err := h.inbox.store.spawnRollbackBlockingRows(ctx)
	if err != nil {
		h.logger.Warn("hub spawn rollback rows unknown", "err", err)
		return
	}
	h.logger.Info("hub spawn rollback rows", "rollback_rows", n, "spawn_rollback_drain", true)
}
