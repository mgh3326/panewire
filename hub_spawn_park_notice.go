package panewire

// One owner notice for a request parked too long (hk 4662; design
// design/2026-11-13/hub-owned-spawn-queue §6 notices, §9b Q2). See
// docs/hub-spawn-operations.md#parked-notice.
//
// An accepted, unbound request that has been parked in one unbroken stretch
// for longer than spawn_park_notice_seconds tells its owner lane once: the
// reason, since when, when it is looked at again, and what wakes it sooner.
// Only an urgent request is told — its bound hk task's priority is 1 or 2, the
// same set that makes a spawn urgent (fleet.UrgentTaskPriority) — because a
// P3 wait is what the queue is for.
//
// The notice is at most one per (stretch, reason set): it repeats nothing
// while the request stays parked for the reason it named, and a new reason set
// may send one more. The marker is durable (park_notice_ms and
// park_notice_reason on the row, migration 0019), so a hub restart neither
// repeats nor loses it; the event id also names the stretch and reason, so a
// resend of the same notice is the same durable handoffkeep event.
//
// hk's batched states route carries no priority, so the priority is read once
// per stretch with one task read (fleet.ReadTaskPriority) and kept on the row
// (park_priority; -1 is not read yet). A failed read keeps it unread so the
// next pass tries again; a request with no task is never urgent.

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	// spawnDefaultParkNotice is how long one unbroken park lasts before an
	// urgent request's owner hears of it (design §9b Q2: one line after
	// 30 min; unmeasured).
	spawnDefaultParkNotice = 30 * time.Minute
	// maxSpawnParkNoticeSec bounds the placement.json override.
	maxSpawnParkNoticeSec = 24 * 3600
	// spawnParkNoticePoll is how often the coordinator looks at parked rows
	// for a due notice. The pass writes nothing until a row is past the
	// limit, and reads hk only for rows it has not judged yet.
	spawnParkNoticePoll = 60 * time.Second
	// spawnParkNoticePage bounds one pass: a parked flood notifies the
	// earliest-parked rows first and the rest on later ticks.
	spawnParkNoticePage = 64
)

// spawnParkNotice is how long a park must last before its owner is told.
func (p PlacementPolicy) spawnParkNotice() time.Duration {
	if p.SpawnParkNoticeSeconds > 0 {
		return time.Duration(p.SpawnParkNoticeSeconds) * time.Second
	}
	return spawnDefaultParkNotice
}

// spawnParkWakes is what ends each park reason early, as the notice tells the
// owner (the wake column of the park table in docs/hub-spawn-operations.md).
var spawnParkWakes = map[string]string{
	spawnParkLaunchBackoff:    "its back-off timer",
	spawnParkPoolCap:          "a job leaving a node's active set",
	spawnParkQuota:            "a finished quota refresh with capacity",
	spawnParkHostFull:         "a node connecting or a job leaving a node's active set",
	spawnParkNoEligibleHost:   "a node connecting with a profile that passes the role rules",
	spawnParkNodeDisconnected: "the node reconnecting",
}

// spawnParkUnpark names what wakes a parked request, for every reason in the
// stored set (comma-separated, first primary).
func spawnParkUnpark(reason string) string {
	wakes := make([]string, 0, 2)
	for _, r := range strings.Split(reason, ",") {
		if wake, ok := spawnParkWakes[r]; ok {
			wakes = append(wakes, wake)
		}
	}
	if len(wakes) == 0 {
		return "a later placement pass"
	}
	return strings.Join(wakes, "; ")
}

// spawnParkNoticeEvent is the lane event of a parked row's notice. The stretch
// start and the reason set both go in, so a resend of one notice is one
// durable event while a new reason is a new event.
func spawnParkNoticeEvent(task hubSpawnTask) hubJobEventPayload {
	return hubJobEventPayload{
		EventID:    fmt.Sprintf("spawn-park:%s:%d:%s", task.Request.RequestID, task.ParkSince.UnixMilli(), task.ParkReason),
		JobID:      task.Job,
		OwnerLane:  task.Owner,
		NotifyType: "info",
		Text:       utf8PrefixWithin(spawnParkNoticeText(task), laneEventTextLimit),
		Host:       "hub",
		Epoch:      1,
	}
}

// spawnParkNoticeText is what the owner lane reads: reason, since when, the
// next look, and what would unpark it.
func spawnParkNoticeText(task hubSpawnTask) string {
	return fmt.Sprintf("[panewire] SPAWN_PARKED %s request=%s reason=%s parked_since=%s next_check_at=%s unpark=%s",
		task.Job, task.Request.RequestID, task.ParkReason, task.ParkSince.UTC().Format(time.RFC3339),
		task.NextCheck.UTC().Format(time.RFC3339), spawnParkUnpark(task.ParkReason))
}

// checkParkedSpawnNotices tells the owner lane of each urgent request parked
// past the limit once, rate-limited to spawnParkNoticePoll by the hub's clock.
func (h *HubServer) checkParkedSpawnNotices(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	now := h.now().UTC()
	if !h.beginParkedSpawnNoticeCheck(now) {
		return
	}
	h.mu.Lock()
	limit := h.placementPolicy.spawnParkNotice()
	h.mu.Unlock()
	rows, err := h.inbox.store.parkedSpawnNoticeCandidates(ctx, now.Add(-limit), spawnParkNoticePage)
	if err != nil {
		h.logger.Warn("hub spawn parked notice scan failed", "error", err)
		return
	}
	unread, firstErr := 0, error(nil)
	for _, task := range rows {
		priority := task.ParkPriority
		if priority < 0 {
			read, err := h.readParkedSpawnPriority(ctx, task)
			if err != nil {
				unread++
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			// The row moved on, so this pass judges nothing.
			if read < 0 {
				continue
			}
			priority = read
		}
		if !fleet.UrgentTaskPriority(priority) {
			continue
		}
		// A request at its limit ends given_up (with its own owner notice), and
		// one with no owner lane has no one to tell.
		if task.Owner == "" || (!task.Expires.IsZero() && !now.Before(task.Expires)) {
			continue
		}
		result := h.relayLaneEventContext(ctx, spawnParkNoticeEvent(task), nil)
		if result.PersistFailed || result.RejectedTooLong {
			// No durable row: the next pass sends the same event again.
			continue
		}
		if _, err := h.inbox.store.recordSpawnParkNotice(ctx, task, now); err != nil {
			h.logger.Warn("hub spawn parked notice not recorded", "request_id", task.Request.RequestID, "error", err)
		}
	}
	if unread > 0 {
		h.logger.Warn("hub spawn parked notice priority unread", "requests", unread, "error", firstErr)
	}
}

// readParkedSpawnPriority reads a parked row's hk priority once and keeps it
// on the row. It answers the priority to judge, or -1 when the row moved on
// (a new stretch, or another pass stored the read first). A row with no bound
// task can never be urgent and stores 0 (not urgent) so it is not read again.
func (h *HubServer) readParkedSpawnPriority(ctx context.Context, task hubSpawnTask) (int, error) {
	priority := 0
	if id := hubSpawnArg(task.Request.Args, "--task"); id != "" {
		read, err := fleet.ReadTaskPriority(ctx, id)
		if err != nil {
			return -1, err
		}
		priority = read
	}
	changed, err := h.inbox.store.recordSpawnParkPriority(ctx, task, priority)
	if err != nil || !changed {
		return -1, err
	}
	return priority, nil
}

// beginParkedSpawnNoticeCheck records this pass's time and reports whether it
// may run; the rate limit keeps a parked queue from one scan per coordinator
// tick (2 s).
func (h *HubServer) beginParkedSpawnNoticeCheck(now time.Time) bool {
	h.parkNoticeMu.Lock()
	defer h.parkNoticeMu.Unlock()
	if !h.parkNoticeCheckedAt.IsZero() && now.Before(h.parkNoticeCheckedAt.Add(spawnParkNoticePoll)) {
		return false
	}
	h.parkNoticeCheckedAt = now
	return true
}

// parkedSpawnNoticeCandidates lists the parked rows whose owner notice is
// still owed and whose stretch began at or before since: accepted, unbound,
// with a reason and a stretch start, and not yet told of this reason set,
// earliest stretch first.
func (s *laneInboxStore) parkedSpawnNoticeCandidates(parent context.Context, since time.Time, limit int) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE status='accepted' AND node_id IS NULL AND park_reason<>'' AND park_since_ms>0 AND park_since_ms<=? AND (park_notice_ms=0 OR park_notice_reason<>park_reason) ORDER BY park_since_ms,request_id LIMIT ?`), since.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []hubSpawnTask
	for rows.Next() {
		task, err := scanSpawnTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

// recordSpawnParkPriority stores the priority read for the row's current
// stretch, only while it is still unread there: a read that raced a new
// stretch must not judge the new one.
func (s *laneInboxStore) recordSpawnParkPriority(parent context.Context, task hubSpawnTask, priority int) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET park_priority=? WHERE request_id=? AND attempt=? AND park_since_ms=? AND park_priority<0`), priority, task.Request.RequestID, task.Attempt, task.ParkSince.UnixMilli())
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

// recordSpawnParkNotice stores the durable notified marker: when the owner was
// told and the reason set it named, for as long as the row stays parked in the
// same stretch for the same reason set.
func (s *laneInboxStore) recordSpawnParkNotice(parent context.Context, task hubSpawnTask, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET park_notice_ms=?,park_notice_reason=? WHERE request_id=? AND attempt=? AND park_since_ms=? AND park_reason=? AND status='accepted' AND node_id IS NULL`), now.UnixMilli(), task.ParkReason, task.Request.RequestID, task.Attempt, task.ParkSince.UnixMilli(), task.ParkReason)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}
