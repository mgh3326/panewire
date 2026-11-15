package panewire

// One owner notice for a request parked too long (hk 4662; design
// design/2026-11-13/hub-owned-spawn-queue §6 notices, §9b Q2). See
// docs/hub-spawn-operations.md#parked-notice.
//
// An accepted, unbound request whose wait (park_since_ms) reaches
// spawn_park_notice_seconds while it is parked tells its owner lane once: the
// reason, since when, when it is looked at again, and what wakes it sooner.
// Only an urgent request is told — its bound hk task's priority is 1 or 2, the
// same set that makes a spawn urgent (fleet.UrgentTaskPriority) — because a
// P3 wait is what the queue is for.
//
// The notice is at most one per (stretch, reason set): it repeats nothing
// while the request stays parked for the reason it named, and a new reason set
// may send one more. Every judged row, told or not, carries a durable
// judgement marker (park_judged_reason, migration 0019) for the reason set it
// was judged under, so a judged non-urgent row leaves the page instead of
// crowding the earliest-parked head. The notice time (park_notice_ms) is the
// durable notified marker: a restart neither repeats nor loses a notice, and
// the event id names the stretch and reason, so a resend of the same notice is
// the same durable handoffkeep event.
//
// hk's batched states route carries no priority, so the priority is read once
// per wait with one task read (fleet.ReadTaskPriority) and kept on the row
// (park_priority; -1 is not read yet). A task hk no longer has, or an id that
// names none, can never be urgent and reads as 0. A refusal or unreadable
// answer about one task is counted (park_priority_failures) and read as 0 after
// spawnParkPriorityFailures passes, so it cannot hold the page; an hk outage
// stops the pass — every later read would fail the same way — so it costs one
// call, judges nothing, and is retried whole by the next pass.

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
	// spawnParkNoticePage bounds one pass. A judged row leaves the candidate
	// set, so a later page always makes progress on the backlog.
	spawnParkNoticePage = 64
	// spawnParkPriorityFailures is how many passes a task hk answers badly for
	// (an unreadable answer, a per-task refusal) may take before the row is
	// judged not urgent, so one broken task cannot hold the page forever.
	spawnParkPriorityFailures = 3
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

// spawnParkNoticeEvent is the lane event of a parked row's notice. The wait
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
	for _, task := range rows {
		priority := task.ParkPriority
		if priority < 0 {
			read, stop, err := h.readParkedSpawnPriority(ctx, task)
			if err != nil {
				if stop {
					// hk fails every later read the same way: stop the pass,
					// judge nothing, and let the next one retry whole.
					h.logger.Warn("hub spawn parked notice priority unread", "request_id", task.Request.RequestID, "error", err)
					return
				}
				// hk answered badly for this one task: counted, and the pass
				// goes on so the rows behind it are still judged.
				continue
			}
			// The row moved on, so this pass judges nothing.
			if read < 0 {
				continue
			}
			priority = read
		}
		told := fleet.UrgentTaskPriority(priority) && task.Owner != "" && (task.Expires.IsZero() || now.Before(task.Expires))
		if told {
			result := h.relayLaneEventContext(ctx, spawnParkNoticeEvent(task), nil)
			if result.PersistFailed || result.RejectedTooLong {
				// No durable row: leave the row unjudged, and the next pass
				// sends the same event again.
				continue
			}
		}
		changed, err := h.inbox.store.recordSpawnParkJudgement(ctx, task, now, told)
		if err != nil {
			h.logger.Warn("hub spawn parked notice not recorded", "request_id", task.Request.RequestID, "error", err)
			continue
		}
		if !changed {
			// The row moved to another reason set while the notice was being
			// sent; that park is judged by its own pass.
			h.logger.Warn("hub spawn parked notice left unrecorded", "request_id", task.Request.RequestID, "reason", task.ParkReason)
		}
	}
}

// readParkedSpawnPriority reads a parked row's hk priority once and keeps it
// on the row. It answers the priority to judge, or -1 when the row moved on
// (a new wait, or another pass stored the read first). A task hk does not have
// and an id that names none store 0, so the row leaves the candidate set as
// never urgent; a per-task refusal or an unreadable answer is counted and
// stores 0 after spawnParkPriorityFailures passes. stop reports an hk outage,
// which fails every later read the same way, so the caller ends the pass
// without judging anything.
func (h *HubServer) readParkedSpawnPriority(ctx context.Context, task hubSpawnTask) (priority int, stop bool, err error) {
	priority = 0
	if id := hubSpawnArg(task.Request.Args, "--task"); id != "" {
		read, err := fleet.ReadTaskPriority(ctx, id)
		switch {
		case err == nil:
			priority = read
		case fleet.TaskReadPermanent(err):
		case fleet.TaskReadOutage(err):
			return -1, true, err
		default:
			count, gaveUp, failErr := h.inbox.store.recordSpawnParkPriorityFailure(ctx, task, spawnParkPriorityFailures)
			if failErr != nil {
				return -1, false, failErr
			}
			if !gaveUp {
				if count == 1 {
					h.logger.Warn("hub spawn parked notice task unreadable", "request_id", task.Request.RequestID, "error", err)
				}
				return -1, false, nil
			}
			h.logger.Warn("hub spawn parked notice task unreadable; judged not urgent", "request_id", task.Request.RequestID, "passes", count, "error", err)
		}
	}
	changed, err := h.inbox.store.recordSpawnParkPriority(ctx, task, priority)
	if err != nil || !changed {
		return -1, false, err
	}
	return priority, false, nil
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

// parkedSpawnNoticeCandidates lists the parked rows still to judge: accepted,
// unbound, with a reason, past the limit, and not yet judged for the reason
// set they now name. Earliest wait first, at most limit rows.
func (s *laneInboxStore) parkedSpawnNoticeCandidates(parent context.Context, since time.Time, limit int) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE status='accepted' AND node_id IS NULL AND park_reason<>'' AND park_since_ms>0 AND park_since_ms<=? AND park_judged_reason<>park_reason ORDER BY park_since_ms,request_id LIMIT ?`), since.UnixMilli(), limit)
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

// recordSpawnParkPriority stores the priority read for the row's current wait,
// only while it is still unread there: a read that raced a new attempt must
// not judge the new one. A stored read clears the failure count.
func (s *laneInboxStore) recordSpawnParkPriority(parent context.Context, task hubSpawnTask, priority int) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET park_priority=?,park_priority_failures=0 WHERE request_id=? AND attempt=? AND park_since_ms=? AND park_priority<0`), priority, task.Request.RequestID, task.Attempt, task.ParkSince.UnixMilli())
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}

// recordSpawnParkPriorityFailure counts a pass that could not read the row's
// task, while the row is still unread for this wait. It answers the count and
// whether max passes have now failed; the caller then stores priority 0 (never
// urgent) so the row is judged and leaves the candidate page.
func (s *laneInboxStore) recordSpawnParkPriorityFailure(parent context.Context, task hubSpawnTask, max int) (int, bool, error) {
	ctx, done := s.op(parent)
	defer done()
	if _, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET park_priority_failures=park_priority_failures+1 WHERE request_id=? AND attempt=? AND park_since_ms=? AND park_priority<0`), task.Request.RequestID, task.Attempt, task.ParkSince.UnixMilli()); err != nil {
		return 0, false, err
	}
	var count int
	if err := s.db.QueryRowContext(ctx, s.q(`SELECT park_priority_failures FROM spawn_operations WHERE request_id=?`), task.Request.RequestID).Scan(&count); err != nil {
		return 0, false, err
	}
	return count, count >= max, nil
}

// recordSpawnParkJudgement stores the durable judgement of a parked row: the
// reason set it was judged under, and — when the owner was told — when. Both
// last while the row stays parked for that same reason set.
func (s *laneInboxStore) recordSpawnParkJudgement(parent context.Context, task hubSpawnTask, now time.Time, told bool) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	toldFlag := 0
	if told {
		toldFlag = 1
	}
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET park_judged_reason=?,park_notice_ms=CASE WHEN ?<>0 THEN CAST(? AS BIGINT) ELSE park_notice_ms END WHERE request_id=? AND attempt=? AND park_reason=? AND status='accepted' AND node_id IS NULL`), task.ParkReason, toldFlag, now.UnixMilli(), task.Request.RequestID, task.Attempt, task.ParkReason)
	if err != nil {
		return false, err
	}
	n, _ := result.RowsAffected()
	return n == 1, nil
}
