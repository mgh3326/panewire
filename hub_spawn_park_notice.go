package panewire

// One owner notice for a request parked too long (hk 4662; design
// design/2026-11-13/hub-owned-spawn-queue §6 notices, §9b Q2). See
// docs/hub-spawn-operations.md#parked-notice.
//
// An accepted, unbound request whose wait (park_since_ms) reaches
// spawn_park_notice_seconds while it is parked tells its owner lane once: the
// reason, since when, when it is looked at again, and what wakes it sooner.
// The request's own urgency decides whether it is told — the --urgency flag
// the submitter passed, else the bound hk task's priority 1 or 2, exactly as
// placement resolved it at submit — because a P3 wait is what the queue is for.
//
// The pass is a sweep, not a queue head: it examines at most spawnParkNoticePage
// rows from a keyset cursor and moves the cursor past every row it examines,
// whatever that row's outcome. That is the invariant that keeps one row from
// holding the page: a judged row leaves the candidate set (park_judged_reason
// for the reason set it named), a row whose task cannot be read is counted and
// judged after spawnParkPriorityFailures passes, a row whose notice hk refuses
// for good (a 4xx) is judged not told, and a row that cannot be judged this
// pass is picked up again when the sweep wraps. An hk outage stops the pass
// after spawnParkNoticeOutages failed reads, and the cursor resumes after them.
//
// The durable markers (park_judged_reason, park_notice_ms, migration 0019) are
// what a restart reads: it repeats no notice and loses none, and it starts the
// cursor over at the head, which only repeats a sweep. The event id names the
// wait start and the reason set, so a resend of one notice is one durable
// handoffkeep event.

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
	// spawnParkNoticePoll is how often the coordinator runs a sweep. It writes
	// nothing and reads no hk until a row is past the limit.
	spawnParkNoticePoll = 60 * time.Second
	// spawnParkNoticePage bounds one sweep's page; the cursor carries the
	// backlog to later passes.
	spawnParkNoticePage = 64
	// spawnParkPriorityFailures is how many passes a task hk answers badly for
	// (an unreadable answer, a per-task refusal) may take before the row is
	// judged not urgent, so one broken task cannot hold the page.
	spawnParkPriorityFailures = 3
	// spawnParkNoticeOutages is how many failed task reads one sweep tolerates
	// before it stops: one bad record gives one, a down service answers the
	// same for every row, and the cursor spreads the next sweep over later rows.
	spawnParkNoticeOutages = 2
)

// spawnParkNoticeCursor is where the candidate sweep resumes: the keyset
// (wait start, request) of the last examined row. It is hub memory; a restart
// starts at the head, which only repeats a sweep.
type spawnParkNoticeCursor struct {
	since   int64
	request string
}

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
	spawnParkQuotaReset:       "the limit's end time",
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
		Notice:     ownerNoticeFor(task.Job, "spawn_park", false),
	}
}

// spawnParkNoticeText is what the owner lane reads: reason, since when, the
// next look, and what would unpark it.
func spawnParkNoticeText(task hubSpawnTask) string {
	return fmt.Sprintf("[panewire] SPAWN_PARKED %s request=%s reason=%s parked_since=%s next_check_at=%s unpark=%s",
		task.Job, task.Request.RequestID, task.ParkReason, task.ParkSince.UTC().Format(time.RFC3339),
		task.NextCheck.UTC().Format(time.RFC3339), spawnParkUnpark(task.ParkReason))
}

// checkParkedSpawnNotices judges one sweep of parked requests, rate-limited to
// spawnParkNoticePoll by the hub's clock, and tells the owner of each urgent
// one once.
func (h *HubServer) checkParkedSpawnNotices(ctx context.Context) {
	if h.inbox == nil {
		return
	}
	now := h.now().UTC()
	cursor, ok := h.beginParkedSpawnNoticeCheck(now)
	if !ok {
		return
	}
	h.mu.Lock()
	limit := h.placementPolicy.spawnParkNotice()
	h.mu.Unlock()
	rows, err := h.inbox.store.parkedSpawnNoticeCandidates(ctx, now.Add(-limit), cursor, spawnParkNoticePage)
	if err != nil {
		h.logger.Warn("hub spawn parked notice scan failed", "error", err)
		return
	}
	last := len(rows) < spawnParkNoticePage
	outages := 0
	for _, task := range rows {
		h.advanceParkedSpawnNoticeCursor(task)
		urgent, skip, outage := h.parkedSpawnUrgent(ctx, task, now)
		if outage {
			outages++
			if outages >= spawnParkNoticeOutages {
				h.logger.Warn("hub spawn parked notice priority unread", "request_id", task.Request.RequestID)
				return
			}
			continue
		}
		if skip {
			continue
		}
		told := urgent
		if urgent {
			result := h.relayLaneEventContext(ctx, spawnParkNoticeEvent(task), nil)
			switch {
			case spawnParkNoticeRejected(result):
				// hk refuses this event for good (text or contract): judge the
				// row instead of retrying it at the head of every sweep.
				h.logger.Warn("hub spawn parked notice rejected", "request_id", task.Request.RequestID, "status", result.PersistStatus)
				told = false
			case result.PersistFailed:
				// No durable row (transport or 5xx): leave the row unjudged,
				// and a later sweep sends the same event again.
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
			// sent; that park is judged by its own sweep.
			h.logger.Warn("hub spawn parked notice left unrecorded", "request_id", task.Request.RequestID, "reason", task.ParkReason)
		}
	}
	if last {
		// This was the last page of the sweep; the next pass starts at the head.
		h.resetParkedSpawnNoticeCursor()
	}
}

// spawnParkNoticeRejected reports whether hk refused the notice for good: the
// text no longer fits, or a 4xx the inbox batch path also treats as permanent
// (inboxBatchPermanentlyRejected: auth, timeout and rate-limit statuses stay
// retryable). Anything else — a transport failure, a 5xx, a 401/403/408/425/429
// — leaves the row unjudged for a later sweep.
func spawnParkNoticeRejected(result relayLaneEventResult) bool {
	return result.RejectedTooLong || (result.PersistFailed && inboxBatchPermanentlyRejected(result.PersistStatus))
}

// parkedSpawnUrgent decides whether the row's owner is owed a notice. The
// request's own urgency wins — the --urgency flag the submitter passed, exactly
// as placement read it — and only a request without one asks hk for the task
// priority, once per wait. skip reports a row this pass cannot judge, outage an
// hk read failure that may mean the service is down.
func (h *HubServer) parkedSpawnUrgent(ctx context.Context, task hubSpawnTask, now time.Time) (urgent, skip, outage bool) {
	flag := hubSpawnFlagArg(task.Request.Args, "--urgency")
	priority := task.ParkPriority
	if flag == "" && priority < 0 {
		read, down, err := h.readParkedSpawnPriority(ctx, task)
		if down {
			return false, false, true
		}
		if err != nil || read < 0 {
			return false, true, false
		}
		priority = read
	}
	// A request at its limit ends given_up (with its own owner notice), and one
	// with no owner lane has no one to tell.
	if task.Owner == "" || (!task.Expires.IsZero() && !now.Before(task.Expires)) {
		return false, false, false
	}
	if urgent, explicit := fleet.UrgencyFlag(flag); explicit {
		return urgent, false, false
	}
	return fleet.UrgentTaskPriority(priority), false, false
}

// readParkedSpawnPriority reads a parked row's hk priority once and keeps it on
// the row. It answers the priority to judge, or -1 when the row moved on (a new
// wait, or another pass stored the read first) or could not be read this pass.
// A task hk does not have and an id that names none store 0, so the row leaves
// the candidate set as never urgent; a per-task refusal or an unreadable answer
// is counted and stores 0 after spawnParkPriorityFailures passes. down reports
// an hk outage, which the caller counts so a down service ends the sweep.
func (h *HubServer) readParkedSpawnPriority(ctx context.Context, task hubSpawnTask) (priority int, down bool, err error) {
	priority = 0
	if id := hubSpawnArg(task.Request.Args, "--task"); id != "" {
		read, err := fleet.ReadTaskPriority(ctx, id)
		switch {
		case err == nil:
			priority = read
		case fleet.TaskReadPermanent(err):
		case fleet.TaskReadOutage(err):
			return -1, true, nil
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

// parkedNoticePollAt is when the next parked-notice sweep may run: the poll
// after the last one, or now when none has run (hk 5302 gates the coordinator
// on it while rows are parked).
func (h *HubServer) parkedNoticePollAt(now time.Time) time.Time {
	h.parkNoticeMu.Lock()
	defer h.parkNoticeMu.Unlock()
	if h.parkNoticeCheckedAt.IsZero() {
		return now
	}
	return h.parkNoticeCheckedAt.Add(spawnParkNoticePoll)
}

// beginParkedSpawnNoticeCheck records this pass's time and returns the candidate
// cursor, or false when the poll gate holds this tick.
func (h *HubServer) beginParkedSpawnNoticeCheck(now time.Time) (spawnParkNoticeCursor, bool) {
	h.parkNoticeMu.Lock()
	defer h.parkNoticeMu.Unlock()
	if !h.parkNoticeCheckedAt.IsZero() && now.Before(h.parkNoticeCheckedAt.Add(spawnParkNoticePoll)) {
		return spawnParkNoticeCursor{}, false
	}
	h.parkNoticeCheckedAt = now
	return h.parkNoticeCursor, true
}

// advanceParkedSpawnNoticeCursor moves the sweep past an examined row, whatever
// its outcome: that is what keeps one row from holding the page.
func (h *HubServer) advanceParkedSpawnNoticeCursor(task hubSpawnTask) {
	h.parkNoticeMu.Lock()
	defer h.parkNoticeMu.Unlock()
	h.parkNoticeCursor = spawnParkNoticeCursor{since: task.ParkSince.UnixMilli(), request: task.Request.RequestID}
}

// resetParkedSpawnNoticeCursor starts the next sweep at the head.
func (h *HubServer) resetParkedSpawnNoticeCursor() {
	h.parkNoticeMu.Lock()
	defer h.parkNoticeMu.Unlock()
	h.parkNoticeCursor = spawnParkNoticeCursor{}
}

// parkedSpawnNoticeCandidates lists the parked rows still to judge: accepted,
// unbound, with a reason, past the limit, not yet judged for the reason set
// they now name, and after the cursor. Ordered by wait, at most limit rows.
func (s *laneInboxStore) parkedSpawnNoticeCandidates(parent context.Context, since time.Time, after spawnParkNoticeCursor, limit int) ([]hubSpawnTask, error) {
	ctx, done := s.op(parent)
	defer done()
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT `+spawnTaskColumns+` FROM spawn_operations WHERE status='accepted' AND node_id IS NULL AND park_reason<>'' AND park_since_ms>0 AND park_since_ms<=? AND park_judged_reason<>park_reason AND (park_since_ms>? OR (park_since_ms=? AND request_id>?)) ORDER BY park_since_ms,request_id LIMIT ?`), since.UnixMilli(), after.since, after.since, after.request, limit)
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
	n, _ := s.dialect.rowsMatched(result)
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
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE spawn_operations SET park_judged_reason=?,park_notice_ms=CASE WHEN ?<>0 THEN `+s.dialect.castInt("?")+` ELSE park_notice_ms END WHERE request_id=? AND attempt=? AND park_reason=? AND status='accepted' AND node_id IS NULL`), task.ParkReason, toldFlag, now.UnixMilli(), task.Request.RequestID, task.Attempt, task.ParkReason)
	if err != nil {
		return false, err
	}
	n, _ := s.dialect.rowsMatched(result)
	return n == 1, nil
}
