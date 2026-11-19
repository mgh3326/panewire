package panewire

// Contract: docs/hub-spawn-operations.md, "Owner notices by cause" (hk 5360,
// operator decision 433, split agreed with mergewake-design).
//
// A spawn failure the requester replays by itself (replay=yes) is not sent to
// a design session's inbox. It is recorded and counted per cause in the daily
// loops row (spawn_notices). A design owner hears it only when:
//
//   - the request is given up (given_up, or a failure that cannot be replayed);
//   - the same cause repeats for the same owner, spawnNoticeRepeatCount times
//     within spawnNoticeRepeatWindow: that third occurrence notifies once;
//   - the cause is on relay.spawn_notice_must_fix, a cause someone must fix.
//
// A requester may instead pass --owner-notify final|never (fleet.OwnerNotifyArg),
// which overrides those rules for the owner lane only: never silences every
// notice the spawn produces for the owner; final sends only a terminal or
// given-up failure. The requester's own copy of a failure is never silenced,
// because that copy is what makes the requester replay.

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

const (
	// spawnNoticeRepeatWindow and spawnNoticeRepeatCount: the same cause
	// occurring spawnNoticeRepeatCount times for one owner within the window
	// is itself something the session must hear once.
	spawnNoticeRepeatWindow = 30 * time.Minute
	spawnNoticeRepeatCount  = 3
	// spawnNoticeMemoTTL bounds how long a routed event id is remembered, so a
	// retried outbox row decides and counts the same way. It is longer than
	// the window and the retry horizon of a stuck row.
	spawnNoticeMemoTTL = 2 * time.Hour
)

// defaultSpawnNoticeMustFix is the built-in must-fix cause list: causes a
// person has to repair, so they are never silenced for repetition or replay.
// The policy file's relay.spawn_notice_must_fix replaces it.
var defaultSpawnNoticeMustFix = []string{"workspace_unresolved", "auth_unavailable", "report_path", "brief_upload", "identity_unproven"}

// spawnNoticeCauseShape is a cause key as spawnNoticeCause produces it: a
// refusal code (fleet's closed table) or a notice's own cause word, optionally
// with its detail after one colon, as the documented checkout reasons read
// (checkout_refused:branch-elsewhere).
var spawnNoticeCauseShape = regexp.MustCompile(`^[a-z0-9_]{1,40}(:[a-z0-9_-]{1,40})?$`)

func validSpawnNoticeCause(cause string) bool {
	return spawnNoticeCauseShape.MatchString(cause)
}

// spawnNoticeCause is the cause key of a failure's reason: the refusal code
// after spawn_failed:, or the bare reason (timeout, checkout_refused:locked).
func spawnNoticeCause(reason string) string {
	code, _, _ := strings.Cut(reason, ": ")
	return strings.TrimPrefix(code, "spawn_failed:")
}

// spawnNoticeOutcomeCause is the cause key an outcome is counted under. A
// success is counted as spawned, or lane_unregistered when the pane's lane is
// not routed; a failure by its reason's cause.
func spawnNoticeOutcomeCause(result hubSpawnOutcome) string {
	if result.RC == 0 {
		if result.LaneUnregistered {
			return "lane_unregistered"
		}
		return "spawned"
	}
	return spawnNoticeCause(result.Reason)
}

// ownerNotifyMode is the spawn's --owner-notify mode from its saved request:
// "final", "never", or "" when the flag is absent or not a valid value.
func ownerNotifyMode(args []string) string {
	mode := hubSpawnArg(args, fleet.OwnerNotifyArg)
	if !fleet.ValidOwnerNotify(mode) {
		return ""
	}
	return mode
}

// hasDistinctRequester reports whether the spawn names a submitting lane other
// than its owner. Only then does the requester hold its own copy of a failure.
func hasDistinctRequester(task hubSpawnTask) bool {
	requester := spawnSubmitterLane(task.Request.Args)
	return requester != "" && requester != task.Owner
}

// spawnNoticeSilence is the in-process state of the silence rules: the recent
// silent occurrences per owner and cause, the event ids already decided and
// the ids already counted. A hub restart clears it, as it clears the daily
// counts; a restart therefore restarts the repeat window.
type spawnNoticeSilence struct {
	mu       sync.Mutex
	windows  map[string][]spawnNoticeSeen
	notified map[string]time.Time
	counted  map[string]time.Time
}

type spawnNoticeSeen struct {
	id string
	at time.Time
}

// repeated records one silent occurrence of key (an owner and cause) for event
// id and reports whether it is the spawnNoticeRepeatCount-th within the window.
// The same id always gets the same answer, so a retried row cannot notify
// twice or lose its notice.
func (s *spawnNoticeSilence) repeated(key, id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(now)
	if _, ok := s.notified[id]; ok {
		return true
	}
	recent := s.windows[key]
	for _, seen := range recent {
		if seen.id == id {
			return false
		}
	}
	recent = append(recent, spawnNoticeSeen{id: id, at: now})
	if len(recent) < spawnNoticeRepeatCount {
		s.windows[key] = recent
		return false
	}
	// The repeat notifies: the window starts over, and this id is remembered as
	// the one that notified.
	delete(s.windows, key)
	s.notified[id] = now
	return true
}

// firstCount reports whether id has not been counted before, and marks it
// counted. A silenced event is counted once however often its row is routed.
func (s *spawnNoticeSilence) firstCount(id string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prune(now)
	if _, ok := s.counted[id]; ok {
		return false
	}
	s.counted[id] = now
	return true
}

// prune drops window entries older than the repeat window and memo entries
// older than the memo TTL. The caller holds s.mu.
func (s *spawnNoticeSilence) prune(now time.Time) {
	if s.windows == nil {
		s.windows = make(map[string][]spawnNoticeSeen)
		s.notified = make(map[string]time.Time)
		s.counted = make(map[string]time.Time)
	}
	for key, recent := range s.windows {
		kept := recent[:0]
		for _, seen := range recent {
			if now.Sub(seen.at) < spawnNoticeRepeatWindow {
				kept = append(kept, seen)
			}
		}
		if len(kept) == 0 {
			delete(s.windows, key)
		} else {
			s.windows[key] = kept
		}
	}
	for id, at := range s.notified {
		if now.Sub(at) >= spawnNoticeMemoTTL {
			delete(s.notified, id)
		}
	}
	for id, at := range s.counted {
		if now.Sub(at) >= spawnNoticeMemoTTL {
			delete(s.counted, id)
		}
	}
}

// mustFix reports whether a cause is on the policy's must-fix list.
func (p *deliveryPolicy) spawnNoticeMustFix(cause string) bool {
	for _, must := range p.Relay.SpawnNoticeMustFix {
		if must == cause {
			return true
		}
	}
	return false
}

// spawnNoticeStatus is the status an outbox row's event id names
// (<request>:<attempt>:<status>[:<lane>]). A row is judged by its own status:
// the request's current status can be later than the row's (hk 5360 review).
func spawnNoticeStatus(id string) string {
	parts := strings.SplitN(id, ":", 4)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// spawnNoticeSuperseded reports whether a retryable failure row has been
// overtaken: the request moved to a later attempt, or its current attempt ended
// with another outcome. Such a row is settled without a notice or a count; the
// outcome that overtook it is what the owner hears.
func spawnNoticeSuperseded(n spawnNotification, task hubSpawnTask) bool {
	return spawnNoticeStatus(n.id) == "retryable" && (task.Attempt > spawnNoticeAttempt(n.id) || task.Status != "retryable")
}

// spawnFailureNoticeAction decides a failure's notice for a design owner when
// the spawn names no --owner-notify mode: the cause rules.
func (h *HubServer) spawnFailureNoticeAction(n spawnNotification, task hubSpawnTask, result hubSpawnOutcome) spawnNoticeAction {
	cause := spawnNoticeCause(result.Reason)
	switch {
	case spawnNoticeSuperseded(n, task):
		return spawnNoticeSettle
	case !result.Retryable:
		return spawnNoticeDeliver // replay=no: nothing the requester can replay
	case spawnNoticeStatus(n.id) == "given_up":
		return spawnNoticeDeliver
	case n.owner == task.Owner && !hasDistinctRequester(task):
		return spawnNoticeDeliver // the owner's copy is the only notice it can act on
	case h.inbox.policy.spawnNoticeMustFix(cause):
		return spawnNoticeDeliver
	case h.noticeSilence.repeated(n.owner+"\x00"+cause, n.id, h.now()):
		return spawnNoticeDeliver
	}
	return spawnNoticeCount
}

// ownerModeNoticeAction decides a notice for the owner lane of a spawn that
// names --owner-notify. A requester's copy is not decided here: it always
// reaches the requester, which replays from it.
func (h *HubServer) ownerModeNoticeAction(n spawnNotification, task hubSpawnTask, result hubSpawnOutcome, mode string) spawnNoticeAction {
	if result.RC == 0 {
		// SPAWNED and LANE_UNREGISTERED are not terminal: neither mode sends them.
		return spawnNoticeCount
	}
	if spawnNoticeSuperseded(n, task) {
		return spawnNoticeSettle
	}
	if mode == fleet.OwnerNotifyNever {
		return spawnNoticeCount
	}
	// final: only a failure that cannot be replayed, or a given-up request.
	if !result.Retryable || spawnNoticeStatus(n.id) == "given_up" {
		return spawnNoticeDeliver
	}
	return spawnNoticeCount
}

// countSilencedSpawnNotice counts one silenced notice for its owner lane and
// cause, once per event id, in the daily loops row.
func (h *HubServer) countSilencedSpawnNotice(id, lane, cause string) {
	if !h.noticeSilence.firstCount(id, h.now()) {
		return
	}
	h.noteSpawnNoticeSilenced(lane, cause)
}

// ownerNotifyJobSilenced reports whether a job event for the owner lane of a
// spawn that names --owner-notify is held back. Only job.completed is covered
// here: a completion is not a terminal notice, so neither mode sends it.
func (h *HubServer) ownerNotifyJobSilenced(ctx context.Context, kind string, event hubJobEventPayload, lane string) bool {
	if kind != "job.completed" {
		return false
	}
	return h.ownerNotifyWithheld(ctx, event.JobID, lane)
}

// ownerNotifyWithheld reports whether the spawn that started job names an
// --owner-notify mode and owner is that spawn's owner lane. An unreadable spawn
// record withholds nothing: the notice takes the ordinary path.
func (h *HubServer) ownerNotifyWithheld(ctx context.Context, job, owner string) bool {
	if h.inbox == nil || job == "" || owner == "" {
		return false
	}
	task, err := h.inbox.store.latestSpawnForJob(ctx, job)
	if err != nil {
		return false
	}
	return ownerNotifyMode(task.Request.Args) != "" && owner == task.Owner
}

// ownerNotifyRecordReason is the reason a withheld completion is filed under.
// hk's idempotency key is (kind, job, epoch, report path, reason) and excludes
// the event id, so the event id is part of the reason: a second completion of
// the same job gets its own row, not the first one's (mergewake review of
// PR 732). An event without an id keeps the bare reason.
func ownerNotifyRecordReason(eventID string) string {
	if eventID == "" {
		return "owner-notify"
	}
	return "owner-notify:" + eventID
}

// receiveOwnerNotifySilenced files a silenced job event as an hk row under its
// own reason, the same record the design filter writes, and counts it. The
// producer is acknowledged and the event counted only once that row is filed
// under this event; if hk returns no such row, the event is not acknowledged
// and not counted, so the producer's retry finds it again. handled is false only
// when no hk client is configured, so the event takes the ordinary path (the
// design filter's rule too: nothing is recorded, so nothing is silenced).
func (h *HubServer) receiveOwnerNotifySilenced(kind string, event *hubJobEventPayload, sender *hubAgent, senderID, lane string) (result relayLaneEventResult, handled bool) {
	if h.handoffkeep == nil {
		return relayLaneEventResult{}, false
	}
	reason := ownerNotifyRecordReason(event.EventID)
	// hk's idempotency reason is the record's own Reason field, not the delivered
	// label, so the copy filed carries the event id there.
	recorded := *event
	recorded.Reason = reason
	stored, filed, ok := h.fileFilteredInHK(h.closeCtx, kind, recorded, senderID, reason)
	if !ok || !filed {
		h.logger.Warn("owner-notify record not filed under this event; not acknowledged", "kind", kind, "job", event.JobID, "event_id", event.EventID, "lane", lane)
		return relayLaneEventResult{PersistFailed: true}, true
	}
	h.countSilencedSpawnNotice(event.EventID, lane, "job_completed")
	if sender != nil {
		if kind == "lane.event" {
			h.queueLaneRelayPersisted(*event, sender, stored.ID)
		} else {
			h.queueRelayPersisted(kind, *event, sender, stored.ID)
		}
	}
	return relayLaneEventResult{ID: stored.ID, Routed: true, Machine: filteredDeliveryPrefix + reason, AlreadyDelivered: true}, true
}
