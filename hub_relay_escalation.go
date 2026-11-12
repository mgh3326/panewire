package panewire

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Contract: docs/relay-harness-delivery.md.
//
// hk 3276: review findings, ESC answers and orders go straight to the job's
// worker. When the worker's node gives up on one -- herdr kept rejecting the
// send, or the pane never showed the text taken -- nobody else knows the worker
// did not get it. The hub then tells the job's design lane once, naming the
// event, instead of leaving a quiet gap.
//
// hk 3431: only when someone can act on it, and at most once per window. A
// worker lane whose hub jobs have all ended, or whose live job's hk task has
// ended, is logged instead (unconfirmedNoticeMoot); drops inside the window
// are folded into the next notice (unconfirmedNoticeWindows).

const (
	// unconfirmedEscalationPrefix starts the producer event id of the
	// escalation. It is deterministic in the durable row id, so a second drop
	// of the same row (a replay that also fails) re-posts the same lane.event
	// and the durable idempotency answers it as a duplicate.
	unconfirmedEscalationPrefix = "delivery-unconfirmed-"
	// relayDroppedUnconfirmed are the relay.dropped reasons that end a delivery
	// the node could not prove: the retry budget spent on rejected sends, and a
	// send that may already be in the pane and is never re-injected.
	relayDroppedRetriesExhausted = "inject_failed_max_attempts"
	relayDroppedMaybeInPane      = "maybe_in_pane"
	relayDroppedCheckPane        = "delivery_uncertain_check_pane"
	relayDroppedPaneReadFailed   = "pane_read_failed"
	// relayDroppedWorkingExpired ends a row held for a working cmd pane that
	// never turned idle within relay.cmd_hold_max (hk 3370, hk 3411). Nothing
	// was typed; the next idle wake re-delivers it once
	// (hub_relay_idle_redelivery.go).
	relayDroppedWorkingExpired = "working_wait_expired"
)

// unconfirmedEscalationBackoff spaces the retries of an escalation whose
// durable write failed (a handoffkeep outage). The drop was already consumed,
// so nothing else would send it; the last wait is the final try.
var unconfirmedEscalationBackoff = []time.Duration{2 * time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}

func relayDroppedIsUnconfirmed(reason string) bool {
	return reason == relayDroppedRetriesExhausted || reason == relayDroppedMaybeInPane || reason == relayDroppedCheckPane || reason == relayDroppedPaneReadFailed || reason == relayDroppedWorkingExpired
}

// designLaneFor names the lane that owns the job a worker lane belongs to: the
// lane its route reports to (the parent job.escalate already follows), else the
// owner lane of the live job that runs in that lane. It returns "" when neither
// is a registered route, and never the worker lane itself.
func (h *HubServer) designLaneFor(lane string) string {
	routes := loadReportRelayRoutes(h.reportRelayPath)
	if parent := routes[lane].Parent; parent != "" && parent != lane {
		if _, registered := routes[parent]; registered {
			return parent
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, job := range h.jobs {
		if job.Completed || job.Orphaned || job.AgentLabel != lane || job.OwnerLane == "" || job.OwnerLane == lane {
			continue
		}
		if _, registered := routes[job.OwnerLane]; registered {
			return job.OwnerLane
		}
	}
	return ""
}

// escalatesWhenUnconfirmed picks the rows worth a design lane's attention: a
// producer's lane.event. A job.* record has its own routing, an idle-wake
// reports one instant that a replay would only restate, a chat row belongs to
// the chat store, the unconfirmed marker is bookkeeping, and an escalation that
// is itself lost must not start another.
func escalatesWhenUnconfirmed(record handoffkeepRelayEvent) bool {
	return record.Kind == "lane.event" &&
		!isUnconfirmedMarkRecord(record) &&
		!strings.HasPrefix(record.EventID, unconfirmedEscalationPrefix) &&
		!strings.HasPrefix(record.EventID, "idle-wake:") &&
		!chatRelayEventPattern.MatchString(record.EventID)
}

// escalateUnconfirmedRelay sends the one escalation for a relay row the node
// dropped as unconfirmed. It runs after the drop was consumed, so the held
// projection and ack window are already clear. A row the pane already confirmed,
// a row escalatesWhenUnconfirmed refuses, and a lane with no design lane to
// tell are skipped. A transient handoffkeep failure, reading the row or writing
// the escalation, is retried on unconfirmedEscalationBackoff until the
// escalation is durable: the durable id keeps the retries to one escalation.
func (h *HubServer) escalateUnconfirmedRelay(machine string, dropped relayDroppedPayload) {
	h.escalateUnconfirmedRelayContext(context.Background(), machine, dropped)
}

func (h *HubServer) escalateUnconfirmedRelayContext(ctx context.Context, machine string, dropped relayDroppedPayload) {
	h.escalateUnconfirmedRelayAttempt(ctx, machine, dropped, 0)
}

func (h *HubServer) escalateUnconfirmedRelayAttempt(parent context.Context, machine string, dropped relayDroppedPayload, attempt int) {
	if !relayDroppedIsUnconfirmed(dropped.Reason) || dropped.OriginalEventID < 1 || dropped.Lane == "" || h.handoffkeep == nil {
		return
	}
	design := h.designLaneFor(dropped.Lane)
	retry := func(why string) {
		if attempt >= len(unconfirmedEscalationBackoff) {
			if design != "" {
				// Nobody was told: the next notice names this drop.
				h.unconfirmedNotices.fold(design, dropped.Lane, dropped.OriginalEventID, h.now())
			}
			h.logger.Error("relay unconfirmed escalation gave up", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why)
			return
		}
		h.logger.Warn("relay unconfirmed escalation will retry", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why, "attempt", attempt+1)
		time.AfterFunc(unconfirmedEscalationBackoff[attempt], func() { h.escalateUnconfirmedRelayAttempt(context.Background(), machine, dropped, attempt+1) })
	}
	ctx, cancel := context.WithTimeout(parent, nodeLoopHKTimeout)
	defer cancel()
	record, found, err := h.handoffkeep.relayEvent(ctx, dropped.OriginalEventID)
	if err != nil {
		retry("row_read_" + hubHKReadErrorClass(err))
		return
	}
	if !found || record.DeliveredAt != "" || !escalatesWhenUnconfirmed(record) {
		return
	}
	if design == "" {
		h.logger.Warn("relay unconfirmed escalation has no design lane", "event_id", record.ID, "lane", dropped.Lane, "reason", dropped.Reason)
		return
	}
	if why := h.unconfirmedNoticeMoot(dropped.Lane); why != "" {
		h.logger.Info("relay unconfirmed escalation skipped; nobody can act on it", "event_id", record.ID, "lane", dropped.Lane, "design_lane", design, "reason", dropped.Reason, "why", why)
		return
	}
	note, folded, send := h.unconfirmedNotices.admit(design, dropped.Lane, record.ID, h.now())
	if !send {
		h.logger.Info("relay unconfirmed escalation folded into the next notice", "event_id", record.ID, "lane", dropped.Lane, "design_lane", design, "reason", dropped.Reason, "why", "folded", "folded", folded)
		return
	}
	eventID := fmt.Sprintf("%s%d", unconfirmedEscalationPrefix, record.ID)
	event := hubJobEventPayload{
		JobID:      laneEventTransportID(design, eventID),
		Epoch:      1,
		OwnerLane:  design,
		EventID:    eventID,
		NotifyType: "automation-failed",
		Host:       "hub",
		Label:      "delivery-unconfirmed",
		Reason:     dropped.Reason,
		Text:       unconfirmedEscalationText(record, dropped, machine) + note.text,
	}
	result := h.relayLaneEventContext(ctx, event, nil)
	switch {
	case result.ID != 0 && result.Duplicate && attempt > 0:
		// The first writer's body is authoritative. Clear no folds from this
		// retry's newly built note; they were not in the stored text.
		h.unconfirmedNotices.record(design, dropped.Lane, record.ID, unconfirmedNoticeNote{}, h.now())
	case result.ID != 0 && !result.Duplicate:
		// Stored: a new row, or a retry whose earlier post was stored though
		// its reply was lost.
		h.unconfirmedNotices.record(design, dropped.Lane, record.ID, note, h.now())
		if !result.Duplicate {
			h.logger.Warn("relay delivery unconfirmed; design lane told", "event_id", record.ID, "lane", dropped.Lane, "design_lane", design, "reason", dropped.Reason)
		}
	case result.Duplicate:
		// A second drop of the same row posts the same event id and the
		// durable idempotency answers it: the design lane was told already.
	case result.PersistFailed && (result.PersistStatus == 0 || result.PersistStatus >= 500):
		if ctx.Err() != nil {
			retry("persist_" + hubHKReadErrorClass(ctx.Err()))
		} else {
			retry("persist_failed")
		}
	default:
		// The store refused the notice: nobody was told.
		h.unconfirmedNotices.fold(design, dropped.Lane, record.ID, h.now())
	}
}

// unconfirmedEscalationText is the one line the design lane reads. It carries
// the durable event id, the worker lane and pane, the producer's event id and
// the reason, so the receiver can re-send the original or look at the pane.
func unconfirmedEscalationText(record handoffkeepRelayEvent, dropped relayDroppedPayload, machine string) string {
	action := "The worker may not have it; read the pane or re-send."
	if dropped.Reason == relayDroppedCheckPane {
		action = "Check the pane before resending; it may already contain the message."
	} else if dropped.Reason == relayDroppedPaneReadFailed {
		action = "The pane could not be read before sending; restore pane access and resend."
	}
	return fmt.Sprintf("[delivery unconfirmed] event %d (%s, producer id %s) for lane %s was not confirmed on %s pane %s: %s. %s Reported once.",
		record.ID, record.Kind, truncateRelayText(record.EventID, 64), dropped.Lane, truncateRelayText(machine, 64), truncateRelayText(dropped.Pane, 64), dropped.Reason, action)
}

// unconfirmedNoticeMoot names why nobody can act on a notice about the worker
// lane: "job_ended" when the hub knows jobs in that lane and every one is
// completed or orphaned, "task_ended" when every live one's hk task has ended.
// It returns "" -- send the notice -- when the hub knows no job in the lane (a
// hub restart forgets ended jobs, and a lane may run work the hub never
// placed), when a live job has no task, and when a job record or task cannot
// be read.
func (h *HubServer) unconfirmedNoticeMoot(lane string) string {
	known := false
	var live []string
	h.mu.Lock()
	for id, job := range h.jobs {
		if job.AgentLabel != lane {
			continue
		}
		known = true
		if !job.Completed && !job.Orphaned {
			live = append(live, id)
		}
	}
	h.mu.Unlock()
	if !known {
		return ""
	}
	if len(live) == 0 {
		return "job_ended"
	}
	ctx, cancel := context.WithTimeout(h.closeCtx, handoffkeepTimeout)
	defer cancel()
	for _, id := range live {
		task, app, err := h.jobTaskID(ctx, id)
		if err != nil {
			h.logger.Warn("job record unreadable; unconfirmed delivery reported", "job", id, "lane", lane)
			return ""
		}
		if task == "" {
			return ""
		}
		status, err := app.TaskStatus(ctx, task)
		if err != nil {
			h.logger.Warn("task unreadable; unconfirmed delivery reported", "job", id, "task", task, "lane", lane)
			return ""
		}
		if !status.Ended() {
			return ""
		}
	}
	return "task_ended"
}

// unconfirmedNoticeSpacing is how long one stored notice speaks for a worker
// lane: a later drop inside it is folded into the next notice.
const unconfirmedNoticeSpacing = 30 * time.Minute

// unconfirmedNoticeFoldLimit bounds the folded rows one window tracks by id;
// a fold past it is only counted, so a store that refuses every notice cannot
// grow the window without limit.
const unconfirmedNoticeFoldLimit = 64

// unconfirmedNoticeWindows records, per (design lane, worker lane), the last
// notice the store accepted and the drops folded since. Nothing is held before
// a notice is stored, so a notice that is never stored (every retry gave up,
// the store refused it, or a retry stopped early) records nothing and the next
// drop is sent. Two drops at the same moment may both be sent: one extra
// notice is accepted, a lost one is not. It lives in memory: a hub restart
// forgets it, so the first drop after a restart is sent at once.
type unconfirmedNoticeWindows struct {
	mu      sync.Mutex
	byLanes map[[2]string]*unconfirmedNoticeWindow
}

type unconfirmedNoticeWindow struct {
	told     time.Time           // when the last stored notice was recorded; zero for none
	row      int64               // the row that notice reports
	folded   map[int64]time.Time // rows folded and not yet announced, by fold time
	over     int                 // folds past unconfirmedNoticeFoldLimit, counted only
	overAt   time.Time           // the first of them
	overRows map[int64]struct{}  // bounded identities of overflow rows
}

// unconfirmedNoticeNote is what one notice announces: its text, and the folds
// it names, which recording the notice clears.
type unconfirmedNoticeNote struct {
	text string
	rows []int64
	over int
}

// window returns the window of the two lanes. The caller holds n.mu.
func (n *unconfirmedNoticeWindows) window(design, worker string) *unconfirmedNoticeWindow {
	if n.byLanes == nil {
		n.byLanes = make(map[[2]string]*unconfirmedNoticeWindow)
	}
	key := [2]string{design, worker}
	if n.byLanes[key] == nil {
		n.byLanes[key] = &unconfirmedNoticeWindow{folded: make(map[int64]time.Time), overRows: make(map[int64]struct{})}
	}
	return n.byLanes[key]
}

// admit decides whether row's notice is posted now. A drop of another row
// while a stored notice is younger than unconfirmedNoticeSpacing is folded and
// not posted; folded is the count waiting. Otherwise the note names the drops
// folded since, row itself excluded, from the earliest fold time. The row of
// the stored notice is posted again: the durable idempotency answers it.
func (n *unconfirmedNoticeWindows) admit(design, worker string, row int64, now time.Time) (note unconfirmedNoticeNote, folded int, send bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.window(design, worker)
	if !w.told.IsZero() && now.Sub(w.told) < unconfirmedNoticeSpacing && row != w.row {
		w.fold(row, now)
		return note, len(w.folded) + w.over, false
	}
	note = unconfirmedNoticeNote{rows: []int64{row}, over: w.over}
	since := w.overAt
	for id, at := range w.folded {
		if id == row {
			continue
		}
		note.rows = append(note.rows, id)
		if since.IsZero() || at.Before(since) {
			since = at
		}
	}
	if count := len(note.rows) - 1 + note.over; count > 0 {
		note.text = fmt.Sprintf(" (+%d more unconfirmed for this lane since %s UTC)", count, since.UTC().Format("15:04"))
	}
	return note, 0, true
}

// record notes that the store accepted row's notice: it holds the window from
// now, and the folds its note named are cleared. Folds made while it was in
// flight stay for the next notice.
func (n *unconfirmedNoticeWindows) record(design, worker string, row int64, note unconfirmedNoticeNote, now time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.window(design, worker)
	w.told, w.row = now, row
	for _, id := range note.rows {
		delete(w.folded, id)
	}
	if w.over -= note.over; w.over <= 0 {
		w.over, w.overAt = 0, time.Time{}
		clear(w.overRows)
	}
}

// fold counts row, whose notice nobody received, for the next notice.
func (n *unconfirmedNoticeWindows) fold(design, worker string, row int64, now time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.window(design, worker).fold(row, now)
}

func (w *unconfirmedNoticeWindow) fold(row int64, now time.Time) {
	if w.overRows == nil {
		w.overRows = make(map[int64]struct{})
	}
	if _, seen := w.folded[row]; seen {
		return
	}
	if len(w.folded) < unconfirmedNoticeFoldLimit {
		w.folded[row] = now
		return
	}
	if _, seen := w.overRows[row]; seen {
		return
	}
	if len(w.overRows) < unconfirmedNoticeFoldLimit {
		w.overRows[row] = struct{}{}
	}
	if w.over == 0 {
		w.overAt = now
	}
	w.over++
}
