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
	return reason == relayDroppedRetriesExhausted || reason == relayDroppedMaybeInPane || reason == relayDroppedWorkingExpired
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
	h.escalateUnconfirmedRelayAttempt(machine, dropped, 0)
}

func (h *HubServer) escalateUnconfirmedRelayAttempt(machine string, dropped relayDroppedPayload, attempt int) {
	if !relayDroppedIsUnconfirmed(dropped.Reason) || dropped.OriginalEventID < 1 || dropped.Lane == "" || h.handoffkeep == nil {
		return
	}
	retry := func(why string) {
		if attempt >= len(unconfirmedEscalationBackoff) {
			h.logger.Error("relay unconfirmed escalation gave up", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why)
			// Nobody was told: the next drop must not be folded behind it.
			h.unconfirmedNotices.release(dropped.Lane, dropped.OriginalEventID, true)
			return
		}
		h.logger.Warn("relay unconfirmed escalation will retry", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why, "attempt", attempt+1)
		time.AfterFunc(unconfirmedEscalationBackoff[attempt], func() { h.escalateUnconfirmedRelayAttempt(machine, dropped, attempt+1) })
	}
	ctx := context.Background()
	record, found, err := h.handoffkeep.relayEvent(ctx, dropped.OriginalEventID)
	if err != nil {
		retry("row_read_failed")
		return
	}
	if !found || record.DeliveredAt != "" || !escalatesWhenUnconfirmed(record) {
		return
	}
	design := h.designLaneFor(dropped.Lane)
	if design == "" {
		h.logger.Warn("relay unconfirmed escalation has no design lane", "event_id", record.ID, "lane", dropped.Lane, "reason", dropped.Reason)
		return
	}
	if why := h.unconfirmedNoticeMoot(dropped.Lane); why != "" {
		h.logger.Info("relay unconfirmed escalation skipped; nobody can act on it", "event_id", record.ID, "lane", dropped.Lane, "design_lane", design, "reason", dropped.Reason, "why", why)
		return
	}
	note, folded, send, fresh := h.unconfirmedNotices.admit(design, dropped.Lane, record.ID, h.now())
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
		Text:       unconfirmedEscalationText(record, dropped, machine) + note,
	}
	result := h.relayLaneEventContext(ctx, event, nil)
	if result.PersistFailed && (result.PersistStatus == 0 || result.PersistStatus >= 500) {
		retry("persist_failed")
		return
	}
	if result.Duplicate && attempt > 0 {
		// A retry answered as a duplicate: an earlier attempt's post was
		// stored though its reply was lost, so this notice was told.
		h.unconfirmedNotices.posted(dropped.Lane, record.ID)
		return
	}
	if result.ID == 0 || result.Duplicate {
		if fresh {
			// Nothing new reached the design lane: the claim this drop made
			// is taken back so the next drop is told. A duplicate was told
			// before (in a window a restart forgot); a notice never stored
			// counts as folded.
			h.unconfirmedNotices.release(dropped.Lane, record.ID, !result.Duplicate)
		}
		// A second drop of the same row posts the same event id and the
		// durable idempotency answers it: the design lane was told already.
		return
	}
	h.unconfirmedNotices.posted(dropped.Lane, record.ID)
	h.logger.Warn("relay delivery unconfirmed; design lane told", "event_id", record.ID, "lane", dropped.Lane, "design_lane", design, "reason", dropped.Reason)
}

// unconfirmedEscalationText is the one line the design lane reads. It carries
// the durable event id, the worker lane and pane, the producer's event id and
// the reason, so the receiver can re-send the original or look at the pane.
func unconfirmedEscalationText(record handoffkeepRelayEvent, dropped relayDroppedPayload, machine string) string {
	return fmt.Sprintf("[delivery unconfirmed] event %d (%s, producer id %s) for lane %s was not confirmed on %s pane %s: %s. The worker may not have it; read the pane or re-send. Reported once.",
		record.ID, record.Kind, truncateRelayText(record.EventID, 64), dropped.Lane, truncateRelayText(machine, 64), truncateRelayText(dropped.Pane, 64), dropped.Reason)
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

// unconfirmedNoticeSpacing is how long one notice speaks for a worker lane: a
// later drop inside it is folded into the next notice.
const unconfirmedNoticeSpacing = 30 * time.Minute

// unconfirmedNoticeWindows spaces the notices per (design lane, worker lane).
// It lives in memory: a hub restart forgets it, so the first drop after a
// restart is told at once.
type unconfirmedNoticeWindows struct {
	mu      sync.Mutex
	byLanes map[[2]string]*unconfirmedNoticeWindow
}

// unconfirmedNoticeClaim is the notice that holds a window: the row it
// reports, when it was admitted, the folded-count note it carries, and whether
// the store took it.
type unconfirmedNoticeClaim struct {
	at     time.Time
	row    int64
	note   string
	posted bool
}

type unconfirmedNoticeWindow struct {
	claim  unconfirmedNoticeClaim
	folded map[int64]time.Time // rows dropped since claim.at and not told, by drop time
	// prev is the claim this one replaced, and announced the folded rows its
	// note names. Until the claim is posted, release puts both back.
	prev      unconfirmedNoticeClaim
	announced map[int64]time.Time
}

// admit decides whether row's notice is sent now. send is false for a drop
// inside the claim's window, which is counted in folded. A drop once the
// window has expired, a folded row dropped again included, makes a new claim
// with a note naming the drops folded into it. The row that holds the claim is
// always admitted again with its note (a retry, or a re-drop the durable
// idempotency answers), and fresh is true only when this call made the claim.
func (n *unconfirmedNoticeWindows) admit(design, worker string, row int64, now time.Time) (note string, folded int, send, fresh bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.byLanes == nil {
		n.byLanes = make(map[[2]string]*unconfirmedNoticeWindow)
	}
	key := [2]string{design, worker}
	w := n.byLanes[key]
	if w == nil {
		w = &unconfirmedNoticeWindow{folded: make(map[int64]time.Time)}
		n.byLanes[key] = w
	}
	if row == w.claim.row {
		return w.claim.note, len(w.folded), true, false
	}
	if !w.claim.at.IsZero() && now.Sub(w.claim.at) < unconfirmedNoticeSpacing {
		if _, seen := w.folded[row]; !seen {
			w.folded[row] = now
		}
		return "", len(w.folded), false, false
	}
	delete(w.folded, row)
	if len(w.folded) > 0 {
		note = fmt.Sprintf(" (+%d more unconfirmed for this lane since %s UTC)", len(w.folded), w.since().UTC().Format("15:04"))
	}
	*w = unconfirmedNoticeWindow{claim: unconfirmedNoticeClaim{at: now, row: row, note: note}, folded: make(map[int64]time.Time), prev: w.claim, announced: w.folded}
	return note, 0, true, true
}

// since is when the folded drops began: the last claim, or, when no claim
// holds the window (a released first claim), the earliest folded drop. It is
// never the zero time while a drop is folded.
func (w *unconfirmedNoticeWindow) since() time.Time {
	if !w.claim.at.IsZero() {
		return w.claim.at
	}
	var first time.Time
	for _, at := range w.folded {
		if first.IsZero() || at.Before(first) {
			first = at
		}
	}
	return first
}

// claimed returns the window whose claim, posted or not, row holds. A row
// id is one durable row, so at most one window's claim names it.
func (n *unconfirmedNoticeWindows) claimed(worker string, row int64) *unconfirmedNoticeWindow {
	for key, w := range n.byLanes {
		if key[1] == worker && w.claim.row == row {
			return w
		}
	}
	return nil
}

// posted records that row's notice reached the store: its claim can no longer
// be taken back.
func (n *unconfirmedNoticeWindows) posted(worker string, row int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if w := n.claimed(worker, row); w != nil {
		w.claim.posted = true
		w.prev, w.announced = unconfirmedNoticeClaim{}, nil
	}
}

// release takes back row's own claim when its notice was not posted: the claim
// it replaced holds the window again, and the drops its note named stay folded
// beside every drop folded since. untold also folds row itself, whose notice
// nobody received. A posted claim, or a window another row now holds, is left
// alone.
func (n *unconfirmedNoticeWindows) release(worker string, row int64, untold bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.claimed(worker, row)
	if w == nil || w.claim.posted {
		return
	}
	folded := w.announced
	if folded == nil {
		folded = make(map[int64]time.Time)
	}
	for id, at := range w.folded {
		folded[id] = at
	}
	if _, seen := folded[row]; untold && !seen {
		folded[row] = w.claim.at
	}
	*w = unconfirmedNoticeWindow{claim: w.prev, folded: folded}
}
