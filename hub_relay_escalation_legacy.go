package panewire

// The escalation of a hub with no inbox store (--delivery-policy off, or no
// store): it keeps no delivery obligations, so it has no incidents (hk 6821)
// and tells the design lane from the node's drop, spaced by the hk 3431 window.
// The window and the retries of a failed write are memory only: a restart
// forgets both. Contract: docs/relay-harness-delivery.md, "Escalation".

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// legacyEscalationBackoff spaces the retries of an escalation whose durable
// write failed (a handoffkeep outage). The drop was already consumed, so
// nothing else would send it; the last wait is the final try.
var legacyEscalationBackoff = []time.Duration{2 * time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}

// escalateLegacyUnconfirmedRelay sends the one escalation for a relay row the
// node dropped as unconfirmed, on a hub with no inbox store. It runs after the
// drop was consumed, so the held projection and ack window are already clear. A
// row the pane already confirmed, a row escalatesWhenUnconfirmed refuses, and a
// lane with no design lane to tell are skipped. A transient handoffkeep
// failure, reading the row or writing the escalation, is retried on
// legacyEscalationBackoff until the escalation is durable: the durable id keeps
// the retries to one escalation.
func (h *HubServer) escalateLegacyUnconfirmedRelay(ctx context.Context, machine string, dropped relayDroppedPayload) {
	h.escalateLegacyUnconfirmedAttempt(ctx, machine, dropped, 0)
}

func (h *HubServer) escalateLegacyUnconfirmedAttempt(parent context.Context, machine string, dropped relayDroppedPayload, attempt int) {
	if !relayDroppedIsUnconfirmed(dropped.Reason) || dropped.OriginalEventID < 1 || dropped.Lane == "" || h.handoffkeep == nil {
		return
	}
	design := h.designLaneFor(dropped.Lane)
	retry := func(why string) {
		if attempt >= len(legacyEscalationBackoff) {
			if design != "" {
				// Nobody was told: the next notice names this drop.
				h.legacyNotices.fold(design, dropped.Lane, dropped.OriginalEventID, h.now())
			}
			h.logger.Error("relay unconfirmed escalation gave up", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why)
			return
		}
		h.logger.Warn("relay unconfirmed escalation will retry", "event_id", dropped.OriginalEventID, "lane", dropped.Lane, "why", why, "attempt", attempt+1)
		time.AfterFunc(legacyEscalationBackoff[attempt], func() { h.escalateLegacyUnconfirmedAttempt(context.Background(), machine, dropped, attempt+1) })
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
	note, folded, send := h.legacyNotices.admit(design, dropped.Lane, record.ID, h.now())
	if !send {
		h.logger.Info("relay unconfirmed escalation folded into the next notice", "event_id", record.ID, "lane", dropped.Lane, "design_lane", design, "reason", dropped.Reason, "why", "folded", "folded", folded)
		return
	}
	eventID := fmt.Sprintf("%s%d", unconfirmedEscalationPrefix, record.ID)
	text := unconfirmedEscalationText(record, dropped.Lane, machine, dropped.Pane, dropped.Reason, false)
	result := h.relayLaneEventContext(ctx, unconfirmedNoticeEvent(design, eventID, dropped.Reason, text+note.text), nil)
	switch {
	case result.ID != 0 && result.Duplicate && attempt > 0:
		// The first writer's body is authoritative. Clear no folds from this
		// retry's newly built note; they were not in the stored text.
		h.legacyNotices.record(design, dropped.Lane, record.ID, legacyNoticeNote{}, h.now())
	case result.ID != 0 && !result.Duplicate:
		// Stored: a new row, or a retry whose earlier post was stored though
		// its reply was lost.
		h.legacyNotices.record(design, dropped.Lane, record.ID, note, h.now())
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
		h.legacyNotices.fold(design, dropped.Lane, record.ID, h.now())
	}
}

// legacyNoticeFoldLimit bounds the folded rows one window tracks by id;
// a fold past it is only counted, so a store that refuses every notice cannot
// grow the window without limit.
const legacyNoticeFoldLimit = 64

// legacyNoticeWindows records, per (design lane, worker lane), the last
// notice the store accepted and the drops folded since. Nothing is held before
// a notice is stored, so a notice that is never stored (every retry gave up,
// the store refused it, or a retry stopped early) records nothing and the next
// drop is sent. Two drops at the same moment may both be sent: one extra
// notice is accepted, a lost one is not. It lives in memory: a hub restart
// forgets it, so the first drop after a restart is sent at once.
type legacyNoticeWindows struct {
	mu      sync.Mutex
	byLanes map[[2]string]*legacyNoticeWindow
}

type legacyNoticeWindow struct {
	told     time.Time           // when the last stored notice was recorded; zero for none
	row      int64               // the row that notice reports
	folded   map[int64]time.Time // rows folded and not yet announced, by fold time
	over     int                 // folds past legacyNoticeFoldLimit, counted only
	overAt   time.Time           // the first of them
	overRows map[int64]struct{}  // bounded identities of overflow rows
}

// legacyNoticeNote is what one notice announces: its text, and the folds
// it names, which recording the notice clears.
type legacyNoticeNote struct {
	text string
	rows []int64
	over int
}

// window returns the window of the two lanes. The caller holds n.mu.
func (n *legacyNoticeWindows) window(design, worker string) *legacyNoticeWindow {
	if n.byLanes == nil {
		n.byLanes = make(map[[2]string]*legacyNoticeWindow)
	}
	key := [2]string{design, worker}
	if n.byLanes[key] == nil {
		n.byLanes[key] = &legacyNoticeWindow{folded: make(map[int64]time.Time), overRows: make(map[int64]struct{})}
	}
	return n.byLanes[key]
}

// admit decides whether row's notice is posted now. A drop of another row
// while a stored notice is younger than unconfirmedNoticeSpacing is folded and
// not posted; folded is the count waiting. Otherwise the note names the drops
// folded since, row itself excluded, from the earliest fold time. The row of
// the stored notice is posted again: the durable idempotency answers it.
func (n *legacyNoticeWindows) admit(design, worker string, row int64, now time.Time) (note legacyNoticeNote, folded int, send bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	w := n.window(design, worker)
	if !w.told.IsZero() && now.Sub(w.told) < unconfirmedNoticeSpacing && row != w.row {
		w.fold(row, now)
		return note, len(w.folded) + w.over, false
	}
	note = legacyNoticeNote{rows: []int64{row}, over: w.over}
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
func (n *legacyNoticeWindows) record(design, worker string, row int64, note legacyNoticeNote, now time.Time) {
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
func (n *legacyNoticeWindows) fold(design, worker string, row int64, now time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.window(design, worker).fold(row, now)
}

func (w *legacyNoticeWindow) fold(row int64, now time.Time) {
	if w.overRows == nil {
		w.overRows = make(map[int64]struct{})
	}
	if _, seen := w.folded[row]; seen {
		return
	}
	if len(w.folded) < legacyNoticeFoldLimit {
		w.folded[row] = now
		return
	}
	if _, seen := w.overRows[row]; seen {
		return
	}
	if len(w.overRows) < legacyNoticeFoldLimit {
		w.overRows[row] = struct{}{}
	}
	if w.over == 0 {
		w.overAt = now
	}
	w.over++
}
