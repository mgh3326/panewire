package panewire

// The hub's side of the delivery obligations (hk 6384, hk 4289 T1;
// docs/r20-relay-persistence.md, "Delivery obligations"). The relay path
// creates an obligation once handoffkeep holds the row. A pass that Sweep
// starts adopts the undelivered rows that have none, then closes obligations
// from stored facts. It types nothing, retries nothing, notifies nobody and
// writes nothing to handoffkeep.

import (
	"context"
	"strings"
	"sync"
	"time"
)

const (
	// obligationFirstCheck is how long a new obligation waits before its first
	// read: past the acknowledgement window, so an ordinary delivery costs one.
	obligationFirstCheck = 30 * time.Second
	// obligationRecheckMin and Max bound the wait between reads of a row that
	// is still owed (obligationRecheck).
	obligationRecheckMin = time.Minute
	obligationRecheckMax = time.Hour
	// obligationParkRecheck is the wait for a row nothing is expected to
	// change soon: an exhausted one, or one handoffkeep no longer lists.
	obligationParkRecheck = 4 * time.Hour
	// obligationInboxGrace is how old an inbox delivery record must be before
	// it settles an obligation whose handoffkeep row is not confirmed.
	obligationInboxGrace = time.Minute
	// obligationAdoptPages and obligationAdoptPageSize cap one pass's adopt
	// walk; the next pass resumes at the stored cursor.
	obligationAdoptPages    = 3
	obligationAdoptPageSize = handoffkeepReplayLimit
	// obligationClassifyLimit caps the obligations one pass reads, and
	// obligationPassBudget the time it takes.
	obligationClassifyLimit = 50
	obligationPassBudget    = 30 * time.Second

	// obligationFaultUnconfirmed marks a row whose one labelled replay was
	// spent: handoffkeep closed it without proof that a pane showed it.
	obligationFaultUnconfirmed = "unconfirmed"
	// obligationSettledInbox and obligationSettledDigest are the settlement
	// reasons the hub's own store proves; the others come from delivered_to
	// (settlementReason).
	obligationSettledInbox  = relayUnconfirmedInboxRetireReason
	obligationSettledDigest = relayDigestNotReplayedRetireReason
)

// relaySinkMachine is the delivered_to machine of a row a sink lane recorded.
const relaySinkMachine = "sink"

// isSyntheticDestination reports whether delivered_to names no pane: the hub's
// own settlements and a sink lane's record.
func isSyntheticDestination(deliveredTo string) bool {
	machine, _, _ := strings.Cut(deliveredTo, "/")
	return machine == relayReplayRetiredMachine || machine == relaySinkMachine
}

// confirmedInHK is the one rule for "this row reached a pane": handoffkeep
// shows it delivered to a real destination. delivered_at alone is also how
// every settlement closes a row.
func confirmedInHK(record handoffkeepRelayEvent) bool {
	return record.DeliveredAt != "" && !isSyntheticDestination(record.DeliveredTo)
}

// settlementReason names why a synthetic destination closed a row with no copy
// owed, or "" when it does not say so: a spent labelled replay
// (replay-retired:unconfirmed) and a hub destination this build does not know.
func settlementReason(deliveredTo string) string {
	machine, pane, _ := strings.Cut(deliveredTo, "/")
	if machine == relaySinkMachine {
		return "sink"
	}
	if reason, retired := strings.CutPrefix(pane, relayReplayRetiredPane); retired {
		if reason == relayUnconfirmedRetireReason {
			return ""
		}
		return reason
	}
	if strings.HasPrefix(pane, filteredDeliveryPrefix) {
		return "filtered"
	}
	if pane == "inbox" || pane == "chat-terminal" {
		return pane
	}
	return ""
}

// obligationRecheck is the wait before an owed row of this age is read again: a
// tenth of its age, within the bounds, so an old backlog costs few reads.
func obligationRecheck(age time.Duration) time.Duration {
	return min(max(age/10, obligationRecheckMin), obligationRecheckMax)
}

// recordObligation tracks a row the relay path just stored. It never holds the
// relay back: a failed write is logged, and the adopt walk creates the
// obligation on a later pass. A hub without an inbox store keeps none.
func (h *HubServer) recordObligation(key, kind string, event hubJobEventPayload, route reportRelayRoute, stored handoffkeepRelayEvent) {
	if h.inbox == nil || stored.ID < 1 {
		return
	}
	now := h.now().UTC()
	err := h.inbox.store.createObligation(h.inbox.store.life, newDeliveryObligation{
		key: key, relayID: stored.ID, kind: kind, lane: event.OwnerLane,
		machine: route.Machine, pane: route.Pane, attempts: stored.Attempts,
	}, now, now.Add(obligationFirstCheck))
	if err != nil {
		h.logger.Warn("delivery obligation was not recorded; adoption retries it", "event_id", stored.ID, "kind", kind, "producer_event_id", event.EventID, "error", h.inbox.store.loggable("delivery obligation", err))
	}
}

// obligationSweep keeps at most one pass running and lets Close wait it out.
type obligationSweep struct {
	// mu is held for a run's whole life, so close takes it to wait the run
	// out; closing then stops every later start.
	mu      sync.Mutex
	closing bool
}

func (s *obligationSweep) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
}

// startObligationReconcile runs one pass in the background unless one is
// running. It takes no replay lock: it changes only the obligation tables.
func (h *HubServer) startObligationReconcile() {
	if h.inbox == nil || h.handoffkeep == nil || !h.obligations.mu.TryLock() {
		return
	}
	if h.obligations.closing {
		h.obligations.mu.Unlock()
		return
	}
	go func() {
		defer h.obligations.mu.Unlock()
		ctx, cancel := context.WithTimeout(h.laneStallBaseContext(), obligationPassBudget)
		defer cancel()
		// Close ends the pass's handoffkeep reads, so it need not wait them out.
		defer context.AfterFunc(h.closeCtx, cancel)()
		h.reconcileObligations(ctx)
	}()
}

// reconcileObligations is one pass: adopt, then classify.
func (h *HubServer) reconcileObligations(ctx context.Context) {
	h.adoptObligations(ctx)
	h.classifyObligations(ctx)
}

// adoptObligations walks a bounded part of handoffkeep's undelivered listing
// and creates the obligations that are missing: a row whose insert failed, or
// that was stored before this build. It runs every pass and wraps to the start
// after the last page, so a row it could not adopt is offered again.
func (h *HubServer) adoptObligations(ctx context.Context) {
	store := h.inbox.store
	cursor, wrappedAt, err := store.adoptCursor(ctx)
	if err != nil {
		h.logger.Warn("delivery obligation adopt cursor was not read", "error", store.loggable("adopt cursor", err))
		return
	}
	for page := 0; page < obligationAdoptPages; page++ {
		records, err := h.handoffkeep.listUndelivered(ctx, "", "", cursor, obligationAdoptPageSize)
		if err != nil {
			h.logger.Warn("delivery obligations could not be adopted", "after_id", cursor, "error_class", hubHKReadErrorClass(ctx.Err()))
			return
		}
		now := h.now().UTC()
		next := cursor
		var missing []newDeliveryObligation
		for _, record := range records {
			next = max(next, record.ID)
			if o, adoptable := adoptableObligation(record); adoptable {
				missing = append(missing, o)
			}
		}
		if err := store.adoptObligations(ctx, missing, now, now.Add(obligationFirstCheck)); err != nil {
			h.logger.Warn("delivery obligations were not all adopted; the next walk retries them", "after_id", cursor, "error", store.loggable("adopt", err))
		}
		last := len(records) < obligationAdoptPageSize
		if !last && next <= cursor {
			h.logger.Warn("delivery obligation adopt cursor did not advance", "after_id", cursor)
			return
		}
		cursor = next
		if last {
			cursor, wrappedAt = 0, now.UnixMilli()
		}
		if err := store.saveAdoptCursor(ctx, cursor, wrappedAt); err != nil {
			h.logger.Warn("delivery obligation adopt cursor was not saved", "error", store.loggable("adopt cursor", err))
			return
		}
		if last {
			return
		}
	}
}

// adoptableObligation is the obligation an undelivered row stands for. A
// delivered or retired row, a marker row and a row with no lane or job (the
// replay skips it too) owe nothing. The destination stays unknown: an HTTP
// ingress row's machine names its sender.
func adoptableObligation(record handoffkeepRelayEvent) (newDeliveryObligation, bool) {
	if record.DeliveredAt != "" || strings.HasPrefix(record.DeliveredTo, relayReplayRetiredMarker) || !emitRelayKinds[record.Kind] || isUnconfirmedMarkRecord(record) {
		return newDeliveryObligation{}, false
	}
	event := relayEventFromRecord(record)
	if event.OwnerLane == "" || event.JobID == "" {
		return newDeliveryObligation{}, false
	}
	return newDeliveryObligation{key: relayEventDedupeKey(record.Kind, event), relayID: record.ID, kind: record.Kind, lane: record.OwnerLane, attempts: record.Attempts}, true
}

// classifyObligations reads the due obligations' rows and closes what stored
// facts close. A failed handoffkeep read ends the pass and changes no state:
// that obligation is read again later.
func (h *HubServer) classifyObligations(ctx context.Context) {
	store := h.inbox.store
	due, err := store.dueObligations(ctx, h.now().UTC(), obligationClassifyLimit)
	if err != nil {
		h.logger.Warn("due delivery obligations were not read", "error", store.loggable("due obligations", err))
		return
	}
	for _, o := range due {
		record, found, readErr := h.handoffkeep.relayEvent(ctx, o.relayID)
		if ctx.Err() != nil {
			return
		}
		now := h.now().UTC()
		switch {
		case readErr != nil:
			h.logger.Warn("delivery obligation row could not be read", "event_id", o.relayID)
			err = store.deferObligation(ctx, o.id, now, now.Add(obligationRecheckMin))
		case !found:
			// Absence proves nothing about delivery, so the row stays open.
			h.logger.Warn("delivery obligation row is not in handoffkeep", "event_id", o.relayID, "obligation", o.id)
			err = store.deferObligation(ctx, o.id, now, now.Add(obligationParkRecheck))
		default:
			err = h.classifyObligation(ctx, o, record, now)
		}
		if err != nil {
			h.logger.Warn("delivery obligation was not classified", "event_id", o.relayID, "error", store.loggable("classify", err))
		}
		if readErr != nil {
			return
		}
	}
}

// classifyObligation applies the first rule that holds, in this order: the
// row's confirmation, its synthetic destination, the receiver's inbox item, a
// released digest batch. Otherwise the obligation stays as it is.
//
// The row speaks for the obligation only when it stores that obligation's
// event: handoffkeep folds a job's later round into the first round's row,
// whose delivery says nothing about the later event.
func (h *HubServer) classifyObligation(ctx context.Context, o openObligation, record handoffkeepRelayEvent, now time.Time) error {
	store := h.inbox.store
	own := store.tracks(o, relayEventDedupeKey(record.Kind, relayEventFromRecord(record)))
	if own && confirmedInHK(record) {
		return store.closeObligation(ctx, o.id, obligationConfirmed, "", now)
	}
	if own && isSyntheticDestination(record.DeliveredTo) {
		if reason := settlementReason(record.DeliveredTo); reason != "" {
			return store.closeObligation(ctx, o.id, obligationSettled, reason, now)
		}
		return store.exhaustObligation(ctx, o.id, obligationFaultUnconfirmed, now, now.Add(obligationParkRecheck))
	}
	settled, err := store.obligationInboxSettled(ctx, o.id, now.Add(-obligationInboxGrace))
	if err != nil {
		return err
	}
	if settled {
		return store.closeObligation(ctx, o.id, obligationSettled, obligationSettledInbox, now)
	}
	if own && record.Kind == "lane.event" {
		released, err := store.digestBatchReleased(ctx, record.OwnerLane, record.EventID)
		if err != nil {
			return err
		}
		if released {
			return store.closeObligation(ctx, o.id, obligationSettled, obligationSettledDigest, now)
		}
	}
	wait := obligationRecheck(now.Sub(time.UnixMilli(o.createdAt)))
	if o.state != obligationOwed {
		wait = obligationParkRecheck
	}
	return store.deferObligation(ctx, o.id, now, now.Add(wait))
}
