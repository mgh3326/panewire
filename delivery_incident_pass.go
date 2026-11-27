package panewire

// Contract: docs/relay-harness-delivery.md, "Escalation".
//
// The incident half of the obligation pass (hk 6821, hk 4289 T4). A design
// lane is told once per host fault per incident: the due rows of one (design
// lane, machine, fault) join one incident, which posts one notice naming one of
// them and counting the rest, no sooner than unconfirmedNoticeSpacing after the
// key's last notice. Everything it decides by is in the hub store, so a restart
// loses only the wait before a refused notice is posted again.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

const (
	// obligationNoticeJoinLimit caps the due rows one pass joins to incidents,
	// and obligationNoticeReadLimit the failed row reads after which the joins,
	// or one notice, stop: handoffkeep is out, and a later pass asks again.
	obligationNoticeJoinLimit = 50
	obligationNoticeReadLimit = 3
	// incidentQuietAfter closes an incident no member has faulted in.
	incidentQuietAfter = 2 * unconfirmedNoticeSpacing
)

// incidentNoticeBackoff spaces the posts of a notice that was not stored; the
// last wait repeats for as long as the incident has someone to tell.
var incidentNoticeBackoff = [...]time.Duration{10 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute}

// noticeWait is a refused notice's next post. It is kept in memory: a restarted
// hub posts at once.
type noticeWait struct {
	failures int
	next     time.Time
}

// mootCache is one pass's answers to unconfirmedNoticeMoot, which reads the
// job record and task of every live job in a lane.
type mootCache map[string]string

func (h *HubServer) cachedNoticeMoot(cache mootCache, lane string) string {
	why, known := cache[lane]
	if !known {
		why = h.unconfirmedNoticeMoot(lane)
		cache[lane] = why
	}
	return why
}

// runDeliveryIncidents joins the due rows to incidents, posts the notices that
// are due, and closes the incidents that have ended. The caller holds
// h.obligations.mu.
func (h *HubServer) runDeliveryIncidents(ctx context.Context) {
	store := h.inbox.store
	moot := mootCache{}
	joins, err := store.noticeJoins(ctx, obligationNoticeJoinLimit)
	if err != nil {
		h.logger.Warn("due delivery notices were not read", "error", store.loggable("notice joins", err))
		return
	}
	unread := 0
	for _, row := range joins {
		if ctx.Err() != nil {
			return
		}
		if h.joinDeliveryIncident(ctx, row, moot) {
			unread = 0
		} else if unread++; unread == obligationNoticeReadLimit {
			break
		}
	}
	incidents, err := store.openIncidents(ctx)
	if err != nil {
		h.logger.Warn("open delivery incidents were not read", "error", store.loggable("open incidents", err))
		return
	}
	// An incident that left the open set takes its wait with it.
	for id := range h.obligations.noticeWaits {
		if !slices.ContainsFunc(incidents, func(i deliveryIncident) bool { return i.id == id }) {
			delete(h.obligations.noticeWaits, id)
		}
	}
	for _, incident := range incidents {
		if ctx.Err() != nil {
			return
		}
		// A notice changes nothing the close reads from the incident itself.
		h.noticeDeliveryIncident(ctx, incident, moot)
		h.closeDeliveryIncident(ctx, incident, moot)
	}
}

// joinDeliveryIncident decides what one due row is: nothing to tell (confirmed,
// not a producer's lane.event, no design lane, or nobody can act), a notice the
// spawn's owner asked not to hear, which is counted, or a member of its key's
// incident. It reports false only when the row could not be read.
func (h *HubServer) joinDeliveryIncident(ctx context.Context, row noticeMember, moot mootCache) (read bool) {
	store := h.inbox.store
	later := func(why string) {
		h.logger.Warn("relay unconfirmed escalation will retry", "event_id", row.relayID, "lane", row.lane, "why", why)
		if err := store.deferNoticeJoin(ctx, row.id, h.now().UTC()); err != nil {
			h.logger.Warn("due delivery notice was not deferred", "event_id", row.relayID, "error", store.loggable("defer notice", err))
		}
	}
	forget := func() {
		if err := store.forgetNotice(ctx, row.id); err != nil {
			h.logger.Warn("due delivery notice was not cleared", "event_id", row.relayID, "error", store.loggable("clear notice", err))
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, nodeLoopHKTimeout)
	record, found, err := h.handoffkeep.relayEvent(readCtx, row.relayID)
	cancel()
	if err != nil {
		later("row_read_" + hubHKReadErrorClass(err))
		return false
	}
	if !found || !noticeOwed(record) || !escalatesWhenUnconfirmed(record) {
		forget()
		return true
	}
	design := h.designLaneFor(row.lane)
	if design == "" {
		h.logger.Warn("relay unconfirmed escalation has no design lane", "event_id", record.ID, "lane", row.lane, "reason", row.fault)
		forget()
		return true
	}
	// The spawn's owner may have asked for no notice of this kind (hk 5360):
	// the row is counted, and the design lane is not told.
	withheld := h.ownerNotifyWithheld(ctx, record.JobID, design)
	if withheld {
		if _, err := store.recordOwnerNoticeDecision(ctx, fmt.Sprintf("unconfirmed:%d", record.ID), design, "delivery_unconfirmed", ownerDecisionCount, h.now()); err != nil {
			later("count_not_recorded")
			return true
		}
	} else if why := h.cachedNoticeMoot(moot, row.lane); why != "" {
		h.logger.Info("relay unconfirmed escalation skipped; nobody can act on it", "event_id", record.ID, "lane", row.lane, "design_lane", design, "reason", row.fault, "why", why)
		forget()
		return true
	}
	machine, pane := h.noticeDestination(row)
	key := incidentKey{design: design, machine: machine, fault: row.fault}
	incident, err := store.joinIncident(ctx, key, h.paneHarness(machine, pane), row.id, withheld, h.now().UTC())
	if errors.Is(err, errNoticeMemberChanged) {
		return true // a drop or a confirmation ended the wait meanwhile
	}
	if err != nil {
		h.logger.Warn("delivery incident was not recorded", "event_id", record.ID, "lane", row.lane, "design_lane", design, "error", store.loggable("join incident", err))
		later("incident_not_recorded")
		return true
	}
	if !withheld {
		h.logFoldedNotice(ctx, key, incident, record.ID, row, design)
	}
	return true
}

// logFoldedNotice says so when a row joined an incident whose key's slot is
// held: it waits for the next notice.
func (h *HubServer) logFoldedNotice(ctx context.Context, key incidentKey, incident, eventID int64, row noticeMember, design string) {
	store := h.inbox.store
	slot, err := store.noticeSlot(ctx, key)
	if err != nil || !noticeSlotHeld(slot, h.now()) {
		return
	}
	members, err := store.incidentMembers(ctx, incident)
	if err != nil {
		return
	}
	h.logger.Info("relay unconfirmed escalation folded into the next notice", "event_id", eventID, "lane", row.lane, "design_lane", design, "reason", row.fault, "why", "folded", "folded", len(dueRows(members)))
}

// noticeOwed reports whether a due row's delivery is still unproven: the row is
// undelivered, was retired with its one labelled copy spent, or is a digest
// batch released for a later one. A confirmed row and one handoffkeep shows
// settled some other way need no notice.
func noticeOwed(record handoffkeepRelayEvent) bool {
	if record.DeliveredAt == "" {
		return true
	}
	if confirmedInHK(record) {
		return false
	}
	reason := settlementReason(record.DeliveredTo)
	return reason == "" || reason == obligationSettledDigest
}

// noticeDestination is the machine and pane a row's fault is about: where the
// obligation last recorded it, or the lane's route for a row that names none.
func (h *HubServer) noticeDestination(row noticeMember) (machine, pane string) {
	machine, pane = row.machine, row.pane
	if machine == "" {
		route := loadReportRelayRoutes(h.reportRelayPath)[row.lane]
		machine, pane = route.Machine, route.Pane
	}
	return machine, pane
}

// paneHarness is the harness the node last reported for the pane, or "".
func (h *HubServer) paneHarness(machine, pane string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if snapshot := h.freshSessionSnapshotLocked(machine); snapshot != nil {
		for _, session := range snapshot.sessions {
			if session.PaneID == pane {
				return session.Harness
			}
		}
	}
	return ""
}

// noticeSlotHeld reports whether a notice stored at slot still speaks for its
// key.
func noticeSlotHeld(slot int64, now time.Time) bool {
	return slot > 0 && now.Sub(time.UnixMilli(slot)) < unconfirmedNoticeSpacing
}

func dueRows(members []noticeMember) []noticeMember {
	return slices.DeleteFunc(slices.Clone(members), func(r noticeMember) bool { return r.dueAt == 0 })
}

// noticeDeliveryIncident posts the incident's notice if it has a member still
// to tell and its key's slot is free. The notice, its slot and the rows it told
// are committed only once the notice is durable; until then the incident stays
// open and the post is repeated under the same id.
func (h *HubServer) noticeDeliveryIncident(ctx context.Context, incident deliveryIncident, moot mootCache) {
	store := h.inbox.store
	members, err := store.incidentMembers(ctx, incident.id)
	if err != nil {
		h.logger.Warn("delivery incident members were not read", "incident", incident.id, "error", store.loggable("incident members", err))
		return
	}
	due := dueRows(members)
	if len(due) == 0 {
		return
	}
	if wait, waiting := h.obligations.noticeWaits[incident.id]; waiting && h.now().Before(wait.next) {
		return
	}
	slot, err := store.noticeSlot(ctx, incident.key)
	if err != nil {
		h.logger.Warn("delivery notice slot was not read", "incident", incident.id, "error", store.loggable("notice slot", err))
		return
	}
	if noticeSlotHeld(slot, h.now()) {
		return
	}
	forget := func(row noticeMember) {
		if err := store.forgetNotice(ctx, row.id); err != nil {
			h.logger.Warn("due delivery notice was not cleared", "event_id", row.relayID, "error", store.loggable("clear notice", err))
		}
	}
	// An ended job's lane is not named.
	due = slices.DeleteFunc(due, func(row noticeMember) bool {
		why := h.cachedNoticeMoot(moot, row.lane)
		if why == "" {
			return false
		}
		h.logger.Info("relay unconfirmed escalation skipped; nobody can act on it", "event_id", row.relayID, "lane", row.lane, "design_lane", incident.key.design, "reason", incident.key.fault, "why", why)
		forget(row)
		return true
	})
	// The notice names the longest due row whose delivery is still unproven. A
	// row that cannot be read stays due, outside this notice, so that it does
	// not hold the others back.
	var record handoffkeepRelayEvent
	unread := 0
	for len(due) > 0 {
		readCtx, cancel := context.WithTimeout(ctx, nodeLoopHKTimeout)
		stored, found, err := h.handoffkeep.relayEvent(readCtx, due[0].relayID)
		cancel()
		if err != nil {
			if unread++; unread == obligationNoticeReadLimit || len(due) == 1 {
				h.deferNotice(incident, due[0].relayID, "row_read_"+hubHKReadErrorClass(err))
				return
			}
			due = due[1:]
			continue
		}
		if found && noticeOwed(stored) {
			record = stored
			break
		}
		forget(due[0])
		due = due[1:]
	}
	if len(due) == 0 {
		return
	}
	named := due[0]
	machine, pane := h.noticeDestination(named)
	text := unconfirmedEscalationText(record, named.lane, machine, pane, incident.key.fault, classifyObligationFault(incident.key.fault) == obligationMaybeTyped)
	if folded := len(due) - 1; folded > 0 {
		// The rows are longest due first, so the named one is the earliest.
		text += fmt.Sprintf(" (+%d more unconfirmed for this host since %s UTC)", folded, time.UnixMilli(named.dueAt).UTC().Format("15:04"))
	}
	// The id is fixed until a notice is committed, so a repeat of a post whose
	// reply was lost is answered by the durable idempotency.
	eventID := fmt.Sprintf("%si%d-n%d", unconfirmedEscalationPrefix, incident.id, incident.announced)
	postCtx, cancel := context.WithTimeout(ctx, nodeLoopHKTimeout)
	defer cancel()
	result := h.relayLaneEventContext(postCtx, unconfirmedNoticeEvent(incident.key.design, eventID, incident.key.fault, text), nil)
	if result.ID == 0 || result.PersistFailed {
		h.deferNotice(incident, named.relayID, "persist_failed")
		return
	}
	delete(h.obligations.noticeWaits, incident.id)
	notice := storedNotice{eventID: eventID, relayID: result.ID}
	if result.Machine == "inbox" {
		notice.relayID = 0
	}
	// A duplicate's stored text is an earlier post's, which may not name these
	// rows: they stay due, and the next notice names them.
	if !result.Duplicate {
		for _, row := range due {
			notice.told = append(notice.told, row.id)
		}
	}
	if err := store.recordNotice(context.WithoutCancel(ctx), incident, notice, h.now().UTC()); err != nil {
		h.logger.Warn("delivery notice was stored but not recorded; it is posted again", "incident", incident.id, "event_id", named.relayID, "error", store.loggable("record notice", err))
		return
	}
	if !result.Duplicate {
		h.logger.Warn("relay delivery unconfirmed; design lane told", "event_id", named.relayID, "lane", named.lane, "design_lane", incident.key.design, "reason", incident.key.fault, "incident", incident.id, "rows", len(due))
	}
}

func (h *HubServer) deferNotice(incident deliveryIncident, relayID int64, why string) {
	if h.obligations.noticeWaits == nil {
		h.obligations.noticeWaits = make(map[int64]noticeWait)
	}
	wait := h.obligations.noticeWaits[incident.id]
	delay := incidentNoticeBackoff[min(wait.failures, len(incidentNoticeBackoff)-1)]
	wait.failures++
	wait.next = h.now().Add(delay)
	h.obligations.noticeWaits[incident.id] = wait
	h.logger.Warn("relay unconfirmed escalation will retry", "event_id", relayID, "design_lane", incident.key.design, "incident", incident.id, "why", why, "attempt", wait.failures)
}

// closeDeliveryIncident closes the incident if it has ended: a member was
// confirmed since it opened (recovered), every member is closed (all-terminal),
// every open or due member's lane has nobody to act (moot), or no member has
// faulted for incidentQuietAfter and none is still to be told (quiet). A
// confirmation of anything but a member says nothing about this fault.
func (h *HubServer) closeDeliveryIncident(ctx context.Context, incident deliveryIncident, moot mootCache) {
	store := h.inbox.store
	members, err := store.incidentMembers(ctx, incident.id)
	if err != nil {
		h.logger.Warn("delivery incident members were not read", "incident", incident.id, "error", store.loggable("incident members", err))
		return
	}
	recovered, open, due, live := false, 0, 0, 0
	last := incident.lastMemberAt
	for _, m := range members {
		recovered = recovered || m.state == obligationConfirmed && m.updated >= incident.openedAt
		last = max(last, m.lastFaultAt)
		if !m.terminal() {
			open++
		}
		if m.dueAt > 0 {
			due++
		}
		if !m.terminal() || m.dueAt > 0 {
			// The pass reads a lane's jobs from handoffkeep only for a notice;
			// between notices the hub's own job records decide.
			why, known := moot[m.lane]
			if !known {
				why = h.laneJobsAllEnded(m.lane)
			}
			if why == "" {
				live++
			}
		}
	}
	var reason string
	switch {
	case recovered:
		reason = incidentRecovered
	case len(members) > 0 && open == 0 && due == 0:
		reason = incidentAllTerminal
	case open+due > 0 && live == 0:
		reason = incidentMoot
	case due == 0 && h.now().Sub(time.UnixMilli(last)) >= incidentQuietAfter:
		reason = incidentQuiet
	default:
		return
	}
	closed, err := store.closeIncident(ctx, incident, reason, h.now().UTC())
	if err != nil {
		h.logger.Warn("delivery incident was not closed", "incident", incident.id, "reason", reason, "error", store.loggable("close incident", err))
		return
	}
	if closed {
		delete(h.obligations.noticeWaits, incident.id)
		h.logger.Info("relay delivery incident closed", "incident", incident.id, "design_lane", incident.key.design, "machine", incident.key.machine, "fault", incident.key.fault, "reason", reason, "members", len(members))
	}
}

// noteIncidentDelivery is called once handoffkeep holds a real delivery of a
// row: an incident the row is a member of is read by the next pass.
func (h *HubServer) noteIncidentDelivery(ctx context.Context, relayID int64) {
	if h.inbox == nil || relayID < 1 {
		return
	}
	store := h.inbox.store
	if err := store.readIncidentMemberNext(ctx, relayID); err != nil {
		h.logger.Warn("delivered incident member was not scheduled for a read", "event_id", relayID, "error", store.loggable("read member", err))
	}
}
