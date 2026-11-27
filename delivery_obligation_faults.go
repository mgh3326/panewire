package panewire

// The per-fault retry policy of a delivery obligation (hk 6820, hk 4289 T3;
// docs/r20-relay-persistence.md, "Faults and copies"). A node says why it
// stopped, in a relay.unconfirmed report and then in a relay.dropped; the hub
// stores one fault for that, and the fault decides what copy is still owed.

import (
	"context"
	"slices"
	"strings"
	"time"
)

const (
	// obligationAckTimeout: a copy was queued and nothing was heard back.
	obligationAckTimeout = "ack_timeout"
	// obligationFaultUnrouted: a pass found no node or pane to queue a copy to.
	obligationFaultUnrouted = "unrouted"
	// obligationFaultUnconfirmed is a legacy marker or a retired labelled
	// replay: some report was made, and which one is not known.
	obligationFaultUnconfirmed = "unconfirmed"
	// obligationFaultUnproven is a report the hub cannot read as safe and has
	// no name of its own for.
	obligationFaultUnproven = "unproven"
	// obligationFaultReadUnproven is the report ahead of a pane_read_failed
	// drop. The node sends it too when an earlier attempt may have typed the
	// text, so it counts as maybe typed until the drop says which it was.
	obligationFaultReadUnproven = "pane_read_unproven"
	// obligationFaultPromptUnproven is a prompt_failed report: herdr rejected
	// the send. The report names no harness, and devin may have typed part of
	// the text, so it counts as maybe typed until a drop from a harness whose
	// rejected send types nothing settles it.
	obligationFaultPromptUnproven = "prompt_unproven"

	// relayUnconfirmedReadFailed and relayUnconfirmedPromptFailed are
	// relay.unconfirmed reasons that prove more than their class does.
	relayUnconfirmedReadFailed   = relayDroppedMaybeInPane + " presend:unproven:read_failed"
	relayUnconfirmedPromptFailed = "prompt_failed"
)

type obligationClass int

const (
	// obligationMaybeTyped is the zero value, so a fault nothing here names is
	// treated as text that may already be in the pane: one labelled copy ever.
	obligationMaybeTyped obligationClass = iota
	// obligationSafe: nothing is known to be in the pane; retry to the budget.
	obligationSafe
	// obligationWorkingExpired: nothing was typed because the pane stayed
	// working. No budget retry; one revived copy at an idle wake.
	obligationWorkingExpired
	// obligationNeverRetry: the lane's job ended, so no pane will take it.
	obligationNeverRetry
)

func classifyObligationFault(fault string) obligationClass {
	switch fault {
	case obligationAckTimeout, obligationFaultUnrouted, relayDroppedPaneReadFailed, relayDroppedRetriesExhausted:
		return obligationSafe
	case relayDroppedWorkingExpired:
		return obligationWorkingExpired
	case relayDroppedJobEnded:
		return obligationNeverRetry
	}
	return obligationMaybeTyped
}

// obligationFaultFor names the fault a node's report or drop reason stands for.
func obligationFaultFor(reason string) string {
	switch reason {
	case obligationAckTimeout, relayDroppedPaneReadFailed, relayDroppedRetriesExhausted, relayDroppedWorkingExpired,
		relayDroppedCheckPane, relayDroppedJobEnded:
		return reason
	case relayUnconfirmedPromptFailed:
		return obligationFaultPromptUnproven
	case relayUnconfirmedReadFailed:
		return obligationFaultReadUnproven
	}
	if strings.HasPrefix(reason, relayDroppedMaybeInPane) {
		return relayDroppedMaybeInPane
	}
	return obligationFaultUnproven
}

// promptRejectProvesUntyped reports whether a send herdr rejected on harness
// typed nothing. devin's may have typed part of the text
// (devinRelayInjectGuarded), and a drop that names no harness proves nothing.
func promptRejectProvesUntyped(harness string) bool {
	return harness != "" && !strings.EqualFold(harness, "devin")
}

// obligationDropFault names the fault a node's drop on harness stands for. A
// node spends its inject budget on rejected sends, so that drop is safe only
// where a rejected send types nothing.
func obligationDropFault(reason, harness string) string {
	if reason == relayDroppedRetriesExhausted && !promptRejectProvesUntyped(harness) {
		return relayDroppedMaybeInPane
	}
	return obligationFaultFor(reason)
}

// obligationDropReplaces is obligationFaultReplaces for a drop. One from a
// harness whose rejected send types nothing also settles a prompt_failed
// report: the safe fault or working expiry it names takes that report's place.
func obligationDropReplaces(fault, harness string) (condition string, args []any) {
	condition, args = obligationFaultReplaces(fault)
	class := classifyObligationFault(fault)
	if promptRejectProvesUntyped(harness) && (class == obligationSafe || class == obligationWorkingExpired) {
		return "(" + condition + " OR fault=?)", append(args, obligationFaultPromptUnproven)
	}
	return condition, args
}

// obligationReopenWhen is the condition under which a drop reopens the row it
// settles to a safe fault (hk 6935). Exhaustion is a verdict on the fault the
// row held when the budget ran out: a maybe-typed fault that a later drop
// settles to a safe one ended copies no pane ever owed it, so while budget
// remains the row is owed again. replace and replaced are the drop's own
// replacement condition and its arguments; they are read against the stored
// row, so the reopen must be written before the fault is.
func obligationReopenWhen(fault, replace string, replaced []any) (string, []any) {
	if classifyObligationFault(fault) != obligationSafe {
		return "1=0", nil
	}
	// Only a stored maybe-typed verdict reopens: a working expiry or a safe fault
	// ended its copies on purpose (hk 6935 review), so a later safe drop that
	// overwrites it does not earn copies back. A noticed row is an exhausted one
	// in an incident; the reopen detaches it, as a revive does.
	args := append([]any{obligationFaultPromptUnproven, obligationFaultReadUnproven, relayReplayMaxAttempts}, replaced...)
	return `state IN ('` + obligationExhausted + `','` + obligationNoticed + `') AND fault IN (?,?) AND spent<? AND ` + replace, args
}

// obligationFaultReplaces is the condition on the stored fault under which
// fault takes its place. A timeout, a missing route and a legacy marker name no
// outcome, so they replace only a timeout or a missing route. Among the other
// safe faults and the working expiry the latest one wins. A maybe-typed fault
// replaces those and is never replaced by them: the text may be in the pane
// whatever happened later. job_ended ends the row whatever it held. The drop
// that follows an unproven pane read settles it either way.
func obligationFaultReplaces(fault string) (condition string, args []any) {
	if classifyObligationFault(fault) == obligationNeverRetry {
		return "1=1", nil
	}
	replaced := []string{"", obligationAckTimeout, obligationFaultUnrouted}
	if fault != obligationAckTimeout && fault != obligationFaultUnrouted && fault != obligationFaultUnconfirmed {
		replaced = append(replaced, relayDroppedPaneReadFailed, relayDroppedRetriesExhausted, relayDroppedWorkingExpired)
	}
	if fault == relayDroppedPaneReadFailed || fault == relayDroppedCheckPane {
		replaced = append(replaced, obligationFaultReadUnproven)
	}
	for _, name := range replaced {
		args = append(args, name)
	}
	return "fault IN (" + strings.TrimSuffix(strings.Repeat("?,", len(replaced)), ",") + ")", args
}

// noticedAtReport reports whether a design lane is told of fault at its first
// report: the node has said the text may be in the pane. A prompt_failed or
// unproven pane read report is not yet its last word; the drop that settles it
// decides (noteObligationDrop). Every other noticed fault waits for the row to
// be exhausted.
func noticedAtReport(fault string) bool {
	return fault == relayDroppedMaybeInPane || fault == relayDroppedCheckPane || fault == obligationFaultUnproven
}

// retriesDone reports whether no further budget copy of a row of kind is owed.
// A revived copy is not a budget retry (revivable).
func (o openObligation) retriesDone(kind string) bool {
	class := classifyObligationFault(o.fault)
	switch {
	case o.spent >= relayReplayMaxAttempts || class == obligationNeverRetry:
		return true
	case kind == "lane.event":
		// Every lane.event copy is labelled, so this caps a maybe-typed row at
		// one labelled copy in its life.
		return class == obligationWorkingExpired || class == obligationMaybeTyped && o.labelled > 0
	default:
		// A job.* copy carries no label and no idle wake revives it. A working
		// expiry has no budget copy at all (design v4 §6); a maybe-typed fault
		// earns one; only a safe fault earns the whole budget (hk 6935).
		return class == obligationWorkingExpired || class != obligationSafe && o.spent > o.attemptsSeen
	}
}

// noteObligationDrop applies a node's relay.dropped, its last word on a
// delivery, to a fault the obligation already has. The report ahead of it has
// usually retired the ack window, so this is where a working expiry after a
// held retry, and the outcome of an unproven pane read, are learned. A row with
// no fault yet takes its first one from the ack window; one with neither -- a
// row the node held across a hub restart -- is only made due a notice
// (noteOrphanedDrop).
func (h *HubServer) noteObligationDrop(ctx context.Context, machine string, dropped relayDroppedPayload) {
	if h.inbox == nil || dropped.OriginalEventID < 1 || !relayDroppedIsUnconfirmed(dropped.Reason) && dropped.Reason != relayDroppedJobEnded {
		return
	}
	// A report from a machine the lane no longer routes to speaks only for an
	// obligation recorded on that machine.
	routes := loadReportRelayRoutes(h.reportRelayPath)
	current := routes[dropped.Lane].Machine == machine
	store := h.inbox.store
	due, err := store.noteObligationDrop(ctx, dropped.OriginalEventID, dropped.Lane, machine, current, dropped.Reason, dropped.Harness, h.now().UTC())
	if err != nil {
		h.logger.Warn("relay drop was not recorded on its obligation", "event_id", dropped.OriginalEventID, "reason", dropped.Reason, "error", store.loggable("drop fault", err))
	}
	// An orphaned drop is handed on whether or not that write landed: the node
	// does not send it again, and the orphan path holds it until it is recorded.
	if relayDroppedIsUnconfirmed(dropped.Reason) && !h.relayWindowOpen(dropped.OriginalEventID, dropped.JobID) {
		h.noteOrphanedDrop(ctx, machine, current, dropped)
	}
	if due {
		h.startObligationReconcile()
	}
}

func (h *HubServer) relayWindowOpen(eventID int64, jobID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, open := h.relayControl.relayPending[relayPendingKey(eventID, jobID)]
	return open
}

// orphanedDropBackoff spaces the retries of an orphaned drop that could not be
// recorded; the last wait repeats until it is. The node does not send the drop
// again, so the hub holds it, in memory, until its row is due a notice or is
// found to owe none.
var orphanedDropBackoff = [...]time.Duration{2 * time.Second, 10 * time.Second, time.Minute, 5 * time.Minute}

// noteOrphanedDrop makes a row due a notice on a drop nothing else will follow
// up: it has no ack window and no fault, so no retry or exhaustion will come.
// The design lane is told from the drop, as it was before there were
// incidents; the row's fault and retry schedule are left alone. A failed read or
// write is retried (orphanedDropBackoff).
func (h *HubServer) noteOrphanedDrop(ctx context.Context, machine string, current bool, dropped relayDroppedPayload) {
	h.noteOrphanedDropAttempt(ctx, machine, current, dropped, 0)
}

func (h *HubServer) noteOrphanedDropAttempt(ctx context.Context, machine string, current bool, dropped relayDroppedPayload, attempt int) {
	store := h.inbox.store
	retry := func(why string) {
		h.logger.Warn("orphaned relay drop was not recorded; it is retried", "event_id", dropped.OriginalEventID, "reason", dropped.Reason, "why", why, "attempt", attempt+1)
		time.AfterFunc(orphanedDropBackoff[min(attempt, len(orphanedDropBackoff)-1)], func() {
			if h.closeCtx.Err() == nil {
				h.noteOrphanedDropAttempt(h.closeCtx, machine, current, dropped, attempt+1)
			}
		})
	}
	fault := obligationDropFault(dropped.Reason, dropped.Harness)
	tracked, err := store.trackedRow(ctx, dropped.OriginalEventID)
	if err == nil && !tracked && h.handoffkeep != nil {
		// The row's obligation was never written and adoption has not reached
		// it; the drop is not sent again.
		readCtx, cancel := context.WithTimeout(ctx, nodeLoopHKTimeout)
		defer cancel()
		record, found, readErr := h.handoffkeep.relayEvent(readCtx, dropped.OriginalEventID)
		if readErr != nil {
			retry("row_read_" + hubHKReadErrorClass(readErr))
			return
		}
		if o, adoptable := adoptableObligation(record); found && adoptable {
			now := h.now().UTC()
			err = store.createObligation(ctx, o, now, now.Add(obligationFirstCheck))
		}
	}
	if err == nil {
		err = store.markOrphanedDropDue(ctx, dropped.OriginalEventID, dropped.Lane, machine, current, fault, h.now().UTC())
	}
	if err != nil {
		h.logger.Warn("orphaned relay drop was not recorded on its obligation", "event_id", dropped.OriginalEventID, "reason", dropped.Reason, "error", store.loggable("orphaned drop", err))
		retry("store")
	}
}

func (s *laneInboxStore) trackedRow(parent context.Context, relayID int64) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	var n int
	err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM delivery_obligations WHERE relay_id=?`), relayID).Scan(&n)
	return n > 0, err
}

// dropScope is the predicate of the obligations a node's drop speaks for: the
// row's, for its lane, and only one recorded on that machine when the lane no
// longer routes there.
func dropScope(relayID int64, lane, machine string, current bool) (where string, keys []any) {
	where, keys = "relay_id=? AND lane=?", []any{relayID, lane}
	if !current {
		where, keys = where+" AND machine=?", append(keys, machine)
	}
	return where, keys
}

// markOrphanedDropDue makes the row with no fault due a notice of fault.
func (s *laneInboxStore) markOrphanedDropDue(parent context.Context, relayID int64, lane, machine string, current bool, fault string, now time.Time) error {
	where, keys := dropScope(relayID, lane, machine, current)
	return s.markNoticeDue(parent, fault, now, "fault='' AND "+where, keys...)
}

// obligationDropStatement is the one UPDATE of a node's drop: the fault, the
// due mark it makes, and the reopen of a safe fault that settles a maybe-typed
// verdict (obligationReopenWhen). Each column is assigned once, since Postgres
// rejects a column assigned twice in one SET list; the statement tests check
// that for every dialect. MySQL reads an assigned column's new value, so the
// reopen reads the stored row before state is assigned, and fault is assigned
// last.
func obligationDropStatement(fault, harness string, now time.Time, where string, keys []any) (string, []any) {
	replace, replaced := obligationDropReplaces(fault, harness)
	// The due mark is made or ended in the same write as the fault.
	due, dued := obligationReportDue(fault, now.UnixMilli())
	if fault == relayDroppedJobEnded {
		due = `notice_due_at=0,`
	}
	reopen, reopened := obligationReopenWhen(fault, replace, replaced)
	// A fault that makes a notice due carries notice_due_at in due. A safe fault
	// has none, so the reopen withdraws a due notice there. The reopen never
	// applies to job_ended or a maybe-typed fault, so the two never meet.
	withdraw, withdrawn := ``, []any(nil)
	if due == `` {
		withdraw = `notice_due_at=CASE WHEN ` + reopen + ` THEN 0 ELSE notice_due_at END,`
		withdrawn = reopened
	}
	args := slices.Concat(reopened, []any{now.UnixMilli()}, reopened, withdrawn, reopened, []any{obligationOwed}, replaced, []any{now.UnixMilli()}, dued, replaced, []any{fault}, keys)
	query := `UPDATE delivery_obligations
		SET next_at=CASE WHEN ` + reopen + ` THEN ? ELSE next_at END,
		    incident_id=CASE WHEN ` + reopen + ` THEN 0 ELSE incident_id END,
		    ` + withdraw + `
		    state=CASE WHEN ` + reopen + ` THEN ? ELSE state END,
		    last_fault_at=CASE WHEN ` + replace + ` THEN ? ELSE last_fault_at END,
		    ` + due + `
		    fault=CASE WHEN ` + replace + ` THEN ? ELSE fault END
		WHERE ` + where
	return query, args
}

// noteObligationDrop reports whether an untaken revive of the row was made due:
// the drop ends the node's hold, which is what kept the pass from queueing it.
// A maybe-typed drop makes the row due a notice, and job_ended ends any notice
// the row awaits. A safe fault that settles a maybe-typed one reopens an
// exhausted row that has budget left (obligationReopenWhen).
func (s *laneInboxStore) noteObligationDrop(parent context.Context, relayID int64, lane, machine string, current bool, reason, harness string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	scope, keys := dropScope(relayID, lane, machine, current)
	where := "fault<>'' AND " + obligationOpen + " AND " + scope
	query, args := obligationDropStatement(obligationDropFault(reason, harness), harness, now, where, keys)
	if _, err := s.db.ExecContext(ctx, s.q(query), args...); err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET next_at=0 WHERE revive_requested>0 AND revived=0 AND `+where), keys...)
	if err != nil {
		return false, err
	}
	count, err := s.dialect.rowsMatched(result)
	return count > 0, err
}
