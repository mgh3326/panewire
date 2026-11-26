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
		// A job.* copy carries no label and no idle wake revives it: only a
		// safe fault earns a second one.
		return class != obligationSafe && o.spent > o.attemptsSeen
	}
}

// noteObligationDrop applies a node's relay.dropped, its last word on a
// delivery, to a fault the obligation already has. The report ahead of it has
// usually retired the ack window, so this is where a working expiry after a
// held retry, and the outcome of an unproven pane read, are learned. A row with
// no fault yet takes its first one from the ack window.
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
		return
	}
	if due {
		h.startObligationReconcile()
	}
}

// noteObligationDrop reports whether an untaken revive of the row was made due:
// the drop ends the node's hold, which is what kept the pass from queueing it.
func (s *laneInboxStore) noteObligationDrop(parent context.Context, relayID int64, lane, machine string, current bool, reason, harness string, now time.Time) (bool, error) {
	ctx, done := s.op(parent)
	defer done()
	where, keys := "relay_id=? AND lane=? AND fault<>'' AND "+obligationOpen, []any{relayID, lane}
	if !current {
		where, keys = where+" AND machine=?", append(keys, machine)
	}
	fault := obligationDropFault(reason, harness)
	replace, replaced := obligationDropReplaces(fault, harness)
	// fault is assigned last: MySQL reads an assigned column's new value.
	args := slices.Concat(replaced, []any{now.UnixMilli()}, replaced, []any{fault}, keys)
	if _, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations
		SET last_fault_at=CASE WHEN `+replace+` THEN ? ELSE last_fault_at END,
		    fault=CASE WHEN `+replace+` THEN ? ELSE fault END
		WHERE `+where), args...); err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, s.q(`UPDATE delivery_obligations SET next_at=0 WHERE revive_requested>0 AND revived=0 AND `+where), keys...)
	if err != nil {
		return false, err
	}
	count, err := s.dialect.rowsMatched(result)
	return count > 0, err
}
