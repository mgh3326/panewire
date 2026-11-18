package fleet

import (
	"slices"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// ledgerView is the one fold of a job's ledger: the current attempt and what
// ended it, read in numeric sequence order and never in timestamp order (a
// node clock stepped backwards stamps a later event earlier). watch,
// rearmWatchers, announcedQuotaStop, reassignedInLedger, the quota
// announcement's guard and the watcher's stale-decision check all read it, so
// they cannot disagree about an attempt (hk 5224, consult-4761-bb8ce409).
//
// Events that share one numeric prefix form a group and are folded together:
// a revocation is applied after the other events of its group, because the
// hub allocated its sequence without the events lock before hk 5044 and a
// tied revocation may have been published after the spawn it shares a number
// with. A claim, reclaim or spawn at a strictly greater sequence clears it.
type ledgerView struct {
	// Attempt identifies the current attempt: the ledger name of the newest
	// claim, reclaim, real spawn or re-prompt. A re-prompt starts a new
	// attempt on the same pane, so a decision observed before it is stale.
	Attempt string
	// Resume is the ledger name of the newest re-prompt of this run, the
	// watcher's resume marker; empty when none follows the last claim or spawn.
	Resume string
	// Pane is the pane of the newest job.spawned receipt, adopted or not.
	Pane string
	// Ended is the kind of the newest end of the current run (job.completed,
	// job.joined, job.revoked, job.handed_off, job.lost); a re-prompt clears it.
	Ended string
	// Reaped and Revoked are sticky: a re-prompt or an adopted receipt keeps
	// them, only a real claim, reclaim or spawn clears them.
	Reaped, Revoked bool
	// Reassigned is whether a hub revocation record stands (hubRevocation).
	Reassigned bool
	// Settled is whether a settled hub close ended the run (hubCloseSettledLoss);
	// SettledMark names the newest one over the whole ledger.
	Settled     bool
	SettledMark string

	// closed is an end quota actions must not follow: a completion, a settled
	// record, an escalation, a join, a hand-off or a loss that is not a
	// non-final quota stop.
	closed bool
	// quota is the non-final quota stop the newest job.lost announced.
	quota *quotastop.Stop
}

// viewLedger folds evs, in any order, into the job's ledgerView.
func viewLedger(evs []Event) ledgerView {
	var v ledgerView
	for _, ev := range sequenceGroups(evs) {
		v.apply(ev)
	}
	return v
}

// sequenceGroups is ledgerOrder with each group of events sharing a numeric
// prefix reordered so its revocations come last.
func sequenceGroups(evs []Event) []Event {
	out := ledgerOrder(evs)
	for i := 0; i < len(out); {
		seq := ledgerSeq(out[i].name)
		j := i + 1
		for seq >= 0 && j < len(out) && ledgerSeq(out[j].name) == seq {
			j++
		}
		if j-i > 1 {
			slices.SortStableFunc(out[i:j], func(x, y Event) int {
				return revokedLast(x) - revokedLast(y)
			})
		}
		i = j
	}
	return out
}

func revokedLast(ev Event) int {
	if ev.Kind == "job.revoked" {
		return 1
	}
	return 0
}

func (v *ledgerView) apply(ev Event) {
	p := ev.Payload
	switch ev.Kind {
	case "job.claim", "job.claimed", "job.reclaim":
		v.start(ev.name)
	case "job.spawned":
		if str(p, "adopted_from") != "" {
			// A receipt `done` adopted from job.brief_sent (adoptBriefSent) is
			// this node's own late bookkeeping for the attempt already
			// running: it supplies the pane and clears no end, revocation or
			// reassignment, and it is not a new attempt (hk 5174).
			v.Pane = str(p, "pane_id")
			return
		}
		v.start(ev.name)
		v.Pane = str(p, "pane_id")
	case "job.reprompted":
		// A re-prompt revives a live attempt on the same pane; it restores no
		// ownership: a reap and a revocation survive it.
		v.Attempt, v.Resume = ev.name, ev.name
		v.Ended, v.closed, v.quota, v.Settled = "", false, nil, false
	case "job.completed", "job.joined", "job.handed_off":
		v.Ended, v.closed = ev.Kind, true
	case "job.completion", eventRecordSettled, "job.escalate":
		// Ends for quota actions that the watcher still reads as a live run:
		// report recovery and credit publication follow them.
		v.closed = true
	case "job.revoked":
		v.Ended, v.Revoked = ev.Kind, true
		if hubRevocation(ev) {
			v.Reassigned = true
		}
	case "job.reaped":
		v.Reaped = true
	case "job.lost":
		v.Ended = ev.Kind
		if hubCloseSettledLoss(ev) {
			v.Settled, v.SettledMark = true, ev.name
		}
		if str(p, "reason") == "quota_stop" && !boolean(p, "final", false) {
			stop := quotastop.Stop{Pool: str(p, "pool")}
			if reset, err := time.Parse(time.RFC3339, str(p, "reset_at")); err == nil {
				stop.Reset = reset
			}
			stop.CreditExhaustedAt, _ = time.Parse(time.RFC3339Nano, str(p, "credit_exhausted_at"))
			v.quota = &stop
			return
		}
		// A final stop is a reviewer whose hk record the watcher already wrote
		// lost: nothing is left to hand over.
		v.closed, v.quota = true, nil
	}
}

// start is a real claim, reclaim or spawn: a new attempt that clears every end.
func (v *ledgerView) start(name string) {
	settledMark := v.SettledMark
	*v = ledgerView{Attempt: name, Pane: v.Pane, SettledMark: settledMark}
}

// Announced is the quota stop a job.lost of the current attempt announced and
// that no other end follows: the hub may still replace the job. A final stop,
// a revoked or reaped attempt and any other end are not hand-overs.
func (v ledgerView) Announced() (quotastop.Stop, bool) {
	if v.quota == nil || v.Ended != "job.lost" || v.closed || v.Revoked || v.Reaped {
		return quotastop.Stop{}, false
	}
	return *v.quota, v.quota.Pool != ""
}

// Superseded reports whether a quota action observed on pane, in the attempt
// named attempt, must not be taken now: the attempt ended, was revoked or
// reaped, or is not the one that was observed. An empty pane or attempt
// leaves that observation unchecked.
func (v ledgerView) Superseded(pane, attempt string) bool {
	if v.closed || v.Revoked || v.Reaped {
		return true
	}
	if pane != "" && v.Pane != "" && v.Pane != pane {
		return true
	}
	if attempt == "" {
		return false
	}
	current := v.Attempt
	if current == "" {
		current = NoAttempt
	}
	return current != attempt
}

// decision is what a watcher decision depends on: it is stale once any of it
// differs under the end lock (endUnlessHubClose).
type decisionMark struct {
	Attempt, SettledMark, Ended string
	Reaped, Revoked             bool
}

func (v ledgerView) decision() decisionMark {
	return decisionMark{v.Attempt, v.SettledMark, v.Ended, v.Reaped, v.Revoked}
}
