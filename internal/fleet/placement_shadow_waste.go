package fleet

import (
	"fmt"
	"slices"
	"strings"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// The waste_first arm (hk 3651, docs/placement.md "Shadow comparison").
//
// The live walk takes the best host first and the best account on it, so an
// account at waste risk that exists only on a lower-ranked host is never
// reached. This arm asks which account a pool-first order would have spent
// instead: the pass-1 candidates, sorted by the live comparator (sortSelCands),
// over the hosts the walk could still use. It makes no call and reads no new
// reading. Each candidate's state is the one the live run already holds:
//
//	admitted    the live gate passed it on some host
//	admissible  its readings passed and the live run never gated it
//	refused     the live gate, or a cached exhaustion, refused it
//	unknown     no reading reached it; never chosen, never assumed admitted

const (
	shadowArmWasteFirst   = "waste_first"
	shadowAccountDiffers  = "differs"
	shadowNoChoice        = "no_choice"
	wasteStateAdmitted    = "admitted"
	wasteStateAdmissible  = "admissible"
	wasteStateRefused     = "refused"
	wasteStateUnknown     = "unknown"
	wasteRefusedPrefix    = "dropped:"
	wasteLiveOutcomeChose = "chosen"
)

// wasteSnap is the bound candidates as they stood before admission, in the pass-1
// order. The live walk rewrites behindCredit per host, so the order is taken
// first. Its entries are copies, so the walk's later writes never reach the
// order; orig maps each copy back to the candidate the walk judged.
type wasteSnap struct {
	order []*selCand
	orig  map[*selCand]*selCand
}

// takeWasteSnap copies every candidate bound to an account, pass-1 band only,
// and sorts the copies with the live comparator. Candidates the readings already
// dropped are kept: wasteState reads them as refused or unknown, so the order
// needs no separate rule for them. A candidate in the credit band (pass 2) is
// left out, as the live walk leaves it out of pass 1.
func takeWasteSnap(cands []*selCand) wasteSnap {
	snap := wasteSnap{orig: map[*selCand]*selCand{}}
	for _, c := range cands {
		if c.accountKey == "" || c.behindCredit || c.creditPool && !c.risk {
			continue
		}
		cp := *c
		snap.order = append(snap.order, &cp)
		snap.orig[&cp] = c
	}
	sortSelCands(snap.order)
	return snap
}

// wasteSnapOf takes the snapshot under a recover. A failure is recorded as the
// arm's shadow_error; the live walk goes on.
func wasteSnapOf(cands []*selCand) (snap wasteSnap, failure string) {
	defer func() {
		if r := recover(); r != nil {
			snap, failure = wasteSnap{}, fmt.Sprint("panic: ", r)
		}
	}()
	return takeWasteSnap(cands), ""
}

// quotaUnobserved reports that no reading reached the candidate: its rung could
// not be classified (quota_unknown), or its account's reading was missing or
// failed (an unknown judgment).
func quotaUnobserved(c *selCand) bool {
	return c.code == outcomeQuotaUnknown || slices.ContainsFunc(c.judgments, func(j quotaJudgment) bool { return j.Outcome == "unknown" })
}

// wasteState is the candidate's quota state as the live run observed it. It is
// read after admission, so a gated candidate carries its outcome. A candidate
// with a drop reason is refused or unknown, never admissible.
func wasteState(c *selCand) string {
	switch {
	case c.outcome == wasteLiveOutcomeChose:
		return wasteStateAdmitted
	case quotaUnobserved(c):
		return wasteStateUnknown
	case c.reason != "" || strings.HasPrefix(c.outcome, wasteRefusedPrefix):
		return wasteStateRefused
	}
	return wasteStateAdmissible
}

// wasteHost is the first usable host, in rank order, that holds the candidate's
// account, or "" when none does.
func wasteHost(c *selCand, usable []selHost) string {
	for _, h := range usable {
		if slices.ContainsFunc(c.hosts, func(x selHost) bool { return x.name == h.name }) {
			return h.name
		}
	}
	return ""
}

// wasteAccount names one waste-risk account as the event records it.
func wasteAccount(c *selCand, host string) *fleetevent.ShadowAccount {
	return &fleetevent.ShadowAccount{Host: host, Pool: c.row.Pool, AccountKey: c.accountKey, UnusedAtResetPct: c.unused, ResetHours: c.resetHrs}
}

// wasteChoice is one account choice as the event records it.
func wasteChoice(c *selCand, host string, pass int) *fleetevent.ShadowChoice {
	return &fleetevent.ShadowChoice{Host: host, Profile: c.row.Profile, Seat: c.seatName, AccountKey: c.accountKey, Pool: c.row.Pool, Tier: c.tierName, Pass: pass, WasteRisk: c.risk, UnusedAtResetPct: c.unused, ResetHours: c.resetHrs}
}

// wasteFirst is the pure arm: the first admitted or admissible candidate in the
// snapshot order that a usable host holds. It reads p and the snapshot only.
func wasteFirst(snap wasteSnap, p *selPlan, usable []selHost, livePass int) *fleetevent.WasteFirst {
	x := &fleetevent.WasteFirst{Outcome: shadowNoChoice}
	liveAccount := ""
	if p.chosen != nil {
		x.Live = wasteChoice(p.chosen, p.chosen.host, livePass)
		liveAccount = p.chosen.accountKey
	}
	var pick *selCand
	pickHost := ""
	for _, c := range snap.order {
		o := snap.orig[c]
		host := wasteHost(c, usable)
		if host == "" {
			continue
		}
		if st := wasteState(o); st == wasteStateAdmitted || st == wasteStateAdmissible {
			pick, pickHost = o, host
			break
		}
	}
	if pick != nil {
		x.Choice = wasteChoice(pick, pickHost, 1)
		x.Outcome = shadowAgree
		if x.Live == nil || x.Live.AccountKey != pick.accountKey {
			x.Outcome = shadowAccountDiffers
		}
	}
	if x.Outcome == shadowAccountDiffers {
		// Name the waste-risk account the live choice left unspent: the first
		// risk candidate, in this order, that is spendable and not the live one.
		for _, c := range snap.order {
			o := snap.orig[c]
			host := wasteHost(c, usable)
			if !o.risk || host == "" || o.accountKey == liveAccount {
				continue
			}
			if st := wasteState(o); st == wasteStateAdmitted || st == wasteStateAdmissible {
				x.LeftUnspent = wasteAccount(o, host)
				break
			}
		}
	}
	seen := map[string]bool{}
	for _, c := range p.cands {
		if c.accountKey == "" || seen[c.accountKey] || !quotaUnobserved(c) || wasteHost(c, usable) == "" {
			continue
		}
		seen[c.accountKey] = true
		if len(x.Unknown) < fleetevent.MaxCandidates {
			x.Unknown = append(x.Unknown, c.accountKey)
		}
	}
	return x
}

// runWasteFirst runs the arm under a recover. A snapshot failure or a panic in
// the arm is recorded as shadow_error with the reason; the spawn goes on.
func (a *App) runWasteFirst(snap wasteSnap, snapErr string, p *selPlan, usable []selHost, livePass int) (x *fleetevent.WasteFirst) {
	defer func() {
		if r := recover(); r != nil {
			x = &fleetevent.WasteFirst{Outcome: shadowError, Error: fmt.Sprint("panic: ", r)}
		}
	}()
	if snapErr != "" {
		return &fleetevent.WasteFirst{Outcome: shadowError, Error: snapErr}
	}
	if a.shadowProbe != nil {
		a.shadowProbe(shadowArmWasteFirst)
	}
	return wasteFirst(snap, p, usable, livePass)
}
