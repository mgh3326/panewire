package fleet

import (
	"fmt"
	"slices"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// The waste_first arm (hk 3651, docs/placement.md "Shadow comparison").
//
// The live walk takes the best host first and the best account on it, so an
// account at waste risk that exists only on a lower-ranked host is never
// reached. This arm asks which account a pool-first order would have spent
// instead: the pass-1 candidates, sorted by the live comparator (sortSelCands),
// over the hosts the walk could still use. It makes no call and reads no new
// reading. Each candidate's state is read per host, from what the live run
// already holds:
//
//	admitted    the live gate passed it on that host
//	admissible  its readings passed and the live run did not refuse it there
//	refused     refused before admission, refused by the gate on some host (the
//	            live walk skips it everywhere after that), refused by a cached
//	            account-wide exhaustion, or failed by the gate on that host
//	unknown     no reading reached it; never chosen, never assumed admitted

const (
	shadowArmWasteFirst   = "waste_first"
	shadowAccountDiffers  = "differs"
	shadowNoChoice        = "no_choice"
	wasteStateAdmitted    = "admitted"
	wasteStateAdmissible  = "admissible"
	wasteStateRefused     = "refused"
	wasteStateUnknown     = "unknown"
	wasteLiveOutcomeChose = "chosen"
)

// wasteSnap is the candidates the arm orders, as copies in the pass-1 order.
// The copies keep the live walk's later writes (behindCredit, reason, outcome)
// away from the order; orig maps each copy back to the candidate the walk judged.
type wasteSnap struct {
	order []*selCand
	orig  map[*selCand]*selCand
	// pre marks a copy the readings dropped before admission: refused on
	// every host, whatever its reason field says.
	pre map[*selCand]bool
}

// wasteSnapBuilder collects the candidates while admission runs. Each walk
// records a copy per candidate per host it ranks the candidate in pass 1 on,
// with that host's behindCredit: the walk re-derives the flag per host (T2997).
// A candidate the readings dropped before admission is recorded under host "".
// done picks each candidate's copy from its first usable host, so a host the
// walk could not use never supplies the order.
type wasteSnapBuilder struct {
	originals []*selCand
	copies    map[*selCand]map[string]*selCand
}

func newWasteSnapBuilder() *wasteSnapBuilder {
	return &wasteSnapBuilder{copies: map[*selCand]map[string]*selCand{}}
}

// add records c as judged in pass 1 on host ("" for a candidate the readings
// dropped before admission). Call it right after that host's walk has ordered
// its present candidates, so the copy takes that host's flag.
func (b *wasteSnapBuilder) add(c *selCand, host string) {
	if c.accountKey == "" {
		return
	}
	m := b.copies[c]
	if m == nil {
		m = map[string]*selCand{}
		b.copies[c] = m
		b.originals = append(b.originals, c)
	}
	if _, ok := m[host]; !ok {
		cp := *c
		m[host] = &cp
	}
}

// done builds the order from the usable hosts (rank order). A failure is
// recorded as the arm's shadow_error; the live walk goes on.
func (b *wasteSnapBuilder) done(usable []selHost) (snap wasteSnap, failure string) {
	defer func() {
		if r := recover(); r != nil {
			snap, failure = wasteSnap{}, fmt.Sprint("panic: ", r)
		}
	}()
	snap = wasteSnap{orig: map[*selCand]*selCand{}, pre: map[*selCand]bool{}}
	for _, o := range b.originals {
		m := b.copies[o]
		cp, pre := m[""], m[""] != nil
		for _, h := range usable {
			if cp != nil {
				break
			}
			cp = m[h.name]
		}
		if cp == nil {
			continue
		}
		snap.order = append(snap.order, cp)
		snap.orig[cp] = o
		snap.pre[cp] = pre
	}
	sortSelCands(snap.order)
	return snap, ""
}

// quotaUnobserved reports that no reading reached the candidate: its rung could
// not be classified (quota_unknown), or its account's reading was missing or
// failed (an unknown judgment).
func quotaUnobserved(c *selCand) bool {
	return c.code == outcomeQuotaUnknown || slices.ContainsFunc(c.judgments, func(j quotaJudgment) bool { return j.Outcome == "unknown" })
}

// wasteStateOn is the state of candidate o (pre: dropped before admission) on one usable host, as the
// live run left it. Only the admitting host is admitted; a gate refusal the walk
// skips on other hosts (refusedOn) and a cached exhaustion hold everywhere; a
// gate failure holds on the host it failed on only.
func wasteStateOn(o *selCand, pre bool, host string, st *hostAdmission) string {
	switch {
	case o.outcome == wasteLiveOutcomeChose && o.host == host:
		return wasteStateAdmitted
	case quotaUnobserved(o):
		return wasteStateUnknown
	case pre:
		return wasteStateRefused
	}
	if _, ok := st.refusedOn[o]; ok {
		return wasteStateRefused
	}
	if _, ok := st.exhausted[o.accountKey]; ok {
		return wasteStateRefused
	}
	if st.failedOn[o][host] {
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

// wasteFind returns the first candidate in the snapshot order that matches and
// is admitted or admissible on some usable host it is present on, with that
// host. Hosts are tried in rank order.
func wasteFind(snap wasteSnap, usable []selHost, st *hostAdmission, match func(o *selCand) bool) (*selCand, string) {
	for _, c := range snap.order {
		o := snap.orig[c]
		if !match(o) {
			continue
		}
		for _, h := range usable {
			if !slices.ContainsFunc(c.hosts, func(x selHost) bool { return x.name == h.name }) {
				continue
			}
			if s := wasteStateOn(o, snap.pre[c], h.name, st); s == wasteStateAdmitted || s == wasteStateAdmissible {
				return o, h.name
			}
		}
	}
	return nil, ""
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
// snapshot order that a usable host holds. It reads p, the snapshot and the
// admission record only.
func wasteFirst(snap wasteSnap, p *selPlan, usable []selHost, st *hostAdmission, livePass int) *fleetevent.WasteFirst {
	x := &fleetevent.WasteFirst{Outcome: shadowNoChoice}
	liveAccount := ""
	if p.chosen != nil {
		x.Live = wasteChoice(p.chosen, p.chosen.host, livePass)
		liveAccount = p.chosen.accountKey
	}
	pick, pickHost := wasteFind(snap, usable, st, func(*selCand) bool { return true })
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
		if o, host := wasteFind(snap, usable, st, func(o *selCand) bool { return o.risk && o.accountKey != liveAccount }); o != nil {
			x.LeftUnspent = wasteAccount(o, host)
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
func (a *App) runWasteFirst(snap wasteSnap, snapErr string, p *selPlan, usable []selHost, st *hostAdmission, livePass int) (x *fleetevent.WasteFirst) {
	defer func() {
		if r := recover(); r != nil {
			x = &fleetevent.WasteFirst{Outcome: shadowError, Error: fmt.Sprint("panic: ", r)}
		}
	}()
	if snapErr != "" {
		return &fleetevent.WasteFirst{Outcome: shadowError, Error: snapErr}
	}
	if a.shadowSnapProbe != nil {
		a.shadowSnapProbe(snap.order)
	}
	if a.shadowProbe != nil {
		a.shadowProbe(shadowArmWasteFirst)
	}
	return wasteFirst(snap, p, usable, st, livePass)
}
