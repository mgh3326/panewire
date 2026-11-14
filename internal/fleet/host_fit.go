package fleet

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
)

// Host fit is the static half of host-first placement: which work a host can
// take before any account or quota is read. Three host keys decide it, all
// data in hosts.toml ([local] or [hosts.NAME]):
//
//   - tags (alias caps): the capabilities the host offers. A task's --tags
//     must be a subset. A capability the host does not declare is unknown and
//     fails closed for that host only; other hosts stay eligible.
//   - roles: the spawn roles the host accepts. Unset accepts every role.
//   - grades: the task grades the host accepts. Unset accepts every grade. A
//     consult has no grade, so a host with grades set refuses it.

var spawnRoles = []string{"implementer", "reviewer", "consult"}
var hostCapabilities = []string{"macos", "linux", "unity", "xcode", "gpu", "heavy-go"}

// hostCaps returns the declared capabilities of a host: tags and caps merged.
func hostCaps(cfg map[string]any) []string {
	var out []string
	for _, t := range append(stringsValue(cfg, "tags"), stringsValue(cfg, "caps")...) {
		if t = strings.TrimSpace(t); t != "" && !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out
}

// missingCaps lists the required tags a host does not declare.
func missingCaps(cfg map[string]any, required []string) []string {
	caps := hostCaps(cfg)
	var out []string
	for _, t := range required {
		if !slices.Contains(caps, t) {
			out = append(out, t)
		}
	}
	return out
}

// roleRefusal names why a host does not take this role and grade, or "".
// An empty grade is a consult, which a grade-limited host refuses.
func roleRefusal(cfg map[string]any, role, grade string) string {
	if roles := stringsValue(cfg, "roles"); len(roles) > 0 && !slices.Contains(roles, role) {
		return "role_not_accepted:" + role
	}
	if grades := stringsValue(cfg, "grades"); len(grades) > 0 && !slices.Contains(grades, grade) {
		return "grade_not_accepted:" + firstNonempty(grade, "none")
	}
	return ""
}

// validHostFit checks the caps, roles, and grades lists of one host table.
func validHostFit(h map[string]any, key string) error {
	if v, ok := h["caps"]; ok {
		list, isList := v.([]any)
		if !isList {
			return fail(exitConfig, "%s.caps must be a list of known capability strings, got %#v", key, v)
		}
		for _, x := range list {
			if s, isStr := x.(string); !isStr || !slices.Contains(hostCapabilities, s) {
				return fail(exitConfig, "%s.caps contains an invalid capability %#v", key, x)
			}
		}
	}
	for _, f := range []struct {
		name  string
		valid func(string) bool
	}{
		{"roles", func(v string) bool { return slices.Contains(spawnRoles, v) }},
		{"grades", func(v string) bool { _, ok := gradeOrder[v]; return ok }},
	} {
		v, ok := h[f.name]
		if !ok {
			continue
		}
		list, isList := v.([]any)
		if !isList || len(list) == 0 {
			return fail(exitConfig, "%s.%s must be a nonempty list", key, f.name)
		}
		for _, x := range list {
			if x == "builder" && f.name == "roles" {
				return fail(exitConfig, "%s.roles lists the retired seat role \"builder\"; delete \"builder\" from %s.roles", key, key)
			}
			if s, isStr := x.(string); !isStr || !f.valid(s) {
				return fail(exitConfig, "%s.%s contains an invalid value %v", key, f.name, x)
			}
		}
	}
	return nil
}

// hostBucket orders hosts for host-first selection the way placeBucket orders
// placement candidates: awake hosts under the load threshold, then awake hosts
// without a load reading, then awake hosts at or over it, then sleepers.
func hostBucket(h selHost) int {
	switch {
	case !h.awake:
		return 3
	case h.over:
		return 2
	case h.load == nil:
		return 1
	}
	return 0
}

// floorKey is a host's free-floor ranking key from the hub's signals: whether
// its free fraction is under the floor (the hub's verdict) and the value that
// orders hosts when every host is under it: the host's score, which is free
// times power less the bridge cost of the jobs already decided onto it (so a
// burst spreads), or free times power when the host has no score. A host the
// hub sent no terms for is never under the floor.
func floorKey(sig PlacementSignals, score *float64) (below bool, value float64) {
	if sig.Terms == nil {
		return false, 0
	}
	if score != nil {
		return sig.BelowFreeFloor, *score
	}
	power := 100.0
	if sig.Power != nil && *sig.Power > 0 {
		power = *sig.Power
	}
	return sig.BelowFreeFloor, sig.Terms.Free() * power
}

// floorSide is one host as the free floor sees it.
type floorSide struct {
	awake, below, usable bool
	value                float64
}

// tier is a host's place under the free floor: 0 eligible (usable and at or
// above the floor), 1 usable but under it, 2 unusable by the hub's verdict
// (throttled, at its job cap, slot-full, not accepting).
func (s floorSide) tier() int {
	switch {
	case !s.usable:
		return 2
	case s.below:
		return 1
	}
	return 0
}

// floorCtx is what the free floor knows about the whole list.
type floorCtx struct {
	// anyBelow: some awake host is under the floor. With none, the floor does
	// nothing and the ranking is the one it was before the floor existed.
	anyBelow bool
	// anyUsable: some awake host is usable by the hub's verdict. With none the
	// floor has nothing to prefer.
	anyUsable bool
	// anyEligible: some awake host is usable and not under the floor. When
	// some host is usable and none is eligible, every usable host is under it.
	anyEligible bool
}

func floorContext(sides []floorSide) floorCtx {
	var c floorCtx
	for _, s := range sides {
		if !s.awake {
			continue
		}
		c.anyBelow = c.anyBelow || s.below
		c.anyUsable = c.anyUsable || s.usable
		c.anyEligible = c.anyEligible || s.usable && !s.below
	}
	return c
}

// floorOrder is the free floor as an eligibility rule that runs before the load
// bucket (decision 231). It looks only at awake hosts: a sleeper stays behind
// every awake host through the bucket order, and has no metrics to judge.
// Awake hosts rank by tier: eligible first, then usable hosts under the floor,
// then the hub's unusable ones, which stay as fallbacks and never outrank a
// usable host, whatever their free reading or bucket (a synthetic slot-full view
// has no terms and reads as above the floor). Within a tier the other rules
// order the hosts, as they did before the floor, except that when every usable
// host is under the floor (none is eligible) the higher floorValue, the score,
// ranks first and the bucket does not apply. With no awake host under the floor
// or none usable it does nothing. 0 means the floor does not separate the two.
func floorOrder(x, y floorSide, c floorCtx) int {
	if !x.awake || !y.awake || !c.anyBelow || !c.anyUsable {
		return 0
	}
	if tx, ty := x.tier(), y.tier(); tx != ty {
		return tx - ty
	}
	if !c.anyEligible {
		return cmp.Compare(y.value, x.value)
	}
	return 0
}

// rankHosts is the placement-aligned host order used by the shadow arms: the
// free floor, then bucket, the hub's decided host, score when every ranked
// host has one, else raw load and configuration order (local first).
func rankHosts(hosts []selHost) []selHost {
	return rankHostsWithCapacity(hosts, false)
}

// rankSelectionHosts compares current capacity before the hub's decided host.
// Selection must choose a seat before placement can see its hosts, so the
// bridge cost in the hub score can move auto selection off a saturated seat.
func rankSelectionHosts(hosts []selHost) []selHost {
	return rankHostsWithCapacity(hosts, true)
}

func rankHostsWithCapacity(hosts []selHost, capacityFirst bool) []selHost {
	var out []selHost
	for _, h := range hosts {
		if h.fit == "" {
			out = append(out, h)
		}
	}
	byScore := !slices.ContainsFunc(out, func(h selHost) bool { return h.score == nil })
	side := func(h selHost) floorSide {
		return floorSide{awake: h.awake, below: h.below, usable: hubUsable(h, h.memExcluded), value: h.floorValue}
	}
	sides := make([]floorSide, len(out))
	for i, h := range out {
		sides[i] = side(h)
	}
	floor := floorContext(sides)
	slices.SortStableFunc(out, func(x, y selHost) int {
		if c := floorOrder(side(x), side(y), floor); c != 0 {
			return c
		}
		if bx, by := hostBucket(x), hostBucket(y); bx != by {
			return bx - by
		}
		if capacityFirst && byScore {
			if c := cmp.Compare(*y.score, *x.score); c != 0 {
				return c
			}
		}
		// The hub's decision carries its hysteresis margin and all-loaded rule;
		// it breaks a tie after selection has compared current host capacity.
		if x.decided != y.decided {
			if x.decided {
				return -1
			}
			return 1
		}
		if !capacityFirst && byScore {
			if c := cmp.Compare(*y.score, *x.score); c != 0 {
				return c
			}
		}
		if !byScore {
			return cmp.Compare(loadOrInf(x.load), loadOrInf(y.load))
		}
		return 0
	})
	return out
}

// hostTry is one (account, rung) attempt on a ranked host.
type hostTry struct {
	cand   *selCand
	result string // admitted, refused, skipped
	reason string
	code   string
}

// hostTrace explains one host's part in host-first selection.
type hostTrace struct {
	drops []readinessDrop
	host  selHost
	rank  int
	// pass is the band the row belongs to: 1 for the ranked candidates, 2 for
	// the credit band tried only when pass 1 chose nothing.
	pass    int
	missing []string
	tries   []hostTry
	outcome string
}

func (t hostTrace) record() map[string]any {
	h := t.host
	caps := hostCaps(h.cfg)
	if caps == nil {
		caps = []string{}
	}
	row := map[string]any{"host": h.name, "caps": caps, "caps_match": len(t.missing) == 0, "awake": h.awake, "over_threshold": h.over, "outcome": t.outcome, "admission_outcome": t.admissionOutcome()}
	if h.local {
		row["local"] = true
	}
	if len(t.missing) > 0 {
		row["missing_caps"] = t.missing
	}
	if t.rank > 0 {
		row["rank"] = t.rank
		row["pass"] = t.pass
	}
	if h.load != nil {
		row["load_ratio"] = *h.load
	}
	if h.score != nil {
		row["score"] = *h.score
	}
	if len(t.drops) > 0 {
		drops := []any{}
		for _, d := range t.drops {
			drops = append(drops, map[string]any{"profile": d.profile, "code": d.code})
		}
		row["readiness_drops"] = drops
	}
	tried := []any{}
	for _, x := range t.tries {
		c := x.cand
		r := map[string]any{"account_key": c.accountKey, "profile": firstNonempty(c.seatName, c.row.Profile), "effort": c.row.Effort, "pool": c.row.Pool, "tier": c.tierName, "result": x.result, "outcome": admissionOutcome(x.code)}
		if x.reason != "" {
			r["reason"] = x.reason
		}
		if x.code != "" {
			r["outcome_code"] = x.code
		}
		tried = append(tried, r)
	}
	row["tried"] = tried
	return row
}

func admissionOutcome(code string) string {
	switch code {
	case outcomeAdmitted, outcomeQuotaExhausted, outcomeQuotaUnknown:
		return strings.ToLower(code)
	default:
		return "no_rung"
	}
}

func (t hostTrace) admissionOutcome() string {
	if t.outcome == "not_tried" {
		return "not_tried"
	}
	outcome := "no_rung"
	for _, x := range t.tries {
		switch x.code {
		case outcomeAdmitted:
			return "admitted"
		case outcomeQuotaUnknown:
			outcome = "quota_unknown"
		case outcomeQuotaExhausted:
			if outcome == "no_rung" {
				outcome = "quota_exhausted"
			}
		}
	}
	return outcome
}

// capsGap names the required tags no usable host declares, for a refusal.
func capsGap(hosts []selHost, required []string) string {
	if len(required) == 0 {
		return ""
	}
	var missing []string
	all := false
	for _, t := range required {
		if !slices.ContainsFunc(hosts, func(h selHost) bool { return !h.excluded && slices.Contains(hostCaps(h.cfg), t) }) {
			missing = append(missing, t)
		}
	}
	for _, h := range hosts {
		if !h.excluded && len(missingCaps(h.cfg, required)) == 0 {
			all = true
		}
	}
	switch {
	case len(missing) > 0:
		return fmt.Sprintf("; no host declares cap %s", strings.Join(missing, ","))
	case !all:
		return fmt.Sprintf("; no single host declares all caps %s", strings.Join(required, ","))
	}
	return ""
}

// admitHostFirst picks the machine first, then the model: hosts that fit the
// work are ranked (rankHosts), and on each ranked host the ranked (account,
// rung) candidates present there are judged in pool order through the
// unchanged quota gate. The first admitted pair wins. A refusal about the
// account itself holds on every host. Exhaustion is cached by account, while
// other refusals remain scoped to the rung. A host-level failure skips that
// host for the candidate, and an unreachable host is skipped for the rest. A host
// whose present candidates are all refused is skipped, not fatal.
//
// Credit is a fleet-wide last band. Pass 1 walks the ranked hosts with every
// candidate except plain credit and the pools ranked behind credit; pass 2
// walks the same hosts with those, only when pass 1 chose nothing. Quota
// policy, including expiring credit, still orders rungs inside a host in each
// pass: expiring credit is waste-risk, so it stays in pass 1. A pool is paced
// behind credit (behindCredit) only by credit present on its own host (T2997);
// credit on another host is only the pass 2 last band and never demotes it.
// See docs/placement.md for the selection order and trace fields.
func (sp *selPresence) admitHostFirst(ctx context.Context, s Spawn, p *selPlan, pending []*selCand) {
	ranked := rankSelectionHosts(sp.hosts)
	role := s.Role
	if p.req.route.top() {
		role = p.req.route.role
	}
	// Record how the shadow arms would have ordered the same hosts. The live
	// placement order stays separate from the capacity-first seat order above.
	// The event is written after the passes below, once the waste_first arm can
	// read the quota states they produce.
	shadow := sp.a.recordPlacementShadow(s, role, p.req.tags, sp.hosts, rankHosts(sp.hosts), sp.now)
	// The waste_first order is collected as the walk below judges each host
	// (wasteSnapBuilder). Candidates the readings dropped before admission go in
	// first: the arm reads them as refused or unknown.
	wb := newWasteSnapBuilder()
	for _, c := range p.cands {
		if !slices.Contains(pending, c) {
			wb.add(c, "")
		}
	}
	// Order each host's present candidates once, on the whole host, so the
	// credit-pacing rule sees every candidate the host holds. The passes then
	// take their share in that order.
	bands := make([][2][]*selCand, len(ranked))
	pacedBand := false
	for i, h := range ranked {
		var present []*selCand
		for _, c := range pending {
			if slices.ContainsFunc(c.hosts, func(x selHost) bool { return x.name == h.name }) {
				present = append(present, c)
			}
		}
		orderSelCands(present)
		for _, c := range present {
			// Credit is a free-band candidate and walks in pass 1. Only a
			// subscription pool paced behind credit waits for pass 2.
			band := 0
			if c.behindCredit {
				band, pacedBand = 1, true
			}
			bands[i][band] = append(bands[i][band], c)
			if band == 0 {
				wb.add(c, h.name)
			}
		}
	}
	st := &hostAdmission{refusedOn: map[*selCand]string{}, exhausted: map[string]hostTry{}, down: map[string]string{}, failedOn: map[*selCand]map[string]bool{}}
	sp.admitPass(ctx, s, p, pending, ranked, st, 1, func(i int) []*selCand { return bands[i][0] })
	livePass := 1
	if p.chosen == nil && pacedBand {
		sp.admitPass(ctx, s, p, pending, ranked, st, 2, func(i int) []*selCand { return bands[i][1] })
		if p.chosen != nil {
			livePass = 2
		}
	}
	// The waste_first arm reads the states the passes left. A host the walk
	// could not reach, or that failed its gate, is not usable for it.
	var usable []selHost
	for _, h := range ranked {
		if sp.unreachable[h.name] == "" && st.down[h.name] == "" {
			usable = append(usable, h)
		}
	}
	snap, snapErr := wb.done(usable)
	sp.a.finishPlacementShadow(shadow, snap, snapErr, st, p, usable, livePass)
	for _, h := range sp.hosts {
		if h.fit != "" {
			p.hosts = append(p.hosts, hostTrace{host: h, missing: missingCaps(h.cfg, p.req.tags), outcome: h.fit})
		}
	}
	for _, c := range pending {
		if p.chosen != nil && c.outcome == "" {
			c.outcome = "below_choice"
		}
	}
}

// hostAdmission is the state one decision's passes share: refusals about an
// account, or about a rung, hold in the later pass too.
type hostAdmission struct {
	refusedOn map[*selCand]string
	exhausted map[string]hostTry
	down      map[string]string
	// failedOn holds each host a candidate's gate failed on (hostGateFailure).
	// Unlike refusedOn, the walk does not skip the candidate on other hosts for
	// it; the waste_first arm reads it per host.
	failedOn map[*selCand]map[string]bool
}

func (st *hostAdmission) markFailed(c *selCand, host string) {
	if st.failedOn[c] == nil {
		st.failedOn[c] = map[string]bool{}
	}
	st.failedOn[c][host] = true
}

// admitPass walks the ranked hosts once with the candidates cands(i) gives
// for the i-th host, and records one trace row per host for this pass.
func (sp *selPresence) admitPass(ctx context.Context, s Spawn, p *selPlan, pending []*selCand, ranked []selHost, st *hostAdmission, pass int, cands func(int) []*selCand) {
	for i, h := range ranked {
		present := cands(i)
		if pass > 1 && len(present) == 0 {
			// Pass 1 already traced this host's readiness; a host with no
			// credit-band candidate has nothing to add.
			continue
		}
		t := hostTrace{host: h, rank: i + 1, pass: pass, missing: missingCaps(h.cfg, p.req.tags)}
		if pass == 1 {
			t.drops = sp.dropsOn(h.name)
		}
		if p.chosen != nil {
			t.outcome = "not_tried"
			p.hosts = append(p.hosts, t)
			continue
		}
		unreachable := ""
		if why := sp.unreachable[h.name]; why != "" {
			unreachable = "host_unavailable:" + why
		}
		down, ok := st.down[h.name]
		if !ok {
			down = unreachable
		}
		// Classification may have refused an account before admission. Keep
		// those rungs in the host trace, once, even though they never reach
		// the gate.
		for _, c := range p.cands {
			if pass == 1 && c.reason != "" && slices.ContainsFunc(c.hosts, func(x selHost) bool { return x.name == h.name }) && !slices.Contains(pending, c) {
				t.tries = append(t.tries, hostTry{cand: c, result: "refused", reason: c.reason, code: c.code})
			}
		}
		for _, c := range present {
			if refusal, ok := st.exhausted[c.accountKey]; ok {
				c.reason, c.code, c.outcome = refusal.reason, refusal.code, "dropped:quota_exhausted"
				refusal.cand = c
				t.tries = append(t.tries, refusal)
				continue
			}
			if on, refused := st.refusedOn[c]; refused {
				t.tries = append(t.tries, hostTry{cand: c, result: "skipped", reason: "refused_on:" + on + ":" + c.reason, code: c.code})
				continue
			}
			if down != "" {
				t.tries = append(t.tries, hostTry{cand: c, result: "skipped", reason: down})
				continue
			}
			c.reason, c.code, c.outcome = "", "", ""
			role := s.Role
			if p.req.route.top() {
				role = p.req.route.role
			}
			qr := QuotaRequest{Profile: c.seat.Canon, GateProfile: c.seat.gateName(), Effort: c.row.Effort, Role: role, OperatorRequest: s.OperatorRequest, RequestedBy: gateRequestedBy(s), Purpose: s.Purpose, AccountKey: c.accountKey}
			judgments, err := sp.admitAccount(ctx, qr, c, p, []selHost{h})
			c.judgments = append(c.judgments, judgments...)
			if err == nil {
				c.host, c.outcome, c.code = judgments[len(judgments)-1].Host, "chosen", outcomeAdmitted
				p.chosen, p.outcome = c, outcomeAdmitted
				t.tries = append(t.tries, hostTry{cand: c, result: "admitted", code: outcomeAdmitted})
				break
			}
			if c.reason == "" {
				c.code = gateOutcome(err)
				if c.code == outcomeNoRoleCandidate && strings.HasPrefix(err.Error(), "role_restricted:") {
					c.reason = clipGateReason(err.Error(), 200)
				} else if note := gateCodeNote(err); note != "" {
					c.reason = note
				} else {
					c.reason = fmt.Sprintf("gate_refused:%d", exitCode(err))
				}
				c.outcome = "dropped:gate_refused"
			} else {
				c.outcome = "dropped:" + c.reason
			}
			t.tries = append(t.tries, hostTry{cand: c, result: "refused", reason: c.reason, code: c.code})
			if c.code == outcomeQuotaExhausted && c.reason != "projected_exhaust" {
				st.exhausted[c.accountKey] = hostTry{result: "skipped", reason: "refused_on:" + h.name + ":" + c.reason, code: c.code}
			}
			if hostGateFailure(err) {
				st.markFailed(c, h.name)
			}
			switch {
			case !hostGateFailure(err):
				st.refusedOn[c] = h.name
			case c.code == outcomeNoEligibleHost:
				down = "host_unavailable:" + c.reason
				st.down[h.name] = down
			}
		}
		switch {
		case p.chosen != nil:
			t.outcome = "chosen"
		case unreachable != "":
			t.outcome = unreachable
		case len(t.tries) == 0 && len(t.drops) > 0:
			// No pair was left on this host: name the check that removed them.
			t.outcome = t.drops[0].code
		case len(t.tries) == 0:
			t.outcome = "no_candidate_present"
		default:
			t.outcome = "all_refused"
		}
		p.hosts = append(p.hosts, t)
	}
}
