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
//     lead or consult has no grade, so a host with grades set refuses it.

var spawnRoles = []string{"worker", "builder", "tester", "reviewer", "counsel"}
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
// An empty grade is a lead or consult, which a grade-limited host refuses.
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

// rankHosts orders the fit hosts: bucket, then the hub score (free capacity
// weighted by the host's power) when every ranked host has one, else the raw
// load, then configuration order (local first).
func rankHosts(hosts []selHost) []selHost {
	var out []selHost
	for _, h := range hosts {
		if h.fit == "" {
			out = append(out, h)
		}
	}
	byScore := !slices.ContainsFunc(out, func(h selHost) bool { return h.score == nil })
	slices.SortStableFunc(out, func(x, y selHost) int {
		if bx, by := hostBucket(x), hostBucket(y); bx != by {
			return bx - by
		}
		if byScore {
			return cmp.Compare(*y.score, *x.score)
		}
		return cmp.Compare(loadOrInf(x.load), loadOrInf(y.load))
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
	host    selHost
	rank    int
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
	row := map[string]any{"host": h.name, "caps": caps, "caps_match": len(t.missing) == 0, "awake": h.awake, "over_threshold": h.over, "outcome": t.outcome}
	if h.local {
		row["local"] = true
	}
	if len(t.missing) > 0 {
		row["missing_caps"] = t.missing
	}
	if t.rank > 0 {
		row["rank"] = t.rank
	}
	if h.load != nil {
		row["load_ratio"] = *h.load
	}
	if h.score != nil {
		row["score"] = *h.score
	}
	tried := []any{}
	for _, x := range t.tries {
		c := x.cand
		r := map[string]any{"account_key": c.accountKey, "profile": firstNonempty(c.seatName, c.row.Profile), "effort": c.row.Effort, "pool": c.row.Pool, "tier": c.tierName, "result": x.result}
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
// account itself (quota exhausted or unknown) holds on every host, so that
// candidate is not judged again; a host-level failure only skips the host for
// that candidate, and an unreachable host is skipped for the rest. A host
// whose present candidates are all refused is skipped, not fatal.
//
// A credit balance in its final window before expiry is lost unless it is
// used, so the hosts that hold such a candidate are tried first, keeping
// host rank among them and among the rest.
func (sp *selPresence) admitHostFirst(ctx context.Context, s Spawn, p *selPlan, pending []*selCand) {
	ranked := rankHosts(sp.hosts)
	holdsFinal := func(h selHost) bool {
		return slices.ContainsFunc(pending, func(c *selCand) bool {
			return c.credit != nil && slices.ContainsFunc(c.hosts, func(x selHost) bool { return x.name == h.name })
		})
	}
	slices.SortStableFunc(ranked, func(x, y selHost) int {
		if fx, fy := holdsFinal(x), holdsFinal(y); fx != fy {
			if fx {
				return -1
			}
			return 1
		}
		return 0
	})
	refusedOn := map[*selCand]string{}
	for i, h := range ranked {
		t := hostTrace{host: h, rank: i + 1, missing: missingCaps(h.cfg, p.req.tags)}
		if p.chosen != nil {
			t.outcome = "not_tried"
			p.hosts = append(p.hosts, t)
			continue
		}
		down := ""
		if why := sp.unreachable[h.name]; why != "" {
			down = "host_unavailable:" + why
		}
		unreachable := down
		for _, c := range pending {
			if !slices.ContainsFunc(c.hosts, func(x selHost) bool { return x.name == h.name }) {
				continue
			}
			if on, refused := refusedOn[c]; refused {
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
			qr := QuotaRequest{Profile: c.seat.Canon, GateProfile: c.seat.gateName(), Effort: c.row.Effort, Role: role, OperatorRequest: s.OperatorRequest, RequestedBy: s.RequestedBy, Purpose: s.Purpose, AccountKey: c.accountKey}
			judgments, err := sp.admitAccount(ctx, qr, c, p, []selHost{h})
			c.judgments = append(c.judgments, judgments...)
			if err == nil {
				c.host, c.outcome, c.code = judgments[len(judgments)-1].Host, "chosen", outcomeAdmitted
				p.chosen, p.outcome = c, outcomeAdmitted
				t.tries = append(t.tries, hostTry{cand: c, result: "admitted", code: outcomeAdmitted})
				break
			}
			if c.reason == "" {
				c.reason, c.code = fmt.Sprintf("gate_refused:%d", exitCode(err)), gateOutcome(err)
				c.outcome = "dropped:gate_refused"
			} else {
				c.outcome = "dropped:" + c.reason
			}
			t.tries = append(t.tries, hostTry{cand: c, result: "refused", reason: c.reason, code: c.code})
			switch {
			case !hostGateFailure(err):
				refusedOn[c] = h.name
			case c.code == outcomeNoEligibleHost:
				down = "host_unavailable:" + c.reason
			}
		}
		switch {
		case p.chosen != nil:
			t.outcome = "chosen"
		case unreachable != "":
			t.outcome = unreachable
		case len(t.tries) == 0:
			t.outcome = "no_candidate_present"
		default:
			t.outcome = "all_refused"
		}
		p.hosts = append(p.hosts, t)
	}
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
