package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"
)

// `panewire plan` answers "right now, which host and which model would a spawn
// get" without a task or brief. It runs the spawn's selection core
// (selectFor: catalog, pool status, presence, the per-host quota gates) and
// then walks the same placement plan placeAuto would, asking the chosen
// seat's quota gate on each host in order — the shared-account gate for
// remote hosts, the local launch resolution and gate for this host. It never
// spawns, wakes, writes an hk task or job record, claims an arbiter job or
// emits a job event. A stale remote account is measured on its host exactly
// as a spawn would (pacelever refresh), since the gate refuses unmeasured
// evidence.
//
// It also prints the remote-spawn preflight of every remote host (see
// preflight.go), as information only.
//
// Exit codes: 0 a host and model were found; 2 usage; 70 configuration or
// unreadable catalog; 77 no catalog rung was admitted; 80 local spawning is
// disabled and no remote host admitted the seat; 81 no host admitted the
// seat (hosts a spawn would wake are listed); a local launch or gate refusal
// keeps its own code.
type planOpts struct {
	grade, t, role, tags, effort, urgency, exclude, excludePool, builderFamily, routingRole, creditAt string
	allowEscalation, json                                                                             bool

	// expected is the job's expected duration for the headroom key (--expected).
	expected string
}

func (a *App) planCmd(ctx context.Context, args []string) (err error) {
	defer func() { a.refusalEvent("plan", "", err) }()
	var o planOpts
	f := flags("plan")
	f.StringVar(&o.grade, "grade", "", "task grade (C, B, A, A+, S, S+, SS); for consult, the asker's grade, optional")
	f.StringVar(&o.t, "t", "", "task scale T0..T3")
	f.StringVar(&o.role, "role", "worker", "seat role")
	f.StringVar(&o.tags, "tags", "", "required host capability tags")
	f.StringVar(&o.effort, "effort", "", "pin a catalog effort rung")
	f.StringVar(&o.urgency, "urgency", "", "normal or urgent")
	f.StringVar(&o.exclude, "exclude-host", "", "hosts placement must not pick")
	f.StringVar(&o.excludePool, "exclude-pool", "", "quota pools placement must not pick")
	f.StringVar(&o.builderFamily, "builder-family", "", "builder model family (tester/reviewer)")
	f.StringVar(&o.routingRole, "routing-role", "", "override the policy/model-routing role")
	f.BoolVar(&o.allowEscalation, "allow-escalation", false, "admit escalation and consult-only rungs")
	f.BoolVar(&o.json, "json", false, "print the full decision as JSON")
	f.StringVar(&o.creditAt, "credit-at", "", "judge credit expiry as of this RFC3339 time or YYYY-MM-DD (dry run; quota and presence keep the clock)")
	f.StringVar(&o.expected, "expected", "", "expected job duration (6h, 1d); the headroom key ranks accounts whose shortest window carries it; unset leaves the key off")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "plan takes no positional arguments")
	}
	expected, unreadable := parseExpected(o.expected)
	if unreadable {
		return fail(exitUsage, "invalid --expected %q: want a duration like 6h or a day count like 2d", o.expected)
	}
	o.grade = strings.ToUpper(o.grade)
	top := topRoutingRole(builtinRoleMap[o.role]) || topRoutingRole(o.routingRole)
	if _, ok := gradeOrder[o.grade]; !ok && (!top || flagPassed(f, "grade")) {
		return fail(exitUsage, "plan needs --grade C|B|A|A+|S|S+|SS")
	}
	o.t = strings.ToUpper(o.t)
	if !slices.Contains([]string{"T0", "T1", "T2", "T3"}, o.t) {
		return fail(exitUsage, "plan needs --t T0..T3")
	}
	if !slices.Contains([]string{"worker", "tester", "reviewer", "counsel"}, o.role) {
		return fail(exitUsage, "invalid role %q", o.role)
	}
	if (o.role == "tester" || o.role == "reviewer") != (o.builderFamily != "") && !(top && o.builderFamily != "") {
		return fail(exitUsage, "--builder-family goes with --role tester|reviewer, and they need it; consult takes it optionally")
	}
	if o.urgency != "" && o.urgency != urgencyNormal && o.urgency != urgencyUrgent {
		return fail(exitUsage, "--urgency must be normal or urgent")
	}
	tags, e := parseTags(o.tags)
	if e != nil {
		return e
	}
	if _, e := parseHosts(o.exclude); e != nil {
		return e
	}
	excludePools, e := parsePools(o.excludePool)
	if e != nil {
		return e
	}
	var creditAt time.Time
	if o.creditAt != "" {
		if creditAt, e = time.Parse(time.RFC3339, o.creditAt); e != nil {
			if creditAt, e = time.Parse("2006-01-02", o.creditAt); e != nil {
				return fail(exitUsage, "invalid --credit-at %q: want RFC3339 or YYYY-MM-DD", o.creditAt)
			}
		}
	}
	if a.Quota == nil {
		return fail(exitConfig, "plan needs the quota catalog")
	}
	cwd, _ := os.Getwd()
	s := Spawn{CWD: cwd, Profile: "auto", Host: "auto", Role: o.role, T: o.t, Tags: o.tags, Effort: o.effort, ExcludeHosts: o.exclude, ExcludePools: o.excludePool, AllowEscalation: o.allowEscalation, ParentFamily: o.builderFamily, Urgency: o.urgency, DryRun: true}
	req := selReq{grade: o.grade, t: o.t, role: o.role, tags: tags, effort: o.effort, allowEscalation: o.allowEscalation, builderFamily: o.builderFamily, excludePools: excludePools, urgency: urgencyNormal, urgencySource: "default", creditAt: creditAt}
	req.expected = expected
	req.gradeSource = "without_task"
	if flagPassed(f, "grade") {
		req.gradeSource = "flag"
	}
	if o.urgency != "" {
		req.urgency, req.urgencySource = o.urgency, "flag"
	}
	if req.route, e = a.routeFor(ctx, o.role, o.routingRole); e != nil {
		a.printPlan(o.json, map[string]any{"mode": "plan", "routing_role": firstNonempty(o.routingRole, builtinRoleMap[o.role]), "outcome_code": outcomeForError(e)}, nil, nil, nil, "", e)
		return e
	}
	sel, selErr := a.selectFor(ctx, s, req)
	out := map[string]any{"mode": "plan", "routing_role": req.route.role, "outcome_code": outcomeForError(selErr)}
	if sel != nil {
		rec := sel.record()
		rec["mode"] = "plan"
		out["selection"] = rec
	}
	if selErr != nil {
		a.printPlan(o.json, out, sel, nil, nil, "", selErr)
		return selErr
	}
	c := sel.chosen
	s.Profile = c.seatName
	s.AccountKey, s.RoutingRole, s.SelHost, s.route = c.accountKey, sel.req.route.role, c.host, sel.req.route
	if c.row.Effort != "" {
		s.Effort = c.row.Effort
	}
	pp, host, placeErr := a.planHosts(ctx, s, o.grade)
	var pre []hostPreflight
	if pp != nil {
		out["placement"] = pp.record(host)
		pre = a.planPreflight(ctx, pp)
		out["preflight"] = pre
	}
	if placeErr == nil {
		c.host = host
		rec := sel.record()
		rec["mode"] = "plan"
		out["selection"] = rec
		out["account_key"], out["host"] = c.accountKey, host
		out["chosen"] = map[string]any{"host": host, "account_key": c.accountKey, "routing_role": req.route.role, "outcome_code": outcomeAdmitted, "seat": c.seatName, "model_id": c.row.ModelID, "effort": c.row.Effort, "pool": c.row.Pool, "tier": c.tierName, "grade": c.row.Grade, "waste_risk": c.risk}
	}
	out["outcome_code"] = outcomeForError(placeErr)
	a.printPlan(o.json, out, sel, pp, pre, host, placeErr)
	return placeErr
}

// planHosts walks the placement plan for the selected seat the way placeAuto
// does, stopping at the first host whose quota gate admits it. It returns the
// plan (for its record) and the chosen host name.
func (a *App) planHosts(ctx context.Context, s Spawn, grade string) (*placePlan, string, error) {
	if topRoutingRole(s.RoutingRole) {
		grade = ""
	}
	pool := a.quotaPool(ctx, s)
	s.TaskGrade = grade
	p, e := a.plan(ctx, s, pool, 0)
	if e != nil {
		return nil, "", e
	}
	qr, ok := a.quotaRequest(s)
	if !ok {
		return p, "", fail(exitConfig, "missing seat policy for %s", s.Profile)
	}
	var localRefusal error
	admitLocal := func(c *placeCandidate) bool {
		launch, err := a.Quota.Launch(ctx, qr)
		if err == nil {
			seat := a.Policy.Profiles[s.Profile]
			err = seat.Check(launch, s.Role, grade, s.ParentFamily, a.e6Arm(s.E6Arm, s.e6ArmLocked), a.Config.Local)
		}
		if err != nil {
			c.outcome, localRefusal = "admission_refused", err
			return false
		}
		j, err := a.judgeHost(ctx, qr, pool, c.name, true, "", SharedAccount{})
		c.judgment = &j
		if err != nil {
			c.outcome, localRefusal = firstNonempty(gateCodeNote(err), "quota_refused"), err
			return false
		}
		return true
	}
	var deferredLocal *placeCandidate
	for _, c := range p.ordered {
		if c.local {
			if c.over && len(p.sleepers) > 0 && p.waited >= a.wakeAfter() && p.allOver() {
				c.outcome = "deferred_over_threshold"
				deferredLocal = c
				continue
			}
			if admitLocal(c) {
				c.outcome = "would_spawn"
				return p, c.name, nil
			}
			continue
		}
		key, acc := a.remoteAccount(ctx, pool, c.name, c.cfg)
		j, err := a.judgeHost(ctx, qr, pool, c.name, false, key, acc)
		c.judgment = &j
		if err != nil {
			c.outcome = hostGateNote(j, err)
			continue
		}
		c.outcome = "would_spawn"
		return p, c.name, nil
	}
	if targets, reasons := a.wakeTargets(s, p, nil); len(targets) > 0 {
		p.wake = map[string]any{"would_wake": targets, "reasons": reasons}
		return p, "", fail(exitPlacement, "no awake host admitted %s; a spawn would wake %s (%s)", s.Profile, strings.Join(targets, ","), strings.Join(reasons, ","))
	}
	if deferredLocal != nil && admitLocal(deferredLocal) {
		deferredLocal.outcome = "would_spawn"
		return p, deferredLocal.name, nil
	}
	if !boolean(a.Config.Local, "spawn", true) {
		return p, "", fail(exitLocalDisabled, "local spawns are disabled on this host; no remote host admitted %s", s.Profile)
	}
	if localRefusal != nil {
		return p, "", localRefusal
	}
	return p, "", fail(exitPlacement, "no host admitted %s", s.Profile)
}

// planPreflight runs the remote-spawn preflight for every remote host the
// plan evaluated, reusing the hub and presence reads the plan was built from.
// It is informational: it never changes the plan's outcome or exit status.
func (a *App) planPreflight(ctx context.Context, pp *placePlan) []hostPreflight {
	env := a.preflightEnvFrom(ctx, pp)
	var out []hostPreflight
	for _, c := range pp.candidates {
		if !c.local {
			out = append(out, a.preflightHost(ctx, env, c.name, c.cfg, pp.pool, true))
		}
	}
	return out
}

// printPlan renders the plan: the full decision as JSON, or a short text
// summary — the chosen host and model, the runners-up in rank order, every
// dropped rung with its reason and every host's outcome.
func (a *App) printPlan(asJSON bool, out map[string]any, sel *selPlan, pp *placePlan, pre []hostPreflight, host string, err error) {
	if asJSON {
		if err != nil {
			out["error"] = err.Error()
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Fprintln(a.Out, string(b))
		return
	}
	if sel == nil {
		return
	}
	fmt.Fprintf(a.Out, "plan grade=%s t=%s role=%s urgency=%s tiers=%s\n", sel.req.grade, sel.req.t, sel.req.role, sel.req.urgency, strings.Join(sel.order, ">"))
	if rt := sel.req.route; rt != nil {
		fmt.Fprintf(a.Out, "routing role=%s version=%d policy_source=%s%s\n", firstNonempty(rt.role, "-"), rt.version, rt.source, map[bool]string{true: " downgraded_from=" + rt.downgradedFrom}[rt.downgradedFrom != ""])
	}
	if fe := sel.familyEffort; fe != nil {
		// The chosen rung's role effort came from family_effort, which also
		// opted its escalation gate in.
		fmt.Fprintf(a.Out, "family_effort %s=%s chosen=%s@%s gate=%s\n", fe["key"], fe["effort"], fe["profile"], fe["effort"], fe["gate"])
	}
	label := func(c *selCand) string {
		return fmt.Sprintf("%s@%s model=%s pool=%s grade=%s", c.seatName, c.row.Effort, c.row.ModelID, c.row.Pool, c.row.Grade)
	}
	if c := sel.chosen; c != nil {
		h := host
		if h == "" {
			h = "none"
		}
		fmt.Fprintf(a.Out, "chosen host=%s seat=%s tier=%s waste_risk=%t%s\n", h, label(c), c.tierName, c.risk, creditNote(c))
	}
	for _, c := range sel.ranked {
		if c == sel.chosen {
			continue
		}
		state := c.outcome
		if c.reason != "" {
			state += " reason=" + c.reason
		}
		fmt.Fprintf(a.Out, "runner-up %s tier=%s waste_risk=%t%s %s\n", label(c), c.tierName, c.risk, creditNote(c), state)
	}
	for _, c := range sel.cands {
		if c.tier >= 0 {
			continue
		}
		name := firstNonempty(c.seatName, c.row.Profile)
		capNote := ""
		if c.cap != "" {
			capNote = fmt.Sprintf(" cap=%s live=%d limit=%d", c.cap, c.capLive, c.capLimit)
		}
		if !c.creditSince.IsZero() {
			capNote += " since=" + c.creditSince.UTC().Format(time.RFC3339Nano)
		}
		fmt.Fprintf(a.Out, "dropped %s@%s pool=%s reason=%s%s grade=%s\n", name, c.row.Effort, c.row.Pool, c.reason, capNote, c.row.Grade)
	}
	for _, t := range sel.hosts {
		var tried []string
		for _, x := range t.tries {
			tried = append(tried, fmt.Sprintf("%s/%s@%s:%s", x.cand.accountKey, firstNonempty(x.cand.seatName, x.cand.row.Profile), x.cand.row.Effort, firstNonempty(x.reason, x.result)))
		}
		fmt.Fprintf(a.Out, "host-first %s rank=%d pass=%d caps=%v outcome=%s tried=[%s]\n", t.host.name, t.rank, t.pass, hostCaps(t.host.cfg), t.outcome, strings.Join(tried, " "))
	}
	if pp != nil {
		for _, c := range pp.candidates {
			fmt.Fprintf(a.Out, "host %s outcome=%s\n", c.name, firstNonempty(c.outcome, "not_tried"))
		}
		if pp.wake != nil {
			fmt.Fprintf(a.Out, "would_wake %v\n", pp.wake["would_wake"])
		}
	}
	for _, h := range pre {
		printPreflightSummary(a.Out, h)
	}
	if err != nil {
		fmt.Fprintf(a.Out, "result refused exit=%d: %v\n", exitCode(err), err)
	}
}

// creditNote describes a credit balance and its expiry.
func creditNote(c *selCand) string {
	if c.credit == nil {
		return ""
	}
	return " " + c.credit.note()
}
