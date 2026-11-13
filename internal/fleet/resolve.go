package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
)

// `panewire resolve --role R` answers "which profile and effort should a
// subagent in this role run on now" for an in-session agent that makes its
// own subagent call. It runs spawn's selection core (selectFor) under the
// role's routing — the policy/model-routing filter, then the pool order and
// the quota gates — and prints the choice. It places no host, spawns
// nothing and writes no job record. The answer is the last thing printed.
//
// --t takes the same values as plan's (T0..T3) and defaults to T0, so the
// bare `resolve --role R --grade G` form keeps working. The seat role is worker, or reviewer for the reviewer and judge routing
// roles, whose family rule needs --builder-family (the author's or parent's
// family). The effort is the one the chosen rung launches at.
type resolveResult struct {
	RoutingRole    string         `json:"routing_role"`
	SeatRole       string         `json:"seat_role"`
	Grade          string         `json:"grade"`
	T              string         `json:"t"`
	RoutingVersion int            `json:"routing_version"`
	PolicySource   string         `json:"policy_source"`
	DowngradedFrom string         `json:"downgraded_from,omitempty"`
	Selection      map[string]any `json:"selection,omitempty"`
	Error          string         `json:"error,omitempty"`
	Seat           string         `json:"seat,omitempty"`
	ModelID        string         `json:"model_id,omitempty"`
	Pool           string         `json:"pool,omitempty"`
	Tier           string         `json:"tier,omitempty"`
	RungEffort     string         `json:"rung_effort,omitempty"`
	AccountKey     string         `json:"account_key,omitempty"`
	Host           string         `json:"host,omitempty"`
	OutcomeCode    string         `json:"outcome_code"`
	OMPAgent       string         `json:"omp_agent,omitempty"`
	Profile        string         `json:"profile,omitempty"`
	Effort         string         `json:"effort,omitempty"`
}

func (a *App) resolveCmd(ctx context.Context, args []string) error {
	var role, grade, t, builderFamily, harness, lane string
	var asJSON, allowEscalation bool
	f := flags("resolve")
	f.StringVar(&role, "role", "", "routing role")
	f.StringVar(&grade, "grade", "", "task grade (C, B, A, A+, S, S+, SS); for consult, the asker's grade, optional")
	f.StringVar(&t, "t", "T0", "task scale T0..T3")
	f.StringVar(&builderFamily, "builder-family", "", "author or parent family (reviewer/judge/consult)")
	f.StringVar(&lane, "lane", "", "asker's lane (consult)")
	f.StringVar(&harness, "harness", "", "restrict selection to an agent harness, e.g. omp")
	f.BoolVar(&allowEscalation, "allow-escalation", false, "admit escalation rungs, and consult rungs above the role effort")
	f.BoolVar(&asJSON, "json", false, "print the decision as JSON")
	if e := f.Parse(args); e != nil {
		return fail(exitUsage, "%v", e)
	}
	if f.NArg() > 0 {
		return fail(exitUsage, "resolve takes no positional arguments")
	}
	if !slices.Contains(routingRoles, role) {
		if role == "explorer" {
			return fail(exitUsage, "explorer role was removed; use --role implementer")
		}
		return fail(exitUsage, "resolve needs --role %s", strings.Join(routingRoles, "|"))
	}
	grade = strings.ToUpper(grade)
	if topRoutingRole(role) {
		// A consult asker's grade is optional; a grade that is passed must be one.
		if _, ok := gradeOrder[grade]; !ok && flagPassed(f, "grade") {
			return fail(exitUsage, "resolve needs --grade C|B|A|A+|S|S+|SS")
		}
	} else {
		if grade == "" {
			grade = "C"
		}
		if _, ok := gradeOrder[grade]; !ok {
			return fail(exitUsage, "resolve needs --grade C|B|A|A+|S|S+|SS")
		}
	}
	if lane != "" && !topRoutingRole(role) {
		return fail(exitUsage, "--lane goes with --role consult")
	}
	t = strings.ToUpper(t)
	if !slices.Contains([]string{"T0", "T1", "T2", "T3"}, t) {
		return fail(exitUsage, "resolve needs --t T0..T3")
	}
	seatRole := "worker"
	if role == "reviewer" || role == "judge" {
		seatRole = "reviewer"
		if builderFamily == "" {
			return fail(exitUsage, "--role %s needs --builder-family (its family rule)", role)
		}
	} else if builderFamily != "" && !topRoutingRole(role) {
		return fail(exitUsage, "--builder-family goes with --role reviewer|judge|consult")
	}
	if a.Quota == nil {
		return fail(exitConfig, "resolve needs the quota catalog")
	}
	route, e := a.routeFor(ctx, seatRole, role)
	if e != nil {
		if asJSON {
			_ = writeJSON(a.Out, resolveResult{RoutingRole: role, OutcomeCode: outcomeForError(e), Error: e.Error()})
		}
		return e
	}
	cwd, _ := os.Getwd()
	s := Spawn{CWD: cwd, Profile: "auto", Host: "auto", Role: seatRole, T: t, ParentFamily: builderFamily, RoutingRole: role, AllowEscalation: allowEscalation, DryRun: true}
	gradeSource := "without_task"
	if flagPassed(f, "grade") {
		gradeSource = "flag"
	}
	req := selReq{grade: grade, lane: lane, gradeSource: gradeSource, t: t, role: seatRole, allowEscalation: allowEscalation, builderFamily: builderFamily, harness: harness, urgency: urgencyNormal, urgencySource: "default", route: route}
	sel, selErr := a.selectFor(ctx, s, req)
	r := resolveResult{RoutingRole: role, SeatRole: seatRole, Grade: grade, T: t, RoutingVersion: route.version, PolicySource: route.source}
	r.OutcomeCode = outcomeForError(selErr)
	if sel != nil {
		// A downgrade retry carries its own routing decision.
		route = sel.req.route
		r.DowngradedFrom = route.downgradedFrom
		r.Selection = sel.record()
		r.Selection["mode"] = "resolve"
	}
	if selErr != nil {
		r.Error = selErr.Error()
	} else {
		c := sel.chosen
		r.Seat, r.ModelID, r.Pool, r.Tier, r.RungEffort = c.seatName, c.row.ModelID, c.row.Pool, c.tierName, c.row.Effort
		r.Profile, r.Effort = c.seat.Canon, route.effort(c.row)
		if r.Effort == "" {
			// A rung-less row launches at the seat's declared default.
			r.Effort = c.seat.Effort
		}
		r.AccountKey, r.Host = c.accountKey, c.host
		if c.seat.Kind == "omp" && role == "implementer" {
			r.OMPAgent = a.ompAgent(c.row.ModelID)
		}
	}
	if asJSON {
		b, _ := json.MarshalIndent(r, "", "  ")
		fmt.Fprintln(a.Out, string(b))
		return selErr
	}
	fmt.Fprintf(a.Out, "resolve role=%s seat_role=%s grade=%s t=%s routing_version=%d policy_source=%s", r.RoutingRole, r.SeatRole, r.Grade, r.T, r.RoutingVersion, r.PolicySource)
	if topRoutingRole(role) {
		fmt.Fprintf(a.Out, " floor=%s", consultFloorFor(r.Grade, lane))
	}
	if r.DowngradedFrom != "" {
		fmt.Fprintf(a.Out, " downgraded_from=%s", r.DowngradedFrom)
	}
	fmt.Fprintln(a.Out)
	if selErr != nil {
		fmt.Fprintf(a.Out, "result refused exit=%d: %v\n", exitCode(selErr), selErr)
		return selErr
	}
	fmt.Fprintf(a.Out, "chosen seat=%s model=%s pool=%s tier=%s rung_effort=%s\n", r.Seat, r.ModelID, r.Pool, r.Tier, firstNonempty(r.RungEffort, "default"))
	fmt.Fprintf(a.Out, "profile=%s effort=%s", r.Profile, firstNonempty(r.Effort, "default"))
	if r.OMPAgent != "" {
		fmt.Fprintf(a.Out, " omp_agent=%s", r.OMPAgent)
	}
	fmt.Fprintln(a.Out)
	return nil
}
