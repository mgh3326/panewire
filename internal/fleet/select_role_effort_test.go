package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// roleEffortFixture is the hk 2495 incident: codex-sol@high is an unplaced E6
// rung (its gate refuses e6_arm_required), codex-sol@max is admitted, and the
// codex pool ranks ahead of claude by waste. opus@high is admitted.
func roleEffortFixture(t *testing.T) (*App, Spawn, *fakeQuota) {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"},
		{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
		{Profile: "codex-sol", Effort: "max", Grade: "S+", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	}
	pools := map[string]PoolView{"codex": riskPool(30, 8), "claude": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "A", rows, pools, []string{"claude", "codex"})
	admit := gatePools(map[string]string{"opus": "claude", "codex-sol": "codex"})
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.Profile == "codex-sol" && r.Effort == "high" {
			return "", fail(exitSeatRefused, "e6_arm_required: codex-sol@high is an unplaced E6 seat")
		}
		return admit(r)
	}
	serveRouting(t, routingDocV1)
	return a, s, q
}

func gateAsked(q *fakeQuota, profile, effort string) bool {
	for _, c := range q.Calls {
		if c.Profile == profile && c.Effort == effort {
			return true
		}
	}
	return false
}

// Acceptance 1: a lead never climbs to codex-sol@max when codex-sol@high is
// unplaced; it falls through to the claude family at the role effort, and
// the record names codex-sol skipped with role_effort_unplaced.
func TestLeadSkipsFamilyWhoseRoleEffortRungIsUnplaced(t *testing.T) {
	for _, tc := range []struct{ name, role, override string }{
		{"resolve --role consult (implementer seat)", "implementer", "consult"},
		{"--role consult", "consult", ""},
	} {
		a, s, q := roleEffortFixture(t)
		s.Role, s.RoutingRole = tc.role, tc.override
		sel, e := a.selectProfile(context.Background(), s)
		if e != nil {
			t.Fatalf("%s: %v", tc.name, e)
		}
		if c := sel.chosen; c.seat.Canon != "opus" || c.row.Effort != "high" {
			t.Fatalf("%s: want opus@high, got %s@%s", tc.name, c.seat.Canon, c.row.Effort)
		}
		rec := sel.record()
		if got := candOutcome(rec, "codex-sol", "max"); got["reason"] != reasonRoleEffortUnplaced || got["outcome"] != "dropped:"+reasonRoleEffortUnplaced {
			t.Fatalf("%s: codex-sol@max must be skipped %s: %v", tc.name, reasonRoleEffortUnplaced, got)
		}
		if got := candOutcome(rec, "codex-sol", "high")["outcome"]; got != "dropped:gate_refused" {
			t.Fatalf("%s: codex-sol@high is refused at its gate: %v", tc.name, got)
		}
		rt := routingStamp(t, sel)
		skipped, _ := rt["skipped"].([]map[string]any)
		if len(skipped) != 1 || skipped[0]["profile"] != "codex-sol" || skipped[0]["reason"] != reasonRoleEffortUnplaced || skipped[0]["role_effort"] != "high" || !strings.HasPrefix(skipped[0]["cause"].(string), "gate_refused") {
			t.Fatalf("%s: routing stamp must name codex-sol skipped: %v", tc.name, rt)
		}
		if _, ok := rt["escalation"]; ok {
			t.Fatalf("%s: no escalation without --allow-escalation: %v", tc.name, rt)
		}
		if gateAsked(q, "codex-sol", "max") {
			t.Fatalf("%s: the max rung's gate must never be asked: %+v", tc.name, q.Calls)
		}
	}

	// Above-role-effort rungs of a family whose role-effort rung is fine are
	// dropped above_role_effort.
	a, s, _ := roleEffortFixture(t)
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = append(q.CatalogRows, CatalogRow{Profile: "opus", Effort: "max", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "opus-m"})
	s.RoutingRole = "consult"
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if got := candOutcome(sel.record(), "opus", "max")["reason"]; got != reasonAboveRoleEffort {
		t.Fatalf("opus@max must be %s: %v", reasonAboveRoleEffort, got)
	}
}

// Acceptance 2: --allow-escalation may climb to codex-sol@max and the
// selection record carries the escalation.
func TestLeadAllowEscalationRecordsEscalation(t *testing.T) {
	a, s, _ := roleEffortFixture(t)
	s.RoutingRole, s.AllowEscalation = "consult", true
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if c := sel.chosen; c.seat.Canon != "codex-sol" || c.row.Effort != "max" {
		t.Fatalf("with --allow-escalation the codex-sol max rung may win by waste: %s@%s", c.seat.Canon, c.row.Effort)
	}
	rec := sel.record()
	if rec["requested"].(map[string]any)["allow_escalation"] != true {
		t.Fatalf("requested.allow_escalation: %v", rec["requested"])
	}
	esc, ok := routingStamp(t, sel)["escalation"].(map[string]any)
	if !ok || esc["role_effort"] != "high" || esc["chosen_effort"] != "max" || esc["profile"] != "codex-sol" || esc["by"] != "allow_escalation" {
		t.Fatalf("routing stamp must record the escalation: %v", routingStamp(t, sel))
	}

	// An escalated rung is reported at its own effort, not the role's.
	if got := sel.req.route.effort(sel.chosen.row); got != "max" {
		t.Fatalf("escalated effort must be the rung's: %s", got)
	}
}

// Acceptance 3: non-top routing roles keep grade-driven selection with
// today's ordering: the cap does not apply, so the worker climbs to the
// wasted codex pool's max rung exactly as before.
func TestNonLeadRolesKeepTodaysOrdering(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"worker → implementer", routingDocV1},
		{"worker unrouted (builtin_default)", ""},
	} {
		a, s, q := roleEffortFixture(t)
		serveRouting(t, tc.body)
		s.Role = "implementer"
		q.CatalogRows = append(q.CatalogRows, CatalogRow{Profile: "sonnet", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "sonnet-m"})
		sel, e := a.selectProfile(context.Background(), s)
		if e != nil {
			t.Fatalf("%s: %v", tc.name, e)
		}
		if c := sel.chosen; c.seat.Canon != "codex-sol" || c.row.Effort != "max" {
			t.Fatalf("%s: want today's codex-sol@max, got %s@%s", tc.name, c.seat.Canon, c.row.Effort)
		}
		rec := sel.record()
		if got := candOutcome(rec, "sonnet", "high")["reason"]; got != "grade_insufficient" {
			t.Fatalf("%s: grade still filters: %v", tc.name, got)
		}
		if rt, ok := rec["routing"].(map[string]any); ok {
			if _, ok := rt["skipped"]; ok {
				t.Fatalf("%s: no family skip outside lead/consult: %v", tc.name, rt)
			}
		}
	}

	// A reviewer with no min_effort keeps today's grade-driven ordering: the
	// cheapest covering rung wins and nothing is dropped for a floor.
	a, s, _ := reviewerRungFixture(t, routingBlockV1)
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if c := sel.chosen; c.row.Profile != "codex-luna" || c.row.Effort != "low" {
		t.Fatalf("no min_effort must keep today's codex-luna@low, got %s@%s", c.row.Profile, c.row.Effort)
	}
	if got := candOutcome(sel.record(), "codex-luna", "low"); got["outcome"] == "dropped:"+reasonBelowRoleMinEffort {
		t.Fatalf("no min_effort must not drop a rung for the floor: %v", got)
	}
}

// Acceptance 4: `panewire resolve --role consult` prints the choice spawn's
// selection makes, and `plan --role consult` agrees.
func TestResolveLeadMatchesSpawnUnderRoleEffortCap(t *testing.T) {
	a, s, _ := roleEffortFixture(t)
	s.RoutingRole = "consult"
	spawnSel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	want := spawnSel.chosen

	a, _, _ = roleEffortFixture(t)
	out := captureOut(a)
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--grade", "A", "--json"}); e != nil {
		t.Fatalf("resolve: %v\n%s", e, out)
	}
	var r resolveResult
	if e := json.Unmarshal(out.Bytes(), &r); e != nil {
		t.Fatalf("%v\n%s", e, out)
	}
	if r.Seat != want.seatName || r.Profile != want.seat.Canon || r.RungEffort != want.row.Effort || r.Effort != "high" {
		t.Fatalf("resolve %+v must print spawn's choice %s %s@%s", r, want.seatName, want.seat.Canon, want.row.Effort)
	}
	if got := candOutcome(r.Selection, "codex-sol", "max")["reason"]; got != reasonRoleEffortUnplaced {
		t.Fatalf("resolve records the skip: %v", got)
	}

	a, _, _ = roleEffortFixture(t)
	out = captureOut(a)
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--grade", "A"}); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "rung_effort=high") || !strings.HasSuffix(strings.TrimSpace(out.String()), "profile=opus effort=high") {
		t.Fatalf("text resolve:\n%s", out)
	}

	a, _, _ = roleEffortFixture(t)
	out = captureOut(a)
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--allow-escalation"}); e != nil {
		t.Fatal(e)
	}
	if !strings.HasSuffix(strings.TrimSpace(out.String()), "profile=codex-sol effort=max") {
		t.Fatalf("resolve --allow-escalation reports the escalated rung:\n%s", out)
	}

	a, _, q := roleEffortFixture(t)
	q.LaunchValue = Launch{Profile: "opus", ModelID: "opus-m", Effort: "high", Grade: "S", Gate: "default", Pool: "claude"}
	out = captureOut(a)
	if e := a.Run(context.Background(), []string{"plan", "--t", "T1", "--role", "consult", "--grade", "A", "--json"}); e != nil {
		t.Fatalf("plan: %v\n%s", e, out)
	}
	var plan map[string]any
	if e := json.Unmarshal(out.Bytes(), &plan); e != nil {
		t.Fatalf("%v\n%s", e, out)
	}
	sel := plan["selection"].(map[string]any)
	if c := sel["chosen"].(map[string]any); c["canon"] != want.seat.Canon || c["effort"] != want.row.Effort {
		t.Fatalf("plan must agree with spawn: %v", c)
	}
	if got := candOutcome(sel, "codex-sol", "max")["reason"]; got != reasonRoleEffortUnplaced {
		t.Fatalf("plan records the skip: %v", got)
	}
}

func captureOut(a *App) *bytes.Buffer {
	out := &bytes.Buffer{}
	a.Out = out
	return out
}
