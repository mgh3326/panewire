package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// reviewerMinEffortBlock is routingBlockV1 with the reviewer's min_effort set.
func reviewerMinEffortBlock(minEffort string) string {
	old := `"reviewer":    {"allowed": ["*"], "effort": "high", "on_exhaustion": "escalate", "family_rule": "not_author"}`
	repl := `"reviewer":    {"allowed": ["*"], "effort": "high", "min_effort": "` + minEffort + `", "on_exhaustion": "escalate", "family_rule": "not_author"}`
	return strings.Replace(routingBlockV1, old, repl, 1)
}

// reviewerRungFixture is the reviewer incident: a grade-C task where
// codex-luna@low is the cheapest covering rung. The author is anthropic, so
// the openai seat clears the reviewer family rule.
func reviewerRungFixture(t *testing.T, block string) (*App, Spawn, *fakeQuota) {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "codex-luna", Effort: "low", Grade: "C", Pool: "codex", Gate: "default", ModelID: "luna-m"},
		{Profile: "codex-luna", Effort: "medium", Grade: "C", Pool: "codex", Gate: "default", ModelID: "luna-m"},
		{Profile: "codex-luna", Effort: "high", Grade: "C", Pool: "codex", Gate: "default", ModelID: "luna-m"},
	}
	a, s, q := selFixture(t, "C", rows, map[string]PoolView{"codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}, []string{"codex"})
	q.GateDo = gatePools(map[string]string{"codex-luna": "codex"})
	serveRouting(a, routingDoc(block))
	s.Role, s.ParentFamily = "reviewer", "anthropic"
	return a, s, q
}

// Acceptance 1: a reviewer with min_effort medium drops codex-luna@low as
// below_role_min_effort and launches a medium-or-higher rung.
func TestRoutingMinEffortDropsRungsBelowFloor(t *testing.T) {
	a, s, _ := reviewerRungFixture(t, reviewerMinEffortBlock("medium"))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	rec := sel.record()
	if got := candOutcome(rec, "codex-luna", "low"); got["reason"] != reasonBelowRoleMinEffort || got["outcome"] != "dropped:"+reasonBelowRoleMinEffort {
		t.Fatalf("codex-luna@low must be %s: %v", reasonBelowRoleMinEffort, got)
	}
	if c := sel.chosen; c.row.Profile != "codex-luna" || c.row.Effort != "medium" {
		t.Fatalf("want the cheapest admitted rung codex-luna@medium, got %s@%s", c.row.Profile, c.row.Effort)
	}
	if got := routingStamp(t, sel)["min_effort"]; got != "medium" {
		t.Fatalf("routing stamp must record min_effort: %v", routingStamp(t, sel))
	}
}

// A rung-less row ("") has no catalog effort: it launches at the selected
// seat's declared default, the same seat.Effort fallback quotaRequest uses,
// and the floor is judged on that effort. A seat that declares an effort
// rejects a rung-less row as effort_empty before the floor, so it keeps that
// reason; an admitted rung-less row with no default is rank 0 and below any
// floor.
func TestRoutingMinEffortResolvesRunglessRowAtSelectedSeat(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "", Grade: "C", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "nodefault", Effort: "", Grade: "C", Pool: "devin", Gate: "default", ModelID: "none"},
	}
	a, s, q := selFixture(t, "C", rows, map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(0)}}, []string{"devin"})
	// A seat that launches without an effort flag and declares no default is
	// the only shape that admits a rung-less row.
	a.Policy.Profiles["nodefault"] = Seat{Canon: "nodefault", Kind: "devin", Roles: []string{"worker", "tester", "reviewer", "counsel"}}
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin"})
	serveRouting(a, routingDoc(reviewerMinEffortBlock("medium")))
	s.Role, s.ParentFamily = "reviewer", "anthropic"
	sel, _ := a.selectProfile(context.Background(), s)
	rec := sel.record()

	// devin-swe2's seat declares effort high: the seat rule rejects the
	// rung-less row effort_empty before the floor, so it keeps its own reason.
	if got := candOutcome(rec, "devin-swe2", ""); got["reason"] != "effort_empty" {
		t.Fatalf("a rung-less row whose seat declares an effort keeps effort_empty: %v", got)
	}
	// nodefault admits the rung at its seat default ""; that is rank 0 and
	// below the medium floor.
	if got := candOutcome(rec, "nodefault", ""); got["reason"] != reasonBelowRoleMinEffort {
		t.Fatalf("a rung-less row is judged at its seat default: %v", got)
	}
}

// Acceptance 4: the spawn record's routing stamp carries min_effort, and a
// rung below the floor is dropped in the selection the job records.
func TestSpawnRecordCarriesRoleMinEffort(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	s.Profile, s.Host = "auto", "auto"
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = []CatalogRow{
		{Profile: "cmd-dsflash41", Effort: "low", Grade: "A+", Pool: "example", Gate: "default", ModelID: "provider/example-model"},
		{Profile: "cmd-dsflash41", Effort: "high", Grade: "A+", Pool: "example", Gate: "default", ModelID: "provider/example-model"},
	}
	q.PoolViews = map[string]PoolView{"example": {Status: "ok", Class: "spend"}}
	key := "example:0123456789abcdef"
	a.Source = &fakePlacementSource{
		presence: []PresenceRecord{{HostID: "local", AccountKey: key, LastSeen: a.Now()}},
		accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(0)}}},
	}
	q.GateDo = gatePools(map[string]string{"cmd-dsflash41": "example"})
	block := strings.Replace(routingBlockV1,
		`"cmd-dsflash41"], "effort": "high", "on_exhaustion": "wait"`,
		`"cmd-dsflash41"], "effort": "high", "min_effort": "medium", "on_exhaustion": "wait"`, 1)
	serveRouting(a, routingDoc(block))
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	var placement map[string]any
	evs, _ := a.events(s.Job)
	for _, ev := range evs {
		if ev.Kind == "job.placement" {
			placement = ev.Payload
		}
	}
	selection, _ := placement["selection"].(map[string]any)
	rt, _ := selection["routing"].(map[string]any)
	if rt["min_effort"] != "medium" {
		t.Fatalf("job.placement routing stamp must carry min_effort: %v", selection["routing"])
	}
	if got := candOutcome(selection, "cmd-dsflash41", "low"); got["reason"] != reasonBelowRoleMinEffort {
		t.Fatalf("the recorded selection must drop the low rung: %v", got)
	}
}

// Acceptance 4: resolve reports the effort the chosen rung launches at, not
// the role's declared effort.
func TestResolveReportsLaunchedRungEffort(t *testing.T) {
	a, _, _ := reviewerRungFixture(t, reviewerMinEffortBlock("medium"))
	out := captureOut(a)
	if e := a.Run(context.Background(), []string{"resolve", "--role", "reviewer", "--grade", "C", "--builder-family", "anthropic", "--json"}); e != nil {
		t.Fatalf("resolve: %v\n%s", e, out)
	}
	var r resolveResult
	if e := json.Unmarshal(out.Bytes(), &r); e != nil {
		t.Fatalf("%v\n%s", e, out)
	}
	if r.Profile != "codex-luna" || r.RungEffort != "medium" || r.Effort != "medium" {
		t.Fatalf("resolve must report the launched rung: %+v", r)
	}
}
