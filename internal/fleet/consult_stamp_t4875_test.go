package fleet

import (
	"context"
	"strings"
	"testing"
)

// hk 4875: the planner pins the consult seat, so selection never runs where the
// job's placement record is written. The planner carries the stamp it selected
// with, and that record states the same consult object an auto-selected
// consult's does.

// plannedConsultStamp is the stamp the consult floor writes for an A asker in
// anthropic's family: one rung up (A+), another family, no fallback.
var plannedConsultStamp = map[string]string{
	"asker_grade": "A", "asker_family": "anthropic", "grade_source": "flag",
	"floor": "A+", "used_floor": "A+", "fallback": "",
}

func consultStampFixture(t *testing.T) (*App, ConsultPlan) {
	t.Helper()
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex"), consultFloorRung("codex-astra", "high", "SS", "codex")}
	a, _, q := selFixture(t, "A", rows, map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}, consultFloorBothPools)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDoc(routingBlockV1))
	useFakeHK(t).SetDoc("design/consult-q", "## Question\nWhy did the round-3 findings happen?")
	repo := &Repository{
		Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: t.TempDir(),
		Worktree: "author-checkout",
		Checkout: &HubCheckout{Origin: "https://github.com/example/project.git", Mode: "branch", Branch: "wk/x", Sha: strings.Repeat("a", 40), Dir: "author-checkout"},
	}
	plan, err := a.PlanConsult(context.Background(), ConsultPlanInput{
		Task: "42", Question: "hk:doc/design/consult-q", AuthorFamily: "anthropic",
		Requester: "mergewake/example/project#7", Owner: "operator-desk", Repository: repo,
	})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	return a, plan
}

func assertPlannedStamp(t *testing.T, kind string, stamp map[string]string) {
	t.Helper()
	if len(stamp) != len(plannedConsultStamp) {
		t.Fatalf("%s stamp = %v, want %v", kind, stamp, plannedConsultStamp)
	}
	for key, want := range plannedConsultStamp {
		if got := stamp[key]; got != want {
			t.Fatalf("%s stamp %s = %q, want %q (stamp %v)", kind, key, got, want, stamp)
		}
	}
}

// TestPlannedConsultCarriesSelectionStamp_t4875: the planner's real output
// carries the stamp its own seat choice wrote, the hub binds that stamp to the
// spawn it dispatches, and the record writer states it in the consult object.
func TestPlannedConsultCarriesSelectionStamp_t4875(t *testing.T) {
	a, plan := consultStampFixture(t)
	at := indexArg(plan.Request.Args, ConsultStampArg)
	if at < 0 {
		t.Fatalf("the planned consult carries no %s: %v", ConsultStampArg, plan.Request.Args)
	}
	stamp, err := parseConsultStamp(plan.Request.Args[at+1])
	if err != nil {
		t.Fatalf("stamp: %v", err)
	}
	assertPlannedStamp(t, "planned", stamp)

	s, cleanup, err := a.hubSpawn(plan.Request)
	if err != nil {
		t.Fatalf("hub binds the planned consult: %v", err)
	}
	defer cleanup()
	assertPlannedStamp(t, "hub-bound", s.ConsultStamp)

	// attachSelection is what writes the job.placement record (spawn.go) and the
	// hub's placement trace (placement.go) from the spawn.
	rec := map[string]any{"source": "forced", "profile": s.Profile, "host": "local"}
	a.attachSelection(rec, s)
	selection, _ := rec["selection"].(map[string]any)
	consult, _ := selection["consult"].(map[string]any)
	if len(consult) != len(plannedConsultStamp) {
		t.Fatalf("the placement record carries no consult stamp: %v", selection)
	}
	for key, want := range plannedConsultStamp {
		if got := consult[key]; got != want {
			t.Fatalf("recorded %s = %v, want %v", key, got, want)
		}
	}
}

// TestConsultStampRoundTrip_t4875: the argument value is bounded and exact, and
// an asker without a task grade still round-trips (its asker_grade is empty).
func TestConsultStampRoundTrip_t4875(t *testing.T) {
	noGrade := map[string]string{"asker_grade": "", "asker_family": "unknown", "grade_source": "without_task", "floor": "S+", "used_floor": "S+", "fallback": ""}
	value := consultStampValue(noGrade)
	stamp, err := parseConsultStamp(value)
	if err != nil {
		t.Fatalf("no-grade stamp: %v", err)
	}
	for key, want := range noGrade {
		if stamp[key] != want {
			t.Fatalf("%s = %q, want %q", key, stamp[key], want)
		}
	}
	for name, bad := range map[string]string{
		"empty value":   "",
		"not base64url": "eyJh*b",
		"not an object": "W10",
		"missing key":   consultStampValue(map[string]string{"asker_grade": "A"}),
		"empty family":  consultStampValue(map[string]string{"asker_grade": "A", "asker_family": "", "grade_source": "flag", "floor": "A+", "used_floor": "A+", "fallback": ""}),
		"unknown keys":  consultStampValue(map[string]string{"asker_grade": "A", "asker_family": "anthropic", "grade_source": "flag", "floor": "A+", "used_floor": "A+", "fallback": "", "extra": "x"}),
	} {
		if _, err := parseConsultStamp(bad); err == nil {
			t.Fatalf("%s: parse accepted %q", name, bad)
		}
	}
}

// TestManualConsultStampUnchanged_t4875: a spawn with no planned stamp writes
// the selection it always wrote; the carry never invents a consult object.
func TestManualConsultStampUnchanged_t4875(t *testing.T) {
	sel, err := consultFloorSpawnSelect(t, []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex")}, consultFloorBothPools, "A", "anthropic", "operator-desk")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	// The auto-selected consult's own record is the baseline this change leaves
	// alone; the same record is what attachSelection writes for it.
	want := consultFloorStamp(t, sel)
	s := Spawn{Role: "consult", Profile: sel.chosen.seatName}
	s.Sel = sel.record()
	a := testApp(t)
	rec := map[string]any{}
	a.attachSelection(rec, s)
	got, _ := rec["selection"].(map[string]any)["consult"].(map[string]any)
	if len(got) != len(want) {
		t.Fatalf("auto-selected consult stamp = %v, want %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("auto-selected consult %s = %v, want %v", key, got[key], value)
		}
	}
}
