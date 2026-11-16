package fleet

import (
	"context"
	"os"
	"strings"
	"testing"
)

// hk 4868: the planner's submission must pass the same admission the hub runs.
// PlanConsult ends at ValidateSubmission, which only parses the args; the
// author-family rule lives in Spawn, which the hub reaches through hubSpawn.
// This binds the planner's real output the way the hub does, then runs Spawn as
// a dry run, so the planner's output is judged exactly as the hub judges it.
// The hub's coordinator never places on itself (spawn: false) and this fixture
// has no node to place on, so the test lifts that one placement switch after
// the hub's binding; Spawn's admission checks run unchanged.
func TestPlannedConsultPassesHubAdmission_t4868(t *testing.T) {
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
	ctx := context.Background()
	plan, err := a.PlanConsult(ctx, ConsultPlanInput{
		Task: "42", Question: "hk:doc/design/consult-q", AuthorFamily: "anthropic",
		PR: "example/project#7", Head: strings.Repeat("b", 40), Requester: "mergewake/example/project#7",
		Owner: "operator-desk", Repository: repo,
	})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	s, cleanup, err := a.hubSpawn(plan.Request)
	if err != nil {
		t.Fatalf("hub binds the planned consult: %v", err)
	}
	defer cleanup()
	// The hub runs the consult in a checkout of the head it was asked to review.
	if err := os.MkdirAll(s.CWD, 0700); err != nil {
		t.Fatal(err)
	}
	// The launch the hub's catalog answers for the planned seat at its rung.
	for _, row := range rows {
		if row.Profile == plan.Seat.Profile {
			q.LaunchValue = Launch{Profile: row.Profile, ModelID: row.ModelID, Effort: row.Effort, Grade: row.Grade, Gate: row.Gate, Pool: row.Pool}
		}
	}
	a.Config.Local = map[string]any{"workspace": "workspace"}
	s.DryRun = true
	a.planOnly, a.planFromHub = true, true
	if err := a.Spawn(ctx, s); err != nil {
		t.Fatalf("hub admission refused the planned consult: %v", err)
	}
	if a.lastPlan == nil {
		t.Fatal("admission placed no seat")
	}
	if a.lastPlan.Role != "consult" {
		t.Fatalf("admitted role = %q, want consult", a.lastPlan.Role)
	}
	if fam := a.Policy.Profiles[a.lastPlan.Profile].Family; fam == "" || fam == "anthropic" {
		t.Fatalf("admitted seat %s is in family %q; a consult must take another family than the author's anthropic", a.lastPlan.Profile, fam)
	}
}
