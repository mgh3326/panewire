package fleet

import (
	"context"
	"slices"
	"testing"
)

// PlanStatusGrades is the fleet status snapshot's per-grade plan: the same
// selection core `panewire plan` uses, read-only, and one row per grade.
func TestPlanStatusGradesAnswersEachGradeWithoutWriting(t *testing.T) {
	a, _, _, store, _ := planFixture(t)
	plans := a.PlanStatusGrades(context.Background(), StatusPlanGrades)
	if len(plans) != len(StatusPlanGrades) {
		t.Fatalf("plans %d, want %d: %+v", len(plans), len(StatusPlanGrades), plans)
	}
	for i, plan := range plans {
		if plan.Grade != StatusPlanGrades[i] || plan.T != StatusPlanT || plan.Outcome == "" {
			t.Fatalf("plan %d = %+v, want grade %s at %s with an outcome", i, plan, StatusPlanGrades[i], StatusPlanT)
		}
	}
	// The fixture's grade A rung is the remote claude account.
	gradeA := plans[slices.Index(StatusPlanGrades, "A")]
	if gradeA.Outcome != outcomeAdmitted || gradeA.Host != "host-b" || gradeA.Seat != "sonnet" || gradeA.Pool != "claude" || gradeA.AccountKey != "claude:00000000000000aa" {
		t.Fatalf("grade A plan = %+v, want the fixture's remote claude seat", gradeA)
	}
	if gradeA.Tier == "" {
		t.Fatalf("grade A plan names no selection band: %+v", gradeA)
	}
	if len(store.calls) != 0 {
		t.Fatalf("the status plan must not touch the job store: %v", store.calls)
	}
}

// A plan row without an admitted seat is still one row per grade, so the
// document's shape never changes with the fleet's state.
func TestPlanStatusGradesKeepsEveryGradeOnRefusal(t *testing.T) {
	a, _, _, _, _ := planFixture(t)
	a.Config.Local["spawn"] = false
	a.Source = nil
	plans := a.PlanStatusGrades(context.Background(), StatusPlanGrades)
	if len(plans) != len(StatusPlanGrades) {
		t.Fatalf("plans %d, want %d", len(plans), len(StatusPlanGrades))
	}
	for _, plan := range plans {
		if plan.Grade == "" || plan.T != StatusPlanT || plan.Outcome == "" {
			t.Fatalf("refused plan row = %+v", plan)
		}
	}
}
