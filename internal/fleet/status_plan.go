package fleet

import (
	"context"
	"os"
)

// StatusPlanT is the task scale the fleet status snapshot's per-grade plan
// assumes. Grade and scale are separate inputs to selection, so the snapshot
// names the one it used; T1 keeps the plan comparable across cycles.
const StatusPlanT = "T1"

// StatusPlanGrades are the grades the fleet status snapshot plans for, weakest
// first. SS and S+ are left out because no catalog rung is expected to cover
// them routinely; the console page reads these five.
var StatusPlanGrades = []string{"C", "B", "A", "A+", "S"}

// StatusPlan is one grade's dry-run placement for the fleet status snapshot:
// the seat, host, pool, account and selection band a T1 implementer spawn
// would get now, or the first refusal.
type StatusPlan struct {
	Grade      string `json:"grade"`
	T          string `json:"t"`
	Seat       string `json:"seat,omitempty"`
	Host       string `json:"host,omitempty"`
	Pool       string `json:"pool,omitempty"`
	AccountKey string `json:"account,omitempty"`
	Tier       string `json:"tier,omitempty"`
	Outcome    string `json:"outcome"`
}

// PlanStatusGrades answers what a T1 implementer spawn would get now for each
// grade in grades, as a read-only dry run: it measures no account, reserves no
// host or slot, writes no job state and dispatches nothing (planOnly). The
// per-grade selection record is the same core spawn and `panewire plan` use;
// the chosen host is the host that selection admitted the account on.
func (a *App) PlanStatusGrades(ctx context.Context, grades []string) []StatusPlan {
	out := make([]StatusPlan, 0, len(grades))
	a.planOnly = true
	cwd, _ := os.Getwd()
	route, routeErr := a.routeFor(ctx, "implementer", "")
	for _, grade := range grades {
		entry := StatusPlan{Grade: grade, T: StatusPlanT}
		if routeErr != nil {
			entry.Outcome = outcomeForError(routeErr)
			out = append(out, entry)
			continue
		}
		s := Spawn{CWD: cwd, Profile: "auto", Host: "auto", Role: "implementer", T: StatusPlanT, DryRun: true}
		req := selReq{
			grade: grade, t: StatusPlanT, role: "implementer",
			urgency: urgencyNormal, urgencySource: "default",
			gradeSource: "without_task", route: route,
		}
		sel, err := a.selectFor(ctx, s, req)
		if sel != nil && sel.chosen != nil {
			chosen := sel.chosen
			entry.Seat, entry.Host, entry.Pool = chosen.seatName, chosen.host, chosen.row.Pool
			entry.AccountKey, entry.Tier = chosen.accountKey, chosen.tierName
		}
		entry.Outcome = outcomeForError(err)
		out = append(out, entry)
	}
	return out
}
