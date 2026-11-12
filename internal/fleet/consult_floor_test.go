package fleet

import (
	"context"
	"encoding/json"
	"testing"
)

// consultFloorRung is a synthetic catalog rung for the consult floor tests.
func consultFloorRung(profile, effort, grade, pool string) CatalogRow {
	return CatalogRow{Profile: profile, Effort: effort, Grade: grade, Pool: pool, Gate: "default", ModelID: profile + "-m"}
}

// consultFloorSelect runs the selection core the way resolve builds its
// request for a consult asker. grade "" is a session without a task, family ""
// an asker whose family is unknown.
func consultFloorSelect(t *testing.T, rows []CatalogRow, present []string, grade, family string) (*selPlan, error) {
	t.Helper()
	pools := map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "S", rows, pools, present)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex"})
	serveRouting(t, routingDocV1)
	route, e := a.routeFor(context.Background(), "worker", "consult")
	if e != nil {
		t.Fatalf("route: %v", e)
	}
	s.Role, s.RoutingRole, s.Task = "worker", "consult", ""
	req := selReq{grade: grade, t: "T1", role: "worker", builderFamily: family, urgency: urgencyNormal, urgencySource: "default", route: route}
	return a.selectFor(context.Background(), s, req)
}

// consultFloorStamp reads the consult stamp of a selection record.
func consultFloorStamp(t *testing.T, sel *selPlan) map[string]any {
	t.Helper()
	if sel == nil {
		t.Fatal("no selection plan")
	}
	c, ok := sel.record()["consult"].(map[string]any)
	if !ok {
		t.Fatalf("selection record has no consult stamp: %v", sel.record())
	}
	return c
}

var consultFloorBothPools = []string{"claude", "codex"}

// TestConsultFloorUnratedRungNeverChosen: an unrated launch rung is dropped
// with grade_unrated, even when it would rank first on the codex pool.
func TestConsultFloorUnratedRungNeverChosen(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "SS", "claude"), consultFloorRung("codex-sol", "high", gradeUnrated, "codex")}
	sel, e := consultFloorSelect(t, rows, consultFloorBothPools, "S", "")
	if e != nil {
		t.Fatalf("select: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "opus" {
		t.Fatalf("unrated codex-sol must never be chosen; got %s", got)
	}
	if got := candOutcome(sel.record(), "codex-sol", "high")["reason"]; got != "grade_unrated" {
		t.Fatalf("codex-sol@high reason = %v, want grade_unrated", got)
	}
}

// TestConsultFloorBelowFloorRungDropped: with asker S the floor is S+, so an A
// rung is dropped with grade_insufficient and an S+ rung is chosen.
func TestConsultFloorBelowFloorRungDropped(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A", "claude"), consultFloorRung("codex-sol", "high", "S+", "codex")}
	sel, e := consultFloorSelect(t, rows, consultFloorBothPools, "S", "")
	if e != nil {
		t.Fatalf("select: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "codex-sol" {
		t.Fatalf("asker S must get an S+ rung; got %s", got)
	}
	if got := candOutcome(sel.record(), "opus", "high")["reason"]; got != "grade_insufficient" {
		t.Fatalf("opus@high reason = %v, want grade_insufficient", got)
	}
}

// TestConsultFloorNoTaskFloorIsSPlus: a session without a task has floor S+,
// stamped as such, with no fallback.
func TestConsultFloorNoTaskFloorIsSPlus(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A", "claude"), consultFloorRung("codex-sol", "high", "S+", "codex")}
	sel, e := consultFloorSelect(t, rows, consultFloorBothPools, "", "")
	if e != nil {
		t.Fatalf("select: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "codex-sol" {
		t.Fatalf("no task must still get an S+ rung; got %s", got)
	}
	c := consultFloorStamp(t, sel)
	if c["floor"] != "S+" || c["used_floor"] != "S+" || c["fallback"] != "" || c["asker_family"] != "unknown" {
		t.Fatalf("no-task consult stamp = %v, want floor S+, used S+, no fallback, family unknown", c)
	}
}

// TestConsultFloorOtherFamilyExcluded: the asker's own family is excluded for
// consult with reason family_excluded.
func TestConsultFloorOtherFamilyExcluded(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "S+", "claude"), consultFloorRung("codex-sol", "high", "S+", "codex")}
	sel, e := consultFloorSelect(t, rows, consultFloorBothPools, "S", "anthropic")
	if e != nil {
		t.Fatalf("select: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "codex-sol" {
		t.Fatalf("anthropic asker must get the other family; got %s", got)
	}
	if got := candOutcome(sel.record(), "opus", "high")["reason"]; got != "family_excluded" {
		t.Fatalf("opus@high reason = %v, want family_excluded", got)
	}
}

// TestConsultFloorSSRungStamped: an S+ asker has floor SS; an SS rung is
// chosen, the S+ rung is grade_insufficient, and nothing falls back.
func TestConsultFloorSSRungStamped(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "S+", "claude"), consultFloorRung("codex-sol", "high", "SS", "codex")}
	sel, e := consultFloorSelect(t, rows, consultFloorBothPools, "S+", "")
	if e != nil {
		t.Fatalf("select: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "codex-sol" {
		t.Fatalf("S+ asker must get the SS rung; got %s", got)
	}
	if got := candOutcome(sel.record(), "opus", "high")["reason"]; got != "grade_insufficient" {
		t.Fatalf("opus@high reason = %v, want grade_insufficient", got)
	}
	c := consultFloorStamp(t, sel)
	if c["asker_grade"] != "S+" || c["floor"] != "SS" || c["used_floor"] != "SS" || c["fallback"] != "" || c["asker_family"] != "unknown" {
		t.Fatalf("SS consult stamp = %v", c)
	}
}

// TestConsultFloorFallbackToAskerGrade: with no SS rung on the other family,
// an S+ asker falls back once to floor S+ and records it.
func TestConsultFloorFallbackToAskerGrade(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "S+", "claude"), consultFloorRung("codex-sol", "high", "S+", "codex")}
	sel, e := consultFloorSelect(t, rows, consultFloorBothPools, "S+", "anthropic")
	if e != nil {
		t.Fatalf("select: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "codex-sol" {
		t.Fatalf("fallback must pick the other family's S+ rung; got %s", got)
	}
	c := consultFloorStamp(t, sel)
	if c["floor"] != "SS" || c["used_floor"] != "S+" || c["fallback"] != "no_rung_at_floor" {
		t.Fatalf("fallback stamp = %v, want floor SS, used S+, no_rung_at_floor", c)
	}
}

// TestConsultFloorNoFallbackForUnratedOrAbsentFloorRung: fallback applies only
// when a rated rung below the floor passes every other rule. An unrated rung,
// or an SS rung that has no presence, is role_no_candidate and never falls back.
func TestConsultFloorNoFallbackForUnratedOrAbsentFloorRung(t *testing.T) {
	for _, tc := range []struct {
		name    string
		rows    []CatalogRow
		present []string
		reason  string
	}{
		{"unrated other-family rung", []CatalogRow{consultFloorRung("opus", "high", "S+", "claude"), consultFloorRung("codex-sol", "high", gradeUnrated, "codex")}, consultFloorBothPools, "grade_unrated"},
		{"SS rung without presence", []CatalogRow{consultFloorRung("opus", "high", "S+", "claude"), consultFloorRung("codex-sol", "high", "SS", "codex")}, []string{"claude"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sel, e := consultFloorSelect(t, tc.rows, tc.present, "S+", "anthropic")
			if e == nil {
				t.Fatalf("expected no selection, got %s", sel.chosen.seat.Canon)
			}
			if c := consultFloorStamp(t, sel); c["fallback"] != "" {
				t.Fatalf("fallback must not apply: %v", c)
			}
			if tc.reason != "" {
				if got := candOutcome(sel.record(), "codex-sol", "high")["reason"]; got != tc.reason {
					t.Fatalf("codex-sol@high reason = %v, want %s", got, tc.reason)
				}
			}
		})
	}
}

// TestConsultFloorResolveGradeFlag: resolve takes --grade for consult, refuses
// an empty or unknown grade with exit 2, and prints the asker grade and family.
func TestConsultFloorResolveGradeFlag(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "SS", "claude"), consultFloorRung("codex-sol", "high", "SS", "codex")}
	pools := map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, _, q := selFixture(t, "S", rows, pools, consultFloorBothPools)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex"})
	serveRouting(t, routingDocV1)
	out := captureOut(a)
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", "T1", "--grade", "S+", "--builder-family", "anthropic", "--json"}); e != nil {
		t.Fatalf("resolve --grade S+: %v", e)
	}
	var r map[string]any
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatalf("resolve json: %v\n%s", err, out)
	}
	if r["grade"] != "S+" || r["profile"] != "codex-sol" {
		t.Fatalf("resolve --grade S+ --builder-family anthropic = grade %v profile %v, want S+ codex-sol", r["grade"], r["profile"])
	}
	for _, grade := range []string{"Z", ""} {
		a2, _, _ := selFixture(t, "S", rows, pools, consultFloorBothPools)
		serveRouting(t, routingDocV1)
		e := a2.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", "T1", "--grade", grade})
		if exitCode(e) != exitUsage {
			t.Fatalf("resolve --grade %q: exit %d, want %d (%v)", grade, exitCode(e), exitUsage, e)
		}
	}
}
