package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// consultFloorRung is a synthetic catalog rung for the consult floor tests.
func consultFloorRung(profile, effort, grade, pool string) CatalogRow {
	return CatalogRow{Profile: profile, Effort: effort, Grade: grade, Pool: pool, Gate: "default", ModelID: profile + "-m"}
}

// consultFloorOpts varies one consult selection: the routing block (default
// routingBlockV1), --allow-escalation, and pools cooling for an hour.
type consultFloorOpts struct {
	block           string
	allowEscalation bool
	cooling         []string
}

// consultFloorSelect runs the selection core the way resolve builds its
// request for a consult asker. grade "" is a session without a task, family ""
// an asker whose family is unknown.
func consultFloorSelect(t *testing.T, rows []CatalogRow, present []string, grade, family string) (*selPlan, error) {
	t.Helper()
	return consultFloorSelectWith(t, consultFloorOpts{}, rows, present, grade, family)
}

func consultFloorSelectWith(t *testing.T, o consultFloorOpts, rows []CatalogRow, present []string, grade, family string) (*selPlan, error) {
	t.Helper()
	block := o.block
	if block == "" {
		block = routingBlockV1
	}
	pools := map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, "S", rows, pools, present)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	if len(o.cooling) > 0 {
		until := a.Now().Add(time.Hour)
		cooling := map[string]time.Time{}
		for _, pool := range o.cooling {
			cooling[pool] = until
		}
		a.State = &fakeStateReader{snap: FleetSnapshot{Cooling: cooling}}
	}
	serveRouting(t, routingDoc(block))
	route, e := a.routeFor(context.Background(), "implementer", "consult")
	if e != nil {
		t.Fatalf("route: %v", e)
	}
	s.Role, s.RoutingRole, s.Task, s.AllowEscalation = "implementer", "consult", "", o.allowEscalation
	req := selReq{grade: grade, t: "T1", role: "implementer", authorFamily: family, allowEscalation: o.allowEscalation, urgency: urgencyNormal, urgencySource: "default", route: route}
	return a.selectFor(context.Background(), s, req)
}

func consultFloorSpawnSelect(t *testing.T, rows []CatalogRow, present []string, grade, family, parentLane string) (*selPlan, error) {
	t.Helper()
	pools := map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, s, q := selFixture(t, grade, rows, pools, present)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDoc(routingBlockV1))
	s.Role, s.RoutingRole, s.Parent, s.ParentFamily = "implementer", "consult", parentLane, family
	return a.selectProfile(context.Background(), s)
}

func TestConsultDesignSessionUsesSSFloor(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex"), consultFloorRung("codex-astra", "high", "SS", "codex")}
	for _, lane := range []string{"panewire-design", "routing-policy", "fleet-policy"} {
		t.Run(lane, func(t *testing.T) {
			sel, err := consultFloorSpawnSelect(t, rows, consultFloorBothPools, "A", "anthropic", lane)
			if err != nil {
				t.Fatalf("select: %v", err)
			}
			if got := sel.chosen.seat.Canon; got != "codex-astra" {
				t.Fatalf("design-session asker must select the other family's SS row; got %s", got)
			}
			c := consultFloorStamp(t, sel)
			if c["floor"] != "SS" || c["used_floor"] != "SS" || c["fallback"] != "" {
				t.Fatalf("design-session consult stamp = %v, want SS without fallback", c)
			}
		})
	}
}

func TestConsultDesignSessionFallsBackWhenNoOtherFamilySS(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex")}
	sel, err := consultFloorSpawnSelect(t, rows, consultFloorBothPools, "A", "anthropic", "panewire-design")
	if err != nil {
		t.Fatalf("select with fallback: %v", err)
	}
	if got := sel.chosen.seat.Canon; got != "codex-sol" {
		t.Fatalf("fallback must select the other family's rung; got %s", got)
	}
	c := consultFloorStamp(t, sel)
	if c["floor"] != "SS" || c["used_floor"] != "A" || c["fallback"] != "no_rung_at_floor" {
		t.Fatalf("design-session fallback stamp = %v, want floor SS, used A, no_rung_at_floor", c)
	}
}

func TestConsultOtherLaneKeepsAskerPlusOneFloor(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex"), consultFloorRung("codex-astra", "high", "SS", "codex")}
	sel, err := consultFloorSpawnSelect(t, rows, consultFloorBothPools, "A", "", "operator-desk")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if got := sel.chosen.row.Grade; got != "A+" {
		t.Fatalf("operator-desk A asker selected grade %s, want A+", got)
	}
	c := consultFloorStamp(t, sel)
	if c["floor"] != "A+" || c["used_floor"] != "A+" {
		t.Fatalf("operator-desk consult stamp = %v, want A+ floor", c)
	}
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
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDocV1)
	out := captureOut(a)
	if e := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", "T1", "--grade", "S+", "--author-family", "anthropic", "--json"}); e != nil {
		t.Fatalf("resolve --grade S+: %v", e)
	}
	var r map[string]any
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatalf("resolve json: %v\n%s", err, out)
	}
	if r["grade"] != "S+" || r["profile"] != "codex-sol" {
		t.Fatalf("resolve --grade S+ --author-family anthropic = grade %v profile %v, want S+ codex-sol", r["grade"], r["profile"])
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

func TestConsultResolveLaneUsesDesignFloor(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex"), consultFloorRung("codex-astra", "high", "SS", "codex")}
	pools := map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	a, _, q := selFixture(t, "A", rows, pools, consultFloorBothPools)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDoc(routingBlockV1))
	out := captureOut(a)
	if err := a.Run(context.Background(), []string{"resolve", "--role", "consult", "--t", "T1", "--grade", "A", "--lane", "routing-policy", "--author-family", "anthropic", "--json"}); err != nil {
		t.Fatalf("resolve consult: %v", err)
	}
	var r map[string]any
	if err := json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatalf("resolve json: %v\n%s", err, out)
	}
	if r["profile"] != "codex-astra" {
		t.Fatalf("resolve --lane routing-policy selected %v, want codex-astra SS", r["profile"])
	}
	selection, ok := r["selection"].(map[string]any)
	if !ok {
		t.Fatalf("resolve selection missing: %v", r["selection"])
	}
	consult, ok := selection["consult"].(map[string]any)
	if !ok || consult["floor"] != "SS" || consult["used_floor"] != "SS" {
		t.Fatalf("resolve consult stamp = %v, want SS floor", selection["consult"])
	}
}

// TestConsultFloorFamilyEffortBelowFloorRow: family_effort names codex-sol at
// xhigh, which the catalog grades S. An S+ asker must not get that row: its
// launched effort fails the floor (grade_insufficient). The family's max rung
// sits above the role effort, so without --allow-escalation nothing is chosen
// and the refusal is role_no_candidate; with --allow-escalation the S+ max
// rung is the escalation.
func TestConsultFloorFamilyEffortBelowFloorRow(t *testing.T) {
	block := consultFamilyEffortBlock(t, `{"codex-sol": "xhigh"}`)
	rows := []CatalogRow{consultFloorRung("codex-sol", "xhigh", "S", "codex"), consultFloorRung("codex-sol", "max", "S+", "codex")}
	sel, e := consultFloorSelectWith(t, consultFloorOpts{block: block}, rows, []string{"codex"}, "S+", "anthropic")
	if e == nil || !strings.Contains(e.Error(), reasonRoleNoCandidate) {
		t.Fatalf("S+ asker without escalation must refuse with %s; got %v", reasonRoleNoCandidate, e)
	}
	if sel == nil {
		t.Fatal("no selection plan")
	}
	if got := candOutcome(sel.record(), "codex-sol", "xhigh")["reason"]; got != "grade_insufficient" {
		t.Fatalf("codex-sol@xhigh reason = %v, want grade_insufficient", got)
	}
	if got := candOutcome(sel.record(), "codex-sol", "max")["reason"]; got != reasonRoleEffortUnplaced {
		t.Fatalf("codex-sol@max reason = %v, want %s", got, reasonRoleEffortUnplaced)
	}

	sel, e = consultFloorSelectWith(t, consultFloorOpts{block: block, allowEscalation: true}, rows, []string{"codex"}, "S+", "anthropic")
	if e != nil {
		t.Fatalf("select with --allow-escalation: %v", e)
	}
	if got := sel.chosen.row.Effort; got != "max" {
		t.Fatalf("escalated S+ asker must get codex-sol@max; got %s", got)
	}
}

// TestConsultFloorJudgesLaunchedRung: a row launches at its own effort, so the
// floor judges that rung, not the role-effort rung. codex-sol@min graded C
// must not borrow codex-sol@high's S+ grade, and codex-sol@min graded S+ must
// not be refused for a C role-effort rung.
func TestConsultFloorJudgesLaunchedRung(t *testing.T) {
	t.Run("below_floor_own_rung", func(t *testing.T) {
		rows := []CatalogRow{consultFloorRung("codex-sol", "low", "C", "codex"), consultFloorRung("codex-sol", "high", "S+", "codex")}
		sel, e := consultFloorSelect(t, rows, []string{"codex"}, "S", "")
		if e != nil {
			t.Fatalf("select: %v", e)
		}
		if got := candOutcome(sel.record(), "codex-sol", "low")["reason"]; got != "grade_insufficient" {
			t.Fatalf("codex-sol@low launches at low (grade C); reason = %v, want grade_insufficient", got)
		}
	})
	t.Run("above_floor_own_rung", func(t *testing.T) {
		rows := []CatalogRow{consultFloorRung("codex-sol", "low", "S+", "codex"), consultFloorRung("codex-sol", "high", "C", "codex")}
		sel, e := consultFloorSelect(t, rows, []string{"codex"}, "S", "")
		if e != nil {
			t.Fatalf("select: %v", e)
		}
		if got := sel.chosen.row.Effort; got != "low" {
			t.Fatalf("codex-sol@low (grade S+) launches at low; chose effort %s", got)
		}
	})
}

// TestConsultFloorCoolingBelowFloorRetries: a below-floor rung on a cooling
// pool is a fallback candidate, not a passing one, so no_rung_at_floor retries
// at the asker's grade and admits the healthy rung.
func TestConsultFloorCoolingBelowFloorRetries(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("codex-sol", "high", "S", "codex"), consultFloorRung("opus", "high", "S", "claude")}
	sel, e := consultFloorSelectWith(t, consultFloorOpts{cooling: []string{"codex"}}, rows, consultFloorBothPools, "S", "")
	if e != nil {
		t.Fatalf("select: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "opus" {
		t.Fatalf("retry at asker grade must admit opus@high; got %s", got)
	}
	if c := consultFloorStamp(t, sel); c["fallback"] != "no_rung_at_floor" {
		t.Fatalf("fallback stamp = %v, want no_rung_at_floor", c)
	}
	// The record is the retry pass at the asker's grade, where the cooling
	// codex-sol@high passes the grade and drops on its pool.
	if got := candOutcome(sel.record(), "codex-sol", "high")["reason"]; got != "pool_cooling" {
		t.Fatalf("cooling codex-sol@high at retry grade S; reason = %v, want pool_cooling", got)
	}
}

// TestConsultFloorCoolingUnratedDoesNotSuppressFallback: an unrated rung in a
// cooling pool is still unrated, so it must not count as a passing floor rung
// and suppress fallback for a separate below-floor candidate.
func TestConsultFloorCoolingUnratedDoesNotSuppressFallback(t *testing.T) {
	rows := []CatalogRow{
		consultFloorRung("codex-sol", "high", gradeUnrated, "codex"),
		consultFloorRung("opus", "high", "S+", "claude"),
	}
	sel, e := consultFloorSelectWith(t, consultFloorOpts{cooling: []string{"codex"}}, rows, consultFloorBothPools, "S+", "")
	if e != nil {
		t.Fatalf("select with fallback: %v", e)
	}
	if got := sel.chosen.seat.Canon; got != "opus" {
		t.Fatalf("fallback must select the healthy S+ rung; got %s", got)
	}
	if c := consultFloorStamp(t, sel); c["fallback"] != "no_rung_at_floor" {
		t.Fatalf("fallback stamp = %v, want no_rung_at_floor", c)
	}
	if got := candOutcome(sel.record(), "codex-sol", "high")["reason"]; got != "pool_cooling" {
		t.Fatalf("cooling unrated codex-sol@high reason = %v, want pool_cooling", got)
	}
}
