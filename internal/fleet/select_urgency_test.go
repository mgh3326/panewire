package fleet

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// urgencyRows spans every tier: the free SWE-2 bucket, a WASTE subscription
// pool, the credit pool and two ranked pools of different grades.
func urgencyRows() ([]CatalogRow, map[string]PoolView, []string, map[string]string) {
	waste, noWaste := true, false
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "opus", Effort: "high", Grade: "A", Pool: "anthropic-api", Gate: "default", ModelID: "m"},
		{Profile: "codex-luna", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "kiro-glm", Effort: "high", Grade: "S", Pool: "kiro", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Waste: &waste, Blocking: ptrF(90)},
		"anthropic-api": {Status: "ok", Class: "spend", Waste: &noWaste, Blocking: ptrF(30)},
		"codex":         {Status: "ok", Class: "spend", Waste: &noWaste, Blocking: ptrF(5)},
		"kiro":          {Status: "ok", Class: "spend", Waste: &noWaste, Blocking: ptrF(60)},
	}
	gate := map[string]string{"devin-swe2": "devin", "sonnet": "claude", "opus": "anthropic-api", "codex-luna": "codex", "kiro-glm": "kiro"}
	return rows, pools, []string{"devin", "claude", "anthropic-api", "codex", "kiro"}, gate
}

// urgencyFixture is selFixture with an hk task priority on the bound task.
func urgencyFixture(t *testing.T, priority string, rows []CatalogRow, pools map[string]PoolView, present []string) (*App, Spawn, *fakeQuota) {
	a, s, q := selFixture(t, "A", rows, pools, present)
	base := a.Runner.(*fakeRunner).Do
	a.Runner.(*fakeRunner).Do = func(c Command) Result {
		if c.Name == "handoffkeep" && strings.HasPrefix(strings.Join(c.Args, " "), "tasks show") {
			return Result{Out: fmt.Sprintf(`{"id":42,"title":"work (grade A, T1)","state":"ready","priority":%s,"refs":{}}`, priority)}
		}
		return base(c)
	}
	return a, s, q
}

func TestSelectNormalConsumesWasteBeforeFree(t *testing.T) {
	rows, pools, present, gate := urgencyRows()
	a, s, q := urgencyFixture(t, "3", rows, pools, present)
	q.GateDo = gatePools(gate)
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if sel.chosen.seatName != "sonnet" {
		t.Fatalf("normal work consumes expiring quota before SWE-2: %s", sel.chosen.seatName)
	}
	rec := sel.record()
	if rec["urgency"] != "normal" || rec["urgency_source"] != "default" {
		t.Fatalf("normal default must be recorded: %v %v", rec["urgency"], rec["urgency_source"])
	}
	if got := strings.Join(rec["tiers"].([]string), ","); got != "waste,free,ranked,credit" {
		t.Fatalf("normal tier order: %s", got)
	}
}

// Urgency is recorded but never reorders the tiers (policy/quota-stop: free
// after waste, always) or prefers a stronger model than the task needs.
func TestSelectUrgentKeepsPolicyOrder(t *testing.T) {
	cases := []struct {
		name  string
		drop  []string // profiles removed from the catalog
		want  string
		tiers string
	}{
		{"waste before free", nil, "sonnet", "waste"},
		{"free after waste", []string{"sonnet"}, "devin-swe2", "free"},
		{"ranked before credit", []string{"devin-swe2", "sonnet"}, "codex-luna", "ranked"},
		{"credit last", []string{"devin-swe2", "sonnet", "codex-luna", "kiro-glm"}, "opus", "credit"},
		{"ranked lowest sufficient grade", []string{"devin-swe2", "sonnet", "opus"}, "codex-luna", "ranked"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			rows, pools, present, gate := urgencyRows()
			var kept []CatalogRow
			for _, r := range rows {
				if !strings.Contains(strings.Join(tt.drop, ","), r.Profile) {
					kept = append(kept, r)
				}
			}
			a, s, q := urgencyFixture(t, "3", kept, pools, present)
			q.GateDo = gatePools(gate)
			s.Urgency = "urgent"
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			if sel.chosen.seatName != tt.want || sel.chosen.tierName != tt.tiers {
				t.Fatalf("want %s in %s, got %s in %s", tt.want, tt.tiers, sel.chosen.seatName, sel.chosen.tierName)
			}
			rec := sel.record()
			if rec["urgency"] != "urgent" || rec["urgency_source"] != "flag" {
				t.Fatalf("urgency record: %v %v", rec["urgency"], rec["urgency_source"])
			}
			if got := strings.Join(rec["tiers"].([]string), ","); got != "waste,free,ranked,credit" {
				t.Fatalf("urgent tier order: %s", got)
			}
		})
	}
}

func TestSelectUrgentRankedFallsToHeadroomOnEqualGrade(t *testing.T) {
	rows, pools, present, gate := urgencyRows()
	rows = rows[3:4] // one ranked rung; add a same-grade rival with less headroom
	rows = append(rows, CatalogRow{Profile: "codex-terra61", Effort: "high", Grade: "A", Pool: "terra", Gate: "default", ModelID: "m"})
	gate["codex-terra61"] = "terra"
	pools["terra"] = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60)}
	present = append(present, "terra")
	a, s, q := urgencyFixture(t, "3", rows, pools, present)
	q.GateDo = gatePools(gate)
	s.Urgency = "urgent"
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if sel.chosen.seatName != "codex-luna" {
		t.Fatalf("equal grade falls back to quota headroom: %s", sel.chosen.seatName)
	}
}

func TestSelectUrgentDoesNotProbeFreeBeforeAdmittedWaste(t *testing.T) {
	rows, pools, present, gate := urgencyRows()
	delete(gate, "devin-swe2") // a live SWE-2 hold: the gate refuses free
	a, s, q := urgencyFixture(t, "3", rows, pools, present)
	q.GateDo = gatePools(gate)
	s.Urgency = "urgent"
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if sel.chosen.seatName != "sonnet" || sel.chosen.tierName != "waste" {
		t.Fatalf("available waste must precede the free rung: %s", sel.chosen.seatName)
	}
	if got := candOutcome(sel.record(), "devin-swe2", "high")["outcome"]; got != "below_choice" {
		t.Fatalf("free rung should remain unprobed: %v", got)
	}
}

func TestSelectUrgentUnknownQuotaStillFailsClosed(t *testing.T) {
	rows, _, present, gate := urgencyRows()
	a, s, q := urgencyFixture(t, "1", rows[1:2], nil, present)
	q.GateDo = gatePools(gate)
	if _, e := a.selectProfile(context.Background(), s); e == nil {
		t.Fatal("urgent work must not guess an admissible rung from unknown quota")
	}
}

func TestSelectTaskPriorityDerivesUrgency(t *testing.T) {
	for _, tt := range []struct {
		priority, flag, want, source string
	}{
		{"2", "", "urgent", "task-priority"},
		{"1", "", "urgent", "task-priority"},
		{"3", "", "normal", "default"},
		{"null", "", "normal", "default"},
		{"1", "normal", "normal", "flag"},
		{"3", "urgent", "urgent", "flag"},
	} {
		t.Run(tt.priority+"/"+tt.flag, func(t *testing.T) {
			rows, pools, present, gate := urgencyRows()
			a, s, q := urgencyFixture(t, tt.priority, rows, pools, present)
			q.GateDo = gatePools(gate)
			s.Urgency = tt.flag
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			rec := sel.record()
			if rec["urgency"] != tt.want || rec["urgency_source"] != tt.source {
				t.Fatalf("got urgency=%v source=%v", rec["urgency"], rec["urgency_source"])
			}
			if sel.chosen.tierName != "waste" {
				t.Fatalf("waste must be chosen for %s work: %s", tt.want, sel.chosen.tierName)
			}
		})
	}
}

func TestParseSpawnRejectsUnknownUrgency(t *testing.T) {
	a, s, _ := urgencyFixture(t, "3", nil, nil, nil)
	s.Urgency = "asap"
	s.Profile = "auto"
	if e := a.Spawn(context.Background(), s); e == nil || exitCode(e) != 2 {
		t.Fatalf("unknown urgency must be a usage error: %v", e)
	}
}
