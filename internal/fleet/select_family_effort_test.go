package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// hk 2595: roles.<role>.family_effort sets a lead/consult family's
// role-effort rung and opts that exact escalation rung in.

const consultRuleV1 = `"consult":     {"allowed": ["opus", "fable", "codex-sol", "codex-astra"], "effort": "high", "on_exhaustion": "wait"}`

func consultFamilyEffortBlock(t *testing.T, familyEffort string) string {
	t.Helper()
	lead := strings.Replace(consultRuleV1, `"on_exhaustion": "wait"}`, `"on_exhaustion": "wait", "family_effort": `+familyEffort+`}`, 1)
	block := strings.Replace(routingBlockV1, consultRuleV1, lead, 1)
	if block == routingBlockV1 {
		t.Fatal("routingBlockV1 lead rule moved; update consultRuleV1")
	}
	return block
}

// Acceptance 1: a valid map parses; a bad effort, an unknown key, or a map
// on a role without the lead/consult cap is refused by name.
func TestRoutingFamilyEffortParse(t *testing.T) {
	p, e := parseRoutingDoc(routingDoc(consultFamilyEffortBlock(t, `{"codex-sol": "xhigh", "opus": "high"}`)))
	if e != nil {
		t.Fatalf("valid family_effort refused: %v", e)
	}
	if got := p.Roles["consult"].FamilyEffort; got["codex-sol"] != "xhigh" || got["opus"] != "high" || len(got) != 2 {
		t.Fatalf("family_effort not parsed: %v", got)
	}
	for name, tc := range map[string]struct{ block, want string }{
		"bad effort":   {consultFamilyEffortBlock(t, `{"codex-sol": "turbo"}`), `family_effort codex-sol has unknown effort "turbo"`},
		"empty effort": {consultFamilyEffortBlock(t, `{"codex-sol": ""}`), `unknown effort ""`},
		"unknown key":  {consultFamilyEffortBlock(t, `{"sonnet": "xhigh"}`), `names unknown profile "sonnet"`},
		"not a map":    {consultFamilyEffortBlock(t, `"xhigh"`), "cannot unmarshal"},
		"non-top role": {strings.Replace(routingBlockV1, `"verifier":    {"allowed": ["opus", "codex-terra61"], "effort": "high"`, `"verifier":    {"allowed": ["opus", "codex-terra61"], "family_effort": {"opus": "max"}, "effort": "high"`, 1), "only consult takes one"},
	} {
		_, e := parseRoutingDoc(routingDoc(tc.block))
		if e == nil || !strings.Contains(e.Error(), tc.want) {
			t.Errorf("%s: want refusal containing %q, got %v", name, tc.want, e)
		}
		if tc.want != "cannot unmarshal" && (e == nil || !strings.HasPrefix(e.Error(), reasonFamilyEffortInvalid+":")) {
			t.Errorf("%s: refusal must be named %s: %v", name, reasonFamilyEffortInvalid, e)
		}
	}
	// A wildcard role may name any profile id, OMP profiles included.
	star := strings.Replace(consultFamilyEffortBlock(t, `{"omp-sol": "xhigh"}`), `"allowed": ["opus", "fable", "codex-sol", "codex-astra"], "effort": "high", "on_exhaustion": "wait", "family`, `"allowed": ["*"], "effort": "high", "on_exhaustion": "wait", "family`, 1)
	if _, e := parseRoutingDoc(routingDoc(star)); e != nil {
		t.Fatalf("wildcard lead may key omp-sol: %v", e)
	}
}

// familyEffortFixture is the mbp catalog shape: codex-sol@high is an
// unplaced E6 rung, codex-sol@xhigh carries the escalation gate,
// codex-sol@max the default gate, and the codex pool ranks first by waste.
func familyEffortFixture(t *testing.T, familyEffort string) (*App, Spawn, *fakeQuota) {
	t.Helper()
	a, s, q := roleEffortFixture(t)
	q.CatalogRows = append(q.CatalogRows, CatalogRow{Profile: "codex-sol", Effort: "xhigh", Grade: "S+", Pool: "codex", Gate: "escalation", ModelID: "sol-m"})
	if familyEffort != "" {
		serveRouting(t, routingDoc(consultFamilyEffortBlock(t, familyEffort)))
	}
	return a, s, q
}

// Acceptance 2: with family_effort codex-sol=xhigh a lead admits
// codex-sol@xhigh through its escalation gate and still drops codex-sol@max.
func TestLeadFamilyEffortAdmitsEscalationRung(t *testing.T) {
	for _, tc := range []struct{ name, role, override string }{
		{"resolve --role consult (implementer seat)", "implementer", "consult"},
		{"--role consult", "consult", ""},
	} {
		a, s, q := familyEffortFixture(t, `{"codex-sol": "xhigh"}`)
		s.Role, s.RoutingRole = tc.role, tc.override
		sel, e := a.selectProfile(context.Background(), s)
		if e != nil {
			t.Fatalf("%s: %v", tc.name, e)
		}
		if c := sel.chosen; c.seat.Canon != "codex-sol" || c.row.Effort != "xhigh" {
			t.Fatalf("%s: want codex-sol@xhigh, got %s@%s", tc.name, c.seat.Canon, c.row.Effort)
		}
		rec := sel.record()
		if got := candOutcome(rec, "codex-sol", "max"); got["reason"] != reasonAboveRoleEffort {
			t.Fatalf("%s: codex-sol@max stays above the family's role effort: %v", tc.name, got)
		}
		if gateAsked(q, "codex-sol", "max") {
			t.Fatalf("%s: the max rung's gate must never be asked", tc.name)
		}
		rt := routingStamp(t, sel)
		if fe, _ := rt["family_effort"].(map[string]string); fe["codex-sol"] != "xhigh" {
			t.Fatalf("%s: routing.family_effort must carry the policy map: %v", tc.name, rt)
		}
		chosen, _ := rt["family_effort_chosen"].(map[string]any)
		if chosen["profile"] != "codex-sol" || chosen["key"] != "codex-sol" || chosen["effort"] != "xhigh" || chosen["gate"] != "escalation" {
			t.Fatalf("%s: routing.family_effort_chosen: %v", tc.name, rt)
		}
		if _, ok := rt["escalation"]; ok {
			t.Fatalf("%s: a family_effort rung is not an escalation: %v", tc.name, rt)
		}
		if _, ok := rt["skipped"]; ok {
			t.Fatalf("%s: nothing is skipped: %v", tc.name, rt)
		}
		if got := sel.req.route.effort(sel.chosen.row); got != "xhigh" {
			t.Fatalf("%s: reported effort must be the family rung: %s", tc.name, got)
		}
	}

	// Without the map the escalation rung stays gate_class and the lead falls
	// through to opus@high exactly as before.
	a, s, _ := familyEffortFixture(t, "")
	s.RoutingRole = "consult"
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if c := sel.chosen; c.seat.Canon != "opus" || c.row.Effort != "high" {
		t.Fatalf("no map: want opus@high, got %s@%s", c.seat.Canon, c.row.Effort)
	}
	rec := sel.record()
	if got := candOutcome(rec, "codex-sol", "xhigh")["reason"]; got != "gate_class" {
		t.Fatalf("no map: codex-sol@xhigh is gate_class: %v", got)
	}
	if got := candOutcome(rec, "codex-sol", "max")["reason"]; got != reasonRoleEffortUnplaced {
		t.Fatalf("no map: codex-sol@max is %s: %v", reasonRoleEffortUnplaced, got)
	}
	if _, ok := routingStamp(t, sel)["family_effort"]; ok {
		t.Fatalf("no map, no routing.family_effort")
	}

	// When the family's own rung is unplaced the family is skipped at that
	// rung's effort; it never climbs to max.
	a, s, q := familyEffortFixture(t, `{"codex-sol": "xhigh"}`)
	refuse := q.GateDo
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.Profile == "codex-sol" && r.Effort == "xhigh" {
			return "", fail(exitSeatRefused, "quota refused for codex-sol@xhigh")
		}
		return refuse(r)
	}
	s.RoutingRole = "consult"
	sel, e = a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	if c := sel.chosen; c.seat.Canon != "opus" || c.row.Effort != "high" {
		t.Fatalf("unplaced family rung: want opus@high, got %s@%s", c.seat.Canon, c.row.Effort)
	}
	skipped, _ := routingStamp(t, sel)["skipped"].([]map[string]any)
	if len(skipped) != 1 || skipped[0]["profile"] != "codex-sol" || skipped[0]["role_effort"] != "xhigh" || !strings.HasPrefix(skipped[0]["cause"].(string), "gate_refused") {
		t.Fatalf("codex-sol must be skipped at its family role effort: %v", skipped)
	}

	// --allow-escalation still climbs, and a max rung above the family rung
	// records the escalation against the family's role effort.
	a, s, _ = familyEffortFixture(t, `{"codex-sol": "xhigh"}`)
	q = a.Quota.(*fakeQuota)
	q.CatalogRows = q.CatalogRows[:len(q.CatalogRows)-1] // drop xhigh
	s.RoutingRole, s.AllowEscalation = "consult", true
	sel, e = a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	esc, _ := routingStamp(t, sel)["escalation"].(map[string]any)
	if sel.chosen.row.Effort != "max" || esc["role_effort"] != "xhigh" || esc["role_effort_cause"] != "no_role_effort_rung" {
		t.Fatalf("escalation above the family rung: chosen %s, %v", sel.chosen.row.Effort, esc)
	}
}

// Plan output names the family rung as the candidate and the policy map.
func TestPlanLeadFamilyEffort(t *testing.T) {
	a, _, q := familyEffortFixture(t, `{"codex-sol": "xhigh"}`)
	q.LaunchValue = Launch{Profile: "codex-sol", ModelID: "sol-m", Effort: "xhigh", Grade: "S+", Gate: "escalation", Pool: "codex"}
	out := captureOut(a)
	if e := a.Run(context.Background(), []string{"plan", "--t", "T1", "--role", "consult", "--json"}); e != nil {
		t.Fatalf("plan: %v\n%s", e, out)
	}
	var plan map[string]any
	if e := json.Unmarshal(out.Bytes(), &plan); e != nil {
		t.Fatalf("%v\n%s", e, out)
	}
	sel := plan["selection"].(map[string]any)
	if c := sel["chosen"].(map[string]any); c["canon"] != "codex-sol" || c["effort"] != "xhigh" {
		t.Fatalf("plan chose %v", c)
	}
	rt := sel["routing"].(map[string]any)
	if fe, _ := rt["family_effort"].(map[string]any); fe["codex-sol"] != "xhigh" {
		t.Fatalf("plan routing.family_effort: %v", rt)
	}
	if _, ok := rt["family_effort_chosen"]; !ok {
		t.Fatalf("plan routing.family_effort_chosen: %v", rt)
	}

	a, _, q = familyEffortFixture(t, `{"codex-sol": "xhigh"}`)
	q.LaunchValue = Launch{Profile: "codex-sol", ModelID: "sol-m", Effort: "xhigh", Grade: "S+", Gate: "escalation", Pool: "codex"}
	out = captureOut(a)
	if e := a.Run(context.Background(), []string{"plan", "--t", "T1", "--role", "consult"}); e != nil {
		t.Fatalf("plan: %v\n%s", e, out)
	}
	if !strings.Contains(out.String(), "family_effort codex-sol=xhigh chosen=codex-sol@xhigh gate=escalation\n") {
		t.Fatalf("text plan must explain the family rung:\n%s", out)
	}
}

// Acceptance 3: omp-sol runs codex-sol's model, so a codex-sol entry sets its
// role effort too; an omp-sol entry applies to omp-sol alone.
func TestLeadFamilyEffortOMPSol(t *testing.T) {
	for _, tc := range []struct {
		name, familyEffort, wantKey string
		codexXhigh                  string // codex-sol@xhigh's reason
	}{
		{"codex-sol key covers omp-sol", `{"codex-sol": "xhigh"}`, "codex-sol", ""},
		{"omp-sol key covers omp-sol only", `{"omp-sol": "xhigh"}`, "omp-sol", "gate_class"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sol := "openai/gpt-6.1-sol"
			rows := []CatalogRow{
				{Profile: "omp-sol", Effort: "high", Grade: "C", Pool: "openai-api", Gate: "default", ModelID: sol},
				{Profile: "omp-sol", Effort: "xhigh", Grade: "S+", Pool: "openai-api", Gate: "escalation", ModelID: sol},
				{Profile: "omp-sol", Effort: "max", Grade: "S+", Pool: "openai-api", Gate: "default", ModelID: sol},
				{Profile: "codex-sol", Effort: "high", Grade: "C", Pool: "codex", Gate: "default", ModelID: "sol-m"},
				{Profile: "codex-sol", Effort: "xhigh", Grade: "S+", Pool: "codex", Gate: "escalation", ModelID: "sol-m"},
			}
			pools := map[string]PoolView{"openai-api": {Class: "credit"}, "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}
			a, s, q := selFixture(t, "A", rows, pools, []string{"openai-api", "codex"})
			admit := gatePools(map[string]string{"omp-sol": "openai-api", "codex-sol": "codex"})
			q.GateDo = func(r QuotaRequest) (string, error) {
				if r.Effort == "high" {
					return "", fail(exitSeatRefused, "e6_arm_required: %s@high is an unplaced E6 seat", r.Profile)
				}
				if r.Profile == "codex-sol" {
					return "", fail(exitSeatRefused, "codex pool closed for this test")
				}
				return admit(r)
			}
			block := strings.Replace(consultFamilyEffortBlock(t, tc.familyEffort), `"allowed": ["opus", "fable", "codex-sol", "codex-astra"], "effort": "high", "on_exhaustion": "wait", "family`, `"allowed": ["*"], "effort": "high", "on_exhaustion": "wait", "family`, 1)
			serveRouting(t, routingDoc(block))
			s.RoutingRole = "consult"
			sel, e := a.selectProfile(context.Background(), s)
			if e != nil {
				t.Fatal(e)
			}
			if c := sel.chosen; c.seat.Canon != "omp-sol" || c.row.Effort != "xhigh" {
				t.Fatalf("want omp-sol@xhigh, got %s@%s", c.seat.Canon, c.row.Effort)
			}
			rec := sel.record()
			if got := candOutcome(rec, "omp-sol", "max")["reason"]; got != reasonAboveRoleEffort {
				t.Fatalf("omp-sol@max stays above the family rung: %v", got)
			}
			chosen, _ := routingStamp(t, sel)["family_effort_chosen"].(map[string]any)
			if chosen["profile"] != "omp-sol" || chosen["key"] != tc.wantKey {
				t.Fatalf("family_effort_chosen: %v", chosen)
			}
			if got, _ := candOutcome(rec, "codex-sol", "xhigh")["reason"].(string); tc.codexXhigh != "" && got != tc.codexXhigh {
				t.Fatalf("codex-sol@xhigh: want %s, got %v", tc.codexXhigh, got)
			}
		})
	}
}
