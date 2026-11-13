package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestUnratedCatalogDecode(t *testing.T) {
	raw := []json.RawMessage{json.RawMessage(`{"profile":"codex-sol","effort":"high","model_id":"sol-m","pool":"codex","grade":"unrated","gate":"default"}`)}
	rows, _, _, err := decodeCatalogRows(raw)
	if err != nil || len(rows) != 1 || rows[0].Grade != "unrated" || rows[0].row().Grade != "unrated" {
		t.Fatalf("unrated catalog: rows=%+v err=%v", rows, err)
	}
}

func TestUnratedGradedSelection(t *testing.T) {
	for _, grade := range []string{"C", "B", "A", "A+", "S", "S+", "SS"} {
		t.Run(grade, func(t *testing.T) {
			a, s, q := consultFixture(t)
			coveredGrade := "S+"
			if grade == "SS" {
				coveredGrade = "SS"
			}
			q.CatalogRows = []CatalogRow{
				{Profile: "codex-sol", Effort: "high", Grade: "unrated", Pool: "codex", Gate: "default", ModelID: "sol-m"},
				{Profile: "sonnet", Effort: "high", Grade: coveredGrade, Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
			}
			s.Grade = grade
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil || sel.chosen.row.Grade == "unrated" {
				t.Fatalf("graded selection: sel=%+v err=%v", sel, err)
			}
			if got := candOutcome(sel.record(), "codex-sol", "high"); got["grade"] != "unrated" || got["reason"] != "grade_unrated" {
				t.Fatalf("unrated skip reason: %v", got)
			}
			if gateAsked(q, "codex-sol", "high") {
				t.Fatal("graded selection asked an unrated rung's quota gate")
			}
		})
	}
}

func TestUnratedLeadConsultResolve(t *testing.T) {
	for _, role := range []string{"consult"} {
		t.Run(role, func(t *testing.T) {
			a, _, q := consultFixture(t)
			q.CatalogRows = []CatalogRow{{Profile: "codex-sol", Effort: "high", Grade: "unrated", Pool: "codex", Gate: "default", ModelID: "sol-m"}}
			err := a.resolveCmd(context.Background(), []string{"--role", role, "--json"})
			if exitCode(err) != exitSeatRefused || !strings.HasPrefix(err.Error(), reasonRoleNoCandidate) {
				t.Fatalf("unrated %s resolve must refuse, never choose the unrated rung: %v", role, err)
			}
		})
	}
}

func TestUnratedSeatCheck(t *testing.T) {
	s := Seat{Canon: "codex-sol", Kind: "codex", Family: "openai", Capability: "standard", Roles: []string{"consult", "implementer", "reviewer"}}
	l := Launch{Profile: "codex-sol", ModelID: "sol-m", Effort: "high", Grade: "unrated", Gate: "default"}
	for _, tc := range []struct {
		role, grade string
		want        int
	}{{"consult", "", 0}, {"implementer", "C", exitSeatRefused}, {"reviewer", "C", exitSeatRefused}} {
		if got := code(s.Check(l, tc.role, tc.grade, "anthropic", "", nil)); got != tc.want {
			t.Errorf("%s grade=%q: exit=%d want=%d", tc.role, tc.grade, got, tc.want)
		}
	}
	l.Grade = "unknown"
	if got := code(s.Check(l, "consult", "", "", "", nil)); got != exitConfig {
		t.Fatalf("unknown grade accepted for consult: exit=%d", got)
	}
}

func TestUnratedPlanOutput(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		for _, top := range []bool{false, true} {
			a, q, _, _, out := planFixture(t)
			q.CatalogRows = append(q.CatalogRows, CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "unrated", Pool: "codex", Gate: "default", ModelID: "sol-m"})
			args := []string{"--grade", "A", "--t", "T1"}
			if top {
				q.CatalogRows = []CatalogRow{{Profile: "opus", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "opus-m"}, {Profile: "codex-sol", Effort: "high", Grade: "unrated", Pool: "codex", Gate: "default", ModelID: "sol-m"}}
				q.GateDo = gatePools(map[string]string{"opus": "claude"})
				serveRouting(t, routingDocV1)
				args = []string{"--role", "consult", "--t", "T1"}
			}
			if asJSON {
				args = append(args, "--json")
			}
			if err := a.planCmd(context.Background(), args); err != nil {
				t.Fatalf("top=%t json=%t: %v\n%s", top, asJSON, err, out.String())
			}
			if asJSON {
				if !strings.Contains(out.String(), `"grade": "unrated"`) {
					t.Fatalf("plan JSON omitted unrated: %s", out.String())
				}
			} else if !strings.Contains(out.String(), "grade=unrated") {
				t.Fatalf("plan text omitted unrated: %s", out.String())
			}
		}
	}
}
