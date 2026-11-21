package fleet

import (
	"context"
	"strings"
	"testing"
)

// Operator decision 467 (hk 5917): a consult of the asker's own family is
// allowed; family is only a tie-break after band and quota room, and
// consult.family_rule "not_author" keeps the hard drop for rollback. The
// reviewer role (decision 462) shares the helper; judge keeps its drop.

// consultBlock is routingBlockV1 with the consult role's family_rule set to
// rule ("" leaves it unset).
func consultBlock(rule string) string {
	old := `"consult":     {"allowed": ["opus", "fable", "codex-sol", "codex-astra"], "effort": "high", "on_exhaustion": "wait", "family_rule": "not_author"}`
	repl := strings.Replace(old, `, "family_rule": "not_author"`, "", 1)
	if rule != "" {
		repl = strings.Replace(old, "not_author", rule, 1)
	}
	if !strings.Contains(routingBlockV1, old) {
		panic("routingBlockV1 consult rule moved; update consultBlock")
	}
	return strings.Replace(routingBlockV1, old, repl, 1)
}

func consultFamilyRows() []CatalogRow {
	return []CatalogRow{consultFloorRung("opus", "high", "S+", "claude"), consultFloorRung("codex-sol", "high", "S+", "codex")}
}

func consultFamilySelect(t *testing.T, rule, family string, claude, codex PoolView, exclude ...string) (*selPlan, error) {
	t.Helper()
	o := consultFloorOpts{block: consultBlock(rule), pools: map[string]PoolView{"claude": claude, "codex": codex}, excludeFamilies: exclude}
	return consultFloorSelectWith(t, o, consultFamilyRows(), consultFloorBothPools, "S", family)
}

// prefer_other keeps a same-family consult seat a candidate: Claude with the
// most room serves a Claude asker. Fails on base, which drops it.
func TestConsultSameFamilyChosenWithMostRoomT5917(t *testing.T) {
	for _, rule := range []string{"prefer_other", ""} {
		t.Run("rule="+rule, func(t *testing.T) {
			sel, e := consultFamilySelect(t, rule, "anthropic", spendPool(5), spendPool(90))
			if e != nil || sel.chosen.seat.Canon != "opus" {
				t.Fatalf("chosen=%v err=%v, want opus (same family, most room)", sel.chosen, e)
			}
			if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got == "dropped:family_excluded" {
				t.Fatalf("same-family consult seat dropped under %q", rule)
			}
		})
	}
}

// Equal band and room: the other family wins the tie. Without the family key
// the claude pool's order and the top-profile rank would pick opus.
func TestConsultOtherFamilyWinsTieT5917(t *testing.T) {
	sel, e := consultFamilySelect(t, "prefer_other", "anthropic", spendPool(40), spendPool(40))
	if e != nil || sel.chosen.seat.Canon != "codex-sol" {
		t.Fatalf("chosen=%v err=%v, want codex-sol on the tie", sel.chosen, e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got == "dropped:family_excluded" {
		t.Fatalf("the same-family seat stays a candidate: %v", got)
	}
}

// The hard drop stays for rollback: even with the most room.
func TestConsultNotAuthorStillDropsT5917(t *testing.T) {
	sel, e := consultFamilySelect(t, "not_author", "anthropic", spendPool(5), spendPool(90))
	if e != nil || sel.chosen.seat.Canon != "codex-sol" {
		t.Fatalf("chosen=%v err=%v, want codex-sol", sel.chosen, e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["reason"]; got != "family_excluded" {
		t.Fatalf("not_author must drop the asker's family: %v", got)
	}
}

// An explicit --exclude-family is the caller's choice and stays a hard drop
// under prefer_other.
func TestConsultExcludeFamilyStillDropsT5917(t *testing.T) {
	sel, e := consultFamilySelect(t, "prefer_other", "anthropic", spendPool(5), spendPool(90), "anthropic")
	if e != nil || sel.chosen.seat.Canon != "codex-sol" {
		t.Fatalf("chosen=%v err=%v, want codex-sol", sel.chosen, e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["reason"]; got != "family_excluded" {
		t.Fatalf("--exclude-family must drop: %v", got)
	}
}

// Judge keeps its hard drop of the parent's family when the consult and the
// reviewer rules are both prefer_other.
func TestJudgeKeepsFamilyDropUnderConsultPreferOtherT5917(t *testing.T) {
	block := strings.Replace(consultBlock("prefer_other"), `"family_rule": "not_author"`, `"family_rule": "prefer_other"`, 1)
	a, s, q := selFixture(t, "S", reviewerRows(), map[string]PoolView{"claude": spendPool(5), "codex": spendPool(90)}, []string{"claude", "codex"})
	serveRouting(t, routingDoc(block))
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-terra61": "codex"})
	route, e := a.routeFor(context.Background(), "reviewer", "judge")
	if e != nil {
		t.Fatal(e)
	}
	s.Role, s.RoutingRole = "reviewer", "judge"
	req := selReq{grade: "S", t: "T1", role: "reviewer", authorFamily: "anthropic", urgency: urgencyNormal, urgencySource: "default", route: route}
	sel, e := a.selectFor(context.Background(), s, req)
	if e != nil || sel.chosen.seatName != "codex-terra61" {
		t.Fatalf("chosen=%v err=%v, want codex-terra61", sel.chosen, e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got != "dropped:family_excluded" {
		t.Fatalf("judge must drop the parent's family: %v", got)
	}
}

// An unknown consult family_rule is a config error naming the key.
func TestConsultFamilyRuleValidationT5917(t *testing.T) {
	if _, e := parseRoutingJSON([]byte(consultBlock("anyone"))); e == nil || !strings.Contains(e.Error(), "family_rule") {
		t.Fatalf("err=%v, want a family_rule config error", e)
	}
	for _, rule := range []string{"", "prefer_other", "not_author"} {
		if _, e := parseRoutingJSON([]byte(consultBlock(rule))); e != nil {
			t.Fatalf("rule %q refused: %v", rule, e)
		}
	}
}

func TestSameFamilyOKConsultT5917(t *testing.T) {
	for _, tc := range []struct {
		r    *routing
		want bool
	}{
		{&routing{role: "consult"}, true},
		{&routing{role: "consult", rule: &RoutingRule{}}, true},
		{&routing{role: "consult", rule: &RoutingRule{FamilyRule: "prefer_other"}}, true},
		{&routing{role: "consult", rule: &RoutingRule{FamilyRule: "not_author"}}, false},
		{&routing{role: "judge", rule: &RoutingRule{FamilyRule: "prefer_other"}}, false},
	} {
		if got := tc.r.sameFamilyOK(); got != tc.want {
			t.Errorf("%+v: %v, want %v", tc.r, got, tc.want)
		}
	}
}
