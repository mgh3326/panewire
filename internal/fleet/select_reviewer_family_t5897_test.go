package fleet

import (
	"context"
	"strings"
	"testing"
)

// Operator decision 462 (hk 5897): a reviewer of the author's own family is
// allowed; family is only a tie-break after band and quota room, and
// reviewer.family_rule "not_author" keeps the hard drop for rollback.

func reviewerBlock(rule string) string {
	repl := `"reviewer":    {"allowed": ["*"], "effort": "high", "on_exhaustion": "escalate"}`
	if rule != "" {
		repl = `"reviewer":    {"allowed": ["*"], "effort": "high", "on_exhaustion": "escalate", "family_rule": "` + rule + `"}`
	}
	return strings.Replace(routingBlockV1, `"reviewer":    {"allowed": ["*"], "effort": "high", "on_exhaustion": "escalate", "family_rule": "not_author"}`, repl, 1)
}

func reviewerRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "m"},
		{Profile: "codex-terra61", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "m"},
	}
}

func reviewerSelect(t *testing.T, rule, family string, claude, codex PoolView) (*selPlan, error) {
	t.Helper()
	a, s, q := selFixture(t, "S", reviewerRows(), map[string]PoolView{"claude": claude, "codex": codex}, []string{"claude", "codex"})
	serveRouting(t, routingDoc(reviewerBlock(rule)))
	s.Role, s.ParentFamily = "reviewer", family
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-terra61": "codex"})
	return a.selectProfile(context.Background(), s)
}

func spendPool(blocking float64) PoolView {
	return PoolView{Status: "ok", Class: "spend", Blocking: ptrF(blocking)}
}

// prefer_other keeps a same-family seat a candidate: Claude with the most room
// reviews a Claude-authored change. Fails on base, which drops it.
func TestReviewerSameFamilyChosenWithMostRoomT5897(t *testing.T) {
	for _, rule := range []string{"prefer_other", ""} {
		t.Run("rule="+rule, func(t *testing.T) {
			sel, e := reviewerSelect(t, rule, "anthropic", spendPool(5), spendPool(90))
			if e != nil || sel.chosen.seatName != "opus" {
				t.Fatalf("chosen=%v err=%v, want opus (same family, most room)", sel.chosen, e)
			}
			if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got == "dropped:family_excluded" {
				t.Fatalf("same-family seat dropped under %q", rule)
			}
		})
	}
}

// Equal band and room: the other family wins the tie. The pool names order
// claude before codex, so without the family key opus would win.
func TestReviewerOtherFamilyWinsTieT5897(t *testing.T) {
	sel, e := reviewerSelect(t, "prefer_other", "anthropic", spendPool(40), spendPool(40))
	if e != nil || sel.chosen.seatName != "codex-terra61" {
		t.Fatalf("chosen=%v err=%v, want codex-terra61 on the tie", sel.chosen, e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got == "dropped:family_excluded" {
		t.Fatalf("the same-family seat stays a candidate: %v", got)
	}
}

// The hard drop stays for rollback: even with the most room.
func TestReviewerNotAuthorStillDropsT5897(t *testing.T) {
	sel, e := reviewerSelect(t, "not_author", "anthropic", spendPool(5), spendPool(90))
	if e != nil || sel.chosen.seatName != "codex-terra61" {
		t.Fatalf("chosen=%v err=%v, want codex-terra61", sel.chosen, e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got != "dropped:family_excluded" {
		t.Fatalf("not_author must drop the author's family: %v", got)
	}
}

// An unreadable author family is "no preference" under prefer_other, recorded
// as degraded; not_author keeps dropping every seat with its own reason.
func TestReviewerUnreadableAuthorFamilyT5897(t *testing.T) {
	sel, e := reviewerSelect(t, "prefer_other", "", spendPool(5), spendPool(90))
	if e != nil || sel.chosen.seatName != "opus" {
		t.Fatalf("chosen=%v err=%v, want opus (no family preference)", sel.chosen, e)
	}
	deg, _ := sel.record()["degraded"].([]string)
	found := false
	for _, d := range deg {
		found = found || d == "author_family_unreadable"
	}
	if !found {
		t.Fatalf("author_family_unreadable not recorded: %v", sel.record()["degraded"])
	}
	_, e = reviewerSelect(t, "not_author", "", spendPool(5), spendPool(90))
	if e == nil {
		t.Fatal("not_author with an unreadable author family must still drop every seat")
	}
}

// An unknown reviewer family_rule is a config error that names the key; other
// roles' values (judge: not_parent) are not read.
func TestReviewerFamilyRuleUnknownIsConfigErrorT5897(t *testing.T) {
	_, e := parseRoutingJSON([]byte(reviewerBlock("anyone")))
	if e == nil || !strings.Contains(e.Error(), "family_rule") {
		t.Fatalf("err=%v, want a family_rule config error", e)
	}
	for _, rule := range []string{"", "prefer_other", "not_author"} {
		if _, e := parseRoutingJSON([]byte(reviewerBlock(rule))); e != nil {
			t.Fatalf("rule %q refused: %v", rule, e)
		}
	}
}

// The spawn admission applies the same rule: Check keeps the strict refusal;
// check with sameFamilyOK admits a same-family or unreadable-family reviewer.
func TestSeatCheckReviewerFamilyRuleT5897(t *testing.T) {
	p, e := LoadPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	s := p.Profiles["cmd-dsflash41"]
	l := Launch{ModelID: "provider/model", Effort: "high", Grade: "A+", Gate: "default"}
	for _, tc := range []struct {
		family string
		ok     bool
		want   int
	}{
		{"deepseek", false, exitSeatRefused}, {"deepseek", true, 0},
		{"", false, exitSeatRefused}, {"", true, 0},
		{"anthropic", false, 0}, {"anthropic", true, 0},
	} {
		if got := code(s.check(l, "reviewer", "B", tc.family, "", true, tc.ok)); got != tc.want {
			t.Errorf("family=%q sameFamilyOK=%v: code %d, want %d", tc.family, tc.ok, got, tc.want)
		}
	}
	if got := code(s.Check(l, "reviewer", "B", "deepseek", "", true)); got != exitSeatRefused {
		t.Errorf("Check must stay strict: %d", got)
	}
}

func TestReviewerSameFamilyOKT5897(t *testing.T) {
	for _, tc := range []struct {
		r    *routing
		want bool
	}{
		{nil, true},
		{&routing{}, true},
		{&routing{rule: &RoutingRule{}}, true},
		{&routing{rule: &RoutingRule{FamilyRule: "prefer_other"}}, true},
		{&routing{rule: &RoutingRule{FamilyRule: "not_author"}}, false},
	} {
		if got := tc.r.sameFamilyOK(); got != tc.want {
			t.Errorf("%+v: %v, want %v", tc.r, got, tc.want)
		}
	}
}

// The judge routing role maps to the reviewer seat role but does not read
// reviewer.family_rule: it keeps the hard drop of the parent's family, and an
// unreadable parent family still drops every seat, under any reviewer rule.
func TestJudgeKeepsFamilyDropUnderPreferOtherT5897(t *testing.T) {
	judgeSelect := func(family string) (*selPlan, error) {
		a, s, q := selFixture(t, "S", reviewerRows(), map[string]PoolView{"claude": spendPool(5), "codex": spendPool(90)}, []string{"claude", "codex"})
		serveRouting(t, routingDoc(reviewerBlock("prefer_other")))
		q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-terra61": "codex"})
		route, e := a.routeFor(context.Background(), "reviewer", "judge")
		if e != nil {
			t.Fatal(e)
		}
		s.Role, s.RoutingRole = "reviewer", "judge"
		req := selReq{grade: "S", t: "T1", role: "reviewer", authorFamily: family, urgency: urgencyNormal, urgencySource: "default", route: route}
		return a.selectFor(context.Background(), s, req)
	}
	sel, e := judgeSelect("anthropic")
	if e != nil || sel.chosen.seatName != "codex-terra61" {
		t.Fatalf("chosen=%v err=%v, want codex-terra61", sel.chosen, e)
	}
	if got := candOutcome(sel.record(), "opus", "high")["outcome"]; got != "dropped:family_excluded" {
		t.Fatalf("judge must drop the parent's family: %v", got)
	}
	if _, e := judgeSelect(""); e == nil {
		t.Fatal("judge with an unreadable parent family must drop every seat")
	}
}
