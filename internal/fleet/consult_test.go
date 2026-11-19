package fleet

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

// The consult seat for an A task is A+ in another family; the exclusion used
// by a respawn drops the failed profile and picks the next eligible seat.
func TestConsultSeatExcludingSkipsFailedProfile(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex"), consultFloorRung("codex-astra", "high", "SS", "codex")}
	a, _, q := selFixture(t, "A", rows, map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}, consultFloorBothPools)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDoc(routingBlockV1))

	seat, err := a.ConsultSeatExcluding(context.Background(), "A", "", "anthropic", "T1", nil)
	if err != nil {
		t.Fatalf("seat: %v", err)
	}
	if seat.Family != "openai" || seat.Grade != "A+" {
		t.Fatalf("seat = %s/%s, want an A+ openai seat", seat.Profile, seat.Grade)
	}
	next, err := a.ConsultSeatExcluding(context.Background(), "A", "", "anthropic", "T1", []string{seat.Profile})
	if err != nil {
		t.Fatalf("next seat: %v", err)
	}
	if next.Profile == seat.Profile || next.Family != "openai" {
		t.Fatalf("respawn seat = %s, want another openai seat than %s", next.Profile, seat.Profile)
	}
	if _, err := a.ConsultSeatExcluding(context.Background(), "A", "", "anthropic", "T1", []string{"codex-sol", "codex-astra"}); err == nil {
		t.Fatal("excluding every eligible seat must refuse")
	}
}

// PlanConsult pins the resolved seat, builds the checkout of the requested
// head and carries the fixed preamble plus the question document.
func TestPlanConsultBuildsPinnedSubmission(t *testing.T) {
	rows := []CatalogRow{consultFloorRung("opus", "high", "A+", "claude"), consultFloorRung("codex-sol", "high", "A+", "codex"), consultFloorRung("codex-astra", "high", "SS", "codex")}
	a, _, q := selFixture(t, "A", rows, map[string]PoolView{"claude": riskPool(30, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(40)}}, consultFloorBothPools)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex", "codex-astra": "codex"})
	serveRouting(t, routingDoc(routingBlockV1))
	useFakeHK(t).SetDoc("design/consult-q", "## Question\nWhy did the round-3 findings happen?")

	head := strings.Repeat("b", 40)
	repo := &Repository{
		Name: "project", Origin: "https://github.com/example/project.git", LocalRoot: "/work",
		Worktree: "author-checkout",
		Checkout: &HubCheckout{Origin: "https://github.com/example/project.git", Mode: "branch", Branch: "wk/x", Sha: strings.Repeat("a", 40), Dir: "author-checkout"},
	}
	plan, err := a.PlanConsult(context.Background(), ConsultPlanInput{
		Task: "42", Question: "hk:doc/design/consult-q", AuthorFamily: "anthropic",
		PR: "example/project#7", Head: head, Requester: "mergewake/example/project#7",
		Owner: "operator-desk", Repository: repo,
	})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if plan.Seat.Family != "openai" || plan.Seat.Grade != "A+" {
		t.Fatalf("seat = %s/%s, want an A+ openai seat", plan.Seat.Profile, plan.Seat.Grade)
	}
	if !strings.HasPrefix(plan.Job, "consult-42-") {
		t.Fatalf("job = %s, want consult-42-*", plan.Job)
	}
	checkout := plan.Request.Repository.Checkout
	if checkout == nil || checkout.Mode != "detached" || checkout.Sha != head || checkout.Fetch != "pull/7/head" || checkout.Dir != plan.Job+"-checkout" {
		t.Fatalf("checkout = %+v", checkout)
	}
	if !strings.HasPrefix(plan.Request.Brief, consultPreamble) || !strings.Contains(plan.Request.Brief, "Why did the round-3 findings happen?") {
		t.Fatalf("brief = %q", plan.Request.Brief)
	}
	args := strings.Join(plan.Request.Args, " ")
	for _, want := range []string{"--role consult", "--task 42", "--containment hk:42", "-m " + plan.Seat.Profile, "--author-family anthropic", "--owner operator-desk"} {
		if !strings.Contains(args, want) {
			t.Fatalf("args %q missing %q", args, want)
		}
	}
	if _, err := ValidateSubmission(plan.Request); err != nil {
		t.Fatalf("submission: %v", err)
	}
}

func TestConsultAnswerProblem(t *testing.T) {
	valid := "## Verdict\napply\n\n## Root cause\nOne wrong point.\n\n## Design\nOne design.\n\n## Findings\n| finding id | action | reason |\n| --- | --- | --- |\n| F1 | fix | real |\n"
	if problem := ConsultAnswerProblem(valid); problem != "" {
		t.Fatalf("valid report refused: %s", problem)
	}
	for name, body := range map[string]string{
		"missing root cause": "## Verdict\napply\n\n## Design\nx\n\n## Findings\n| a | b |\n",
		"bad verdict":        "## Verdict\nmaybe\n\n## Root cause\nx\n\n## Design\nx\n\n## Findings\n| a | b |\n",
		"no table":           "## Verdict\napply\n\n## Root cause\nx\n\n## Design\nx\n\n## Findings\nprose\n",
	} {
		if problem := ConsultAnswerProblem(body); problem == "" {
			t.Fatalf("%s: accepted", name)
		}
	}
}

// A question document over the brief bound is not a refused consult (hk 5521:
// panewire#687's 16-head question was refused "exceeds the consult brief
// bound" and the author got no consult). The brief carries the first part,
// names the key, and stays inside the bound.
func TestConsultBriefKeepsAnOversizedQuestionInsideTheBound(t *testing.T) {
	useFakeHK(t).SetDoc("design/big-q", "## Question\n"+strings.Repeat("finding F1 is open on x.go:3 é\n", 4000))
	a := &App{}
	brief, err := a.consultBrief(context.Background(), ConsultPlanInput{}, "design/big-q")
	if err != nil {
		t.Fatalf("an oversized question must not refuse the consult: %v", err)
	}
	if len(brief) > hubMaxBriefBytes || !utf8.ValidString(brief) {
		t.Fatalf("brief is %d bytes (bound %d), valid UTF-8 %v", len(brief), hubMaxBriefBytes, utf8.ValidString(brief))
	}
	for _, want := range []string{consultPreamble, "hk:doc/design/big-q", "## Question", "handoffkeep doc get design/big-q"} {
		if !strings.Contains(brief, want) {
			t.Fatalf("brief lacks %q", want)
		}
	}
	small := &App{}
	useFakeHK(t).SetDoc("design/small-q", "## Question\nshort")
	if b, err := small.consultBrief(context.Background(), ConsultPlanInput{}, "design/small-q"); err != nil || strings.Contains(b, "brief bound") {
		t.Fatalf("a question inside the bound is sent whole: %v", err)
	}
}

// A configured pool-order file that is missing refuses selection (a consult
// included) and says which setting names it (hk 5521).
func TestPoolPolicyMissingFileNamesTheSetting(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "pool-order.json")
	_, err := LoadPoolPolicy(missing)
	if err == nil || !strings.Contains(err.Error(), "[local].pool_policy") || !strings.Contains(err.Error(), missing) || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err=%v", err)
	}
	t.Setenv("PANEWIRE_POOL_POLICY", missing)
	if _, err := LoadPoolPolicy(""); err == nil || !strings.Contains(err.Error(), "PANEWIRE_POOL_POLICY") {
		t.Fatalf("err=%v", err)
	}
}

// simulate's --policy names its own setting in the error, not a config key
// the run never read (mergewake review of #750).
func TestPoolPolicyMissingFlagFileNamesTheFlag(t *testing.T) {
	_, err := loadPoolPolicyNamedBy(filepath.Join(t.TempDir(), "p.json"), "--policy")
	if err == nil || !strings.Contains(err.Error(), "--policy") || strings.Contains(err.Error(), "[local].pool_policy") {
		t.Fatalf("err=%v", err)
	}
}
