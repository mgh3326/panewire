package fleet

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"testing"
)

// panelEnv offers four seats of three families: cognition (free SWE-2),
// anthropic (Sonnet, and Opus on a credit pool), and openai.
func panelEnv(t *testing.T) *delegateEnv {
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "A+", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "m1"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "anthropic-api", Gate: "default", ModelID: "m2"},
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m3"},
	}
	pools := map[string]PoolView{
		"claude":        {Status: "ok", Class: "spend", Blocking: ptrF(10)},
		"anthropic-api": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
		"codex":         {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	e := newDelegateEnvWith(t, rows, map[string]string{"devin-swe2": "devin", "sonnet": "claude", "opus": "anthropic-api", "codex-sol": "codex"}, pools)
	e.results = map[string]map[string]any{}
	e.failures = map[string]error{}
	// A brand-new ledger races on its own creation when several members open it
	// at once; a host that has ever spawned has one.
	if _, err := e.a.ledger().AppendEvent(context.Background(), "ledger-warmup", "job.warmup", map[string]any{}, nil); err != nil {
		t.Fatal(err)
	}
	return e
}

func famOf(t *testing.T, e *delegateEnv, seat string) string {
	t.Helper()
	s, ok := e.a.Policy.Profiles[seat]
	if !ok {
		t.Fatalf("no seat %s", seat)
	}
	return s.Family
}

func runPanel(t *testing.T, e *delegateEnv, args ...string) (PanelResult, []string, error) {
	t.Helper()
	var seen []Spawn
	var chosen []string
	e.a.spawnFn = e.worker(t, "an answer", &seen, &chosen, nil)
	err := e.a.delegate(context.Background(), delegateArgs(e, append(args, "--json")...))
	var r PanelResult
	if err == nil {
		if je := json.Unmarshal([]byte(e.out()), &r); je != nil {
			t.Fatalf("--json prints one document: %v\n%s", je, e.out())
		}
	}
	return r, chosen, err
}

func families(t *testing.T, e *delegateEnv, r PanelResult) []string {
	var fs []string
	for _, m := range r.Members {
		fs = append(fs, m.Family)
	}
	sort.Strings(fs)
	return fs
}

func TestPanelPicksNDistinctFamilies(t *testing.T) {
	e := panelEnv(t)
	r, chosen, err := runPanel(t, e, "--role", "reader", "--panel", "3")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"anthropic", "cognition", "openai"}; !slices.Equal(families(t, e, r), want) {
		t.Fatalf("three members must be three distinct families: %v (seats %v)", families(t, e, r), chosen)
	}
	if r.Requested != 3 || r.Answered != 3 || r.Reason != "" || len(chosen) != 3 {
		t.Fatalf("%+v", r)
	}
	seenJobs := map[string]bool{}
	for _, m := range r.Members {
		if m.Status != "answered" || m.Answer != "an answer" || seenJobs[m.Job] || !strings.HasPrefix(m.Job, "delegate_") {
			t.Fatalf("each answer is its own delegate job: %+v", m)
		}
		seenJobs[m.Job] = true
		if rec := e.a.JobStore.(*fakeJobStore).record(t, m.Job); rec["state"] != "completed" {
			t.Fatalf("job %s record: %v", m.Job, rec)
		}
	}
	if len(seenJobs) != 3 {
		t.Fatalf("jobs: %v", seenJobs)
	}
}

func TestPanelFollowsPolicyNotFreeFirst(t *testing.T) {
	// A paid pool at waste risk outranks the free rung (the normal policy), so a
	// panel of 2 includes it and the second family is the free one.
	e := panelEnv(t)
	e.a.Quota.(rowQuota).fakeQuota.PoolViews["codex"] = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10), Projected: ptrF(40), Urgency: ptrF(1), ResetHours: ptrF(100), RankProjected: ptrF(40)}
	r, _, err := runPanel(t, e, "--role", "reader", "--panel", "2")
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Members[0].Family; got != "openai" {
		t.Fatalf("the waste-risk paid pool must lead, not the free rung: %v", families(t, e, r))
	}
	if !slices.Contains(families(t, e, r), "cognition") {
		t.Fatalf("the free family is still a candidate for the second seat: %v", families(t, e, r))
	}
}

func TestPanelReviewerExcludesCallerFamily(t *testing.T) {
	e := panelEnv(t)
	r, _, err := runPanel(t, e, "--role", "reviewer", "--panel", "3", "--family", "cognition")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"anthropic", "openai"}; !slices.Equal(families(t, e, r), want) || r.Answered != 2 || !strings.Contains(r.Reason, "2 of 3") {
		t.Fatalf("the caller's family must not review its own work, and the shortfall is explained: %v %q", families(t, e, r), r.Reason)
	}
}

func TestPanelFewerFamiliesThanRequestedReturnsWhatExists(t *testing.T) {
	e := panelEnv(t)
	r, _, err := runPanel(t, e, "--role", "reader", "--panel", "4")
	if err != nil {
		t.Fatalf("a short panel is not an error: %v", err)
	}
	if len(r.Members) != 3 || r.Requested != 4 || r.Answered != 3 {
		t.Fatalf("%+v", r)
	}
	if !strings.Contains(r.Reason, "3 of 4 distinct families") {
		t.Fatalf("the shortfall needs a reason: %q", r.Reason)
	}
	if !strings.Contains(e.errs(), "3 of 4 distinct families") {
		t.Fatalf("and a warning: %q", e.errs())
	}
}

func TestPanelCollectsAnswersWhenAMemberFailsOrTimesOut(t *testing.T) {
	for name, fail := range map[string]error{"timeout": fail(exitWaitTimeout, "wait timeout"), "lost": fail(exitWaitLost, "lost")} {
		t.Run(name, func(t *testing.T) {
			e := panelEnv(t)
			e.failures["anthropic"] = fail
			r, _, err := runPanel(t, e, "--role", "reader", "--panel", "3")
			if err != nil {
				t.Fatalf("one failed member must not fail the panel: %v", err)
			}
			if r.Answered != 2 || len(r.Members) != 3 {
				t.Fatalf("%+v", r)
			}
			for _, m := range r.Members {
				switch m.Family {
				case "anthropic":
					if m.Status != "failed" || m.Exit != code(fail) || m.Answer != "" || m.Error == "" {
						t.Fatalf("the failed member is reported with its exit code: %+v", m)
					}
				default:
					if m.Status != "answered" || m.Answer != "an answer" {
						t.Fatalf("the others are still collected: %+v", m)
					}
				}
			}
		})
	}
}

func TestPanelAllMembersFailedReturnsFirstFailure(t *testing.T) {
	e := panelEnv(t)
	for _, fam := range []string{"cognition", "anthropic", "openai"} {
		e.failures[fam] = fail(exitWaitTimeout, "wait timeout")
	}
	_, _, err := runPanel(t, e, "--role", "reader", "--panel", "3")
	if code(err) != exitWaitTimeout || strings.TrimSpace(e.out()) != "" {
		t.Fatalf("with no answer at all the panel fails with the member's status: %v out=%q", err, e.out())
	}
}

func TestPanelNothingAdmittedKeepsSelectionError(t *testing.T) {
	e := panelEnv(t)
	e.a.Quota.(rowQuota).fakeQuota.CatalogRows = nil
	_, _, err := runPanel(t, e, "--role", "reader", "--panel", "2")
	if err == nil {
		t.Fatal("no admitted family at all must fail")
	}
}

func TestPanelRangeRefused(t *testing.T) {
	e := panelEnv(t)
	for _, n := range []string{"1", "5", "-2"} {
		if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--panel", n)); code(err) != exitUsage {
			t.Errorf("--panel %s must be refused: %v", n, err)
		}
	}
}

func TestPanelHeadlessRunsEveryFamilyInParallelJobs(t *testing.T) {
	e := panelEnv(t)
	base := e.runner.do
	e.runner.do = func(c Command) Result {
		switch c.Name {
		case "devin", "claude", "codex":
			return Result{Out: "answer from " + c.Name + "\n"}
		}
		return base(c)
	}
	e.a.spawnFn = func(context.Context, *App, Spawn) error { t.Fatal("--headless spawns no pane"); return nil }
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--panel", "3", "--headless", "--json")); err != nil {
		t.Fatal(err)
	}
	var r PanelResult
	if err := json.Unmarshal([]byte(e.out()), &r); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, m := range r.Members {
		got[m.Answer] = true
		if m.Mode != "headless" || m.Status != "answered" {
			t.Fatalf("%+v", m)
		}
	}
	if len(got) != 3 || !got["answer from devin"] || !got["answer from claude"] || !got["answer from codex"] {
		t.Fatalf("each family's CLI ran once: %v", got)
	}
}

func TestPanelTextEndsWithTheSideBySideTable(t *testing.T) {
	e := panelEnv(t)
	e.results["cognition"] = map[string]any{"answer": "A", "verdict": "ok", "points": []any{"retry loop in pkg/util.go:12 backs off exponentially", "docs omit the timeout"}}
	e.results["anthropic"] = map[string]any{"answer": "B", "verdict": "issues", "points": []any{"the retry loop in pkg/util.go:12 backs off exponentially", "pkg/util.go:30 leaks a file handle"}}
	e.results["openai"] = map[string]any{"answer": "C", "verdict": "ok", "points": []any{"pkg/util.go:30 closes the handle correctly"}}
	var seen []Spawn
	var chosen []string
	e.a.spawnFn = e.worker(t, "x", &seen, &chosen, nil)
	if err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "reader", "--panel", "3")); err != nil {
		t.Fatal(err)
	}
	out := e.out()
	for _, w := range []string{"panel role=reader mode=pane requested=3 answered=3", "== A job=", "== B job=", "== C job=", "== side by side ==", "agreements (1)", "disagreements (2)", "unique (1)", "verdict | ", "pkg/util.go:30 |", "docs omit the timeout"} {
		if !strings.Contains(out, w) {
			t.Fatalf("missing %q in:\n%s", w, out)
		}
	}
	if i, j := strings.Index(out, "== C job="), strings.Index(out, "== side by side =="); i > j {
		t.Fatalf("the table comes after every answer:\n%s", out)
	}
	if strings.Contains(out, "wait job=") {
		t.Fatalf("spawn chatter stays on stderr:\n%s", out)
	}
}

func member(label, status string, result map[string]any, answer string) PanelMember {
	return PanelMember{Label: label, Status: status, DelegateResult: DelegateResult{Answer: answer, Result: result}}
}

func TestPanelTableAgreementsDisagreementsUnique(t *testing.T) {
	tb := panelTable([]PanelMember{
		member("A", "answered", map[string]any{"verdict": "ok", "points": []any{"The cache key ignores the tenant id", "Tests cover only the happy path", "see cfg/app.go:10 default is 30s"}}, ""),
		member("B", "answered", map[string]any{"verdict": "ok", "points": []any{"cache key ignores tenant id entirely", "cfg/app.go:10 default is 60s"}}, ""),
		member("C", "failed", nil, ""),
	})
	if len(tb.Agreements) != 1 || !slices.Equal(tb.Agreements[0].Members, []string{"A", "B"}) || !strings.Contains(tb.Agreements[0].Text, "tenant") {
		t.Fatalf("agreements: %+v", tb.Agreements)
	}
	if len(tb.Disagreements) != 1 || tb.Disagreements[0].Where != "cfg/app.go:10" || len(tb.Disagreements[0].Claims) != 2 {
		t.Fatalf("same place, different claim is a disagreement (equal verdicts are not): %+v", tb.Disagreements)
	}
	if len(tb.Unique) != 1 || !slices.Equal(tb.Unique[0].Members, []string{"A"}) || !strings.Contains(tb.Unique[0].Text, "happy path") {
		t.Fatalf("a disputed point is not also unique: %+v", tb.Unique)
	}
	if _, failed := tb.Verdicts["C"]; failed {
		t.Fatal("a failed member has no verdict in the table")
	}
}

func TestPanelTableDifferingVerdictsAndAnswerOnlyMembers(t *testing.T) {
	tb := panelTable([]PanelMember{
		member("A", "answered", map[string]any{"verdict": "pass"}, "- the build is green on every target\n- no flaky tests were seen"),
		member("B", "answered", map[string]any{"verdict": "fail"}, "The build is green on every target.\nOne linux target skipped"),
	})
	if len(tb.Disagreements) != 1 || tb.Disagreements[0].Where != "verdict" || tb.Disagreements[0].Claims["B"] != "fail" {
		t.Fatalf("differing verdicts are a disagreement: %+v", tb.Disagreements)
	}
	if len(tb.Agreements) != 1 || len(tb.Unique) != 2 {
		t.Fatalf("answers without points compare line by line: %+v", tb)
	}
}
