package fleet

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

// A task-derived end waits while the job's worktree is on an open PR (hk
// 3401): one task can own more than one PR, so a completed task does not prove
// the pane's work landed.

const paneSweepPRHead = "7bda62550eb5d8146632aa5e7ade839f0f58e036"

func TestNewPRReaderReadsTheTasksChanges(t *testing.T) {
	var path, query, auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, query, auth = r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")
		io.WriteString(w, `{"changes":[{"pr_number":306,"state":"open","head_sha":"`+paneSweepPRHead+`"},{"pr_number":300,"state":"merged","head_sha":"abc"}]}`)
	}))
	defer srv.Close()
	read, err := NewPRReader(srv.URL, "token-1", "", "")
	if err != nil {
		t.Fatal(err)
	}
	changes, err := read(context.Background(), "3389")
	if err != nil {
		t.Fatal(err)
	}
	if path != "/v1/changes" || query != "task=3389" || auth != "Bearer token-1" {
		t.Fatalf("request path=%q query=%q auth=%q", path, query, auth)
	}
	want := []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}, {Number: 300, State: "merged", Head: "abc"}}
	if !slices.Equal(changes, want) {
		t.Fatalf("changes = %v, want %v", changes, want)
	}
	if _, err := read(context.Background(), "not-a-task"); err == nil {
		t.Fatal("a non-numeric task was accepted")
	}
}

// (a) The task is deployed and the job's branch has an open PR: kept.
func TestPaneSweepKeepsAPaneWhoseBranchHasAnOpenPR(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	f.sweptJob("j", "42", cwd)
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	g.head = paneSweepPRHead
	f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed although its branch has an open PR")
	}
	if !f.infoHas("task pane kept", "job=j", "task=42", "task_state=deployed", "reason=open_pr", "pr=306") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// The branch's tip on origin is the PR head hk records, so local commits that
// are not pushed yet do not hide the open PR.
func TestPaneSweepKeepsAPaneWhoseBranchOriginTipIsAnOpenPR(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	f.sweptJob("j", "42", cwd)
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	g.head = "0123456789abcdef0123456789abcdef01234567"
	g.originTip = paneSweepPRHead
	f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed although its branch's origin tip has an open PR")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=open_pr", "pr=306") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// The clone's cached refs/remotes/origin/<branch> is older than the PR head,
// which was pushed from another checkout: origin itself names the open PR, so
// the pane is kept (mergewake review of #310).
func TestPaneSweepKeepsAPaneWhoseCachedOriginRefIsStale(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	f.sweptJob("j", "42", cwd)
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	stale := "0123456789abcdef0123456789abcdef01234567"
	g.head, g.originTip, g.remoteTip = stale, stale, paneSweepPRHead
	f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed although origin's branch tip is an open PR head")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=open_pr", "pr=306") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// origin cannot be read, so the branch's PR head is unknown: the pane is kept.
func TestPaneSweepKeepsAPaneWhenOriginCannotBeRead(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	f.sweptJob("j", "42", cwd)
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	g.head = "0123456789abcdef0123456789abcdef01234567"
	g.lsRemoteErr = true
	f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed although origin could not be read")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=pr_state_unknown") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A branch that never reached origin is identified by HEAD alone: with its PRs
// merged the pane closes.
func TestPaneSweepClosesAPaneWhoseBranchIsNotOnOrigin(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	pane := f.sweptJob("j", "42", cwd)
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	g.head, g.remoteTip = "0123456789abcdef0123456789abcdef01234567", "none"
	f.prChanges["42"] = []PRChange{{Number: 300, State: "merged", Head: paneSweepPRHead}}
	f.at(0)
	f.at(11)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("closed %v, want [%s]", f.closed(), pane)
	}
}

// (b) The PR is merged: the pane closes as before.
func TestPaneSweepClosesAPaneWhoseTaskEndedAndPRsAreMerged(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	pane := f.sweptJob("j", "42", cwd)
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	g.head = paneSweepPRHead
	f.prChanges["42"] = []PRChange{{Number: 300, State: "merged", Head: paneSweepPRHead}}
	f.at(0)
	f.at(11)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("closed %v, want [%s]", f.closed(), pane)
	}
	if payload := f.reaped("j"); payload["reason"] != "task_deployed" {
		t.Fatalf("job.reaped payload = %v", payload)
	}
}

// (c) A detached reviewer worktree sits at an open PR's head, the task is
// deployed and the job has no terminal record: kept.
func TestPaneSweepKeepsADetachedReviewerAtAnOpenPRHead(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "mwr-306")
	f.job("mwr", "worker", "42", map[string]any{"cwd": cwd, "role": "reviewer"})
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	g.detached, g.head = true, paneSweepPRHead
	f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("mwr") != nil {
		t.Fatalf("a detached reviewer at an open PR head was closed")
	}
	if !f.infoHas("task pane kept", "job=mwr", "reason=open_pr", "pr=306") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// (d) The PR lookup fails: the pane is kept and the line says why.
func TestPaneSweepKeepsAPaneWhenThePRLookupFails(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "j")
	f.sweptJob("j", "42", cwd)
	f.tasks["42"], f.category["42"] = "deployed", "completed"
	f.prErr = errors.New("hk down")
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed although the PR state is unknown")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=pr_state_unknown") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// (e) A worker whose own job.completed (READY) ended it stays its PR's author
// (the wake method): while its worktree is on an open PR its pane is kept,
// whether the task is still in review or has ended (hk 3734). The hk 3383
// expectation, that such a job is swept whatever its PRs say, is replaced.
func TestPaneSweepKeepsACompletedAuthorWithAnOpenPR(t *testing.T) {
	for _, tc := range []struct{ state, category string }{{"reviewing", "started"}, {"deployed", "completed"}} {
		t.Run(tc.state, func(t *testing.T) {
			g := newSweepGitFake(t)
			f := newPaneSweepFixture(t)
			f.withGit(g)
			cwd := g.worktree(t, "j")
			f.sweptJob("j", "42", cwd)
			f.tasks["42"], f.category["42"] = tc.state, tc.category
			g.head = paneSweepPRHead
			f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
			putEvent(t, f.a, "j", "job.completed", map[string]any{})
			f.at(0)
			f.at(11)
			if len(f.closed()) != 0 || f.reaped("j") != nil {
				t.Fatalf("a completed author's pane was closed while its PR is open (closed %v)", f.closed())
			}
			if !f.infoHas("task pane kept", "job=j", "task=42", "task_state="+tc.state, "reason=open_pr", "pr=306") {
				t.Fatalf("info lines = %q", f.info)
			}
		})
	}
}

// A completed author whose PR state cannot be read is kept: hk's change list
// fails, or the task has an open PR and origin cannot say whether it is the
// worktree's.
func TestPaneSweepKeepsACompletedAuthorWhenPRStateIsUnknown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(f *paneSweepFixture, g *sweepGitFake)
	}{
		{"change list unreadable", func(f *paneSweepFixture, _ *sweepGitFake) { f.prErr = errors.New("hk down") }},
		{"origin unreadable", func(f *paneSweepFixture, g *sweepGitFake) {
			g.head, g.lsRemoteErr = paneSweepTestHead, true
			f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newSweepGitFake(t)
			f := newPaneSweepFixture(t)
			f.withGit(g)
			cwd := g.worktree(t, "j")
			f.sweptJob("j", "42", cwd)
			f.tasks["42"], f.category["42"] = "reviewing", "started"
			tc.break_(f, g)
			putEvent(t, f.a, "j", "job.completed", map[string]any{})
			f.at(0)
			f.at(11)
			if len(f.closed()) != 0 || f.reaped("j") != nil {
				t.Fatalf("a completed author's pane was closed although its PR state is unknown")
			}
			if !f.infoHas("task pane kept", "job=j", "reason=pr_state_unknown") {
				t.Fatalf("info lines = %q", f.info)
			}
		})
	}
}

// A completed author with no open PR left (merged or closed) is swept as
// before, by its own record.
func TestPaneSweepClosesACompletedAuthorWithNoOpenPR(t *testing.T) {
	for _, state := range []string{"merged", "closed"} {
		t.Run(state, func(t *testing.T) {
			g := newSweepGitFake(t)
			f := newPaneSweepFixture(t)
			f.withGit(g)
			cwd := g.worktree(t, "j")
			pane := f.sweptJob("j", "42", cwd)
			f.tasks["42"], f.category["42"] = "reviewing", "started"
			g.head = paneSweepPRHead
			f.prChanges["42"] = []PRChange{{Number: 306, State: state, Head: paneSweepPRHead}}
			putEvent(t, f.a, "j", "job.completed", map[string]any{})
			f.at(0)
			f.at(11)
			if !slices.Equal(f.closed(), []string{pane}) {
				t.Fatalf("closed %v, want [%s]", f.closed(), pane)
			}
			if payload := f.reaped("j"); payload["reason"] != "job_completed" {
				t.Fatalf("job.reaped payload = %v", payload)
			}
		})
	}
}

// A job its owner closed (panewire job close: abandoned, superseded, or
// released as completed) is swept even with an open PR, without a PR read:
// hk 3383's rule still holds for a job that was given up or released.
func TestPaneSweepClosesAGivenUpJobEvenWithAnOpenPR(t *testing.T) {
	for _, outcome := range []string{"abandoned", "superseded", "completed"} {
		t.Run(outcome, func(t *testing.T) {
			g := newSweepGitFake(t)
			f := newPaneSweepFixture(t)
			f.withGit(g)
			cwd := g.worktree(t, "j")
			pane := f.sweptJob("j", "42", cwd)
			f.tasks["42"], f.category["42"] = "reviewing", "started"
			g.head = paneSweepPRHead
			f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
			f.closeJob("j", outcome)
			f.at(0)
			f.at(11)
			if !slices.Equal(f.closed(), []string{pane}) {
				t.Fatalf("closed %v, want [%s]", f.closed(), pane)
			}
			if payload := f.reaped("j"); payload["reason"] != "job_"+outcome {
				t.Fatalf("job.reaped payload = %v", payload)
			}
			if f.prRead != 0 {
				t.Fatalf("the PR lookup ran %d times for a job its owner closed", f.prRead)
			}
		})
	}
}

// A reviewer (role reviewer, or a mergewake mwr-* label) or a tester never
// authors the PR it looked at: once its own record completed it is swept even
// though its detached worktree sits at the open PR's head.
func TestPaneSweepClosesACompletedReviewerAtAnOpenPRHead(t *testing.T) {
	for _, tc := range []struct {
		name, id, role string
		extra          map[string]any
	}{
		{"reviewer role", "rv", "worker", map[string]any{"role": "reviewer"}},
		{"mwr label", "mw", "worker", map[string]any{"label": "mwr-306"}},
		{"tester", "ts", "tester", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newSweepGitFake(t)
			f := newPaneSweepFixture(t)
			f.withGit(g)
			cwd := g.worktree(t, tc.id)
			extra := map[string]any{"cwd": cwd}
			for key, value := range tc.extra {
				extra[key] = value
			}
			pane := f.job(tc.id, tc.role, "42", extra)
			if label, ok := extra["label"].(string); ok {
				f.names[pane] = label
			}
			f.tasks["42"], f.category["42"] = "reviewing", "started"
			g.detached, g.head = true, paneSweepPRHead
			f.prChanges["42"] = []PRChange{{Number: 306, State: "open", Head: paneSweepPRHead}}
			putEvent(t, f.a, tc.id, "job.completed", map[string]any{})
			f.at(0)
			f.at(11)
			if !slices.Equal(f.closed(), []string{pane}) {
				t.Fatalf("closed %v, want [%s]; info %q", f.closed(), pane, f.info)
			}
			if payload := f.reaped(tc.id); payload["reason"] != "job_completed" {
				t.Fatalf("job.reaped payload = %v", payload)
			}
		})
	}
}
