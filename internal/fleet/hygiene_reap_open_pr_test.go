package fleet

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// `panewire reap --apply` keeps a completed author's pane while its worktree
// is on an open PR, as the node's task pane sweep does (hk 3734,
// docs/pane-sweep.md); a job its owner closed is reaped whatever its PRs say.

// reapOpenPRJob seeds worker job j on task 42 with its worktree at the PR
// head, ends it with terminal (its payload beside the kind), lists its pane
// as idle, answers hk's change list with status and changes, and runs reap
// --apply. It returns the app, reap's output and the panes it closed.
func reapOpenPRJob(t *testing.T, terminal map[string]any, status int, changes string) (*App, string, []string) {
	t.Helper()
	a := testApp(t)
	f := ledgerRunner(t, a)
	base := f.Do
	f.Do = func(c Command) Result {
		line := strings.Join(c.Args, " ")
		switch {
		case c.Name == "herdr" && line == "agent list":
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","tab_id":"w:t1","name":"agent-a","agent_status":"idle"}]}}`}
		case c.Name == "git" && strings.HasSuffix(line, "rev-parse HEAD"):
			return Result{Out: paneSweepPRHead + "\n"}
		case c.Name == "git" && strings.HasSuffix(line, "branch --show-current"):
			return Result{} // detached: HEAD alone names the worktree
		}
		return base(c)
	}
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		return status, changes, r.Route() == "GET /v1/changes" && r.Query == "task=42"
	})
	store := a.JobStore.(*fakeJobStore)
	if err := store.Claim(t.Context(), "j", "42", "test-host"); err != nil {
		t.Fatal(err)
	}
	putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "t_level": "T1", "role": "worker"})
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "cwd": "/work/wt-j"})
	kind, _ := terminal["kind"].(string)
	putEvent(t, a, "j", kind, terminal)
	a.Now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC) }
	if err := a.hygiene(t.Context(), "reap", []string{"--job", "j", "--grace", "0s", "--apply"}); err != nil {
		t.Fatal(err)
	}
	var closed []string
	for _, c := range f.Calls {
		if line := strings.Join(c.Args, " "); c.Name == "herdr" && strings.HasPrefix(line, "pane close") {
			closed = append(closed, line)
		}
	}
	return a, a.Out.(interface{ String() string }).String(), closed
}

const reapOpenPRChanges = `{"changes":[{"pr_number":306,"state":"open","head_sha":"` + paneSweepPRHead + `"}]}`

func TestReapKeepsACompletedAuthorWithAnOpenPR(t *testing.T) {
	a, out, closed := reapOpenPRJob(t, map[string]any{"kind": "job.completed"}, http.StatusOK, reapOpenPRChanges)
	if len(closed) != 0 || slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap closed a completed author with an open PR: closed %v, events %v", closed, eventKindsOf(t, a, "j"))
	}
	if !strings.Contains(out, "job=j pane=w:p1 action=keep reason=open_pr") {
		t.Fatalf("reap output = %q", out)
	}
}

func TestReapKeepsACompletedAuthorWhenTheChangeListFails(t *testing.T) {
	a, out, closed := reapOpenPRJob(t, map[string]any{"kind": "job.completed"}, http.StatusInternalServerError, `{"error":"internal"}`)
	if len(closed) != 0 || slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap closed a completed author whose PR state is unknown: closed %v", closed)
	}
	if !strings.Contains(out, "job=j pane=w:p1 action=keep reason=pr_state_unknown") {
		t.Fatalf("reap output = %q", out)
	}
}

func TestReapClosesACompletedAuthorWithNoOpenPR(t *testing.T) {
	merged := `{"changes":[{"pr_number":306,"state":"merged","head_sha":"` + paneSweepPRHead + `"}]}`
	a, out, closed := reapOpenPRJob(t, map[string]any{"kind": "job.completed"}, http.StatusOK, merged)
	if !slices.Equal(closed, []string{"pane close w:p1"}) || !slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap kept a completed author with no open PR: closed %v, out %q", closed, out)
	}
}

func TestReapClosesAnAbandonedJobEvenWithAnOpenPR(t *testing.T) {
	a, out, closed := reapOpenPRJob(t, map[string]any{"kind": "job.revoked", "outcome": "abandoned"}, http.StatusOK, reapOpenPRChanges)
	if !slices.Equal(closed, []string{"pane close w:p1"}) || !slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap kept an abandoned job: closed %v, out %q", closed, out)
	}
}
