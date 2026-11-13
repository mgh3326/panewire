package fleet

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// `panewire reap --apply` keeps a completed author's pane while its worktree
// is on an open PR, as the node's task pane sweep does (hk 3734,
// docs/pane-sweep.md); a job its owner closed is reaped whatever its PRs say.

// reapPRCase is one reap --apply over worker job j with its worktree at the PR
// head. claim replaces the claim's task_id 42 when set; brokenHead makes the
// worktree's HEAD unreadable; setup runs on the job store after the claim.
type reapPRCase struct {
	claim      map[string]any
	terminal   map[string]any
	status     int
	changes    string
	brokenHead bool
	setup      func(store *fakeJobStore)
}

// reapOpenPRJob seeds worker job j on task 42 with its worktree at the PR
// head, ends it with terminal (its payload beside the kind), lists its pane
// as idle, answers hk's change list with status and changes, and runs reap
// --apply. It returns the app, reap's output and the panes it closed.
func reapOpenPRJob(t *testing.T, terminal map[string]any, status int, changes string) (*App, string, []string) {
	t.Helper()
	return reapPRJob(t, reapPRCase{terminal: terminal, status: status, changes: changes})
}

func reapPRJob(t *testing.T, rc reapPRCase) (*App, string, []string) {
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
			if rc.brokenHead {
				return Result{Code: 128, Err: "fatal: not a git repository"}
			}
			return Result{Out: paneSweepPRHead + "\n"}
		case c.Name == "git" && strings.HasSuffix(line, "branch --show-current"):
			return Result{} // detached: HEAD alone names the worktree
		}
		return base(c)
	}
	useFakeHK(t).Handle(func(r hkRequest) (int, string, bool) {
		return rc.status, rc.changes, r.Route() == "GET /v1/changes" && r.Query == "task=42"
	})
	claim := map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": "42", "t_level": "T1", "role": "implementer"}
	if rc.claim != nil {
		delete(claim, "task_id")
		for key, value := range rc.claim {
			claim[key] = value
		}
	}
	store := a.JobStore.(*fakeJobStore)
	if err := store.Claim(t.Context(), "j", scalar(claim["task_id"]), "test-host"); err != nil {
		t.Fatal(err)
	}
	if rc.setup != nil {
		rc.setup(store)
	}
	putEvent(t, a, "j", "job.claim", claim)
	putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p1", "tab_id": "w:t1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek", "cwd": "/work/wt-j"})
	kind, _ := rc.terminal["kind"].(string)
	putEvent(t, a, "j", kind, rc.terminal)
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

// A worktree whose HEAD cannot be read keeps a completed author's pane even
// when hk lists no open PR for its task: the head that would match one is
// unknown.
func TestReapKeepsACompletedAuthorWhoseWorktreeCannotBeRead(t *testing.T) {
	for _, tc := range []struct{ name, changes string }{
		{"empty change list", `{"changes":[]}`},
		{"merged only", `{"changes":[{"pr_number":306,"state":"merged","head_sha":"` + paneSweepPRHead + `"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, out, closed := reapPRJob(t, reapPRCase{terminal: map[string]any{"kind": "job.completed"}, status: http.StatusOK, changes: tc.changes, brokenHead: true})
			if len(closed) != 0 || slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
				t.Fatalf("reap closed a completed author whose worktree cannot be read: closed %v", closed)
			}
			if !strings.Contains(out, "job=j pane=w:p1 action=keep reason=pr_state_unknown") {
				t.Fatalf("reap output = %q", out)
			}
		})
	}
}

// A completed fix author whose parent is not on this node, and whose parent's
// hk record cannot be read, has a task reap cannot establish: it is kept, not
// reaped as if it had none.
func TestReapKeepsACompletedFixAuthorWhoseTaskIsUnknown(t *testing.T) {
	a, out, closed := reapPRJob(t, reapPRCase{
		claim:    map[string]any{"task_inherited_from": "parent"},
		terminal: map[string]any{"kind": "job.completed"},
		status:   http.StatusOK, changes: reapOpenPRChanges,
		setup: func(store *fakeJobStore) {
			store.records["parent"] = map[string]any{"job_id": "parent", "task_id": float64(42), "state": "completed"}
			store.getErr = errors.New("hk down")
		},
	})
	if len(closed) != 0 || slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap closed a completed fix author whose task is unknown: closed %v, out %q", closed, out)
	}
	if !strings.Contains(out, "job=j pane=w:p1 action=keep reason=pr_state_unknown") {
		t.Fatalf("reap output = %q", out)
	}
}

// The same fix author with its parent's hk record readable is judged on the
// parent's task: its open PR keeps it.
func TestReapKeepsACompletedFixAuthorOnItsParentsOpenPR(t *testing.T) {
	a, out, closed := reapPRJob(t, reapPRCase{
		claim:    map[string]any{"task_inherited_from": "parent"},
		terminal: map[string]any{"kind": "job.completed"},
		status:   http.StatusOK, changes: reapOpenPRChanges,
		setup: func(store *fakeJobStore) {
			store.records["parent"] = map[string]any{"job_id": "parent", "task_id": float64(42), "state": "completed"}
		},
	})
	if len(closed) != 0 || slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap closed a completed fix author with an open PR: closed %v", closed)
	}
	if !strings.Contains(out, "job=j pane=w:p1 action=keep reason=open_pr") {
		t.Fatalf("reap output = %q", out)
	}
}

// A completed fix author whose parent hk answers not_found for (a record that
// has not propagated yet) is kept by reap, not reaped as if it had no task;
// once the parent's record appears, the next reap judges it on the parent's
// task and its open PR keeps it.
func TestReapKeepsACompletedFixAuthorWhileItsParentRecordLags(t *testing.T) {
	a, out, closed := reapPRJob(t, reapPRCase{
		claim:    map[string]any{"task_inherited_from": "parent"},
		terminal: map[string]any{"kind": "job.completed"},
		status:   http.StatusOK, changes: reapOpenPRChanges,
	})
	if len(closed) != 0 || slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap closed a completed fix author whose parent was not found: closed %v, out %q", closed, out)
	}
	if !strings.Contains(out, "job=j pane=w:p1 action=keep reason=pr_state_unknown") {
		t.Fatalf("reap output = %q", out)
	}
	a.JobStore.(*fakeJobStore).records["parent"] = map[string]any{"job_id": "parent", "task_id": float64(42), "state": "completed"}
	if err := a.hygiene(t.Context(), "reap", []string{"--job", "j", "--grace", "0s", "--apply"}); err != nil {
		t.Fatal(err)
	}
	again := strings.TrimPrefix(a.Out.(interface{ String() string }).String(), out)
	if slices.Contains(eventKindsOf(t, a, "j"), "job.reaped") {
		t.Fatalf("reap closed a completed fix author with an open PR once its parent resolved: out %q", again)
	}
	if !strings.Contains(again, "job=j pane=w:p1 action=keep reason=open_pr") {
		t.Fatalf("second reap output = %q", again)
	}
}
