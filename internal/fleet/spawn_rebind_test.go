package fleet

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	rebindParent = "lost-worker"
	rebindOther  = "third-job"
)

// rebindFixture is a spawn that inherits task 42 from rebindParent. The fake hk
// keeps refs.job_id, so a rebind is visible to whatever reads the task next.
type rebindFixture struct {
	a          *App
	s          Spawn
	f          *fakeRunner
	store      *fakeJobStore
	bound      string
	rebindFail string
}

func newRebindFixture(t *testing.T, parentState, bound string) *rebindFixture {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	store := newFakeJobStore()
	store.records[rebindParent] = map[string]any{"job_id": rebindParent, "task_id": "42", "host": "test-host", "state": parentState, "family": "anthropic", "owner_lane": "owner"}
	a.JobStore = store
	s.ParentJob, s.Task = rebindParent, ""
	s.Report = filepath.Join(a.Jobs, s.Job, "report.md")
	fx := &rebindFixture{a: a, s: s, f: f, store: store, bound: bound}
	base := f.Do
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks show"):
			out, e := json.Marshal(map[string]any{"id": 42, "title": "implement (grade B, T1)", "state": "claimed", "refs": map[string]any{"job_id": fx.bound}})
			if e != nil {
				t.Fatal(e)
			}
			return Result{Out: string(out)}
		case c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks rebind"):
			if fx.rebindFail != "" {
				return Result{Code: 1, Err: fx.rebindFail}
			}
			fx.bound = s.Job
			out, e := json.Marshal(map[string]any{"id": 42, "state": "claimed", "claimed_by": s.Label, "refs": map[string]any{"job_id": s.Job}})
			if e != nil {
				t.Fatal(e)
			}
			return Result{Out: string(out)}
		}
		return base(c)
	}
	return fx
}

func (fx *rebindFixture) rebinds() [][]string {
	var calls [][]string
	for _, c := range fx.f.Calls {
		if c.Name == "handoffkeep" && len(c.Args) > 1 && c.Args[0] == "tasks" && c.Args[1] == "rebind" {
			calls = append(calls, c.Args)
		}
	}
	return calls
}

// taskLinkEvent returns the job's only job.task_link payload.
func (fx *rebindFixture) taskLinkEvent(t *testing.T) map[string]any {
	t.Helper()
	events, e := fx.a.ledger().Events(context.Background(), fx.s.Job)
	if e != nil {
		t.Fatal(e)
	}
	var found []map[string]any
	for _, ev := range events {
		if ev.Kind == "job.task_link" {
			found = append(found, ev.Payload)
		}
	}
	if len(found) != 1 {
		t.Fatalf("job.task_link events=%d want 1: %+v", len(found), events)
	}
	for _, key := range []string{"task_id", "job_id", "claimed_by", "status", "inherited_from"} {
		if _, ok := found[0][key]; !ok {
			t.Fatalf("job.task_link lost the existing field %q: %v", key, found[0])
		}
	}
	return found[0]
}

func (fx *rebindFixture) wantRebind(t *testing.T) {
	t.Helper()
	calls := fx.rebinds()
	if len(calls) != 1 {
		t.Fatalf("tasks rebind calls=%v want exactly one", calls)
	}
	got := calls[0]
	if !slices.Equal(got[:9], []string{"tasks", "rebind", "42", "--from-job", rebindParent, "--to-job", fx.s.Job, "--by", fx.s.Label}) {
		t.Fatalf("tasks rebind args=%v", got)
	}
	if len(got) != 11 || got[9] != "--reason" || !strings.Contains(got[10], rebindParent) {
		t.Fatalf("tasks rebind reason=%v", got)
	}
}

func TestRespawnRebindsTerminalParent(t *testing.T) {
	for _, state := range []string{"lost", "failed", "completed"} {
		t.Run(state, func(t *testing.T) {
			fx := newRebindFixture(t, state, rebindParent)
			if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
				t.Fatal(e)
			}
			fx.wantRebind(t)
			if ev := fx.taskLinkEvent(t); ev["status"] != "rebound" || ev["inherited_from"] != rebindParent || ev["job_id"] != fx.s.Job {
				t.Fatalf("event=%v", ev)
			}
		})
	}
}

// A parent found in the local job directory has no state of its own; the state
// still comes from its hk record.
func TestRespawnRebindsLocallyResolvedParent(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "worker"})
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	fx.wantRebind(t)
	if ev := fx.taskLinkEvent(t); ev["status"] != "rebound" {
		t.Fatalf("event=%v", ev)
	}
}

func TestRespawnKeepsTaskOnLiveParent(t *testing.T) {
	for _, state := range []string{"claimed", "spawned", "escalated"} {
		t.Run(state, func(t *testing.T) {
			fx := newRebindFixture(t, state, rebindParent)
			if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
				t.Fatal(e)
			}
			if calls := fx.rebinds(); len(calls) != 0 {
				t.Fatalf("a live parent lost its task: %v", calls)
			}
			ev := fx.taskLinkEvent(t)
			if ev["status"] != "inherited" || ev["bound_job"] != nil || ev["rebind_error"] != nil {
				t.Fatalf("event=%v", ev)
			}
		})
	}
}

// The pane's ARBITER_JOB names the parent when the spawn names neither a task
// nor a parent job.
func TestRespawnKeepsTaskOnLiveParentFromEnvironment(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	t.Setenv("ARBITER_JOB", rebindParent)
	fx.s.ParentJob = ""
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	if calls := fx.rebinds(); len(calls) != 0 {
		t.Fatalf("the calling job lost its task: %v", calls)
	}
	if ev := fx.taskLinkEvent(t); ev["status"] != "inherited" || ev["inherited_from"] != rebindParent {
		t.Fatalf("event=%v", ev)
	}
}

func TestRespawnLeavesTaskBoundToThirdJob(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindOther)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	if calls := fx.rebinds(); len(calls) != 0 {
		t.Fatalf("a task bound to another job was moved: %v", calls)
	}
	if ev := fx.taskLinkEvent(t); ev["status"] != "inherited" || ev["bound_job"] != rebindOther {
		t.Fatalf("event=%v", ev)
	}
}

func TestRespawnRebindFailureKeepsLandedSpawn(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	fx.rebindFail = "task conflict: cannot rebind"
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatalf("a failed rebind failed the landed spawn: %v", e)
	}
	fx.wantRebind(t)
	ev := fx.taskLinkEvent(t)
	if ev["status"] != "inherited" || !strings.Contains(str(ev, "rebind_error"), fx.rebindFail) {
		t.Fatalf("event=%v", ev)
	}
	if st := str(fx.store.record(t, fx.s.Job), "state"); st != "spawned" {
		t.Fatalf("job record state=%q", st)
	}
}

// An unconfirmed answer is a failed rebind, as it is for tasks claim.
func TestRespawnRebindUnconfirmedAnswerIsRecorded(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	base := fx.f.Do
	fx.f.Do = func(c Command) Result {
		if c.Name == "handoffkeep" && strings.HasPrefix(strings.Join(c.Args, " "), "tasks rebind") {
			return Result{Out: `{"id":42,"refs":{"job_id":"` + rebindParent + `"}}`}
		}
		return base(c)
	}
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	if ev := fx.taskLinkEvent(t); ev["status"] != "inherited" || str(ev, "rebind_error") == "" {
		t.Fatalf("event=%v", ev)
	}
}

// Without the parent's state the condition is unproven, so the task stays.
func TestRespawnUnreadableParentStateKeepsTask(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "worker"})
	delete(fx.store.records, rebindParent)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	if calls := fx.rebinds(); len(calls) != 0 {
		t.Fatalf("rebound without a parent state: %v", calls)
	}
	if ev := fx.taskLinkEvent(t); ev["status"] != "inherited" || str(ev, "rebind_error") == "" {
		t.Fatalf("event=%v", ev)
	}
}

// The node rebuilds a saved request from its args. The replay stops at the job
// claim, because the record is past "claimed".
func TestRespawnSavedRequestReplayRebindsOnce(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	fx.s.Report, fx.s.ReportRelative = "", filepath.Join(fx.s.Job, "report.md")
	saved := submissionArgs(fx.s)
	replay := func() error {
		s, e := parseSpawn(saved)
		if e != nil {
			t.Fatal(e)
		}
		return fx.a.Spawn(context.Background(), bindNodeSpawn(s, fx.s.CWD, fx.s.Prompt, fx.s.Workspace))
	}
	if e := replay(); e != nil {
		t.Fatal(e)
	}
	if e := replay(); code(e) != exitDuplicate {
		t.Fatalf("replay: %v", e)
	}
	fx.wantRebind(t)
	if ev := fx.taskLinkEvent(t); ev["status"] != "rebound" {
		t.Fatalf("event=%v", ev)
	}
}

// An attempt can rebind and then fail before its record leaves "claimed", which
// lets the retry reach the link step again. The task is already this job's.
func TestRespawnRetryAfterRebindDoesNotRebindAgain(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	retry := fx.s
	retry.Inherited = rebindParent
	task := Task{Refs: map[string]any{"job_id": fx.bound}}
	ev := map[string]any{}
	if got := fx.a.rebindInherited(context.Background(), retry, task, ev); got != "rebound" {
		t.Fatalf("retry status=%q", got)
	}
	fx.wantRebind(t)
	if len(ev) != 0 {
		t.Fatalf("retry event fields=%v", ev)
	}
}

// Other seats work beside the parent, and a completed parent's author may still
// be working on the PR, so none of them takes the task.
func TestRespawnChildBesideParentKeepsTask(t *testing.T) {
	for _, tc := range []struct{ name, role, routing string }{
		{"reviewer", "reviewer", ""},
		{"tester", "tester", ""},
		{"consult", "counsel", ""},
		{"delegated reader", "worker", "explorer"},
		{"delegated verifier", "worker", "verifier"},
		{"delegated reviewer", "reviewer", "reviewer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newRebindFixture(t, "completed", rebindParent)
			s := fx.s
			s.Role, s.RoutingRole, s.Inherited = tc.role, tc.routing, rebindParent
			if e := fx.a.taskLink(context.Background(), s, Task{Refs: map[string]any{"job_id": rebindParent}}, false); e != nil {
				t.Fatal(e)
			}
			if calls := fx.rebinds(); len(calls) != 0 {
				t.Fatalf("%s took the task: %v", tc.name, calls)
			}
			ev := fx.taskLinkEvent(t)
			if ev["status"] != "inherited" || ev["bound_job"] != nil || ev["rebind_error"] != nil {
				t.Fatalf("event=%v", ev)
			}
		})
	}
}

// The executing host sees these fields only after the hub args or the ssh
// envelope, so the seat decision must survive both. Inherited is set on the node
// when it resolves --parent-job.
func TestRespawnReplacementSeatSurvivesTheEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		name, role, routing string
		want                bool
	}{
		{"worker", "worker", "", true},
		{"implementer", "worker", "implementer", true},
		{"lead", "worker", "lead", true},
		{"reader", "worker", "explorer", false},
		{"verifier", "worker", "verifier", false},
		{"reviewer", "reviewer", "reviewer", false},
		{"consult", "counsel", "consult", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := Spawn{Job: "new-job", Label: "new-job", Profile: "sonnet", T: "T1", Role: tc.role, RoutingRole: tc.routing, ParentJob: rebindParent, Report: "/r/report.md"}
			args, e := hubArgs("/r", s)
			if e != nil {
				t.Fatal(e)
			}
			hub, e := parseSpawn(args)
			if e != nil {
				t.Fatal(e)
			}
			hub.Inherited = hub.ParentJob
			if hub.ParentJob != rebindParent || hub.replacesParent() != tc.want {
				t.Fatalf("after the hub args: replaces=%v want %v (%+v)", hub.replacesParent(), tc.want, hub)
			}
			remote := s
			remote.ParentJob, remote.Inherited = "", rebindParent
			b, e := json.Marshal(remote)
			if e != nil {
				t.Fatal(e)
			}
			var got Spawn
			if e := json.Unmarshal(b, &got); e != nil {
				t.Fatal(e)
			}
			if got.replacesParent() != tc.want {
				t.Fatalf("after the ssh envelope: replaces=%v want %v (%+v)", got.replacesParent(), tc.want, got)
			}
		})
	}
}

// hk refuses from == to; a job that names itself as parent is not a replacement.
func TestRespawnSameJobIsNotAReplacement(t *testing.T) {
	if (Spawn{Job: rebindParent, Inherited: rebindParent, Role: "worker"}).replacesParent() {
		t.Fatal("a job cannot replace itself")
	}
}
