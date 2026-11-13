package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/handoffkeep/client"
)

const (
	rebindParent = "lost-worker"
	rebindOther  = "third-job"
)

// rebindFixture is a spawn that inherits task 42 from rebindParent. The fake hk
// keeps refs.job_id, so a rebind is visible to whatever reads the task next.
// rebindFail, when set, is the hk error body a rebind answers with 409;
// rebindAnswer, when set, replaces the rebind's success answer.
type rebindFixture struct {
	a            *App
	s            Spawn
	f            *fakeRunner
	hk           *spawnHK
	store        *fakeJobStore
	bound        string
	rebindFail   string
	rebindAnswer string
}

func newRebindFixture(t *testing.T, parentState, bound string) *rebindFixture {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	store := newFakeJobStore()
	store.records[rebindParent] = map[string]any{"job_id": rebindParent, "task_id": "42", "host": "test-host", "state": parentState, "family": "anthropic", "owner_lane": "owner"}
	a.JobStore = store
	s.ParentJob, s.Task = rebindParent, ""
	s.Report = filepath.Join(a.Jobs, s.Job, "report.md")
	fx := &rebindFixture{a: a, s: s, f: f, hk: useFakeHK(t), store: store, bound: bound}
	task := func(state, claimedBy, job string) string {
		out, e := json.Marshal(map[string]any{"id": 42, "title": "implement (grade B, T1)", "state": state, "claimed_by": claimedBy, "refs": map[string]any{"job_id": job}})
		if e != nil {
			t.Fatal(e)
		}
		return string(out)
	}
	fx.hk.Handle(func(r hkRequest) (int, string, bool) {
		switch r.Route() {
		case "GET /v1/tasks/42":
			return http.StatusOK, task("claimed", "", fx.bound), true
		case "POST /v1/tasks/42/rebind":
			if fx.rebindFail != "" {
				return http.StatusConflict, fx.rebindFail, true
			}
			if fx.rebindAnswer != "" {
				return http.StatusOK, fx.rebindAnswer, true
			}
			fx.bound = s.Job
			return http.StatusOK, task("claimed", s.Label, s.Job), true
		}
		return 0, "", false
	})
	return fx
}

// rebinds returns the body of every rebind hk received.
func (fx *rebindFixture) rebinds() []map[string]string {
	var calls []map[string]string
	for _, r := range fx.hk.Requests() {
		if r.Route() == "POST /v1/tasks/42/rebind" {
			var body map[string]string
			_ = json.Unmarshal(r.Body, &body)
			calls = append(calls, body)
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
		t.Fatalf("task rebind calls=%v want exactly one", calls)
	}
	got := calls[0]
	if len(got) != 4 || got["from_job"] != rebindParent || got["to_job"] != fx.s.Job || got["by"] != fx.s.Label {
		t.Fatalf("task rebind body=%v", got)
	}
	if !strings.Contains(got["reason"], rebindParent) {
		t.Fatalf("task rebind reason=%v", got)
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
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer"})
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
	fx.rebindFail = `{"error":"conflict","reason":"cannot rebind"}`
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatalf("a failed rebind failed the landed spawn: %v", e)
	}
	fx.wantRebind(t)
	ev := fx.taskLinkEvent(t)
	if ev["status"] != "inherited" || !strings.Contains(str(ev, "rebind_error"), "conflict: cannot rebind") {
		t.Fatalf("event=%v", ev)
	}
	if st := str(fx.store.record(t, fx.s.Job), "state"); st != "spawned" {
		t.Fatalf("job record state=%q", st)
	}
}

// An unconfirmed answer is a failed rebind, as it is for tasks claim.
func TestRespawnRebindUnconfirmedAnswerIsRecorded(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	fx.rebindAnswer = `{"id":42,"refs":{"job_id":"` + rebindParent + `"}}`
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
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer"})
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
	// A submitted request always carries the owner the submitter resolved.
	fx.s.Owner = "owner"
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
	task := client.Task{Refs: client.TaskRefs{JobID: fx.bound}}
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
		{"consult", "consult", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newRebindFixture(t, "completed", rebindParent)
			s := fx.s
			s.Role, s.RoutingRole, s.Inherited = tc.role, tc.routing, rebindParent
			if e := fx.a.taskLink(context.Background(), s, client.Task{Refs: client.TaskRefs{JobID: rebindParent}}, false); e != nil {
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
		{"implementer", "implementer", "", true},
		{"reviewer", "reviewer", "", false},
		{"consult", "consult", "", false},
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
	if (Spawn{Job: rebindParent, Inherited: rebindParent, Role: "implementer"}).replacesParent() {
		t.Fatal("a job cannot replace itself")
	}
}
