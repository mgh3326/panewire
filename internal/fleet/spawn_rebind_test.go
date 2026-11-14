package fleet

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
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
	a              *App
	s              Spawn
	f              *fakeRunner
	hk             *spawnHK
	store          *fakeJobStore
	bound          string
	claimedBy      string
	rebindFail     string
	rebindAnswer   string
	rebindOverride func(map[string]string) (int, string, bool)
	// afterRebind runs once the hub's rebind to the successor has been applied.
	afterRebind func()
	// restoreFail makes the rebind back to the parent answer 409.
	restoreFail bool
	getFail     bool
}

func newRebindFixture(t *testing.T, parentState, bound string) *rebindFixture {
	t.Helper()
	a, s, f, _ := cmdFixture(t)
	store := newFakeJobStore()
	store.records[rebindParent] = map[string]any{"job_id": rebindParent, "task_id": "42", "host": "test-host", "state": parentState, "family": "anthropic", "owner_lane": "owner"}
	a.JobStore = store
	s.ParentJob, s.Task = rebindParent, ""
	s.Report = filepath.Join(a.Jobs, s.Job, "report.md")
	fx := &rebindFixture{a: a, s: s, f: f, hk: useFakeHK(t), store: store, bound: bound, claimedBy: bound}
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
			if fx.getFail {
				return http.StatusServiceUnavailable, `{"error":"unavailable"}`, true
			}
			return http.StatusOK, task("claimed", fx.claimedBy, fx.bound), true
		case "POST /v1/tasks/42/rebind":
			var body map[string]string
			_ = json.Unmarshal(r.Body, &body)
			if fx.rebindOverride != nil {
				if status, answer, handled := fx.rebindOverride(body); handled {
					return status, answer, true
				}
			}
			if fx.rebindFail != "" {
				return http.StatusConflict, fx.rebindFail, true
			}
			if fx.rebindAnswer != "" {
				return http.StatusOK, fx.rebindAnswer, true
			}
			if fx.restoreFail && body["to_job"] == rebindParent {
				return http.StatusConflict, `{"error":"conflict","reason":"cannot rebind"}`, true
			}
			by := body["by"]
			fx.bound, fx.claimedBy = body["to_job"], by
			if fx.afterRebind != nil && body["to_job"] == s.Job {
				fx.afterRebind()
			}
			return http.StatusOK, task("claimed", by, fx.bound), true
		}
		return 0, "", false
	})
	return fx
}

// breakEventLog returns a hook that makes the job's next event unwritable: its
// events directory becomes a file. A chmod would not hold for root.
func (fx *rebindFixture) breakEventLog(t *testing.T) func() {
	return func() {
		dir := filepath.Join(fx.a.Jobs, fx.s.Job, "events")
		if e := os.RemoveAll(dir); e != nil {
			t.Error(e)
		}
		if e := os.WriteFile(dir, nil, 0o600); e != nil {
			t.Error(e)
		}
	}
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

func (fx *rebindFixture) taskRebindEvent(t *testing.T) map[string]any {
	t.Helper()
	events, err := fx.a.ledger().Events(context.Background(), fx.s.Job)
	if err != nil {
		t.Fatal(err)
	}
	spawned := false
	for _, ev := range events {
		if ev.Kind == "job.spawned" {
			spawned = true
		}
		if ev.Kind == "job.task_rebind" {
			if !spawned {
				t.Fatal("task rebind event precedes job.spawned")
			}
			return ev.Payload
		}
	}
	t.Fatal("missing job.task_rebind")
	return nil
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
			if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" || ev["inherited_from"] != rebindParent || ev["job_id"] != fx.s.Job {
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
	if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" {
		t.Fatalf("event=%v", ev)
	}
}

// 2026-11-13: successors spawned with --parent-job landed and worked while the
// task kept naming the first job, so mergewake sent FINDINGS to a pane that was
// gone. Whatever state the parent is in, hk's task ends up claimed by the
// successor and bound to its job.
func TestParentJobSuccessorTakesOverTaskBinding(t *testing.T) {
	for _, state := range []string{"completed", "lost", "escalated", "spawned", "claimed"} {
		t.Run(state, func(t *testing.T) {
			fx := newRebindFixture(t, state, rebindParent)
			if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
				t.Fatal(e)
			}
			if fx.bound != fx.s.Job || fx.claimedBy != fx.s.Label {
				t.Fatalf("task bound to job %q claimed by %q, want %q and %q", fx.bound, fx.claimedBy, fx.s.Job, fx.s.Label)
			}
			fx.wantRebind(t)
			ev := fx.taskRebindEvent(t)
			if ev["status"] != "rebound" || ev["parent_state"] != state || ev["rebind_error"] != nil {
				t.Fatalf("event=%v", ev)
			}
		})
	}
}

// 2026-11-14 (hk 4586): when a parent was itself a successor spawned over an
// earlier job in the chain (e.g. pw-x-2 over pw-x), but the task is still
// bound to the ancestor (pw-x), the successor (pw-x-3) follows the
// --parent-job chain and rebinds from the ancestor holding the task.
func TestParentJobSuccessorTakesOverAncestorTaskBinding(t *testing.T) {
	t.Run("one step ancestor", func(t *testing.T) {
		const ancestor = "ancestor-worker"
		fx := newRebindFixture(t, "lost", ancestor)
		fx.store.records[ancestor] = map[string]any{"job_id": ancestor, "task_id": "42", "host": "test-host", "state": "lost", "family": "anthropic", "owner_lane": "owner"}
		fx.store.records[rebindParent]["parent_job"] = ancestor
		if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
			t.Fatal(e)
		}
		if fx.bound != fx.s.Job || fx.claimedBy != fx.s.Label {
			t.Fatalf("task bound to job %q claimed by %q, want %q and %q", fx.bound, fx.claimedBy, fx.s.Job, fx.s.Label)
		}
		calls := fx.rebinds()
		if len(calls) != 1 {
			t.Fatalf("task rebind calls=%v want exactly one", calls)
		}
		got := calls[0]
		if got["from_job"] != ancestor || got["to_job"] != fx.s.Job || got["by"] != fx.s.Label {
			t.Fatalf("task rebind body=%v, want from_job=%s to_job=%s", got, ancestor, fx.s.Job)
		}
		ev := fx.taskRebindEvent(t)
		if ev["status"] != "rebound" || ev["inherited_from"] != rebindParent {
			t.Fatalf("event=%v", ev)
		}
	})

	t.Run("multi-step ancestor", func(t *testing.T) {
		const (
			greatGrandparent = "great-grandparent-worker"
			grandparent      = "grandparent-worker"
		)
		fx := newRebindFixture(t, "lost", greatGrandparent)
		fx.store.records[greatGrandparent] = map[string]any{"job_id": greatGrandparent, "task_id": "42", "host": "test-host", "state": "lost", "family": "anthropic", "owner_lane": "owner"}
		fx.store.records[grandparent] = map[string]any{"job_id": grandparent, "parent_job": greatGrandparent, "task_id": "42", "host": "test-host", "state": "lost", "family": "anthropic", "owner_lane": "owner"}
		fx.store.records[rebindParent]["parent_job"] = grandparent
		if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
			t.Fatal(e)
		}
		if fx.bound != fx.s.Job || fx.claimedBy != fx.s.Label {
			t.Fatalf("task bound to job %q claimed by %q, want %q and %q", fx.bound, fx.claimedBy, fx.s.Job, fx.s.Label)
		}
		calls := fx.rebinds()
		if len(calls) != 1 {
			t.Fatalf("task rebind calls=%v want exactly one", calls)
		}
		if got := calls[0]; got["from_job"] != greatGrandparent || got["to_job"] != fx.s.Job {
			t.Fatalf("rebind calls=%v, want from_job=%s", calls, greatGrandparent)
		}
		if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" {
			t.Fatalf("event=%v", ev)
		}
	})

	t.Run("ancestor resolved via local ledger", func(t *testing.T) {
		const ancestor = "local-ancestor-worker"
		fx := newRebindFixture(t, "lost", ancestor)
		putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer", "task_inherited_from": ancestor})
		if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
			t.Fatal(e)
		}
		if fx.bound != fx.s.Job || fx.claimedBy != fx.s.Label {
			t.Fatalf("task bound to job %q claimed by %q, want %q and %q", fx.bound, fx.claimedBy, fx.s.Job, fx.s.Label)
		}
		calls := fx.rebinds()
		if len(calls) != 1 || calls[0]["from_job"] != ancestor {
			t.Fatalf("rebind calls=%v, want from_job=%s", calls, ancestor)
		}
		if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" {
			t.Fatalf("event=%v", ev)
		}
	})

	t.Run("cycle in parent chain stays refused", func(t *testing.T) {
		const other = "unrelated-job"
		fx := newRebindFixture(t, "lost", other)
		fx.store.records["loop-1"] = map[string]any{"job_id": "loop-1", "parent_job": rebindParent}
		fx.store.records[rebindParent]["parent_job"] = "loop-1"
		if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
			t.Fatal(e)
		}
		if len(fx.rebinds()) != 0 || fx.bound != other {
			t.Fatalf("bound=%s calls=%v", fx.bound, fx.rebinds())
		}
		if ev := fx.taskRebindEvent(t); ev["status"] != "inherited" || ev["bound_job"] != other {
			t.Fatalf("event=%v", ev)
		}
	})

	t.Run("conflict re-resolves parent and succeeds", func(t *testing.T) {
		const ancestor = "ancestor-worker"
		fx := newRebindFixture(t, "spawned", ancestor)
		fx.store.records[ancestor] = map[string]any{"job_id": ancestor, "task_id": "42", "host": "test-host", "state": "lost", "family": "anthropic", "owner_lane": "owner"}
		fx.store.records[rebindParent]["parent_job"] = ancestor

		first := true
		fx.rebindOverride = func(body map[string]string) (int, string, bool) {
			if first {
				first = false
				// Parent took over the task while successor was in flight.
				fx.bound, fx.claimedBy = rebindParent, "parent-label"
				return http.StatusConflict, `{"error":"conflict","reason":"current job_id is \"` + rebindParent + `\""}`, true
			}
			return 0, "", false
		}

		if err := fx.a.Spawn(context.Background(), fx.s); err != nil {
			t.Fatal(err)
		}
		if fx.bound != fx.s.Job || fx.claimedBy != fx.s.Label {
			t.Fatalf("task bound to job %q claimed by %q, want %q and %q", fx.bound, fx.claimedBy, fx.s.Job, fx.s.Label)
		}
		calls := fx.rebinds()
		if len(calls) != 2 {
			t.Fatalf("task rebind calls=%v want 2 (initial ancestor + retried parent)", calls)
		}
		if calls[0]["from_job"] != ancestor {
			t.Fatalf("first call from_job=%s want %s", calls[0]["from_job"], ancestor)
		}
		if calls[1]["from_job"] != rebindParent {
			t.Fatalf("second call from_job=%s want %s", calls[1]["from_job"], rebindParent)
		}
		ev := fx.taskRebindEvent(t)
		if ev["status"] != "rebound" {
			t.Fatalf("event=%v", ev)
		}
	})

	t.Run("conflict confirms when already rebound", func(t *testing.T) {
		const ancestor = "ancestor-worker"
		fx := newRebindFixture(t, "spawned", ancestor)
		fx.store.records[ancestor] = map[string]any{"job_id": ancestor, "task_id": "42", "host": "test-host", "state": "lost", "family": "anthropic", "owner_lane": "owner"}
		fx.store.records[rebindParent]["parent_job"] = ancestor

		fx.rebindOverride = func(body map[string]string) (int, string, bool) {
			fx.bound, fx.claimedBy = fx.s.Job, fx.s.Label
			return http.StatusConflict, `{"error":"conflict","reason":"already rebound"}`, true
		}

		if err := fx.a.Spawn(context.Background(), fx.s); err != nil {
			t.Fatal(err)
		}
		if fx.bound != fx.s.Job || fx.claimedBy != fx.s.Label {
			t.Fatalf("task bound to job %q claimed by %q", fx.bound, fx.claimedBy)
		}
		ev := fx.taskRebindEvent(t)
		if ev["status"] != "rebound" {
			t.Fatalf("event=%v", ev)
		}
	})
}

func TestTakeoverRebindsOnlyAfterSpawnCommits(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.afterRebind = func() {
		events, err := fx.a.ledger().Events(context.Background(), fx.s.Job)
		if err != nil {
			t.Error(err)
			return
		}
		for _, ev := range events {
			if ev.Kind == "job.spawned" {
				return
			}
		}
		t.Error("takeover rebind happened before job.spawned")
	}
	if err := fx.a.Spawn(context.Background(), fx.s); err != nil {
		t.Fatal(err)
	}
	link := fx.taskLinkEvent(t)
	if link["status"] != "inherited" || link["takeover"] != "pending" {
		t.Fatalf("task link=%v", link)
	}
	if fx.taskRebindEvent(t)["status"] != "rebound" {
		t.Fatal("takeover did not record rebound")
	}
}

// Failure to record job.spawned must leave the parent's binding untouched.
func TestTakeoverFailureBeforeCommitKeepsParentBinding(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.claimedBy = "parent-label"
	original := fx.f.Do
	failed := false
	fx.f.Do = func(c Command) Result {
		if c.Name == "git" && strings.Contains(strings.Join(c.Args, " "), "remote get-url") {
			fx.breakEventLog(t)()
			failed = true
		}
		if failed && c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[{"pane_id":"w:p1","name":"worker-a","agent":"cmd","agent_status":"idle","cwd":` + strconv.Quote(fx.s.CWD) + `,"state_change_seq":1}]}}`}
		}
		return original(c)
	}
	if err := fx.a.Spawn(context.Background(), fx.s); err == nil {
		t.Fatal("expected failed spawn")
	}
	if calls := fx.rebinds(); len(calls) != 0 || fx.bound != rebindParent || fx.claimedBy != "parent-label" {
		t.Fatalf("rebind calls=%v binding=%s claimant=%s", calls, fx.bound, fx.claimedBy)
	}
	for _, c := range fx.f.Calls {
		if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "pane close") {
			return
		}
	}
	t.Fatal("failed successor pane was not closed")
}

// The parent's own ledger notes who replaced it, once, however often the same
// request is replayed. A parent that ran elsewhere keeps no note here.
func TestParentJobSuccessorIsNotedOnLocalParent(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer"})
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	retry := fx.s
	retry.Inherited = rebindParent
	fx.a.noteReplaced(context.Background(), retry)
	events, e := fx.a.ledger().Events(context.Background(), rebindParent)
	if e != nil {
		t.Fatal(e)
	}
	var notes []map[string]any
	for _, ev := range events {
		if ev.Kind == "job.replaced" {
			notes = append(notes, ev.Payload)
		}
	}
	if len(notes) != 1 || notes[0]["replaced_by"] != fx.s.Job || notes[0]["task_id"] != "42" {
		t.Fatalf("parent replaced notes=%v", notes)
	}

	remote := newRebindFixture(t, "spawned", rebindParent)
	if e := remote.a.Spawn(context.Background(), remote.s); e != nil {
		t.Fatal(e)
	}
	if _, e := remote.a.events(rebindParent); e == nil {
		t.Fatal("a parent with no ledger on this host gained one")
	}
}

// A task that names no job binding has nothing to move.
func TestParentJobSuccessorLeavesUnboundTaskAlone(t *testing.T) {
	fx := newRebindFixture(t, "spawned", "")
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	if calls := fx.rebinds(); len(calls) != 0 {
		t.Fatalf("an unbound task was rebound: %v", calls)
	}
	if ev := fx.taskRebindEvent(t); ev["status"] != "inherited" {
		t.Fatalf("event=%v", ev)
	}
}

// The pane's ARBITER_JOB names the parent when the spawn names neither a task
// nor a parent job.
func TestRespawnTakesTaskFromEnvironmentParent(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	t.Setenv("ARBITER_JOB", rebindParent)
	fx.s.ParentJob = ""
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	fx.wantRebind(t)
	if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" || ev["inherited_from"] != rebindParent {
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
	if ev := fx.taskRebindEvent(t); ev["status"] != "inherited" || ev["bound_job"] != rebindOther {
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
	ev := fx.taskRebindEvent(t)
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
	if ev := fx.taskRebindEvent(t); ev["status"] != "inherited" || str(ev, "rebind_error") == "" {
		t.Fatalf("event=%v", ev)
	}
}

// The parent's state only words the audit trail: a parent whose hk record
// cannot be read is replaced all the same.
func TestRespawnUnreadableParentStateStillRebinds(t *testing.T) {
	fx := newRebindFixture(t, "lost", rebindParent)
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer"})
	delete(fx.store.records, rebindParent)
	if e := fx.a.Spawn(context.Background(), fx.s); e != nil {
		t.Fatal(e)
	}
	fx.wantRebind(t)
	if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" || ev["parent_state"] != nil {
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
	if ev := fx.taskRebindEvent(t); ev["status"] != "rebound" {
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

func (fx *rebindFixture) assertNotice(t *testing.T, want bool) {
	t.Helper()
	n := 0
	for _, c := range fx.f.Calls {
		args := strings.Join(c.Args, " ")
		if c.Name == "panewire" && strings.Contains(args, "--event-id task-rebind-failed-") {
			n++
			if !strings.Contains(args, "--type automation-failed") || !strings.Contains(args, "TASK_REBIND_FAILED") {
				t.Fatalf("notice=%s", args)
			}
		}
	}
	if (n == 1) != want {
		t.Fatalf("notices=%d want one=%v", n, want)
	}
}

func TestTakeoverRetryAlreadyBoundToSuccessorRecordsRebound(t *testing.T) {
	fx := newRebindFixture(t, "spawned", "job-a")
	fx.claimedBy = fx.s.Label
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer"})
	if err := fx.a.Spawn(context.Background(), fx.s); err != nil {
		t.Fatal(err)
	}
	if len(fx.rebinds()) != 0 || fx.taskRebindEvent(t)["status"] != "rebound" {
		t.Fatal("retry did not confirm existing binding")
	}
	events, err := fx.a.ledger().Events(context.Background(), rebindParent)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "job.replaced" {
			return
		}
	}
	t.Fatal("missing parent replacement event")
}

func TestTakeoverRefusedAfterCommitKeepsParentAndNotifies(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.rebindFail = `{"error":"conflict","reason":"cannot rebind"}`
	putEvent(t, fx.a, rebindParent, "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "t_level": "T1", "role": "implementer"})
	if err := fx.a.Spawn(context.Background(), fx.s); err != nil {
		t.Fatal(err)
	}
	ev := fx.taskRebindEvent(t)
	if fx.bound != rebindParent || ev["status"] != "inherited" || ev["rebind_error"] == nil {
		t.Fatalf("binding=%s event=%v", fx.bound, ev)
	}
	fx.assertNotice(t, true)
	for _, c := range fx.f.Calls {
		if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "pane close") {
			t.Fatal("closed landed pane")
		}
	}
	events, err := fx.a.ledger().Events(context.Background(), rebindParent)
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "job.replaced" {
			t.Fatal("recorded replacement after refusal")
		}
	}
}

func TestTakeoverUnreadableResponseConfirmsByLookup(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		applied, lookupFails bool
		want                 string
		calls                int
	}{
		{"already applied", true, false, "rebound", 1},
		{"retry parent", false, false, "rebound", 2},
		{"lookup fails", false, true, "inherited", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newRebindFixture(t, "spawned", rebindParent)
			first := true
			fx.rebindOverride = func(body map[string]string) (int, string, bool) {
				if !first {
					return 0, "", false
				}
				first = false
				if tc.applied {
					fx.bound, fx.claimedBy = fx.s.Job, fx.s.Label
				}
				fx.getFail = tc.lookupFails
				return http.StatusOK, `{`, true
			}
			if err := fx.a.Spawn(context.Background(), fx.s); err != nil {
				t.Fatal(err)
			}
			ev := fx.taskRebindEvent(t)
			if ev["status"] != tc.want || len(fx.rebinds()) != tc.calls {
				t.Fatalf("event=%v calls=%v", ev, fx.rebinds())
			}
			if tc.want == "inherited" && ev["rebind_error"] != "unconfirmed" {
				t.Fatalf("event=%v", ev)
			}
			fx.assertNotice(t, tc.want == "inherited")
		})
	}
}

func TestTakeoverConcurrentRebindLeavesForeignBinding(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.rebindOverride = func(body map[string]string) (int, string, bool) {
		fx.bound, fx.claimedBy = rebindOther, "other"
		return http.StatusConflict, `{"error":"conflict"}`, true
	}
	if err := fx.a.Spawn(context.Background(), fx.s); err != nil {
		t.Fatal(err)
	}
	ev := fx.taskRebindEvent(t)
	if fx.bound != rebindOther || ev["status"] != "inherited" || ev["bound_job"] != rebindOther {
		t.Fatalf("binding=%s event=%v", fx.bound, ev)
	}
	fx.assertNotice(t, true)
}

type cancellationAwareRunner struct{ inner Runner }

func (r cancellationAwareRunner) Run(ctx context.Context, c Command) Result {
	if ctx.Err() != nil {
		return Result{Code: 1, Err: ctx.Err().Error()}
	}
	return r.inner.Run(ctx, c)
}

func TestTakeoverReportsFailureAfterSpawnContextCancellation(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.s.Inherited, fx.s.Task = rebindParent, "42"
	fx.rebindFail = `{"error":"conflict","reason":"cannot rebind"}`
	fx.a.Runner = cancellationAwareRunner{inner: fx.f}
	if err := fx.a.event(context.Background(), fx.s.Job, "job.spawned", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fx.a.finishTaskTakeover(ctx, fx.s, client.Task{Refs: client.TaskRefs{JobID: rebindParent}}, "owner")
	fx.assertNotice(t, true)
	ev := fx.taskRebindEvent(t)
	if ev["status"] != "inherited" || ev["rebind_error"] == nil {
		t.Fatalf("event=%v", ev)
	}
}

// A busy ledger must not consume the notice's own deadline.
func TestTakeoverNotifiesWhenOutcomeLedgerIsLocked(t *testing.T) {
	fx := newRebindFixture(t, "spawned", rebindParent)
	fx.s.Inherited, fx.s.Task = rebindParent, "42"
	fx.rebindFail = `{"error":"conflict","reason":"cannot rebind"}`
	fx.a.Runner = cancellationAwareRunner{inner: fx.f}
	if err := fx.a.event(context.Background(), fx.s.Job, "job.spawned", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", fx.a.Arbiter.DB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "ROLLBACK")
	fx.a.finishTaskTakeover(context.Background(), fx.s, client.Task{Refs: client.TaskRefs{JobID: rebindParent}}, "owner")
	fx.assertNotice(t, true)
}
