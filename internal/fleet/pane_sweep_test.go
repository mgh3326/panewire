package fleet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// paneSweepFixture is one sweeper over a fake hk (task states and job
// records), a fake herdr (the agent rows) and a fake clock.
type paneSweepFixture struct {
	t        *testing.T
	a        *App
	runner   *fakeRunner
	store    *fakeJobStore
	sweeper  *TaskPaneSweeper
	now      time.Time
	status   map[string]string // pane id → herdr agent_status
	names    map[string]string // pane id → herdr agent name
	tasks    map[string]string // task id → hk task state
	taskErr  error
	taskRead int
	info     []string
}

func newPaneSweepFixture(t *testing.T) *paneSweepFixture {
	t.Helper()
	a := testApp(t)
	f := &paneSweepFixture{t: t, a: a, store: a.JobStore.(*fakeJobStore), now: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
		status: map[string]string{}, names: map[string]string{}, tasks: map[string]string{}}
	a.Now = func() time.Time { return f.now }
	f.runner = &fakeRunner{}
	f.runner.Do = func(c Command) Result {
		line := strings.Join(c.Args, " ")
		switch {
		case c.Name == "herdr" && line == "agent list":
			var rows []string
			for pane, status := range f.status {
				rows = append(rows, fmt.Sprintf(`{"pane_id":%q,"tab_id":"t-%s","name":%q,"agent_status":%q}`, pane, pane, f.names[pane], status))
			}
			slices.Sort(rows)
			return Result{Out: `{"result":{"agents":[` + strings.Join(rows, ",") + `]}}`}
		case c.Name == "herdr" && strings.HasPrefix(line, "pane read "):
			return Result{Out: "last screen of " + strings.TrimPrefix(line, "pane read ")}
		}
		return Result{}
	}
	a.Runner = f.runner
	f.sweeper = &TaskPaneSweeper{Grace: 10 * time.Minute,
		TaskState: func(_ context.Context, task string) (string, error) {
			f.taskRead++
			if f.taskErr != nil {
				return "", f.taskErr
			}
			return f.tasks[task], nil
		},
		Info: func(message string, attrs ...any) {
			line := message
			for index := 0; index+1 < len(attrs); index += 2 {
				line += fmt.Sprintf(" %v=%v", attrs[index], attrs[index+1])
			}
			f.info = append(f.info, line)
		}}
	return f
}

// job seeds a claimed, spawned job bound to task whose pane is idle.
func (f *paneSweepFixture) job(id, role, task string, extra ...map[string]any) string {
	f.t.Helper()
	pane := "p-" + id
	putEvent(f.t, f.a, id, "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "task_id": task, "t_level": "T1", "role": role})
	spawned := map[string]any{"pane_id": pane, "tab_id": "t-" + pane, "label": "agent-" + id, "profile": "cmd-dsflash41", "family": "deepseek"}
	for _, more := range extra {
		for key, value := range more {
			spawned[key] = value
		}
	}
	putEvent(f.t, f.a, id, "job.spawned", spawned)
	if err := f.store.Claim(f.t.Context(), id, task, "test-host"); err != nil {
		f.t.Fatal(err)
	}
	f.store.records[id]["state"] = "spawned"
	f.status[pane], f.names[pane] = "idle", "agent-"+id
	return pane
}

// at moves the clock to minute m after the start and runs one pass.
func (f *paneSweepFixture) at(minute int) {
	f.t.Helper()
	f.now = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC).Add(time.Duration(minute) * time.Minute)
	if err := f.sweeper.Pass(f.t.Context(), f.a); err != nil {
		f.t.Fatal(err)
	}
}

func (f *paneSweepFixture) closed() []string {
	var panes []string
	for _, c := range f.runner.Calls {
		if c.Name == "herdr" && len(c.Args) == 3 && c.Args[0] == "pane" && c.Args[1] == "close" {
			panes = append(panes, c.Args[2])
		}
	}
	return panes
}

func (f *paneSweepFixture) infoHas(parts ...string) bool {
	for _, line := range f.info {
		match := true
		for _, part := range parts {
			match = match && strings.Contains(line, part)
		}
		if match {
			return true
		}
	}
	return false
}

func (f *paneSweepFixture) reaped(id string) map[string]any {
	f.t.Helper()
	events, err := f.a.events(id)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, event := range events {
		if event.Kind == "job.reaped" {
			return event.Payload
		}
	}
	return nil
}

func TestPaneSweepClosesAMergedTaskPaneAfterTheGrace(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if got := f.closed(); !slices.Equal(got, []string{pane}) {
		t.Fatalf("closed panes = %v, want [%s]", got, pane)
	}
	screen, err := os.ReadFile(filepath.Join(f.a.Jobs, "j", "pane-screen.txt"))
	if err != nil || string(screen) != "last screen of "+pane {
		t.Fatalf("saved screen = %q, %v", screen, err)
	}
	if info, err := os.Stat(filepath.Join(f.a.Jobs, "j", "pane-screen.txt")); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("screen mode = %v, %v", info, err)
	}
	if state := f.store.record(t, "j")["state"]; state != "completed" {
		t.Fatalf("hk job state = %v, want completed", state)
	}
	if payload := f.reaped("j"); payload["reason"] != "task_merged" || payload["pane_id"] != pane {
		t.Fatalf("job.reaped payload = %v", payload)
	}
	if !f.infoHas("task pane closed", "job=j", "task=42", "task_state=merged", "pane="+pane, "idle_seconds=660") {
		t.Fatalf("info lines = %q", f.info)
	}
	// Once reaped the job is not looked at again.
	f.runner.Calls = nil
	f.at(30)
	if len(f.closed()) != 0 {
		t.Fatalf("a reaped job's pane was closed again")
	}
}

func TestPaneSweepKeepsAPaneIdleLessThanTheGrace(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(5)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed after 5 idle minutes")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=grace", "idle_seconds=300") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// The grace counts from the first sweep that saw the pane not working, and any
// sweep that sees it working starts the count again. A grace counted from the
// spawn time, or one that treats a working pane as idle, closes at minute 11.
func TestPaneSweepResetsTheIdleTimeWhenThePaneWorks(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(5)
	f.status[pane] = "working"
	f.at(6)
	f.status[pane] = "idle"
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("pane closed at minute 11 although it worked at minute 6")
	}
	f.at(20)
	if len(f.closed()) != 0 {
		t.Fatalf("pane closed 9 minutes after it went idle again")
	}
	f.at(21)
	if !slices.Equal(f.closed(), []string{pane}) {
		t.Fatalf("pane not closed 10 minutes after it went idle again: %v", f.closed())
	}
}

func TestPaneSweepKeepsAPaneWhoseTaskIsStillLive(t *testing.T) {
	for _, state := range []string{"developing", "backlog", "review", "blocked", ""} {
		t.Run("state "+state, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			f.job("j", "worker", "42")
			f.tasks["42"] = state
			f.at(0)
			f.at(60)
			if len(f.closed()) != 0 || f.reaped("j") != nil {
				t.Fatalf("pane of a %q task was closed", state)
			}
		})
	}
}

func TestPaneSweepEndStateFollowsTheTaskState(t *testing.T) {
	for taskState, want := range map[string]string{"merged": "completed", "done": "completed", "dropped": "lost"} {
		t.Run(taskState, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			f.job("j", "worker", "42")
			f.tasks["42"] = taskState
			f.at(0)
			f.at(11)
			if state := f.store.record(t, "j")["state"]; state != want {
				t.Fatalf("hk job state = %v, want %s", state, want)
			}
			if payload := f.reaped("j"); payload["reason"] != "task_"+taskState {
				t.Fatalf("job.reaped payload = %v", payload)
			}
		})
	}
}

func TestPaneSweepLeavesAFinishedHKRecordAlone(t *testing.T) {
	for _, recorded := range []string{"completed", "escalated", "lost"} {
		t.Run(recorded, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			f.job("j", "worker", "42")
			f.store.records["j"]["state"] = recorded
			f.tasks["42"] = "dropped"
			f.at(0)
			f.at(11)
			if f.store.record(t, "j")["state"] != recorded || len(f.closed()) != 1 {
				t.Fatalf("record %v, closed %v", f.store.record(t, "j")["state"], f.closed())
			}
		})
	}
}

func TestPaneSweepNeverTouchesProtectedJobs(t *testing.T) {
	for name, build := range map[string]func(f *paneSweepFixture){
		"keep":    func(f *paneSweepFixture) { f.job("j", "worker", "42", map[string]any{"keep": true}) },
		"builder": func(f *paneSweepFixture) { f.job("j", "builder", "42") },
		"lead":    func(f *paneSweepFixture) { f.job("j", "lead", "42") },
		"captain": func(f *paneSweepFixture) { f.job("j", "captain", "42") },
		"no task": func(f *paneSweepFixture) { f.job("j", "worker", "") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			build(f)
			f.tasks["42"] = "merged"
			f.at(0)
			f.at(60)
			if len(f.closed()) != 0 || f.reaped("j") != nil {
				t.Fatalf("a %s job's pane was closed", name)
			}
		})
	}
}

func TestPaneSweepSweepsATesterToo(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "tester", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	if len(f.closed()) != 1 {
		t.Fatalf("tester pane not closed")
	}
}

func TestPaneSweepKeepsEveryPaneWhenTheTaskReadFails(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.taskErr = errors.New("hk down")
	f.at(0)
	f.at(60)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed although the task could not be read")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=task-read-failed") {
		t.Fatalf("info lines = %q", f.info)
	}
	// The failure is not remembered as idle time: once hk answers, the pane
	// closes at the next sweep that finds it past the grace.
	f.taskErr = nil
	f.at(61)
	if len(f.closed()) != 1 {
		t.Fatalf("pane not closed once the task read recovered")
	}
}

func TestPaneSweepKeepsAPaneWhoseIdentityDoesNotMatchTheJob(t *testing.T) {
	for name, change := range map[string]func(f *paneSweepFixture, pane string){
		"renamed":  func(f *paneSweepFixture, pane string) { f.names[pane] = "somebody-else" },
		"gone":     func(f *paneSweepFixture, pane string) { delete(f.status, pane) },
		"blocked":  func(f *paneSweepFixture, pane string) { f.status[pane] = "blocked" },
		"unknown":  func(f *paneSweepFixture, pane string) { f.status[pane] = "unknown" },
		"working":  func(f *paneSweepFixture, pane string) { f.status[pane] = "working" },
		"no label": func(f *paneSweepFixture, pane string) { f.names[pane] = "" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			pane := f.job("j", "worker", "42")
			f.tasks["42"] = "merged"
			change(f, pane)
			f.at(0)
			f.at(60)
			if len(f.closed()) != 0 || f.reaped("j") != nil {
				t.Fatalf("pane closed although it is %s", name)
			}
		})
	}
}

// Each job is judged alone: a live job on the same task, such as a fix author,
// does not keep this job's pane open, and this job's close does not touch it.
func TestPaneSweepJudgesEachJobOnItsOwn(t *testing.T) {
	f := newPaneSweepFixture(t)
	first := f.job("a", "worker", "42")
	second := f.job("b", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.status[second] = "working"
	f.at(11)
	if !slices.Equal(f.closed(), []string{first}) {
		t.Fatalf("closed %v, want only %s", f.closed(), first)
	}
	if f.reaped("b") != nil {
		t.Fatalf("the working job was reaped")
	}
	if f.taskRead != 2 {
		t.Fatalf("task read %d times for one task over two sweeps, want one per sweep", f.taskRead)
	}
}

func TestPaneSweepKeepsThePaneWhenItsScreenCannotBeSaved(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && len(c.Args) > 1 && c.Args[1] == "read" {
			return Result{Code: 1}
		}
		return base(c)
	}
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("pane closed without its screen saved")
	}
	if !f.infoHas("reason=screen-not-saved") {
		t.Fatalf("info lines = %q", f.info)
	}
}

func TestPaneSweepKeepsEveryPaneWhenHerdrCannotBeRead(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	base := f.runner.Do
	f.runner.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Code: 1}
		}
		return base(c)
	}
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("pane closed with herdr unreadable")
	}
}

func TestPaneSweepLogsIdsNeverScreenText(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "worker", "42")
	f.tasks["42"] = "merged"
	f.at(0)
	f.at(11)
	for _, line := range f.info {
		if strings.Contains(line, "last screen") {
			t.Fatalf("screen text in log line %q", line)
		}
	}
}
