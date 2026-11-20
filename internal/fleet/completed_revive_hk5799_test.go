package fleet

import (
	"slices"
	"testing"
	"time"
)

// reviveFixture is a sweep fixture with job "j" completed at minute 1 on its
// pane. Its task is still live, so the sweep keeps the pane unless the revive
// acts. Watcher starts are recorded, not spawned.
func reviveFixture(t *testing.T) (*paneSweepFixture, string, *[]string) {
	t.Helper()
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42", map[string]any{"cwd": "/w/j"})
	f.cwds[pane] = "/w/j"
	f.tasks["42"] = "in_progress"
	f.now = f.now.Add(time.Minute)
	putEvent(t, f.a, "j", "job.completed", map[string]any{"pane_id": pane})
	var started []string
	old := reviveStartWatch
	reviveStartWatch = func(_ *App, id string) error { started = append(started, id); return nil }
	t.Cleanup(func() { reviveStartWatch = old })
	return f, pane, &started
}

// reprompts counts the job.reprompted events of job id.
func reprompts(t *testing.T, a *App, id string) []map[string]any {
	t.Helper()
	events, err := a.events(id)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, ev := range events {
		if ev.Kind == "job.reprompted" {
			out = append(out, ev.Payload)
		}
	}
	return out
}

// TestCompletedPaneRevivedWhenBusyAgain_hk5799: a job completes, its pane is seen
// not working, then working again on the job's own session and cwd. The sweep
// revives the job (job.reprompted completed_pane_busy), starts its watcher
// rearmed, and does not close the pane in the pass that revived it. On base no
// revive is written, so this fails there.
func TestCompletedPaneRevivedWhenBusyAgain_hk5799(t *testing.T) {
	f, pane, started := reviveFixture(t)
	f.status[pane] = "idle"
	f.at(2)
	f.status[pane] = "working"
	f.at(3)

	got := reprompts(t, f.a, "j")
	if len(got) != 1 || str(got[0], "reason") != CompletedPaneReviveReason || str(got[0], "pane_id") != pane {
		t.Fatalf("job.reprompted = %v, want one %s on %s", got, CompletedPaneReviveReason, pane)
	}
	if !slices.Equal(*started, []string{"j"}) {
		t.Fatalf("watchers started %v, want the revived job's", *started)
	}
	if slices.Contains(f.closed(), pane) {
		t.Fatalf("the pane was closed in the pass that revived it: %v", f.closed())
	}
	events, err := f.a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	if ended := viewLedger(events).Ended; ended != "" {
		t.Fatalf("revived job still ends as %q", ended)
	}
}

// TestCompletedPaneNotRevivedForAnotherSessionCwdOrJob_hk5799: a busy pane is
// the job's own only on the spawn receipt's agent session and cwd, with no
// other job bound to it since the completion. Otherwise nothing is revived.
func TestCompletedPaneNotRevivedForAnotherSessionCwdOrJob_hk5799(t *testing.T) {
	cases := map[string]func(t *testing.T, f *paneSweepFixture, pane string){
		"another agent session": func(_ *testing.T, f *paneSweepFixture, pane string) {
			f.sessions[pane] = `{"value":"sess-someone-else"}`
		},
		"another cwd": func(_ *testing.T, f *paneSweepFixture, pane string) {
			f.cwds[pane] = "/w/elsewhere"
		},
		"another job spawned on the pane": func(t *testing.T, f *paneSweepFixture, pane string) {
			f.now = f.now.Add(30 * time.Second)
			putEvent(t, f.a, "k", "job.spawned", map[string]any{"pane_id": pane, "harness_session": map[string]any{"kind": "omp", "id": "sess-k"}})
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f, pane, started := reviveFixture(t)
			f.status[pane] = "idle"
			f.at(2)
			change(t, f, pane)
			f.status[pane] = "working"
			f.at(3)
			if got := reprompts(t, f.a, "j"); len(got) != 0 {
				t.Fatalf("revived a pane that is not the job's: %v", got)
			}
			if len(*started) != 0 {
				t.Fatalf("watchers started %v for a pane that is not the job's", *started)
			}
		})
	}
}

// TestCompletedPaneReviveNeedsAnIdleObservationFirst_hk5799: a pane still working
// right after its completion (READY is emitted before the agent stops) is not
// revived. Only a pass that saw it not working after the completion arms the
// revive. A fresh sweeper, as after a node restart, starts unarmed.
func TestCompletedPaneReviveNeedsAnIdleObservationFirst_hk5799(t *testing.T) {
	f, pane, started := reviveFixture(t)
	f.status[pane] = "working"
	f.at(2)
	if got := reprompts(t, f.a, "j"); len(got) != 0 {
		t.Fatalf("revived a pane never seen idle after completion: %v", got)
	}

	f.status[pane] = "idle"
	f.at(3)
	fresh := &TaskPaneSweeper{Grace: f.sweeper.Grace, TaskState: f.sweeper.TaskState, PR: f.sweeper.PR, Info: f.sweeper.Info}
	f.sweeper = fresh
	f.status[pane] = "working"
	f.at(4)
	if got := reprompts(t, f.a, "j"); len(got) != 0 {
		t.Fatalf("a restarted sweeper revived without an idle observation of its own: %v", got)
	}

	f.status[pane] = "idle"
	f.at(5)
	f.status[pane] = "working"
	f.at(6)
	if got := reprompts(t, f.a, "j"); len(got) != 1 || len(*started) != 1 {
		t.Fatalf("after an idle observation the busy pane was not revived: reprompts=%v started=%v", got, *started)
	}
}
