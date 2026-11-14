package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const hubCloseReason = "quota_stop pool=devin-free replaced_by=j2"

// hubCloseFixture is a live reviewer job whose pane is gone from every agent
// list, so the watcher's own missing-pane fallback would end it on the second
// poll unless something defers it.
func hubCloseFixture(t *testing.T) (*App, *fakeHK) {
	t.Helper()
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", "quiet\n")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	// seedLedger records a claim after the fixture's spawn; the attempt a
	// close binds to is the spawn that follows it, as in production.
	if e := a.event(context.Background(), "j", "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "role": "reviewer"}); e != nil {
		t.Fatal(e)
	}
	do := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[]}}`}
		}
		return do(c)
	}
	return a, h
}

// currentAttemptName is the ledger file name of the job's newest attempt
// event, the token the node binds a close operation to.
func currentAttemptName(t *testing.T, a *App) string {
	t.Helper()
	j, e := a.job("j")
	if e != nil {
		t.Fatal(e)
	}
	name := ""
	for _, ev := range j.Events {
		switch ev.Kind {
		case "job.claim", "job.claimed", "job.reclaim", "job.spawned", "job.reprompted":
			name = ev.name
		}
	}
	return name
}

func recordHubClose(t *testing.T, a *App, in HubCloseIntent) {
	t.Helper()
	if e := a.recordHubCloseIntent(context.Background(), "j", in); e != nil {
		t.Fatal(e)
	}
}

func writeJobCloseOp(t *testing.T, a *App, op CloseOp) {
	t.Helper()
	root, e := os.OpenRoot(filepath.Join(a.Jobs, "j"))
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	op.PaneID = "w:p1"
	if e := WriteCloseOp(root, op); e != nil {
		t.Fatal(e)
	}
}

func lostEvents(t *testing.T, a *App) []map[string]any {
	t.Helper()
	evs, e := a.events("j")
	if e != nil {
		t.Fatal(e)
	}
	var out []map[string]any
	for _, ev := range evs {
		if ev.Kind == "job.lost" {
			out = append(out, ev.Payload)
		}
	}
	return out
}

func liveIntent(t *testing.T, a *App) HubCloseIntent {
	return HubCloseIntent{RequestID: "close-1", Attempt: currentAttemptName(t, a), Pane: "w:p1", Label: "agent-a", EndReason: hubCloseReason}
}

func wantHubCloseFinished(t *testing.T, a *App, h *fakeHK) {
	t.Helper()
	rec := h.record(t, "j")
	if str(rec, "state") != "lost" || str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk record %v, want lost with the intended end_reason", rec)
	}
	lost := lostEvents(t, a)
	if len(lost) != 1 || str(lost[0], "reason") != hubCloseLostReason || !boolean(lost[0], "final", false) || str(lost[0], "request_id") != "close-1" {
		t.Fatalf("job.lost events %v, want exactly one final hub_close", lost)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
}

// An open intent with the pane gone: no lost or failed record while the close
// is open; once the operation is final the intended record is written once.
// Dropping the deferral lets the missing-pane fallback write a bare lost.
func TestWatcherDefersToAnOpenHubClose_t4652(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpRegistered})
	polls := 0
	sleep := a.Sleep
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		polls++
		if polls < 20 {
			if s := str(h.record(t, "j"), "state"); s == "lost" || s == "failed" {
				t.Fatalf("hk record %s written while the hub close is open", s)
			}
			if lost := lostEvents(t, a); len(lost) != 0 {
				t.Fatalf("job.lost %v while the hub close is open", lost)
			}
		}
		if polls == 20 {
			writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
		}
		return sleep(ctx, d)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if polls < 20 {
		t.Fatalf("watcher ended after %d polls, before the close was final", polls)
	}
	wantHubCloseFinished(t, a, h)
}

// A final, released operation: the watcher writes the intended record once,
// and an hk record the hub already wrote identically is not rewritten.
func TestWatcherFinishesAReleasedHubClose_t4652(t *testing.T) {
	for _, hubWrote := range []bool{false, true} {
		t.Run(map[bool]string{false: "watcher writes", true: "hub wrote"}[hubWrote], func(t *testing.T) {
			a, h := hubCloseFixture(t)
			in := liveIntent(t, a)
			recordHubClose(t, a, in)
			now := a.Now()
			writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpGone, Released: &now})
			if hubWrote {
				h.mu.Lock()
				h.records["j"]["state"], h.records["j"]["end_reason"] = "lost", hubCloseReason
				h.mu.Unlock()
			}
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			wantHubCloseFinished(t, a, h)
		})
	}
}

// A watcher stopped mid-intent and re-armed reaches the same outcome.
func TestWatcherHubCloseSurvivesRestart_t4652(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosing})
	stop := errors.New("watcher stopped")
	sleep := a.Sleep
	polls := 0
	a.Sleep = func(ctx context.Context, d time.Duration) error {
		if polls++; polls == 5 {
			return stop
		}
		return sleep(ctx, d)
	}
	if e := a.watch(context.Background(), []string{"j"}); !errors.Is(e, stop) {
		t.Fatalf("first watcher = %v, want it stopped mid-intent", e)
	}
	if lost := lostEvents(t, a); len(lost) != 0 || str(h.record(t, "j"), "state") == "lost" {
		t.Fatalf("first watcher wrote an end: %v %v", lost, h.record(t, "j"))
	}
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpUnknown})
	a.Sleep = sleep
	if e := a.watch(context.Background(), []string{"j", "--rearmed"}); e != nil {
		t.Fatal(e)
	}
	wantHubCloseFinished(t, a, h)
}

// An intent bound to another attempt, pane or label is stale: the watcher
// ignores it and its own missing-pane fallback ends the job.
func TestWatcherIgnoresAStaleHubCloseIntent_t4652(t *testing.T) {
	for name, mutate := range map[string]func(*HubCloseIntent){
		"attempt": func(in *HubCloseIntent) { in.Attempt = "0000-job.claim.json" },
		"pane":    func(in *HubCloseIntent) { in.Pane = "w:p9" },
		"label":   func(in *HubCloseIntent) { in.Label = "agent-b" },
	} {
		t.Run(name, func(t *testing.T) {
			a, h := hubCloseFixture(t)
			in := liveIntent(t, a)
			mutate(&in)
			recordHubClose(t, a, in)
			writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpRegistered})
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			lost := lostEvents(t, a)
			if len(lost) != 1 || str(lost[0], "reason") == hubCloseLostReason || str(h.record(t, "j"), "end_reason") == hubCloseReason {
				t.Fatalf("stale intent was honoured: job.lost %v hk %v", lost, h.record(t, "j"))
			}
		})
	}
}

// An intent recorded before a re-prompt belongs to the old attempt.
func TestWatcherHubCloseIntentEndsWithItsAttempt_t4652(t *testing.T) {
	a, h := hubCloseFixture(t)
	recordHubClose(t, a, liveIntent(t, a))
	if e := a.event(context.Background(), "j", "job.reprompted", map[string]any{"pane_id": "w:p1"}); e != nil {
		t.Fatal(e)
	}
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if lost := lostEvents(t, a); len(lost) != 1 || str(lost[0], "reason") == hubCloseLostReason {
		t.Fatalf("job.lost %v, want the watcher's own missing-pane end", lost)
	}
	_ = h
}

// An operation that closed nothing (refused, cancelled, preempted) cancels the
// intent, and the watcher resumes its own duties.
func TestWatcherHubCloseThatClosedNothingCancels_t4652(t *testing.T) {
	for _, state := range []string{CloseOpRefused, CloseOpCancelled, CloseOpPreempted} {
		t.Run(state, func(t *testing.T) {
			a, h := hubCloseFixture(t)
			in := liveIntent(t, a)
			recordHubClose(t, a, in)
			writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: state})
			if e := a.watch(context.Background(), []string{"j"}); e != nil {
				t.Fatal(e)
			}
			if str(h.record(t, "j"), "end_reason") == hubCloseReason {
				t.Fatalf("a close that closed nothing wrote the intended record")
			}
			j, _ := a.job("j")
			if _, open := pendingHubCloseIntent(j); open {
				t.Fatal("intent still open")
			}
		})
	}
}
