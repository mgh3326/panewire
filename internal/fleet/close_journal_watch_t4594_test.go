package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func shortOwnershipTTL(t *testing.T) {
	t.Helper()
	previous := closeOpOwnershipTTL
	closeOpOwnershipTTL = 30 * time.Minute
	t.Cleanup(func() { closeOpOwnershipTTL = previous })
}

// jobAttempt is the attempt key the watcher computes for job.
func jobAttempt(t *testing.T, a *App, job string) string {
	t.Helper()
	j, err := a.job(job)
	if err != nil {
		t.Fatal(err)
	}
	if key := AttemptKey(j.Events); key != "" {
		return key
	}
	t.Fatal("job has no attempt")
	return ""
}

// seedCloseOp journals op against job's current attempt (unless op names one).
func seedCloseOp(t *testing.T, a *App, job string, op CloseOp) {
	t.Helper()
	if op.Attempt == "" {
		op.Attempt = jobAttempt(t, a, job)
	}
	root, err := os.OpenRoot(filepath.Join(a.Jobs, job))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := WriteCloseOp(root, op); err != nil {
		t.Fatal(err)
	}
}

func closedOp(now time.Time) CloseOp {
	return CloseOp{RequestID: "hub-close-1", PaneID: "w:p1", State: CloseOpClosed, Created: now, Updated: now}
}

// A pane the hub closed through a journalled close operation is the hub's to
// end: the watcher's missing-pane fallback must not write a plain lost record
// or release the lease while the operation is unreleased (only the ownership
// backstop lets it act), and acts at once on a released one (hk 4594).
func TestWatcherMissingPaneDefersToOpenCloseOp(t *testing.T) {
	shortOwnershipTTL(t)
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "7200")
	for _, released := range []bool{false, true} {
		a := testApp(t)
		ledgerRunner(t, a)
		seedJob(t, a, "j", "implementer")
		seedLedger(t, a, "j")
		// hk shows the job live: a released close with no hub record left
		// the ending to the watcher.
		a.JobStore.(*fakeJobStore).records["j"] = map[string]any{"state": "spawned"}
		now := a.Now().UTC()
		op := closedOp(now)
		if released {
			op.Released = &now
		}
		seedCloseOp(t, a, "j", op)
		started := a.Now()
		if e := a.watch(context.Background(), []string{"j"}); e != nil {
			t.Fatal(e)
		}
		if kinds := eventKindsOf(t, a, "j"); !slices.Contains(kinds, "job.lost") {
			t.Fatalf("released=%v: no loss recorded: %v", released, kinds)
		}
		waited := a.Now().Sub(started)
		if !released && waited < closeOpOwnershipTTL {
			t.Fatalf("watcher ended the job after %v under an open close operation", waited)
		}
		if released && waited >= closeOpOwnershipTTL {
			t.Fatalf("a released operation still held the watcher back for %v", waited)
		}
	}
}

// The deadline does not end the watch while a close operation owns the
// ending, nor once it is released or past its backstop before the settling
// has run: the watch settles the closed attempt and frees the lease.
func TestWatcherSettlesACloseAcrossTheDeadline(t *testing.T) {
	shortOwnershipTTL(t)
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "1")
	for _, released := range []bool{false, true} {
		a := testApp(t)
		ledgerRunner(t, a)
		seedJob(t, a, "j", "implementer")
		seedLedger(t, a, "j")
		// hk shows the job live: a released close with no hub record left
		// the ending to the watcher.
		a.JobStore.(*fakeJobStore).records["j"] = map[string]any{"state": "spawned"}
		now := a.Now().UTC()
		op := closedOp(now)
		if released {
			op.Released = &now
		}
		seedCloseOp(t, a, "j", op)
		started := a.Now()
		if e := a.watch(context.Background(), []string{"j"}); e != nil {
			t.Fatal(e)
		}
		if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
			t.Fatalf("released=%v: watch left after %v with the lease %s", released, a.Now().Sub(started), st)
		}
		if waited := a.Now().Sub(started); !released && waited < closeOpOwnershipTTL {
			t.Fatalf("watcher settled after %v under an open close operation", waited)
		}
	}
}

// A rearmed watcher that never sees the pane still settles an attempt whose
// pane a released operation closed.
func TestRearmedWatcherSettlesAJournalledClose(t *testing.T) {
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "30")
	a := testApp(t)
	f := ledgerRunner(t, a)
	seedJob(t, a, "j", "implementer")
	seedLedger(t, a, "j")
	original := f.Do
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			return Result{Out: `{"result":{"agents":[]}}`}
		}
		return original(c)
	}
	a.JobStore.(*fakeJobStore).records["j"] = map[string]any{"state": "lost"}
	now := a.Now().UTC()
	op := closedOp(now)
	op.Released = &now
	seedCloseOp(t, a, "j", op)
	if err := a.watch(context.Background(), []string{"j", "--rearmed"}); err != nil {
		t.Fatal(err)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("rearmed watcher kept the lease of a closed attempt: %s", st)
	}
}

// An announced hand-over's grace fallback also waits for an open close
// operation: the hub writes the record with its successor pointer. Only the
// ownership backstop lets the fallback act without the hub's release.
func TestWatcherHandOffGraceDefersToOpenCloseOp(t *testing.T) {
	shortOwnershipTTL(t)
	a, _, h, _ := quotaStopFixture(t, "implementer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = 2 * time.Minute
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "600")
	reset := a.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	if e := a.event(context.Background(), "j", "job.lost", map[string]any{"pane_id": "w:p1", "reason": "quota_stop", "pool": "claude", "reset_at": reset}); e != nil {
		t.Fatal(e)
	}
	now := a.Now().UTC()
	seedCloseOp(t, a, "j", CloseOp{RequestID: "hub-close-1", PaneID: "w:p1", State: CloseOpClosing, Created: now, Updated: now})
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if waited := a.Now().Sub(started); waited < closeOpOwnershipTTL {
		t.Fatalf("grace fallback ended the job after %v under an open close operation", waited)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" {
		t.Fatalf("backstop did not end the job: %v", rec)
	}
}

// Once the hub released its close after writing the record itself, the
// vanished pane costs only the lease: no plain lost write, no second job.lost.
// While hk cannot be read the watcher waits instead of guessing the job live.
func TestWatcherAfterReleasedCloseOnlyReleasesTheLease(t *testing.T) {
	a, f, h, _ := quotaStopFixture(t, "reviewer", "idle", "done\n")
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	h.records["j"]["state"] = "lost"
	h.records["j"]["end_reason"] = "quota_stop pool=claude"
	now := a.Now().UTC()
	op := closedOp(now)
	op.Released = &now
	seedCloseOp(t, a, "j", op)
	do := f.Do
	reads := 0
	f.Do = func(c Command) Result {
		if c.Name == "herdr" && strings.Join(c.Args, " ") == "agent list" {
			reads++
			if reads > 1 {
				return Result{Out: `{"result":{"agents":[]}}`}
			}
		}
		return do(c)
	}
	h.failGets = 5
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if kinds := eventKindsOf(t, a, "j"); slices.Contains(kinds, "job.lost") {
		t.Fatalf("a second loss after the hub ended the job: %v", kinds)
	}
	if got := str(h.record(t, "j"), "end_reason"); got != "quota_stop pool=claude" {
		t.Fatalf("hub's record rewritten: %q", got)
	}
	if st, pools := ledgerState(t, a, "j"); st != "released" || len(pools) != 0 {
		t.Fatalf("lease not released: %s %v", st, pools)
	}
	if h.failGets != 0 {
		t.Fatalf("watcher settled without reading hk: %d failures left", h.failGets)
	}
}

// The journal lock makes a watcher's ending and a node's registration
// exclusive: once the watcher committed its ending of an attempt, a
// registration for it is preempted; once an operation owns it, the watcher
// does not commit.
func TestCommitWatcherEndingExcludesRegistration(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	ok, err := CommitWatcherEnding(dir, "w:p1", "a1", now)
	if err != nil || !ok {
		t.Fatalf("commit=%v err=%v", ok, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	for _, test := range []struct {
		pane, attempt string
		want          bool
	}{{"w:p1", "a1", true}, {"w:p1", "a2", false}, {"w:p2", "a1", false}} {
		if got, err := WatcherEnded(root, test.pane, test.attempt); err != nil || got != test.want {
			t.Fatalf("ended(%s,%s)=%v err=%v", test.pane, test.attempt, got, err)
		}
	}

	other := t.TempDir()
	otherRoot, err := os.OpenRoot(other)
	if err != nil {
		t.Fatal(err)
	}
	defer otherRoot.Close()
	if err := WriteCloseOp(otherRoot, CloseOp{RequestID: "r1", PaneID: "w:p1", Attempt: "a1", State: CloseOpRegistered, Created: now, Updated: now}); err != nil {
		t.Fatal(err)
	}
	if ok, err := CommitWatcherEnding(other, "w:p1", "a1", now); err != nil || ok {
		t.Fatalf("watcher committed under an owning operation: %v %v", ok, err)
	}

	// The lock is exclusive across holders.
	unlock, err := LockCloseOps(root)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = CommitWatcherEnding(dir, "w:p1", "a1", now)
	}()
	select {
	case <-done:
		t.Fatal("watcher committed while the registration lock was held")
	case <-time.After(100 * time.Millisecond): // window proving the commit waits
	}
	unlock()
	<-done
}

// Which journal entries hold the watcher back: an unreleased close of this
// attempt on this pane in any state but refused, cancelled or preempted,
// within the ownership backstop; an unreadable journal holds it back too.
func TestCloseOpOwnership(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	released := now
	cases := []struct {
		name string
		op   *CloseOp
		want bool
	}{
		{"no journal", nil, false},
		{"registered", &CloseOp{State: CloseOpRegistered, Updated: now}, true},
		{"closing", &CloseOp{State: CloseOpClosing, Updated: now}, true},
		{"closed", &CloseOp{State: CloseOpClosed, Updated: now}, true},
		{"gone", &CloseOp{State: CloseOpGone, Updated: now}, true},
		{"unknown", &CloseOp{State: CloseOpUnknown, Updated: now}, true},
		{"refused", &CloseOp{State: CloseOpRefused, Updated: now}, false},
		{"preempted", &CloseOp{State: CloseOpPreempted, Updated: now}, false},
		{"released", &CloseOp{State: CloseOpClosed, Updated: now, Released: &released}, false},
		{"past backstop", &CloseOp{State: CloseOpClosed, Updated: now.Add(-CloseOpOwnershipTTL)}, false},
		{"cancelled", &CloseOp{State: CloseOpCancelled, Updated: now, Released: &released}, false},
		{"another attempt's pane", &CloseOp{PaneID: "w:p0", State: CloseOpClosed, Updated: now}, false},
		{"an earlier attempt on the same pane", &CloseOp{Attempt: "a0", State: CloseOpClosed, Updated: now}, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			if test.op != nil {
				root, err := os.OpenRoot(dir)
				if err != nil {
					t.Fatal(err)
				}
				op := *test.op
				op.RequestID, op.Created = "r1", now
				if op.PaneID == "" {
					op.PaneID = "w:p1"
				}
				if op.Attempt == "" {
					op.Attempt = "a1"
				}
				if err := WriteCloseOp(root, op); err != nil {
					t.Fatal(err)
				}
				root.Close()
				info, err := os.Stat(filepath.Join(dir, CloseOpDir, "r1.json"))
				if err != nil || info.Mode().Perm() != 0o600 {
					t.Fatalf("journal file mode: %v %v", info, err)
				}
			}
			if got := ReadCloseOpState(dir, "w:p1", "a1", now).Owns; got != test.want {
				t.Fatalf("owns=%v want %v", got, test.want)
			}
		})
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, CloseOpDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, CloseOpDir, "r1.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !ReadCloseOpState(dir, "w:p1", "a1", now).Owns {
		t.Fatal("an unreadable journal must hold the watcher back")
	}
}

// An operation id never begins with a dot: those names are the journal's own
// files, which the watcher's scan skips.
func TestCloseOpIDsNeverCollideWithJournalFiles(t *testing.T) {
	for _, id := range []string{".op-1", ".lock", ".watcher-v1", "", "a/b", strings.Repeat("a", 129)} {
		if ValidCloseOpID(id) {
			t.Fatalf("%q accepted", id)
		}
	}
	for _, id := range []string{"op-1", "a.b", "-x", strings.Repeat("a", 128)} {
		if !ValidCloseOpID(id) {
			t.Fatalf("%q refused", id)
		}
	}
	if err := WriteCloseOp(nil, CloseOp{RequestID: ".op-1"}); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dot id written: %v", err)
	}
}

// A reviewer stopped on its quota screen is ended through its close intent;
// while a close operation owns the attempt, that terminal write waits for it
// (here, until the backstop) instead of pre-empting the hub's record.
func TestWatcherReviewerQuotaIntentDefersToOpenCloseOp(t *testing.T) {
	shortOwnershipTTL(t)
	screen := "❭ [pw-x] Read the brief and review the PR\n\n" +
		"▲ Something went wrong — Reached free model rate limit … Your limit will reset in 13 hours 9 minutes (at 22:01 UTC). Send a message to retry\n\n" +
		"──────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n──────────\n  SWE-2 max\n"
	a, _, h, _ := quotaStopFixture(t, "reviewer", "idle", screen)
	a.QuotaHandOffGrace = QuotaStopHandOffGrace
	t.Setenv("PANEWIRE_COMPLETION_TIMEOUT_S", "6000")
	now := a.Now().UTC()
	seedCloseOp(t, a, "j", CloseOp{RequestID: "hub-close-1", PaneID: "w:p1", State: CloseOpRegistered, Created: now, Updated: now})
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if waited := a.Now().Sub(started); waited < closeOpOwnershipTTL {
		t.Fatalf("quota intent ended the reviewer after %v under an open close operation", waited)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" {
		t.Fatalf("backstop did not end the reviewer: %v", rec)
	}
}

// The unanswered-decision ending waits for an owning close operation too.
func TestWatcherDecisionTimeoutDefersToOpenCloseOp(t *testing.T) {
	shortOwnershipTTL(t)
	a, _, report := watchFixture(t)
	writeReport(t, report, "draft\n")
	putLaneEvent(t, a, "w:p1", "ESC | question", a.Now())
	t.Setenv("PANEWIRE_DECISION_TIMEOUT_S", "15")
	now := a.Now().UTC()
	seedCloseOp(t, a, "j", CloseOp{RequestID: "hub-close-1", PaneID: "w:p1", State: CloseOpRegistered, Created: now, Updated: now})
	started := a.Now()
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	events, err := a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Kind == "job.lost" && str(ev.Payload, "reason") == "decision_unanswered" && a.Now().Sub(started) < closeOpOwnershipTTL {
			t.Fatalf("decision timeout ended the job under an open close operation")
		}
	}
}

// Completion stays authoritative under a close operation, and leaves the
// tombstone that preempts a later registration for the attempt.
func TestWatcherCompletionLeavesTheTombstone(t *testing.T) {
	a, _, report := watchFixture(t)
	writeReport(t, report, "done\n")
	putLaneEvent(t, a, "w:p1", "READY", a.Now())
	if err := a.watch(context.Background(), []string{"j"}); err != nil {
		t.Fatal(err)
	}
	if kinds := eventKindsOf(t, a, "j"); !slices.Contains(kinds, "job.completed") {
		t.Fatalf("not completed: %v", kinds)
	}
	root, err := os.OpenRoot(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if ended, err := WatcherEnded(root, "w:p1", jobAttempt(t, a, "j")); err != nil || !ended {
		t.Fatalf("completion left no tombstone: %v %v", ended, err)
	}
}
