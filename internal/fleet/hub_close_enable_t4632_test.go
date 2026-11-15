package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #572 r4576225999: the node journalled closed, then `panewire job close`
// wrote job.revoked before settlement ran. The close is still settled: hk
// takes the intended lost record and the lease is released; the revoke stays
// the job's local outcome. The daemon pass lists the job.
func TestSettlementFinishesAClosedOpAfterAnIndependentEnd_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	jobs, e := PendingHubCloseJobs(a.Jobs)
	if e != nil || len(jobs) != 1 || jobs[0] != "j" {
		t.Fatalf("pending = %v, %v; want [j]", jobs, e)
	}
	settle := HubCloseSettlement{App: a}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk = %v, want the intended lost record", rec)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
	if j, _ := a.job("j"); j.Ended != "job.revoked" {
		t.Fatalf("job ended %q, want the revoke kept as its outcome", j.Ended)
	}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseNone {
		t.Fatalf("second settle = %v, %v; want none", st, e)
	}
	if jobs, _ := PendingHubCloseJobs(a.Jobs); len(jobs) != 0 {
		t.Fatalf("pending after settle = %v", jobs)
	}
}

// An independent end over an operation that closed nothing (or was never
// started) owes nothing: no settlement, no hk write.
func TestSettlementIgnoresAnEndedIntentThatClosedNothing_t4632(t *testing.T) {
	for _, state := range []string{"", CloseOpRegistered, CloseOpRefused, CloseOpPreempted} {
		t.Run("state="+state, func(t *testing.T) {
			a, h := hubCloseFixture(t)
			in := liveIntent(t, a)
			recordHubClose(t, a, in)
			if state != "" {
				writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: state})
			}
			if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
				t.Fatal(e)
			}
			if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseNone {
				t.Fatalf("settle = %v, %v; want none", st, e)
			}
			if str(h.record(t, "j"), "end_reason") == hubCloseReason {
				t.Fatal("hk took the intended record for a close that closed nothing")
			}
		})
	}
}

// #572 r4576226003: the node settled the close after the watcher took its
// snapshot. The watcher's stale decision, re-checked under the end lock, is
// dropped rather than written after the final job.lost.
func TestWatcherDropsADecisionStaleAfterASettledClose_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	ran := false
	deferred, e := a.endUnlessHubClose("j", func(Job) error { ran = true; return nil })
	if e != nil || !deferred || ran {
		t.Fatalf("endUnlessHubClose = %v, %v (ran %v); want the stale decision dropped", deferred, e, ran)
	}
}

// A cancelled intent hands the decision back: it runs.
func TestWatcherRunsADecisionAfterACancelledIntent_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	if e := a.recordHubCloseCancelForTest(in.RequestID); e != nil {
		t.Fatal(e)
	}
	ran := false
	deferred, e := a.endUnlessHubClose("j", func(Job) error { ran = true; return nil })
	if e != nil || deferred || !ran {
		t.Fatalf("endUnlessHubClose = %v, %v (ran %v); want the decision run", deferred, e, ran)
	}
}

func (a *App) recordHubCloseCancelForTest(id string) error {
	return a.eventSynced(context.Background(), "j", hubCloseCancelKind, map[string]any{"request_id": id, "reason": "released"})
}

// The barrier step refuses while an old-binary watcher or a spawn launcher
// runs, and otherwise records a mode-0600 marker that CloseOpsReady reads.
func TestCloseOpsBarrier_t4632(t *testing.T) {
	jobs := t.TempDir()
	if e := os.MkdirAll(filepath.Join(jobs, "j1", watchCapsDir), 0o700); e != nil {
		t.Fatal(e)
	}
	list := func(out string) ProcessLister {
		return func(context.Context) ([]byte, error) { return []byte(withStart(out)), nil }
	}
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	if CloseOpsReady(ctx, jobs, "v1", list("")) {
		t.Fatal("ready before the step")
	}
	oldWatcher := "4242 panewire _watch j1\n"
	if e := EnableCloseOps(ctx, jobs, "v1", now, list(oldWatcher)); !errors.Is(e, ErrCloseOpsNotReady) {
		t.Fatalf("enable with an old watcher = %v", e)
	}
	if e := EnableCloseOps(ctx, jobs, "v1", now, list("4343 /usr/local/bin/panewire _spawn-stdin\n")); !errors.Is(e, ErrCloseOpsNotReady) {
		t.Fatalf("enable with a launcher = %v", e)
	}
	if _, e := os.Stat(filepath.Join(jobs, CloseOpsMarker)); !errors.Is(e, os.ErrNotExist) {
		t.Fatalf("a refused step left a marker: %v", e)
	}
	// A watcher of this binary carries its token.
	if e := os.WriteFile(filepath.Join(jobs, "j1", watchCapsDir, "4242"), []byte(watcherCapsLine(testStart)), 0o600); e != nil {
		t.Fatal(e)
	}
	self := strconv.Itoa(os.Getpid()) + " /usr/local/bin/panewire _spawn-stdin\n"
	if e := EnableCloseOps(ctx, jobs, "v1", now, list(oldWatcher+self)); e != nil {
		t.Fatalf("enable = %v", e)
	}
	info, e := os.Stat(filepath.Join(jobs, CloseOpsMarker))
	if e != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("marker %v, %v; want mode 0600", info, e)
	}
	if !CloseOpsReady(ctx, jobs, "v1", list(oldWatcher)) {
		t.Fatal("not ready after the step")
	}
	// An old-binary watcher that starts later takes readiness away again.
	if CloseOpsReady(ctx, jobs, "v1", list("5151 panewire _watch j1\n")) {
		t.Fatal("ready with a token-less watcher")
	}
	if CloseOpsReady(ctx, jobs, "v1", func(context.Context) ([]byte, error) { return nil, errors.New("ps failed") }) {
		t.Fatal("ready on a failed listing")
	}
}

// Every verb that can start a watcher holds the barrier closed, the marker
// names its binary version, both checks read one listing, and a refused step
// removes an earlier marker (#593 review).
func TestCloseOpsBarrierFencesEveryLaunchPath_t4632(t *testing.T) {
	jobs := t.TempDir()
	ctx := context.Background()
	now := time.Now()
	calls := 0
	once := func(out string) ProcessLister {
		return func(context.Context) ([]byte, error) { calls++; return []byte(withStart(out)), nil }
	}
	if e := EnableCloseOps(ctx, jobs, "v1", now, once("")); e != nil || calls != 1 {
		t.Fatalf("enable = %v after %d listings, want one listing", e, calls)
	}
	for _, verb := range []string{"spawn", "work", "lead", "restore", "_spawn-stdin"} {
		for _, exe := range []string{"/opt/old/panewire", "/usr/local/bin/panewire.prev", "./panewire-linux-amd64", "/tmp/panewire_1.2.0_linux_amd64"} {
			line := "99 " + exe + " " + verb + " --job x\n"
			if CloseOpsReady(ctx, jobs, "v1", once(line)) {
				t.Fatalf("ready while %q runs", line)
			}
		}
	}
	t.Setenv("PANEWIRE_BIN", "/opt/custom/pw-renamed")
	if CloseOpsReady(ctx, jobs, "v1", once("99 /opt/custom/pw-renamed spawn --job x\n")) {
		t.Fatal("ready while a PANEWIRE_BIN-named launcher runs")
	}
	if !CloseOpsReady(ctx, jobs, "v1", once("99 /bin/zsh -c make spawn\n12 /usr/local/bin/panewire daemon\n")) {
		t.Fatal("not ready with only unrelated processes")
	}
	if CloseOpsReady(ctx, jobs, "v2", once("")) {
		t.Fatal("ready for another binary version")
	}
	if e := EnableCloseOps(ctx, jobs, "v1", now, once("99 /opt/old/panewire restore j\n")); !errors.Is(e, ErrCloseOpsNotReady) {
		t.Fatalf("enable with a restore = %v", e)
	}
	if CloseOpsEnabled(jobs, "v1") {
		t.Fatal("a refused step kept the earlier marker")
	}
}

// #593 review: a reap after the independent end does not retire an owed
// close; the settlement records job.hub_close_settled, never a terminal
// job.lost, so the revoke stays the job's local outcome.
func TestOwedCloseSurvivesAReapAndKeepsTheRevoke_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	for _, kind := range []string{"job.revoked", "job.reaped"} {
		if e := a.event(context.Background(), "j", kind, map[string]any{}); e != nil {
			t.Fatal(e)
		}
	}
	if jobs, e := PendingHubCloseJobs(a.Jobs); e != nil || len(jobs) != 1 {
		t.Fatalf("pending = %v, %v; want the reaped job", jobs, e)
	}
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk = %v", rec)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
	if lost := lostEvents(t, a); len(lost) != 0 {
		t.Fatalf("job.lost %v, want none after an independent end", lost)
	}
	j, _ := a.job("j")
	if j.Ended != "job.reaped" && j.Ended != "job.revoked" {
		t.Fatalf("job ended %q, want the independent end kept", j.Ended)
	}
	if jobs, _ := PendingHubCloseJobs(a.Jobs); len(jobs) != 0 {
		t.Fatalf("pending after settle = %v", jobs)
	}
}

// #593 review: the node's own settlement of B retires an older unknown A of
// the same attempt; A is never settled into a second final job.lost.
func TestOwnSettlementRetiresTheAttemptsOtherIntents_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	first := liveIntent(t, a)
	recordHubClose(t, a, first)
	writeJobCloseOp(t, a, CloseOp{RequestID: first.RequestID, Attempt: first.Attempt, State: CloseOpUnknown})
	second := first
	second.RequestID, second.EndReason = "close-2", hubCloseReason
	recordHubClose(t, a, second)
	writeJobCloseOp(t, a, CloseOp{RequestID: second.RequestID, Attempt: second.Attempt, State: CloseOpClosed})
	settle := HubCloseSettlement{App: a}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseNone {
		t.Fatalf("second settle = %v, %v; want none", st, e)
	}
	if lost := lostEvents(t, a); len(lost) != 1 || str(lost[0], "request_id") != "close-2" {
		t.Fatalf("job.lost %v, want one, for close-2", lost)
	}
	_ = h
}

// #593 review: a watcher whose attempt the node's settlement ended exits.
func TestWatcherExitsAfterASettledClose_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosed})
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := a.watch(ctx, []string{"j"}); e != nil {
		t.Fatalf("watch = %v, want a clean exit", e)
	}
	if lost := lostEvents(t, a); len(lost) != 1 {
		t.Fatalf("job.lost %v, want only the settlement's", lost)
	}
}

// #593 od-4a876257a3: a node crash left the operation closing, then a revoke
// ended the attempt. No hub repeat will come; the daemon settles it once the
// pane inventory shows the pane absent, and waits while it is listed.
func TestSettlementRecoversAClosingOpAfterAnIndependentEnd_t4632(t *testing.T) {
	a, h := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpClosing})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	listed := true
	settle := HubCloseSettlement{App: a, PaneListed: func(context.Context, string, string) (bool, error) { return listed, nil }}
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubClosePending {
		t.Fatalf("settle with the pane listed = %v, %v; want pending", st, e)
	}
	listed = false
	if st, e := settle.Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle with the pane gone = %v, %v; want settled", st, e)
	}
	if rec := h.record(t, "j"); str(rec, "state") != "lost" || str(rec, "end_reason") != hubCloseReason {
		t.Fatalf("hk = %v", rec)
	}
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("quota %s, want released", st)
	}
}

// #593 review: an owed close after an independent end fences the watcher's
// decisions (no stale re-prompt revives the job), and its settlement, which
// writes job.hub_close_settled, drops them too.
func TestOwedEndedCloseFencesWatcherDecisions_t4632(t *testing.T) {
	a, _ := hubCloseFixture(t)
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	writeJobCloseOp(t, a, CloseOp{RequestID: in.RequestID, Attempt: in.Attempt, State: CloseOpUnknown})
	if e := a.event(context.Background(), "j", "job.revoked", map[string]any{}); e != nil {
		t.Fatal(e)
	}
	ran := false
	decide := func(Job) error { ran = true; return nil }
	if deferred, e := a.endUnlessHubClose("j", decide); e != nil || !deferred || ran {
		t.Fatalf("owed: deferred=%v err=%v ran=%v; want the decision fenced", deferred, e, ran)
	}
	if st, e := (HubCloseSettlement{App: a}).Settle(context.Background(), "j"); e != nil || st != HubCloseSettled {
		t.Fatalf("settle = %v, %v", st, e)
	}
	if deferred, e := a.endUnlessHubClose("j", decide); e != nil || !deferred || ran {
		t.Fatalf("settled: deferred=%v err=%v ran=%v; want the stale decision dropped", deferred, e, ran)
	}
	if j, _ := a.job("j"); !foldHubClose(j).settled || j.Ended != "job.revoked" {
		t.Fatalf("settled=%v ended=%q", foldHubClose(j).settled, j.Ended)
	}
}

// The token's start time is the one the real ps listing prints for the same
// process, so a live watcher of this binary reads as ready on this host.
func TestWatcherTokenStartMatchesTheRealListing_t4632(t *testing.T) {
	start, e := processStartOf(context.Background(), os.Getpid())
	if e != nil {
		t.Skipf("ps cannot report a start time here: %v", e)
	}
	out, e := psProcesses(context.Background())
	if e != nil {
		t.Skipf("ps listing unavailable: %v", e)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if p, ok := parseProcessLine(line); ok && p.pid == os.Getpid() {
			if p.start != start {
				t.Fatalf("listing start %q, token start %q", p.start, start)
			}
			return
		}
	}
	t.Fatal("own process not in the listing")
}
