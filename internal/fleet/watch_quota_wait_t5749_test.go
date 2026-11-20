package fleet

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quotastop"
)

// The watcher's side of a reviewer's short-reset wait (hk 5749): it adopts a wait
// the quota-stop manager requested for its current attempt, acknowledges it in
// the ledger, and extends its completion deadline for that wait only.

// ledgerEvent appends an event through the ledger, which numbers it after every
// event already there (putEvent numbers by file count, which the ledger's own
// sequence can pass).
func ledgerEvent(t *testing.T, a *App, kind string, payload map[string]any) {
	t.Helper()
	if err := a.event(context.Background(), "j", kind, payload); err != nil {
		t.Fatal(err)
	}
}

func countKind(kinds []string, kind string) (n int) {
	for _, k := range kinds {
		if k == kind {
			n++
		}
	}
	return
}

// requestWait writes the manager's request for the fixture job's current attempt.
func requestWait(t *testing.T, a *App, in time.Duration) (attempt string, until time.Time) {
	t.Helper()
	attempt, err := CurrentAttempt(a.Jobs, "j")
	if err != nil {
		t.Fatal(err)
	}
	until = a.Now().Add(in)
	stop := quotastop.Stop{Pool: "devin-free", Reset: a.Now().Add(in - QuotaReviewerWaitGrace)}
	sent, err := RequestQuotaWait(context.Background(), a.Jobs, "j", "w:p1", attempt, stop, until)
	if err != nil || !sent {
		t.Fatalf("request sent=%v err=%v", sent, err)
	}
	return attempt, until
}

// A watcher that adopted a wait holds the job past its ordinary completion
// deadline, and the ack names the attempt and a deadline that reaches the wait.
// Without the extension it would exit after the 60 s of the fixture.
func TestWatcherAdoptsARequestedWaitAndHoldsTheJobForIt(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	attempt, until := requestWait(t, a, 10*time.Minute)
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if held := a.Now().Sub(started); held < 10*time.Minute {
		t.Fatalf("the watcher exited after %v, before the adopted wait (10m) was over", held)
	}
	evs, err := a.events("j")
	if err != nil {
		t.Fatal(err)
	}
	var acks int
	for _, ev := range evs {
		if ev.Kind != quotaWaitAdoptedKind {
			continue
		}
		acks++
		if str(ev.Payload, "attempt") != attempt || str(ev.Payload, "until") != until.UTC().Format(time.RFC3339) {
			t.Fatalf("ack %v does not name attempt %q and until %s", ev.Payload, attempt, until)
		}
		if deadline, err := time.Parse(time.RFC3339Nano, str(ev.Payload, "deadline")); err != nil || deadline.Before(until) {
			t.Fatalf("ack deadline %q does not reach the wait's until %s: %v", str(ev.Payload, "deadline"), until, err)
		}
	}
	if acks != 1 {
		t.Fatalf("acks=%d, want one", acks)
	}
	if got, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until); err != nil || got.IsZero() {
		t.Fatalf("the manager does not read the ack as adopted: %v %v", got, err)
	}
}

// The deadline moves only for an adopted wait: with no request the watcher exits
// at its ordinary deadline, as before.
func TestWatcherWithoutARequestKeepsItsOrdinaryDeadline(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if held := a.Now().Sub(started); held > 2*time.Minute {
		t.Fatalf("the watcher held the job for %v with no wait requested", held)
	}
	if slices.Contains(eventKindsOf(t, a, "j"), quotaWaitAdoptedKind) {
		t.Fatal("an ack was written with no request")
	}
}

// A request for another attempt is not this watcher's to adopt: no ack, no
// extended deadline.
func TestWatcherIgnoresAWaitRequestedForAnotherAttempt(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	until := a.Now().Add(10 * time.Minute)
	ledgerEvent(t, a, quotaWaitRequestKind, map[string]any{"pane_id": "w:p1", "attempt": "0000-some-other-attempt", "pool": "devin-free", "until": until.UTC().Format(time.RFC3339)})
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if held := a.Now().Sub(started); held > 2*time.Minute {
		t.Fatalf("the watcher extended its deadline (%v) for another attempt's wait", held)
	}
	if slices.Contains(eventKindsOf(t, a, "j"), quotaWaitAdoptedKind) {
		t.Fatal("an ack was written for another attempt's request")
	}
}

// A stop already announced for the hub is not adopted as a wait, even by a
// request that was written before the announcement.
func TestWatcherDoesNotAdoptAWaitOnceTheStopIsAnnounced(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	a.QuotaHandOffGrace = time.Minute
	attempt, err := CurrentAttempt(a.Jobs, "j")
	if err != nil {
		t.Fatal(err)
	}
	until := a.Now().Add(10 * time.Minute)
	ledgerEvent(t, a, quotaWaitRequestKind, map[string]any{"pane_id": "w:p1", "attempt": attempt, "pool": "devin-free", "until": until.UTC().Format(time.RFC3339)})
	announceQuotaStopEvent(t, a, "devin-free")
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if slices.Contains(eventKindsOf(t, a, "j"), quotaWaitAdoptedKind) {
		t.Fatal("a wait was adopted for a stop already announced")
	}
}

// A watcher that starts after an earlier one adopted the wait finds the ack in
// the ledger and holds the job for it without acknowledging a second time.
func TestRestartedWatcherInheritsAnAdoptedWait(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	attempt, err := CurrentAttempt(a.Jobs, "j")
	if err != nil {
		t.Fatal(err)
	}
	until := a.Now().Add(10 * time.Minute)
	ledgerEvent(t, a, quotaWaitRequestKind, map[string]any{"pane_id": "w:p1", "attempt": attempt, "pool": "devin-free", "until": until.UTC().Format(time.RFC3339)})
	ledgerEvent(t, a, quotaWaitAdoptedKind, map[string]any{"pane_id": "w:p1", "attempt": attempt, "pool": "devin-free", "until": until.UTC().Format(time.RFC3339), "deadline": until.Add(time.Minute).UTC().Format(time.RFC3339Nano)})
	started := a.Now()
	if e := a.watch(context.Background(), []string{"j"}); e != nil {
		t.Fatal(e)
	}
	if held := a.Now().Sub(started); held < 10*time.Minute {
		t.Fatalf("the restarted watcher exited after %v, inside the adopted wait", held)
	}
	if n := countKind(eventKindsOf(t, a, "j"), quotaWaitAdoptedKind); n != 1 {
		t.Fatalf("acks=%d, want the one the earlier watcher wrote", n)
	}
}

// QuotaWaitAckedAt accepts only an ack for the attempt the manager observed and
// the until it asked for, whose deadline reaches that until, while the attempt
// is current and not ended.
func TestQuotaWaitAckedAtNeedsAnAckForThisAttemptAndWait(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	attempt, until := requestWait(t, a, 10*time.Minute)
	adopted := func(attempt string, until time.Time) bool {
		t.Helper()
		got, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until)
		if err != nil {
			t.Fatal(err)
		}
		return !got.IsZero()
	}
	if adopted(attempt, until) {
		t.Fatal("a request alone was read as adopted")
	}
	ack := func(attempt string, until, deadline time.Time) {
		ledgerEvent(t, a, quotaWaitAdoptedKind, map[string]any{"pane_id": "w:p1", "attempt": attempt, "pool": "devin-free", "until": until.UTC().Format(time.RFC3339), "deadline": deadline.UTC().Format(time.RFC3339Nano)})
	}
	ack(attempt, until.Add(time.Minute), until.Add(time.Hour))
	if adopted(attempt, until) {
		t.Fatal("an ack for another until was read as adopted")
	}
	ack(attempt, until, until.Add(-time.Second))
	if adopted(attempt, until) {
		t.Fatal("an ack whose deadline falls short of the wait was read as adopted")
	}
	ack(attempt, until, until.Add(time.Minute))
	if !adopted(attempt, until) {
		t.Fatal("the ack for this attempt and wait was not read as adopted")
	}
	if adopted("0000-another-attempt", until) {
		t.Fatal("the ack was read as adopted for another attempt")
	}
	announceQuotaStopEvent(t, a, "devin-free")
	if adopted(attempt, until) {
		t.Fatal("an ack was read as adopted after the attempt ended")
	}
}

// A request for an attempt that is no longer the current one writes nothing and
// says so.
func TestRequestQuotaWaitRefusesASupersededAttempt(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	sent, err := RequestQuotaWait(context.Background(), a.Jobs, "j", "w:p1", "0000-stale-attempt", quotastop.Stop{Pool: "devin-free"}, a.Now().Add(time.Minute))
	if err != nil || sent {
		t.Fatalf("sent=%v err=%v, want a refusal with no error", sent, err)
	}
	if slices.Contains(eventKindsOf(t, a, "j"), quotaWaitRequestKind) {
		t.Fatal("a request was written for a stale attempt")
	}
}

// The same request, written again (the manager restarted before it noted the
// first), is the same request: one event, and the ack stands.
func TestRequestQuotaWaitIsIdempotentAndKeepsTheAck(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	attempt, until := requestWait(t, a, 10*time.Minute)
	ledgerEvent(t, a, quotaWaitAdoptedKind, map[string]any{"pane_id": "w:p1", "attempt": attempt, "pool": "devin-free", "until": until.UTC().Format(time.RFC3339), "deadline": until.Add(time.Minute).UTC().Format(time.RFC3339Nano)})
	stop := quotastop.Stop{Pool: "devin-free", Reset: until.Add(-QuotaReviewerWaitGrace)}
	if sent, err := RequestQuotaWait(context.Background(), a.Jobs, "j", "w:p1", attempt, stop, until); err != nil || !sent {
		t.Fatalf("the repeated request: sent=%v err=%v", sent, err)
	}
	if n := countKind(eventKindsOf(t, a, "j"), quotaWaitRequestKind); n != 1 {
		t.Fatalf("requests=%d, want the one", n)
	}
	if at, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until); err != nil || at.IsZero() {
		t.Fatalf("the ack no longer stands after a repeated request: %v %v", at, err)
	}
	// Even a request event that did get written twice keeps the ack.
	ledgerEvent(t, a, quotaWaitRequestKind, map[string]any{"pane_id": "w:p1", "attempt": attempt, "pool": "devin-free", "until": until.UTC().Format(time.RFC3339)})
	if at, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until); err != nil || at.IsZero() {
		t.Fatalf("a duplicate request event cleared the ack: %v %v", at, err)
	}
}

// A watcher is live on positive evidence only: its own life-long lock. The watch
// lock alone is also held by the hub's close fence, and a hub close intent open in
// the ledger owns the pane.
func TestQuotaWatcherLiveNeedsThePositiveEvidence(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	hold := func(name string) {
		t.Helper()
		f, err := secureOpen(filepath.Join(a.Jobs, "j", name), true, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !tryLock(f) {
			t.Fatalf("%s already held", name)
		}
		t.Cleanup(func() { f.Close() })
	}
	if QuotaWatcherLive(a.Jobs, "j") {
		t.Fatal("live with no watcher")
	}
	hold(jobWatchLock)
	if QuotaWatcherLive(a.Jobs, "j") {
		t.Fatal("the watch lock alone (the hub's fence holds it too) was read as a live watcher")
	}
	hold(hubCloseWatchLock)
	if !QuotaWatcherLive(a.Jobs, "j") {
		t.Fatal("a watcher holding its life-long lock was not read as live")
	}
	// The attempt the intent binds to is a spawn that names the pane and label.
	ledgerEvent(t, a, "job.spawned", map[string]any{"pane_id": "w:p1", "label": "agent-a", "role": "reviewer"})
	if err := a.recordHubCloseIntent(context.Background(), "j", HubCloseIntent{RequestID: "close-1", Attempt: currentAttemptName(t, a), Pane: "w:p1", Label: "agent-a", EndReason: hubCloseReason}); err != nil {
		t.Fatal(err)
	}
	if QuotaWatcherLive(a.Jobs, "j") {
		t.Fatal("a watcher was read as live with a hub close open")
	}
}
