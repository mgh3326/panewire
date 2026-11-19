package panewire

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 5456: the shared quota refresh a request waits on never gets a budget
// past what is left of the attempt, so the wait cannot outlive the request.
func TestSpawnQuotaRefreshBudgetStaysInsideTheRequest_t5456(t *testing.T) {
	now := time.Now()
	task := hubSpawnTask{Deadline: now.Add(90 * time.Second)}
	if got := spawnQuotaRefreshBudget(task, time.Minute, now); got != time.Minute {
		t.Fatalf("budget = %s, want the measurement window %s", got, time.Minute)
	}
	if got, want := spawnQuotaRefreshBudget(task, time.Minute, now.Add(60*time.Second)), 25*time.Second; got != want {
		t.Fatalf("budget = %s, want what is left of the attempt less the use margin (%s)", got, want)
	}
	// Nothing usable is left: the refresh is never started.
	if got := spawnQuotaRefreshBudget(task, time.Minute, now.Add(88*time.Second)); got != 0 {
		t.Fatalf("budget = %s, want 0 with only the use margin left", got)
	}
	if got := spawnQuotaRefreshBudget(task, time.Minute, now.Add(2*time.Minute)); got != 0 {
		t.Fatalf("budget = %s, want 0 past the deadline", got)
	}
	// A caller with no attempt deadline (a local spawn) keeps the window.
	if got := spawnQuotaRefreshBudget(hubSpawnTask{}, time.Minute, now); got != time.Minute {
		t.Fatalf("unbounded budget = %s, want the measurement window", got)
	}
}

// A request that reaches its limit without a candidate says what the last
// attempt tried and why each pool was dropped, and spawn status prints it
// (hk 5456 wanted 3 and 4).
func TestSpawnGivenUpNamesTriedCandidates_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// The attempt waited on one pool's refresh, which finished without making
	// the reading usable.
	if err := hub.inbox.store.noteSpawnRefreshWaits(t.Context(), request.RequestID, task.Attempt, []hubSpawnRefreshWait{{Machine: "machine-a", Pool: "codex"}}); err != nil {
		t.Fatal(err)
	}
	if err := hub.inbox.store.recordQuotaRefreshAttempt(t.Context(), "machine-a", "codex", "", time.Now()); err != nil {
		t.Fatal(err)
	}
	expired, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	expired.Deadline = time.Now().Add(-time.Second)
	expired.Expires = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), expired)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != "given_up" {
		t.Fatalf("status = %s, want given_up", final.Status)
	}
	if len(final.Outcome.Tried) != 1 || final.Outcome.Tried[0].Pool != "codex" || final.Outcome.Tried[0].Host != "machine-a" {
		t.Fatalf("tried = %+v, want the codex refresh this attempt waited on", final.Outcome.Tried)
	}
	for _, want := range []string{"no candidate could take it", "tried: codex/machine-a", "refresh finished without a usable reading"} {
		if !strings.Contains(final.Outcome.Reason, want) {
			t.Fatalf("given up reason %q does not name %q", final.Outcome.Reason, want)
		}
	}
}

// A request given up before any attempt was placed says that too, instead of
// an empty list.
func TestSpawnGivenUpWithoutCandidatesSaysSo_t5456(t *testing.T) {
	task := hubSpawnTask{Attempt: 2, Expires: time.Now().Add(-time.Minute)}
	outcome := spawnGivenUpOutcome(task, "machine-a", nil)
	if !strings.Contains(outcome.Reason, "never found a candidate") {
		t.Fatalf("reason %q does not say no candidate was found", outcome.Reason)
	}
	if len(outcome.Tried) != 0 {
		t.Fatalf("tried = %+v, want none", outcome.Tried)
	}
}

// hk 5456 review: a completion recorded before this request asked says nothing
// about its wait. Only the request's own attempt settles it, and only a
// finished attempt that cannot supply the reading (nothing usable, or another
// account measured) settles it at all.
func TestSpawnRefreshSettledTiesToTheRequestsOwnWait_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	now := time.Now()
	task := hubSpawnTask{RefreshWaits: []hubSpawnRefreshWait{{Machine: "machine-a", Pool: "codex", At: now}}}
	record := func(outcome string, at time.Time) {
		t.Helper()
		if err := hub.inbox.store.recordQuotaRefreshAttempt(t.Context(), "machine-a", "codex", outcome, at); err != nil {
			t.Fatal(err)
		}
	}
	// An older request's successful refresh does not make this wait settled.
	record("", now.Add(-time.Minute))
	if reason, settled := hub.spawnRefreshSettled(task, "machine-a", "codex"); settled {
		t.Fatalf("an older completion settled this wait: %q", reason)
	}
	// This wait's own attempt finished without a usable reading.
	record("", now.Add(time.Second))
	reason, settled := hub.spawnRefreshSettled(task, "machine-a", "codex")
	if !settled || !strings.Contains(reason, "without a usable reading") {
		t.Fatalf("settled=%v reason=%q, want a finished attempt with no usable reading", settled, reason)
	}
	// A node that measured another account cannot supply this one: settled.
	record("account_mismatch", now.Add(2*time.Second))
	reason, settled = hub.spawnRefreshSettled(task, "machine-a", "codex")
	if !settled || !strings.Contains(reason, "machine-a") || !strings.Contains(reason, "another account") {
		t.Fatalf("settled=%v reason=%q, want the account mismatch named", settled, reason)
	}
	// A transient failure is retried, not settled.
	record("node_timeout", now.Add(3*time.Second))
	if reason, settled := hub.spawnRefreshSettled(task, "machine-a", "codex"); settled {
		t.Fatalf("a timeout settled the wait: %q", reason)
	}
	// A pool this request never asked for is never settled by another's row.
	if reason, settled := hub.spawnRefreshSettled(task, "machine-b", "codex"); settled {
		t.Fatalf("a pool the request did not ask for settled: %q", reason)
	}
}

// hk 5456 review: the hub tells the caller whether it actually queued the
// measurement, so a request with no budget for it is not left waiting on one
// that was never asked for.
func TestScheduleSpawnQuotaRefreshReportsWhetherItQueued_t5456(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	now := time.Now()
	near := hubSpawnTask{Deadline: now.Add(3 * time.Second)}
	if hub.scheduleSpawnQuotaRefresh(near, "machine-a", "codex", "codex:0000000000000001", time.Minute, now) {
		t.Fatal("a request with less than the use margin queued a refresh it would not wait for")
	}
	far := hubSpawnTask{Deadline: now.Add(time.Hour)}
	if !hub.scheduleSpawnQuotaRefresh(far, "machine-a", "codex", "codex:0000000000000001", time.Minute, now) {
		t.Fatal("a request with an hour left did not queue its refresh")
	}
}
