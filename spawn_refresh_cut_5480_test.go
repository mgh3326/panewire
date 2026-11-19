package panewire

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// hk 5480 (scope cut of hk 5456): the shared quota refresh a request waits on
// never gets a budget past what is left of the attempt, so the wait cannot
// outlive the request.
func TestSpawnQuotaRefreshBudgetStaysInsideTheRequest_5480(t *testing.T) {
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

// A request that reaches its limit without a candidate names the candidates the
// parked pass tried and why each was dropped, and spawn status prints it.
func TestSpawnGivenUpAfterAParkNamesWhatTheParkedPassTried_5480(t *testing.T) {
	retryHK(t)
	hub := timeoutRefreshHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), &controlPlaneLogCapture{})
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tried := []hubSpawnTried{
		{Host: "oci", Pool: "codex", Reason: "refresh_unavailable"},
		{Host: "mac-dev", Pool: "claude", Reason: "quota exhausted window=7d cutoff=99"},
	}
	if _, err := hub.parkSpawnTask(t.Context(), task, spawnParkQuota, "placement refused", tried, hub.spawnWakeSnapshot()); err != nil {
		t.Fatal(err)
	}
	parked, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if len(parked.Tried) != len(tried) {
		t.Fatalf("parked tried = %+v, want %d candidates persisted", parked.Tried, len(tried))
	}
	parked.Deadline = time.Now().Add(-time.Second)
	parked.Expires = time.Now().Add(-time.Second)
	hub.executeSpawnTask(t.Context(), parked)
	final, err := hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != "given_up" {
		t.Fatalf("status = %s, want given_up", final.Status)
	}
	if len(final.Outcome.Tried) != len(tried) {
		t.Fatalf("outcome tried = %+v, want every dropped candidate", final.Outcome.Tried)
	}
	for _, want := range []string{"no candidate could take it", "codex/oci: refresh_unavailable", "claude/mac-dev", "cutoff=99"} {
		if !strings.Contains(final.Outcome.Reason, want) {
			t.Fatalf("given up reason %q does not name %q", final.Outcome.Reason, want)
		}
	}
}

// A request given up before any attempt was placed says that too, instead of an
// empty list.
func TestSpawnGivenUpWithoutCandidatesSaysSo_5480(t *testing.T) {
	task := hubSpawnTask{Attempt: 2, Expires: time.Now().Add(-time.Minute)}
	outcome := spawnGivenUpOutcome(task, "machine-a", nil)
	if !strings.Contains(outcome.Reason, "never found a candidate") {
		t.Fatalf("reason %q does not say no candidate was found", outcome.Reason)
	}
	if len(outcome.Tried) != 0 {
		t.Fatalf("tried = %+v, want none", outcome.Tried)
	}
}
