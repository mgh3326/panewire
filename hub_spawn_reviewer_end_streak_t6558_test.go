package panewire

import (
	"strings"
	"testing"
	"time"
)

// hk 6558: a full-row page that fails after earlier pages succeeded had its
// silent-error streak cleared by each pass's first page, so the same
// persistent later-page failure was counted and logged on every pass. The
// streak now stays latched until every page of the pass succeeds.
func TestReviewerEndLaterPageFailureStaysLatchedAcrossPasses_t6558(t *testing.T) {
	old := reviewerEndPage
	reviewerEndPage = 1
	t.Cleanup(func() { reviewerEndPage = old })

	// No node is connected, so test-job's reviewer is never judged: its page
	// reads cleanly at the head of every pass while the corrupted row fails
	// the last page every time.
	hub, _, jobs, _ := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	sink := captureHubLog(hub)
	reviewerEndOp(t, hub, jobs, reviewerRole, "older-reviewer", "machine-a", "", 1, time.Now().Add(-time.Minute))
	// A payload that is not JSON fails the full-row scan of this operation's
	// page only; the id listing and every other page stay healthy.
	if _, err := hub.inbox.store.db.ExecContext(t.Context(), `UPDATE spawn_operations SET payload='{' WHERE job='older-reviewer'`); err != nil {
		t.Fatal(err)
	}

	for range 3 {
		hub.endPostedReviewerJobs(t.Context())
	}

	if got := strings.Count(sink.String(), `"reason":"store_unavailable"`); got != 3 {
		t.Fatalf("store_unavailable log lines=%d, want the later-page failure on all 3 passes; log:\n%s", got, sink.String())
	}
	if got := strings.Count(sink.String(), `hub_spawn_reviewer_end.go:succeededSpawnsByID`); got != 1 {
		t.Fatalf("full-row failure log lines=%d, want 1 across a persistent later-page failure streak; log:\n%s", got, sink.String())
	}
	if got := hub.loopSnapshot()[loopReviewerEnd].SilentErrors; got != 1 {
		t.Fatalf("reviewer_end silent_errors=%d, want 1 for the persistent later-page failure", got)
	}
}
