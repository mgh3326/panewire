package panewire

import (
	"net/http"
	"testing"
)

// hk 4673: a failed holder that recorded no pane id keeps its reservation when
// its node refuses job.mark_lost with ledger_missing (hk 3685). The refusal
// means the node cannot rule out a newer claim under another label, so the hub
// must not free the role on the label probe alone (review of PR 589). The hub
// writes nothing to hk, and the held line names the refusal as
// mark_lost_refused rather than release_write_failed.
func TestRoleReleaseHoldsAndNamesLedgerRefusalForFailedHolderWithoutPane(t *testing.T) {
	hub, server, jobs, held := roleHolderHub(t, roleUnpanedOutcome(), "claimed", "failed")
	sink := captureHubLog(hub)
	node := t14Node(t, hub, server, true)
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
	roleReplyMarkLost(t, node, nodeJobMarkLostLedgerMissing)
	result := t14Result(t, response)
	if result.status != http.StatusConflict || jobs.state("test-job") != "claimed" || len(jobs.writes()) != 0 {
		t.Fatalf("status=%d body=%s hk=%v writes=%v", result.status, result.body, jobs.state("test-job"), jobs.writes())
	}
	assertRoleHeldBy(t, hub, held.Request.RequestID)
	assertHeldLine(t, sink, "mark_lost_refused", "claimed", "absent", "ledger_missing")
}
