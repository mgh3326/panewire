package panewire

import (
	"net/http"
	"testing"
)

// hk 4673: a failed holder that recorded no pane, whose pane is gone and whose
// node has lost the job's ledger (ledger_missing, hk 3685), frees its
// reservation. The hub writes nothing to hk: the hk record stays claimed. The
// new request then takes the role.
func TestRoleReleaseFreesFailedHolderWithoutPaneOrLedger(t *testing.T) {
	hub, server, jobs, held := roleHolderHub(t, roleUnpanedOutcome(), "claimed", "failed")
	sink := captureHubLog(hub)
	node := t14Node(t, hub, server, true)
	response := t14HTTPAsync(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, roleSecondRequest())
	roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
	roleReplyMarkLost(t, node, nodeJobMarkLostLedgerMissing)
	result := t14Result(t, response)
	if result.status != http.StatusAccepted {
		t.Fatalf("status=%d body=%s hk=%v writes=%v", result.status, result.body, jobs.state("test-job"), jobs.writes())
	}
	if jobs.state("test-job") != "claimed" || len(jobs.writes()) != 0 {
		t.Fatalf("hub wrote hk: state=%v writes=%v", jobs.state("test-job"), jobs.writes())
	}
	if owner, job, _, err := hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err != nil || job != "test-job-r2" || owner == held.Request.RequestID {
		t.Fatalf("reservation not moved: %s %s %v", owner, job, err)
	}
	if lines := heldRoleLines(t, sink); len(lines) != 0 {
		t.Fatalf("a released role logged %v", lines)
	}
}
