package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// reviewerEndLedger is the node's job.result answer: findings is the decoded
// findings object (nil for none), findingsError what the node says about a
// findings file that is not usable, report whether report.md exists.
func reviewerEndLedger(findings map[string]any, findingsError string, report bool) map[string]any {
	ledger := map[string]any{"found": true, "findings": findings, "findings_error": findingsError,
		"report": map[string]any{"present": report, "bytes": 0, "sha256": "", "last_line": ""}}
	if report {
		ledger["report"] = map[string]any{"present": true, "bytes": 10, "sha256": strings.Repeat("a", 64), "last_line": "done"}
	}
	return ledger
}

var (
	reviewerEndPosted    = reviewerEndLedger(map[string]any{"findings": []any{}}, "", false)
	reviewerEndReportOnl = reviewerEndLedger(nil, "missing", true)
	reviewerEndNoResult  = reviewerEndLedger(nil, "missing", false)
)

func reviewerEndNode(t *testing.T, ledger map[string]any) (*HubServer, *unworkedNode, *roleJobs, *lockedBuffer) {
	t.Helper()
	hub, server, jobs, _ := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("idle", "/work/project.test-job"), ledger))
	return hub, node, jobs, captureHubLog(hub)
}

func (n *unworkedNode) reply(op string, reply map[string]any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replies[op] = reply
}

// A reviewer that posted a result and stopped without `done` keeps a live hk
// record and a lane row (hk 4658). The hub asks its node to write the end, with
// the end_reason a reader can search for, and never touches the pane: closing
// it belongs to the node's ended-job sweep.
func TestReviewerEndWritesTheEndOfAPostedResult(t *testing.T) {
	for name, ledger := range map[string]map[string]any{"findings object": reviewerEndPosted, "report only": reviewerEndReportOnl} {
		t.Run(name, func(t *testing.T) {
			hub, node, _, sink := reviewerEndNode(t, ledger)

			hub.endPostedReviewerJobs(t.Context())

			if want := []string{"job.result", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
				t.Fatalf("node commands %v, want %v (no pane command)", node.seen(), want)
			}
			var mark map[string]any
			if err := json.Unmarshal(node.args["job.mark_lost"], &mark); err != nil {
				t.Fatal(err)
			}
			if mark["job"] != "test-job" || mark["end_reason"] != reviewerResultPosted || mark["pane_id"] != roleHeldPane {
				t.Fatalf("mark lost args %v", mark)
			}
			if logged := sink.String(); strings.Count(logged, "hub ended a reviewer job with a posted result") != 1 {
				t.Fatalf("log %q lacks the one end line", logged)
			}
		})
	}
}

// Idle is not a result: a reviewer between turns, or one whose findings file is
// JSON null, is left alone and nothing is written.
func TestReviewerEndKeepsAJobWithoutAResult(t *testing.T) {
	nullFindings := reviewerEndLedger(nil, "schema", false)
	for name, ledger := range map[string]map[string]any{"no result files": reviewerEndNoResult, "findings null": nullFindings, "job not found": {"found": false, "findings": nil, "findings_error": "", "report": map[string]any{"present": false, "bytes": 0, "sha256": "", "last_line": ""}}} {
		t.Run(name, func(t *testing.T) {
			hub, node, jobs, sink := reviewerEndNode(t, ledger)

			hub.endPostedReviewerJobs(t.Context())
			hub.endPostedReviewerJobs(t.Context())

			if want := []string{"job.result", "job.result"}; !reflect.DeepEqual(node.seen(), want) {
				t.Fatalf("node commands %v, want %v (no write)", node.seen(), want)
			}
			if state := jobs.state("test-job"); state != "spawned" {
				t.Fatalf("hk record %v, want the live spawned record", state)
			}
			if logged := sink.String(); strings.Contains(logged, "reviewer job") {
				t.Fatalf("log %q reports on a reviewer that is just working", logged)
			}
		})
	}
}

// The null findings of a node that sends the bytes "null" are not a result
// either, whatever the hub's decoder makes of them.
func TestNodeResultPostedReadsJSONNullAsAbsent(t *testing.T) {
	if nodeResultPosted(nodeJobResult{Found: true, Findings: json.RawMessage("null")}) {
		t.Fatal("findings null counted as a posted result")
	}
	if !nodeResultPosted(nodeJobResult{Found: true, Findings: json.RawMessage(`{"findings":[]}`)}) {
		t.Fatal("a findings object was not a posted result")
	}
	if !nodeResultPosted(nodeJobResult{Found: true, Report: nodeJobReport{Present: true}}) {
		t.Fatal("a report on disk was not a posted result")
	}
}

// A write that fails is retried on every pass until it lands, and the failure
// is logged once, not once a pass.
func TestReviewerEndRetriesAFailedWrite(t *testing.T) {
	hub, node, _, sink := reviewerEndNode(t, reviewerEndPosted)
	node.reply("job.mark_lost", map[string]any{"error": "mark_lost_failed"})

	hub.endPostedReviewerJobs(t.Context())
	hub.endPostedReviewerJobs(t.Context())

	if want := []string{"job.result", "job.mark_lost", "job.result", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want the write tried on both passes", node.seen())
	}
	logged := sink.String()
	if strings.Count(logged, "hub could not end a reviewer job") != 1 || strings.Contains(logged, "hub ended a reviewer job") {
		t.Fatalf("log %q, want one failure line and no end line", logged)
	}

	node.reply("job.mark_lost", map[string]any{"result": map[string]any{"outcome": "lost"}})
	hub.endPostedReviewerJobs(t.Context())

	if got := node.seen(); len(got) != 6 || got[5] != "job.mark_lost" {
		t.Fatalf("node commands %v, want a third write", got)
	}
	logged = sink.String()
	if strings.Count(logged, "hub ended a reviewer job with a posted result") != 1 || strings.Count(logged, "hub could not end a reviewer job") != 1 {
		t.Fatalf("log %q, want the end line once after the single failure line", logged)
	}
}

// A job whose record is already terminal costs no node command at all.
func TestReviewerEndSkipsATerminalRecord(t *testing.T) {
	hub, node, jobs, _ := reviewerEndNode(t, reviewerEndPosted)
	jobs.mu.Lock()
	jobs.records["test-job"]["state"] = "completed"
	jobs.mu.Unlock()

	hub.endPostedReviewerJobs(t.Context())

	if got := node.seen(); len(got) != 0 {
		t.Fatalf("node commands %v, want none", got)
	}
}

// A quota stop is the hub's hand-over to write (hk 3334). The node refuses the
// end, and the hub keeps the job and says so once.
func TestReviewerEndSkipsAnAnnouncedQuotaStop(t *testing.T) {
	hub, node, jobs, sink := reviewerEndNode(t, reviewerEndPosted)
	node.reply("job.mark_lost", map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostQuotaStopped}})

	hub.endPostedReviewerJobs(t.Context())
	hub.endPostedReviewerJobs(t.Context())

	if state := jobs.state("test-job"); state != "spawned" {
		t.Fatalf("hk record %v, want it untouched", state)
	}
	logged := sink.String()
	if strings.Count(logged, `"reason":"quota-stopped"`) != 1 || strings.Contains(logged, "hub ended a reviewer job") || strings.Contains(logged, "could not end") {
		t.Fatalf("log %q, want one kept line naming the quota stop", logged)
	}
}

// Only a reviewer's job is ended this way: an implementer with a findings file
// is left to its own `done`.
func TestReviewerEndIgnoresOtherRoles(t *testing.T) {
	hub, server, _, _ := reviewerReleaseHub(t, "implementer", "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	node := startUnworkedNode(t, t14Node(t, hub, server, true), unworkedReplies(unworkedRow("idle", "/work/project.test-job"), reviewerEndPosted))

	hub.endPostedReviewerJobs(t.Context())

	if got := node.seen(); len(got) != 0 {
		t.Fatalf("node commands %v, want none", got)
	}
}

// On the hub's own host the hub could write the record itself, but that write
// reads no ledger. The posted-result end is the node's to prove (role, quota
// stop, the result), so it goes through the node there too and the hub writes
// nothing to hk.
func TestReviewerEndOnTheHubHostStillGoesThroughTheNode(t *testing.T) {
	envFile := filepath.Join(t.TempDir(), "hub-node.env")
	if err := os.WriteFile(envFile, []byte("HUB_MACHINE_ID=machine-a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_NODE_ENV", envFile)
	hub, node, jobs, _ := reviewerEndNode(t, reviewerEndPosted)
	node.reply("job.mark_lost", map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostQuotaStopped}})

	hub.endPostedReviewerJobs(t.Context())

	if want := []string{"job.result", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
	if state := jobs.state("test-job"); state != "spawned" {
		t.Fatalf("hk record %v: the hub wrote it without the node's proof", state)
	}
}

// The nodes' active-job heartbeat drops a pane idle for 30 minutes, caps each
// node at 32 jobs and the hub forgets a spawn two minutes after it started, so
// it is not an inventory of unresolved reviewers. The pass reads the persisted
// operations: a reviewer no node reports (no heartbeat registered here) and
// started days ago is still ended.
func TestReviewerEndFindsAnOrphanTheHeartbeatNoLongerReports(t *testing.T) {
	hub, node, _, _ := reviewerEndNode(t, reviewerEndPosted)
	hub.mu.Lock()
	if active := hub.nodes["machine-a"].activeJobs; len(active) != 0 {
		hub.mu.Unlock()
		t.Fatalf("active jobs %v, want the node to report none", active)
	}
	hub.mu.Unlock()
	hub.now = func() time.Time { return time.Now().Add(7 * 24 * time.Hour) }

	hub.endPostedReviewerJobs(t.Context())

	if want := []string{"job.result", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
		t.Fatalf("node commands %v, want %v", node.seen(), want)
	}
}

// A spawn older than the window is left to an operator; the pass never reads it.
func TestReviewerEndIgnoresASpawnOlderThanTheWindow(t *testing.T) {
	hub, node, _, _ := reviewerEndNode(t, reviewerEndPosted)
	hub.now = func() time.Time { return time.Now().Add(reviewerEndWindow + time.Hour) }

	hub.endPostedReviewerJobs(t.Context())

	if got := node.seen(); len(got) != 0 {
		t.Fatalf("node commands %v, want none", got)
	}
}

// A job whose node is not connected is neither read nor written; the pass tries
// it again once the node is back.
func TestReviewerEndWaitsForAnUnconnectedNode(t *testing.T) {
	hub, server, jobs, _ := reviewerReleaseHub(t, reviewerRole, "succeeded", hubSpawnOutcome{RC: 0, Host: "machine-a", Pane: roleHeldPane})
	_ = server
	sink := captureHubLog(hub)

	hub.endPostedReviewerJobs(t.Context())

	if state := jobs.state("test-job"); state != "spawned" {
		t.Fatalf("hk record %v, want it untouched", state)
	}
	if logged := sink.String(); strings.Contains(logged, "reviewer job") {
		t.Fatalf("log %q reports on a node that is simply not connected", logged)
	}
}

// A job found ended is read once, not every pass: the hk record never leaves a
// terminal state.
func TestReviewerEndReadsAnEndedJobOnce(t *testing.T) {
	hub, node, jobs, _ := reviewerEndNode(t, reviewerEndPosted)
	jobs.mu.Lock()
	jobs.records["test-job"]["state"] = "completed"
	jobs.mu.Unlock()
	hub.endPostedReviewerJobs(t.Context())
	// Were the record read again it would be live, and the node would be asked.
	jobs.mu.Lock()
	jobs.records["test-job"]["state"] = "spawned"
	jobs.mu.Unlock()

	hub.endPostedReviewerJobs(t.Context())

	if got := node.seen(); len(got) != 0 {
		t.Fatalf("node commands %v, want none for a job already found ended", got)
	}
}

// The node's refusals that mean "the reviewer is still finishing" are the
// ordinary state of a young result or a working pane: retried each pass, never
// logged.
func TestReviewerEndRetriesAStillFinishingReviewerQuietly(t *testing.T) {
	for _, outcome := range []string{nodeJobMarkLostResultSettling, nodeJobMarkLostPaneWorking, nodeJobMarkLostBusy, nodeJobMarkLostResultMissing} {
		t.Run(outcome, func(t *testing.T) {
			hub, node, jobs, sink := reviewerEndNode(t, reviewerEndPosted)
			node.reply("job.mark_lost", map[string]any{"result": map[string]any{"outcome": outcome}})

			hub.endPostedReviewerJobs(t.Context())
			hub.endPostedReviewerJobs(t.Context())

			if want := []string{"job.result", "job.mark_lost", "job.result", "job.mark_lost"}; !reflect.DeepEqual(node.seen(), want) {
				t.Fatalf("node commands %v, want the write asked on both passes", node.seen())
			}
			if state := jobs.state("test-job"); state != "spawned" {
				t.Fatalf("hk record %v, want it untouched", state)
			}
			if logged := sink.String(); strings.Contains(logged, "reviewer job") {
				t.Fatalf("log %q reports on a reviewer that is still finishing", logged)
			}
		})
	}
}
