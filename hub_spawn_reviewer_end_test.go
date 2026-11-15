package panewire

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// reviewerEndActiveJob registers the reviewer's job in the node's active-job
// list, which liveSpawnTasks reads; the fake node sends no heartbeat of its own.
func reviewerEndActiveJob(t *testing.T, hub *HubServer, machine, job, pane, role string) {
	t.Helper()
	hub.mu.Lock()
	defer hub.mu.Unlock()
	record := hub.nodes[machine]
	if record == nil {
		t.Fatalf("node %s is not registered", machine)
	}
	record.activeJobs[job] = HubActiveJob{JobID: job, Epoch: 1, AgentLabel: job, OwnerLane: "parent", Pane: pane, Role: role}
}

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
	reviewerEndActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, reviewerRole)
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
	reviewerEndActiveJob(t, hub, "machine-a", "test-job", roleHeldPane, "implementer")

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
