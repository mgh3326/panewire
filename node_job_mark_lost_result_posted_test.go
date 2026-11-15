package panewire

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// reviewerLedger is the ledger of a reviewer job, plus any later events.
func reviewerLedger(role string, later ...map[string]any) []map[string]any {
	claim := map[string]any{"kind": "job.claim", "payload": map[string]any{"agent_label": "job-a", "role": role}}
	spawned := map[string]any{"kind": "job.spawned", "payload": map[string]any{"pane_id": "w2:p3", "label": "job-a", "role": role}}
	return append([]map[string]any{claim, spawned}, later...)
}

func postedResultArgs() nodeJobMarkLostArgs {
	return nodeJobMarkLostArgs{Job: "job-a", PaneID: "w2:p3", Label: "job-a", EndReason: reviewerResultPosted}
}

// postedResultNow is the node's clock in these tests; result files are aged
// against it.
var postedResultNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

// writeJobFile writes a result file whose modification time is age before the
// node's clock.
func writeJobFile(t *testing.T, inbox, name, content string, age time.Duration) {
	t.Helper()
	path := filepath.Join(inbox, "jobs", "job-a", name)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, postedResultNow.Add(-age), postedResultNow.Add(-age)); err != nil {
		t.Fatal(err)
	}
}

// postedResultClient is a node whose reviewer pane has the given status ("" lists
// no pane).
func postedResultClient(t *testing.T, inbox, paneStatus string) (*HubClient, *roleJobs, *nodeFakeHerdr) {
	t.Helper()
	// The end appends to the job's ledger, which creates the arbiter state
	// database on first use. Under the package's shared state directory that
	// database would make other tests' background quota reconciliation run, so
	// each test keeps its own.
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	var rows []string
	if paneStatus != "" {
		rows = append(rows, nodeAgentJSON("w2:p3", "job-a", paneStatus, "/work/x"))
	}
	client, store, fake := markLostClient(t, inbox, []string{nodeAgentList(rows...)}, markLostEmptyPanes)
	client.now = func() time.Time { return postedResultNow }
	return client, store, fake
}

// The reviewer's pane is still listed (the node's ended-job sweep closes it
// after the record is terminal), yet a settled posted result lets the node
// write the end. A findings object or a report on disk is a result.
func TestNodeJobMarkLostReviewerResultPostedWritesWithAListedPane(t *testing.T) {
	for _, test := range []struct{ name, file, content string }{
		{"findings object", "findings.json", `{"findings":[]}`},
		{"report only", "report.md", "# review\nno blockers\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
			writeJobFile(t, inbox, test.file, test.content, reviewerResultSettle+time.Minute)
			client, store, _ := postedResultClient(t, inbox, "idle")
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
			if code != "" || result["outcome"] != nodeJobMarkLostLost {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if got := store.records["job-a"]["end_reason"]; store.state("job-a") != "lost" || got != reviewerResultPosted {
				t.Fatalf("hk state=%v end_reason=%v", store.state("job-a"), got)
			}
			if got := jobLostEvents(t, inbox); len(got) != 1 {
				t.Fatalf("ledger job.lost events %v, want the one final event", got)
			}
		})
	}
}

// jobLostEvents lists the job.lost event files in the job's ledger.
func jobLostEvents(t *testing.T, inbox string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(inbox, "jobs", "job-a", "events", "*job.lost*.json"))
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// The node's ended-job pane sweep reads a reviewer's end from the local ledger,
// not from hk, so the end appends a final job.lost with the reason; without it
// the pane would stay open (hk 4693). hk is written first: a sweep that could see
// the ledger end before hk holds this end_reason would close the pane and write
// a generic lost record over it. A failed hk write therefore leaves the ledger
// untouched.
func TestNodeJobMarkLostReviewerResultPostedWritesHkBeforeTheLedger(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
	writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, reviewerResultSettle+time.Hour)
	client, store, _ := postedResultClient(t, inbox, "idle")
	store.putErr = errors.New("hk unavailable")

	if result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs()); code == "" {
		t.Fatalf("result=%v, want the hk failure reported", result)
	}
	if got := jobLostEvents(t, inbox); len(got) != 0 {
		t.Fatalf("ledger job.lost events %v after a failed hk write, want none", got)
	}
	store.mu.Lock()
	store.putErr = nil
	store.mu.Unlock()
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
	if code != "" || result["outcome"] != nodeJobMarkLostLost || store.state("job-a") != "lost" {
		t.Fatalf("retry: code=%q result=%v hk=%v", code, result, store.state("job-a"))
	}
	if got := jobLostEvents(t, inbox); len(got) != 1 {
		t.Fatalf("ledger job.lost events %v after the retry, want one", got)
	}
}

// hk written, ledger not (a crash or failure between the two): the hub asks
// again, and the node, finding its own end_reason in hk, appends only the
// missing event and writes hk no more. A second ask adds nothing.
func TestNodeJobMarkLostReviewerResultPostedFinishesTheLedgerOfAnEndedRecord(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
	client, store, _ := postedResultClient(t, inbox, "idle")
	store.records["job-a"] = map[string]any{"state": "lost", "end_reason": reviewerResultPosted}

	for range 2 {
		result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
		if code != "" || result["outcome"] != nodeJobMarkLostLost {
			t.Fatalf("code=%q result=%v", code, result)
		}
		if len(store.writes()) != 0 {
			t.Fatalf("wrote hk over its own record: %v", store.writes())
		}
		if got := jobLostEvents(t, inbox); len(got) != 1 {
			t.Fatalf("ledger job.lost events %v, want exactly one", got)
		}
	}
}

// A record another end already holds is not this end's to finish.
func TestNodeJobMarkLostReviewerResultPostedLeavesAnotherTerminalRecord(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
	client, store, _ := postedResultClient(t, inbox, "idle")
	store.records["job-a"] = map[string]any{"state": "lost", "end_reason": "quota_stop pool=claude"}
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
	if code != "" || result["outcome"] != nodeJobMarkLostCompleted {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if got := jobLostEvents(t, inbox); len(got) != 0 || len(store.writes()) != 0 {
		t.Fatalf("events %v writes %v, want nothing written over another end", got, store.writes())
	}
}

// A reviewer that creates or updates report.md while the node reads the result
// leaves the files different between the two looks. The settle age taken before
// the read would call the old findings.json settled, so the end is refused.
func TestNodeJobMarkLostReviewerResultPostedRefusesAFileChangedDuringTheRead(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(t *testing.T, inbox string)
	}{
		{"report.md appears", func(t *testing.T, inbox string) { writeJobFile(t, inbox, "report.md", "# review\n", 0) }},
		{"findings.json is rewritten", func(t *testing.T, inbox string) {
			writeJobFile(t, inbox, "findings.json", `{"findings":[{}]}`, reviewerResultSettle+2*time.Hour)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
			writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, reviewerResultSettle+time.Hour)
			client, store, _ := postedResultClient(t, inbox, "idle")
			reviewerResultReadHook = func() { test.write(t, inbox) }
			t.Cleanup(func() { reviewerResultReadHook = nil })

			result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
			if code != "" || result["outcome"] != nodeJobMarkLostResultSettling {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if len(store.writes()) != 0 || len(jobLostEvents(t, inbox)) != 0 {
				t.Fatalf("wrote hk %v / ledger %v for a result that changed during the read", store.writes(), jobLostEvents(t, inbox))
			}
		})
	}
}

// Without a result the node refuses even when the hub asked: findings that are
// JSON null, a findings file that is not the schema, or no result files at all.
func TestNodeJobMarkLostReviewerResultPostedRefusesWithoutAResult(t *testing.T) {
	for _, test := range []struct{ name, file, content string }{
		{"no result files", "", ""},
		{"findings null", "findings.json", "null"},
		{"findings without a findings array", "findings.json", `{"other":1}`},
		{"findings not json", "findings.json", "oops"},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
			if test.file != "" {
				writeJobFile(t, inbox, test.file, test.content, reviewerResultSettle+time.Hour)
			}
			client, store, _ := postedResultClient(t, inbox, "idle")
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
			if code != "" || result["outcome"] != nodeJobMarkLostResultMissing {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if len(store.writes()) != 0 {
				t.Fatalf("wrote hk: %v", store.writes())
			}
		})
	}
}

// A reviewer writes findings.json, then report.md, then runs `done`; hk never
// rewrites a lost record. A result that is still new, or a newer report.md
// behind an old findings.json, is the reviewer still finishing.
func TestNodeJobMarkLostReviewerResultPostedWaitsForTheResultToSettle(t *testing.T) {
	for _, test := range []struct {
		name           string
		findings, repo time.Duration // ages; 0 means the file is absent
	}{
		{"findings just written", time.Minute, 0},
		{"report just written after old findings", reviewerResultSettle + time.Hour, time.Minute},
		{"report being written at the boundary", reviewerResultSettle + time.Hour, reviewerResultSettle - time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
			writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, test.findings)
			if test.repo > 0 {
				writeJobFile(t, inbox, "report.md", "# review\n", test.repo)
			}
			client, store, _ := postedResultClient(t, inbox, "idle")
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
			if code != "" || result["outcome"] != nodeJobMarkLostResultSettling {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if len(store.writes()) != 0 {
				t.Fatalf("wrote hk: %v", store.writes())
			}
		})
	}
}

// A reviewer running `done` is working; a working pane is not a stopped one
// whatever its files say.
func TestNodeJobMarkLostReviewerResultPostedRefusesAWorkingPane(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
	writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, reviewerResultSettle+time.Hour)
	client, store, _ := postedResultClient(t, inbox, "working")
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
	if code != "" || result["outcome"] != nodeJobMarkLostPaneWorking {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 {
		t.Fatalf("wrote hk: %v", store.writes())
	}
}

// An unreadable herdr list fails the command, so the hub retries; it never
// reads as a stopped pane.
func TestNodeJobMarkLostReviewerResultPostedFailsClosedWithoutAPaneList(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
	writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, reviewerResultSettle+time.Hour)
	client, store, fake := postedResultClient(t, inbox, "idle")
	fake.agents = []string{"not json"} // herdr answers something the node cannot read
	if result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs()); code == "" {
		t.Fatalf("result=%v, want an error code", result)
	}
	if len(store.writes()) != 0 {
		t.Fatalf("wrote hk: %v", store.writes())
	}
}

// An announced quota stop is the hub's hand-over to write, with the successor's
// pointer; a posted-result end must not take the record first. A job that is
// not a reviewer is never ended this way.
func TestNodeJobMarkLostReviewerResultPostedRefusals(t *testing.T) {
	quota := map[string]any{"kind": "job.lost", "payload": map[string]any{"reason": "quota_stop"}}
	intent := map[string]any{"kind": "job.quota_stop_intent"}
	for _, test := range []struct {
		name   string
		ledger []map[string]any
		want   string
	}{
		{"announced quota stop", reviewerLedger("reviewer", quota), nodeJobMarkLostQuotaStopped},
		{"open quota-stop intent", reviewerLedger("reviewer", intent), nodeJobMarkLostQuotaStopped},
		{"implementer", reviewerLedger("implementer"), nodeJobMarkLostNotReviewer},
		{"no role", reviewerLedger(""), nodeJobMarkLostNotReviewer},
		{"completed", reviewerLedger("reviewer", map[string]any{"kind": "job.completed"}), nodeJobMarkLostCompleted},
	} {
		t.Run(test.name, func(t *testing.T) {
			inbox := t.TempDir()
			markLostJobEvents(t, inbox, "job-a", test.ledger...)
			writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, reviewerResultSettle+time.Hour)
			client, store, _ := postedResultClient(t, inbox, "idle")
			result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
			if code != "" || result["outcome"] != test.want {
				t.Fatalf("code=%q result=%v", code, result)
			}
			if len(store.writes()) != 0 {
				t.Fatalf("wrote hk: %v", store.writes())
			}
		})
	}
}

// The end holds the job's operation lock across its proofs and the write, so a
// `done` or a watcher's quota-stop record in flight (which hold it too) makes
// the node refuse rather than race; the hub asks again on its next pass.
func TestNodeJobMarkLostReviewerResultPostedRefusesWhileTheOperationLockIsHeld(t *testing.T) {
	inbox := t.TempDir()
	markLostJobEvents(t, inbox, "job-a", reviewerLedger("reviewer")...)
	writeJobFile(t, inbox, "findings.json", `{"findings":[]}`, reviewerResultSettle+time.Hour)
	client, store, _ := postedResultClient(t, inbox, "idle")
	unlock, err := fleet.TryLockJobOperation(filepath.Join(inbox, "jobs", "job-a"))
	if err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs())
	if code != "" || result["outcome"] != nodeJobMarkLostBusy {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if len(store.writes()) != 0 {
		t.Fatalf("wrote hk under a held lock: %v", store.writes())
	}
	unlock()
	if result, code = nodeRun(t, client, nodeCommandJobMarkLost, postedResultArgs()); code != "" || result["outcome"] != nodeJobMarkLostLost {
		t.Fatalf("after the lock was released: code=%q result=%v", code, result)
	}
}
