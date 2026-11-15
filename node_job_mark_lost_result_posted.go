package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// reviewerResultPosted is the end_reason of a reviewer job the hub ends because
// the job posted the result it was spawned for (hk 4684,
// docs/hub-spawn-operations.md). A reviewer that finishes without running
// `done` otherwise keeps a claimed or spawned hk record and a lane row until the
// 64-row node lane cap refuses new lanes.
const reviewerResultPosted = "reviewer_result_posted"

// reviewerResultSettle is how long the newest result file must have been
// unchanged before the node ends the job. A reviewer writes findings.json, then
// report.md, then runs `panewire done`; hk never rewrites a lost record, so an
// end that lands between those steps would lose the completion. Ten minutes is
// the grace the hub already gives an unworked spawn (UnworkedLandingGrace).
const reviewerResultSettle = 10 * time.Minute

// nodeJobMarkReviewerResultPosted is job.mark_lost for end_reason
// reviewer_result_posted. The reviewer's own result stands in for the pane
// proof, so its pane may still be open: the node's ended-job pane sweep closes
// it once the record is terminal. The node proves, from its own job directory
// and while holding the job's operation lock (the one `panewire done` and the
// watcher's quota-stop records take, so none of them can land between the proof
// and the hk write):
//
//   - no completion after the attempt and no newer claim;
//   - the job is a reviewer;
//   - no quota stop is announced or pending (the hub's hand-over writes that
//     record with the successor's pointer);
//   - the pane is not working (a reviewer running `done` is working);
//   - the job posted a result (a non-null findings object or a report on disk)
//     and the newest result file has been unchanged for reviewerResultSettle.
//
// Then it appends a final job.lost event to the job's ledger (what the pane
// sweep reads) and writes the hk record lost.
//
// Any refusal writes nothing; the hub asks again on its next pass.
func (client *HubClient) nodeJobMarkReviewerResultPosted(ctx context.Context, args nodeJobMarkLostArgs) (any, string) {
	refuse := func(outcome string) (any, string) { return nodeJobMarkLostResult{Outcome: outcome}, "" }
	root, dir, found, err := client.openNodeJobDir(args.Job)
	if errors.Is(err, errNodeJobsRootUnset) {
		return nil, "jobs_root_unset"
	}
	if err != nil {
		return nil, "mark_lost_failed"
	}
	if !found {
		return refuse(nodeJobMarkLostLedgerMissing)
	}
	defer root.Close()
	unlock, err := fleet.TryLockJobOperation(dir)
	if errors.Is(err, fleet.ErrJobOperationBusy) {
		return refuse(nodeJobMarkLostBusy)
	}
	if err != nil {
		return nil, "mark_lost_failed"
	}
	defer unlock()

	// Everything below runs under the lock, so a completion or a quota-stop
	// record written before it is seen and one written after it is refused or
	// seen by its writer.
	ledger, err := client.openNodeJobLedger(args.Job, args.PaneID, args.Label)
	if errors.Is(err, os.ErrNotExist) {
		return refuse(nodeJobMarkLostLedgerMissing)
	}
	if err != nil {
		return nil, "mark_lost_failed"
	}
	switch {
	case ledger.completed:
		return refuse(nodeJobMarkLostCompleted)
	case ledger.newerClaim:
		return refuse(nodeJobMarkLostNewerClaim)
	}
	target, err := readNodeCloseTarget(root)
	if err != nil {
		return nil, "mark_lost_failed"
	}
	switch {
	case target.role != reviewerRole:
		return refuse(nodeJobMarkLostNotReviewer)
	case target.announced || target.intent:
		return refuse(nodeJobMarkLostQuotaStopped)
	}
	newest, stamped := newestResultFileTime(root)

	// An unreadable herdr list fails the command (the hub retries); a pane that
	// is not listed is a reviewer that has stopped.
	agents, code := client.nodeAgentsListInSession(ctx, client.herdrSessionFor(args.Job, args.PaneID))
	if code != "" {
		return nil, code
	}
	for _, row := range agents.Agents {
		if row.PaneID == args.PaneID && row.Name == args.Label && row.AgentStatus == "working" {
			return refuse(nodeJobMarkLostPaneWorking)
		}
	}

	raw, code := client.nodeJobResult(nodeJobResultArgs{Job: args.Job})
	if code != "" {
		return nil, code
	}
	result, ok := raw.(nodeJobResult)
	if !ok {
		return nil, "mark_lost_failed"
	}
	if !result.Found || !nodeResultPosted(result) || !stamped {
		return refuse(nodeJobMarkLostResultMissing)
	}
	if client.nowUTC().Sub(newest) < reviewerResultSettle {
		return refuse(nodeJobMarkLostResultSettling)
	}

	store, err := client.nodeJobStoreForCommand()
	if err != nil {
		return nil, "mark_lost_failed"
	}
	// The node's ended-job pane sweep reads the end from the local ledger, so
	// the ledger gets its final job.lost before hk is written: a failure after
	// it is retried by the hub's next pass, which finds the ledger ended and
	// writes hk only.
	if !target.ended {
		if err := fleet.RecordJobEnded(ctx, filepath.Join(client.jobsInboxRoot, "jobs"), args.Job, args.PaneID, reviewerResultPosted); err != nil {
			return nil, "mark_lost_failed"
		}
	}
	record := map[string]any{"state": "lost", "at": client.nowUTC(), "end_reason": reviewerResultPosted}
	if err := store.Put(ctx, args.Job, record); err != nil {
		return nil, "mark_lost_failed"
	}
	return nodeJobMarkLostResult{Outcome: nodeJobMarkLostLost}, ""
}

// newestResultFileTime is the latest modification time of findings.json and
// report.md in the job directory; ok is false when neither is a regular file.
func newestResultFileTime(job *os.Root) (newest time.Time, ok bool) {
	for _, name := range []string{"findings.json", "report.md"} {
		info, err := job.Lstat(name)
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		if !ok || info.ModTime().After(newest) {
			newest, ok = info.ModTime(), true
		}
	}
	return newest, ok
}

// nodeResultPosted reports a job.result that carries a validated findings
// object or a report on disk. A findings file holding JSON null is absent: it
// never reaches Findings as a value, but a node that did send the bytes "null"
// must not read as a posted result either.
func nodeResultPosted(result nodeJobResult) bool {
	return (len(result.Findings) > 0 && !isJSONNull(result.Findings)) || result.Report.Present
}
