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
// it once the job's ledger ends. The node proves, from its own job directory
// and while holding the job's operation lock (the one `panewire done` and the
// watcher's quota-stop records take, so none of them can land between the proof
// and the writes):
//
//   - no completion after the attempt and no newer claim;
//   - the job is a reviewer;
//   - no quota stop is announced or pending (the hub's hand-over writes that
//     record with the successor's pointer);
//   - the pane is not working (a reviewer running `done` is working);
//   - the job posted a result (a non-null findings object or a report on disk)
//     and the result files did not change while the node read them and have
//     been unchanged for reviewerResultSettle.
//
// Then it writes the hk record lost, and only then appends a final job.lost
// event to the job's ledger, which is what the pane sweep reads. That order
// keeps the sweep from acting on a job whose hk write is still pending: it
// cannot see an end before hk holds this end_reason. A crash or failure between
// the two leaves hk lost with this end_reason and a ledger not yet ended; the
// hub's next pass asks again, and the node, finding that record, only appends
// the missing event.
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
	// record written before it is seen and one written after it waits for it.
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
	store, err := client.nodeJobStoreForCommand()
	if err != nil {
		return nil, "mark_lost_failed"
	}
	record, err := store.Get(ctx, args.Job)
	if err != nil {
		return nil, "mark_lost_failed"
	}
	switch state, _ := record["state"].(string); state {
	case "claimed", "spawned":
	case "lost":
		if record["end_reason"] != reviewerResultPosted {
			return refuse(nodeJobMarkLostCompleted)
		}
		// This end's hk write landed; the ledger event may not have.
		return client.recordReviewerResultEnd(ctx, args, target)
	default:
		return refuse(nodeJobMarkLostCompleted)
	}

	switch {
	case target.ended:
		// The ledger already ended another way (a reap, a hand-off, a join, a
		// revoke, an escalation) whose own hk write may still be pending. This
		// end did not cause it and must not take its record.
		return refuse(nodeJobMarkLostCompleted)
	case target.role != reviewerRole:
		return refuse(nodeJobMarkLostNotReviewer)
	case target.announced || target.intent:
		return refuse(nodeJobMarkLostQuotaStopped)
	}

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

	// The result and its files are read between two looks at the files: a write
	// that lands during the read changes the second look and the end is refused.
	before := resultFileStampsOf(root)
	if reviewerResultReadHook != nil {
		reviewerResultReadHook()
	}
	raw, code := client.nodeJobResult(nodeJobResultArgs{Job: args.Job})
	if code != "" {
		return nil, code
	}
	result, ok := raw.(nodeJobResult)
	if !ok {
		return nil, "mark_lost_failed"
	}
	after := resultFileStampsOf(root)
	if !result.Found || !nodeResultPosted(result) || len(after) == 0 {
		return refuse(nodeJobMarkLostResultMissing)
	}
	if !resultFileStampsEqual(before, after) || client.nowUTC().Sub(after.newest()) < reviewerResultSettle {
		return refuse(nodeJobMarkLostResultSettling)
	}

	if err := client.putOwnedNodeJob(ctx, store, args.Job, map[string]any{"state": "lost", "at": client.nowUTC(), "end_reason": reviewerResultPosted}); err != nil {
		if fleet.IsJobReassigned(err) {
			return refuse(nodeJobMarkLostNewerClaim)
		}
		return nil, "mark_lost_failed"
	}
	return client.recordReviewerResultEnd(ctx, args, target)
}

// reviewerResultReadHook is a test seam: it runs between the two looks at the
// result files, where a reviewer's write can land.
var reviewerResultReadHook func()

// recordReviewerResultEnd appends the final job.lost to the ledger unless it
// already ended, and answers lost.
func (client *HubClient) recordReviewerResultEnd(ctx context.Context, args nodeJobMarkLostArgs, target nodeCloseTarget) (any, string) {
	if !target.ended {
		if err := fleet.RecordJobEnded(ctx, filepath.Join(client.jobsInboxRoot, "jobs"), args.Job, args.PaneID, reviewerResultPosted); err != nil {
			return nil, "mark_lost_failed"
		}
	}
	return nodeJobMarkLostResult{Outcome: nodeJobMarkLostLost}, ""
}

// resultFileStamp is what one look at a result file saw.
type resultFileStamp struct {
	mod  time.Time
	size int64
}

// resultFileStamps maps findings.json and report.md, when they are regular
// files, to their modification time and size.
type resultFileStamps map[string]resultFileStamp

func resultFileStampsOf(job *os.Root) resultFileStamps {
	stamps := resultFileStamps{}
	for _, name := range []string{"findings.json", "report.md"} {
		if info, err := job.Lstat(name); err == nil && info.Mode().IsRegular() {
			stamps[name] = resultFileStamp{info.ModTime(), info.Size()}
		}
	}
	return stamps
}

func resultFileStampsEqual(a, b resultFileStamps) bool {
	if len(a) != len(b) {
		return false
	}
	for name, stamp := range a {
		if other, ok := b[name]; !ok || !stamp.mod.Equal(other.mod) || stamp.size != other.size {
			return false
		}
	}
	return true
}

// newest is the latest modification time among the stamped files.
func (stamps resultFileStamps) newest() time.Time {
	var newest time.Time
	for _, stamp := range stamps {
		if stamp.mod.After(newest) {
			newest = stamp.mod
		}
	}
	return newest
}

// nodeResultPosted reports a job.result that carries a validated findings
// object or a report on disk. A findings file holding JSON null is absent: it
// never reaches Findings as a value, but a node that did send the bytes "null"
// must not read as a posted result either.
func nodeResultPosted(result nodeJobResult) bool {
	return (len(result.Findings) > 0 && !isJSONNull(result.Findings)) || result.Report.Present
}
