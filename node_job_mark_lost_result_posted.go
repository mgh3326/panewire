package panewire

// reviewerResultPosted is the end_reason of a reviewer job the hub ends because
// the job posted the result it was spawned for (hk 4684,
// docs/hub-spawn-operations.md). A reviewer that finishes without running
// `done` otherwise keeps a claimed or spawned hk record and a lane row until the
// 64-row node lane cap refuses new lanes.
const reviewerResultPosted = "reviewer_result_posted"

// reviewerResultPostedProof is what job.mark_lost proves, from this node's own
// job directory, before it ends a reviewer job whose pane may still be open:
// the job is a reviewer, no quota stop is announced for the hub's hand-over
// (the hub writes that record itself, with the successor's pointer, and hk
// never rewrites a lost record), and the job posted a result. It returns an
// outcome that refuses the write, or an error code, or both empty to go ahead.
func (client *HubClient) reviewerResultPostedProof(job string) (outcome, code string) {
	root, _, found, err := client.openNodeJobDir(job)
	if err != nil || !found {
		return "", "mark_lost_failed"
	}
	target, err := readNodeCloseTarget(root)
	root.Close()
	if err != nil {
		return "", "mark_lost_failed"
	}
	switch {
	case target.role != reviewerRole:
		return nodeJobMarkLostNotReviewer, ""
	case target.announced || target.intent:
		return nodeJobMarkLostQuotaStopped, ""
	}
	raw, code := client.nodeJobResult(nodeJobResultArgs{Job: job})
	if code != "" {
		return "", code
	}
	result, ok := raw.(nodeJobResult)
	if !ok {
		return "", "mark_lost_failed"
	}
	if !result.Found || !nodeResultPosted(result) {
		return nodeJobMarkLostResultMissing, ""
	}
	return "", ""
}

// nodeResultPosted reports a job.result that carries a validated findings
// object or a report on disk. A findings file holding JSON null is absent: it
// never reaches Findings as a value, but a node that did send the bytes "null"
// must not read as a posted result either.
func nodeResultPosted(result nodeJobResult) bool {
	return (len(result.Findings) > 0 && !isJSONNull(result.Findings)) || result.Report.Present
}
