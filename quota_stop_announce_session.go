package panewire

import "errors"

// errQuotaStopPaneReused refuses a hand-over whose pane, in the job's own herdr
// session, is bound to another job now (hk 5808). Nothing is announced on that
// pane. The stop is retried like any failed announce, and the job's watcher
// ends it as lost without a replacement if the hand-over never lands.
var errQuotaStopPaneReused = errors.New("quota stop pane reused by another job")

// quotaAnnounceSession is the herdr session an announce for job reads pane in
// (hk 5808). A pane id is unique only within one herdr session, and the newest
// binding of a pane across all jobs can belong to another session's job. So the
// job's own newest binding of pane decides the session. A job with no binding
// of pane returns "", the node default. When another job owns pane in that
// session, the announce is refused.
func quotaAnnounceSession(inboxRoot, job, pane string) (string, error) {
	cands := quotaPaneBindings(inboxRoot, pane)
	var own *paneBindingCandidate
	for i := range cands {
		if cands[i].jobID == job {
			own = &cands[i]
		}
	}
	if own == nil {
		return "", nil
	}
	var owner paneBindingCandidate
	found := false
	for _, cand := range cands {
		if cand.session != own.session {
			continue
		}
		if !found || betterPaneBinding(cand, owner) {
			owner = cand
			found = true
		}
	}
	if found && owner.jobID != job {
		return "", errQuotaStopPaneReused
	}
	return own.session, nil
}

// quotaPaneBindings returns, for each job under inboxRoot that binds pane, that
// job's newest binding of pane. It reads the inbox index, as
// nodeJobHerdrBindingByPaneFromRoot does, so a rebound pane is seen at once.
func quotaPaneBindings(inboxRoot, pane string) []paneBindingCandidate {
	if pane == "" || inboxRoot == "" {
		return nil
	}
	var out []paneBindingCandidate
	_ = hubIndexFor(inboxRoot).visitJobs(nil, true, func(job *hubIndexedJob) bool {
		var match *hubPaneBindingFact
		for i := range job.bindings {
			if job.bindings[i].pane == pane {
				match = &job.bindings[i]
			}
		}
		if match != nil {
			out = append(out, paneBindingCandidate{jobID: job.id, session: match.session, live: !job.ended, createdAt: match.createdAt, seq: match.seq})
		}
		return true
	})
	return out
}
