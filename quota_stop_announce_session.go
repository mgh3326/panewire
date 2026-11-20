package panewire

import "errors"

// errQuotaStopPaneReused refuses a hand-over whose pane, in the job's own herdr
// session, has a newer binding by another job (hk 5808). Nothing is announced on
// that pane. The decision is retired as a terminal refusal (handOver), so no
// later tick retries it.
var errQuotaStopPaneReused = errors.New("quota stop pane reused by another job")

// quotaAnnounceSession is the herdr session an announce for job reads pane in
// (hk 5808). A pane id is unique only within one herdr session, and the newest
// binding of a pane across all jobs can belong to another session's job. So the
// job's own newest binding of pane decides the session. A job with no binding of
// pane takes fallback(job), the node's own session for that job, and never
// another job's binding by pane id. When a newer binding of pane in the job's
// session belongs to another job, ended or not, the announce is refused.
func quotaAnnounceSession(inboxRoot, job, pane string, fallback func(job string) string) (string, error) {
	cands := quotaPaneBindings(inboxRoot, pane)
	var own *paneBindingCandidate
	for i := range cands {
		if cands[i].jobID == job {
			own = &cands[i]
		}
	}
	if own == nil {
		if fallback == nil {
			return "", nil
		}
		return fallback(job), nil
	}
	newest := *own
	for _, cand := range cands {
		if cand.session == own.session && newerPaneBinding(cand, newest) {
			newest = cand
		}
	}
	if newest.jobID != job {
		return "", errQuotaStopPaneReused
	}
	return own.session, nil
}

// newerPaneBinding reports whether cand is a later binding than best, whatever
// their jobs' liveness: a reused pane is a reuse even after its new job ended.
func newerPaneBinding(cand, best paneBindingCandidate) bool {
	return cand.createdAt > best.createdAt || (cand.createdAt == best.createdAt && cand.seq > best.seq)
}

// quotaNodeAnnounceSession is the no-binding fallback the daemon gives the quota
// manager (hk 5808): the job's own herdr session, else the node's spawn session.
// It is herdrSessionFor with no pane, so it never consults another job by pane id.
func quotaNodeAnnounceSession(client *HubClient) func(job string) string {
	return func(job string) string {
		return client.herdrSessionFor(job, "")
	}
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
