package panewire

import "time"

// hubJobIdleAfter is how long a job's pane may sit idle or done before the
// heartbeat stops reporting the job as active. A worker that finished but
// never wrote a terminal event, or a session left open after its work, would
// otherwise count as load forever (docs/placement.md).
const hubJobIdleAfter = 30 * time.Minute

// dropLongIdleJobs removes jobs whose pane herdr has reported idle or done
// for at least hubJobIdleAfter while that job was on it. The idle clock is
// kept in memory across heartbeats: a restarted node starts it again, which
// only delays the drop. Only the heartbeat's active list is filtered; the
// job scan still counts these jobs, so a lead waiting on its
// workers keeps its host up.
func (client *HubClient) dropLongIdleJobs(active []HubActiveJob, states []HerdrAgentState, now time.Time) []HubActiveJob {
	client.paneIdleMu.Lock()
	defer client.paneIdleMu.Unlock()
	client.paneIdleSince = trackJobIdleSince(client.paneIdleSince, active, states, now)
	return filterLongIdleJobs(active, client.paneIdleSince, now, hubJobIdleAfter)
}

// resetIdleClock forgets every idle clock. An unreadable herdr snapshot hides
// whether a pane worked in between, so the next idle stretch starts from
// zero rather than from before the outage.
func (client *HubClient) resetIdleClock() {
	client.paneIdleMu.Lock()
	client.paneIdleSince = nil
	client.paneIdleMu.Unlock()
}

// jobIdleKey ties the clock to one job on one pane: a new job claimed on a
// pane that sat idle under an earlier job starts a fresh clock.
func jobIdleKey(job HubActiveJob) string { return job.JobID + "\x00" + job.Pane }

// trackJobIdleSince keeps the first time each job's pane was seen idle or
// done. A pane that is working, blocked, unknown or gone loses its clock, so
// the next idle stretch starts from zero.
func trackJobIdleSince(previous map[string]time.Time, active []HubActiveJob, states []HerdrAgentState, now time.Time) map[string]time.Time {
	status := make(map[string]string, len(states))
	for _, state := range states {
		if state.PaneID != "" {
			status[state.PaneID] = state.Status
		}
	}
	next := make(map[string]time.Time, len(active))
	for _, job := range active {
		if job.Pane == "" || (status[job.Pane] != "idle" && status[job.Pane] != "done") {
			continue
		}
		key := jobIdleKey(job)
		if since, seen := previous[key]; seen {
			next[key] = since
		} else {
			next[key] = now
		}
	}
	return next
}

func filterLongIdleJobs(active []HubActiveJob, idleSince map[string]time.Time, now time.Time, after time.Duration) []HubActiveJob {
	kept := make([]HubActiveJob, 0, len(active))
	for _, job := range active {
		if since, idle := idleSince[jobIdleKey(job)]; job.Pane != "" && idle && now.Sub(since) >= after {
			continue
		}
		kept = append(kept, job)
	}
	return kept
}
