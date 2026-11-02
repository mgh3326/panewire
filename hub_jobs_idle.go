package panewire

import "time"

// hubJobIdleAfter is how long a job's pane may sit idle or done before the
// heartbeat stops reporting the job as active. A worker that finished but
// never wrote a terminal event, or a session left open after its work, would
// otherwise count as load forever (docs/placement.md).
const hubJobIdleAfter = 30 * time.Minute

// dropLongIdleJobs removes jobs whose pane herdr has reported idle or done
// for at least hubJobIdleAfter. The idle clock is kept in memory across
// heartbeats: a restarted node starts it again, which only delays the drop.
// Only the heartbeat's active list is filtered; the idle-shutdown lease scan
// still counts these jobs, so a lead waiting on its workers keeps its host up.
func (client *HubClient) dropLongIdleJobs(active []HubActiveJob, states []HerdrAgentState, now time.Time) []HubActiveJob {
	client.paneIdleMu.Lock()
	defer client.paneIdleMu.Unlock()
	client.paneIdleSince = trackPaneIdleSince(client.paneIdleSince, states, now)
	return filterLongIdleJobs(active, client.paneIdleSince, now, hubJobIdleAfter)
}

// trackPaneIdleSince keeps the first time each pane was seen idle or done.
// A pane that is working, blocked, unknown or gone loses its clock, so the
// next idle stretch starts from zero.
func trackPaneIdleSince(previous map[string]time.Time, states []HerdrAgentState, now time.Time) map[string]time.Time {
	next := make(map[string]time.Time, len(states))
	for _, state := range states {
		if state.PaneID == "" || (state.Status != "idle" && state.Status != "done") {
			continue
		}
		if since, seen := previous[state.PaneID]; seen {
			next[state.PaneID] = since
		} else {
			next[state.PaneID] = now
		}
	}
	return next
}

func filterLongIdleJobs(active []HubActiveJob, idleSince map[string]time.Time, now time.Time, after time.Duration) []HubActiveJob {
	kept := make([]HubActiveJob, 0, len(active))
	for _, job := range active {
		if since, idle := idleSince[job.Pane]; job.Pane != "" && idle && now.Sub(since) >= after {
			continue
		}
		kept = append(kept, job)
	}
	return kept
}
