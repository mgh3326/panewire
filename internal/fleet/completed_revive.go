package fleet

import (
	"context"
	"errors"
	"time"
)

// Reviving a completed job whose pane is busy again (hk 5799, from hk 4983;
// docs/quota-stop.md, "A completed job that its owner re-prompts").
//
// A prompt typed into a completed job's pane writes no job event, so no
// watcher starts for it and no pool trigger reads it. The pane sweep
// (TaskPaneSweeper.pass) already reads the herdr agent list and every job
// record on each pass, so the check runs there, not in a poller of its own.
//
// A completed job is revived only when its pane was seen not working after the
// job's completion (armed) and is now working on the same agent session and
// cwd as the job's spawn receipt, with no other job bound to the pane since.
// The revive writes job.reprompted, which makes the job live again, and starts
// its watcher rearmed. The armed state is memory only, so a restart waits for
// one new idle observation: the rule fails closed.

// CompletedPaneReviveReason is the job.reprompted reason a revive writes. The
// root hub index reads it to make the revived job live again (hub_jobs_index.go).
const CompletedPaneReviveReason = "completed_pane_busy"

// errReviveStale is the refusal of a revive whose job moved on between the
// look at the pane and the write under the events lock.
var errReviveStale = errors.New("completed job moved on before its revive was written")

// reviveStartWatch starts a revived job's watcher. A test replaces it.
var reviveStartWatch = func(a *App, id string) error { return a.startWatchMode(id, true) }

// reviveCandidate is a completed job that still owns its pane: the only jobs a
// busy pane can revive. A reaped or reassigned job is never revived.
func reviveCandidate(j Job) bool {
	return j.Ended == "job.completed" && j.Pane != "" && !j.Reaped && !j.Reassigned && !j.At.IsZero()
}

// reviveCompletedPanes revives the completed jobs among jobs whose panes are
// busy again. It returns the ids it revived, which the caller leaves out of
// this pass's close decisions. It changes no state when herdr or a job record
// is unreadable this pass.
func (s *TaskPaneSweeper) reviveCompletedPanes(ctx context.Context, a *App, jobs []Job, unreadable bool) map[string]bool {
	if s.reviveIdle == nil {
		s.reviveIdle = map[string]time.Time{}
	}
	revived := map[string]bool{}
	var due []Job
	live := map[string]bool{}
	for _, j := range jobs {
		if reviveCandidate(j) {
			due = append(due, j)
			live[j.ID] = true
		} else {
			delete(s.reviveIdle, j.ID)
		}
	}
	for id := range s.reviveIdle {
		if !live[id] {
			delete(s.reviveIdle, id)
		}
	}
	if len(due) == 0 || unreadable {
		return revived
	}
	agents, err := a.agents(ctx)
	if err != nil {
		return revived
	}
	for _, j := range due {
		ag, present := paneAgentRow(agents, j.Pane)
		if !present {
			delete(s.reviveIdle, j.ID)
			continue
		}
		if ag.Status != "working" {
			s.reviveIdle[j.ID] = j.At
			continue
		}
		if armed, ok := s.reviveIdle[j.ID]; !ok || !armed.Equal(j.At) {
			continue
		}
		if why := reviveRefusal(j, ag, jobs); why != "" {
			delete(s.reviveIdle, j.ID)
			s.info("completed job not revived", "job", j.ID, "pane", j.Pane, "reason", why)
			continue
		}
		if a.watchActive(j.ID) {
			continue
		}
		seen := viewLedger(j.Events)
		err := a.eventIf(ctx, j.ID, "job.reprompted", map[string]any{"reason": CompletedPaneReviveReason, "pane_id": j.Pane}, func(v ledgerView) error {
			if v.Attempt != seen.Attempt || v.Ended != "job.completed" || v.Pane != j.Pane || v.Reaped || v.Revoked || v.Reassigned || v.Settled {
				return errReviveStale
			}
			return nil
		})
		if errors.Is(err, errReviveStale) {
			delete(s.reviveIdle, j.ID)
			continue
		}
		if err != nil {
			a.warn("completed job %s revive not written: %v", j.ID, err)
			continue
		}
		delete(s.reviveIdle, j.ID)
		revived[j.ID] = true
		s.info("completed job revived", "job", j.ID, "pane", j.Pane)
		if err := reviveStartWatch(a, j.ID); err != nil {
			a.warn("revived job %s watcher did not start: %v", j.ID, err)
		}
	}
	return revived
}

// reviveRefusal names why a busy pane of a completed job is not that job's
// pane: another agent session or cwd is on it, or another job is bound to it
// since the job's completion. It returns "" when the pane is the job's own.
func reviveRefusal(j Job, ag Agent, jobs []Job) string {
	recorded := paneRecordedSession(j)
	if recorded == "" || agentSessionID(ag) != recorded {
		return "agent_session"
	}
	if !sweepSamePath(ag.CWD, str(j.Receipt, "cwd")) {
		return "cwd"
	}
	for _, other := range jobs {
		if other.ID == j.ID || other.Pane != j.Pane {
			continue
		}
		if other.Ended == "" || !newestSpawnAt(other).Before(j.At) {
			return "pane_reused"
		}
	}
	return ""
}

// paneAgentRow is the herdr agent row of pane, if the list has one.
func paneAgentRow(agents []Agent, pane string) (Agent, bool) {
	for _, ag := range agents {
		if ag.Pane == pane {
			return ag, true
		}
	}
	return Agent{}, false
}

// newestSpawnAt is the time of the job's newest job.spawned event, zero when
// it has none.
func newestSpawnAt(j Job) time.Time {
	var at time.Time
	for _, ev := range j.Events {
		if ev.Kind == "job.spawned" && ev.when.After(at) {
			at = ev.when
		}
	}
	return at
}

// withoutRevivedJobs keeps the jobs this pass did not revive.
func withoutRevivedJobs(jobs []Job, revived map[string]bool) []Job {
	kept := jobs[:0]
	for _, j := range jobs {
		if !revived[j.ID] {
			kept = append(kept, j)
		}
	}
	return kept
}
