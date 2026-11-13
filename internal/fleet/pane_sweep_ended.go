package fleet

import (
	"context"
	"errors"
	"os"
	"slices"
	"time"
)

// SweptPane is one pane the ended-job sweep closed: the job it belonged to,
// the pane, the job's spawn role, and the end reason written on job.reaped.
// The node reports these to the hub, which logs them (docs/pane-sweep.md).
type SweptPane struct {
	Job    string `json:"job"`
	Pane   string `json:"pane"`
	Role   string `json:"role"`
	Reason string `json:"reason"`
}

// paneSweepEndedRule is the ended-job sweep's judgment (hk 4512). A nil rule
// is the local sweep: an idle pane after the grace, judged by its task or its
// job's end (Pass).
type paneSweepEndedRule struct {
	age time.Duration
}

// paneSweepOrderedEnded reports whether the job's own record ended it for the
// ended-job sweep: a revoked, completed, or handed-off job, or a lost job that
// was not an announced quota stop (watch_quota_stop.go). A quota stop is a
// hand-off, not an end, so its pane stays with the job that carries on.
func paneSweepOrderedEnded(j Job) bool {
	if !j.Terminal {
		return false
	}
	switch j.Ended {
	case "job.revoked", "job.completed", "job.handed_off":
		return true
	case "job.lost":
		_, stopped := announcedQuotaStop(j)
		return !stopped
	}
	return false
}

// paneSweepEndedCandidate is the ended-job sweep's admission rule. It mirrors
// paneSweepCandidate's identity and spawn-role rules, and admits a job only
// once its own record ended it at least age before now (its terminal event
// time, not the pane's idle time).
func paneSweepEndedCandidate(j Job, now time.Time, age time.Duration) bool {
	return !j.Reaped && !j.Keep && j.Pane != "" && j.Label != "" &&
		slices.Contains(spawnRoles, j.QuotaRole()) &&
		paneSweepOrderedEnded(j) &&
		!j.At.IsZero() && now.Sub(j.At) >= age
}

// paneSweepStillJudged re-checks, under the exclusive lock and just before the
// irreversible close, that the job is still the one the pass judged. With no
// ended rule that is the local sweep's check; with one it is the ended-job
// rule's (hk 4512).
func paneSweepStillJudged(latest, judged Job, end paneSweepEnd, now time.Time, ended *paneSweepEndedRule) bool {
	if latest.Pane != judged.Pane || latest.Label != judged.Label ||
		(paneSweepWaitsOnPR(latest) && !paneSweepWaitsOnPR(judged)) {
		return false
	}
	if ended != nil {
		return paneSweepEndedCandidate(latest, now, ended.age)
	}
	return paneSweepCandidate(latest) && (!end.byJob || paneSweepClosed(latest))
}

// SweepEnded runs one ended-job pass (hk 4512). It closes the pane of every
// job that its own record ended at least age ago, whatever herdr shows the pane
// doing, and returns one SweptPane per pane closed. A completed worker keeps
// its pane while its PR is open, as the local sweep judges it. Pass remains
// the local idle-pane sweep and is not changed by this.
func (s *TaskPaneSweeper) SweepEnded(ctx context.Context, a *App, age time.Duration) ([]SweptPane, error) {
	s.retryDeferredFailures(ctx, a)
	entries, err := os.ReadDir(a.Jobs)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	now := a.Now()
	var candidates []Job
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		j, err := a.job(entry.Name())
		if err != nil {
			continue
		}
		if paneSweepEndedCandidate(j, now, age) {
			candidates = append(candidates, j)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	agents, err := a.agents(ctx)
	if err != nil {
		return nil, err
	}
	rule := &paneSweepEndedRule{age: age}
	statuses := map[string]paneSweepTaskRead{}
	prs := map[string]paneSweepPRRead{}
	parents := map[string]paneSweepInherited{}
	var closed []SweptPane
	for _, j := range candidates {
		if !slices.ContainsFunc(agents, func(ag Agent) bool { return paneSweepMatches(j, ag) }) {
			s.info("task pane kept", "job", j.ID, "pane", j.Pane, "reason", "pane_not_listed")
			continue
		}
		var task string
		var taskErr error
		if paneSweepTaskRole(j) {
			task = j.Task
			if task == "" {
				task, taskErr = s.inheritedTask(ctx, a, j, parents)
			}
		}
		var status TaskStatus
		if task != "" {
			read, known := statuses[task]
			if !known {
				read = s.readTask(ctx, task)
				statuses[task] = read
			}
			status = read.status
		}
		s.recordMergedOutcomes(ctx, a, j, task, status, prs)
		s.observeFailure(ctx, a, j, task)
		end := paneSweepJobEnd(j, status.State)
		if paneSweepTaskRole(j) && paneSweepWaitsOnPR(j) {
			if reason, number := s.paneSweepPRKeep(ctx, a, j, task, taskErr, prs); reason != "" {
				attrs := []any{"job", j.ID, "task", task, "pane", j.Pane, "reason", reason}
				if number > 0 {
					attrs = append(attrs, "pr", number)
				}
				s.info("task pane kept", attrs...)
				continue
			}
		}
		if reason := s.close(ctx, a, j, task, end, time.Time{}, now, rule); reason != "" {
			s.info("task pane kept", "job", j.ID, "task", task, "pane", j.Pane, "reason", reason)
			continue
		}
		closed = append(closed, SweptPane{Job: j.ID, Pane: j.Pane, Role: j.QuotaRole(), Reason: end.reason})
		s.closedFailure(ctx, a, j, task)
	}
	return closed, nil
}
