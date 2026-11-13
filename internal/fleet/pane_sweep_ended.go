package fleet

import (
	"context"
	"errors"
	"os"
	"slices"
	"time"
)

// SweptPane describes a pane closed by an ended-job sweep. The current herdr
// interface cannot authorize an ownership-bound close, so sweeps return none.
type SweptPane struct {
	Job    string `json:"job"`
	Pane   string `json:"pane"`
	Role   string `json:"role"`
	Reason string `json:"reason"`
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

// SweepEnded runs one ended-job pass (hk 4512). It observes jobs whose own
// record ended at least age ago. It keeps their panes because herdr provides no
// ownership-bound close operation. A completed worker keeps its pane while
// its PR is open, as the local sweep judges it. Pass applies the same
// ownership rule to idle panes.
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
	statuses := map[string]paneSweepTaskRead{}
	prs := map[string]paneSweepPRRead{}
	parents := map[string]paneSweepInherited{}
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
		if reason := s.captureDiagnostic(ctx, a, j); reason != "" {
			s.info("task pane kept", "job", j.ID, "task", task, "pane", j.Pane, "reason", reason)
		}
	}
	return nil, nil
}
