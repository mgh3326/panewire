package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// Failure reps (docs/reps.md §Automatic outcomes). A job's failure row is
// written only once its outcome is final:
//   - it joined no PR, or
//   - its joined PR is closed unmerged.
//
// While the joined PR is open, or its state cannot be read, nothing is written
// and the job is retried later. A merged PR is left to the merged path, which
// writes the job's one row. Every row shares the job's origin id, so a job has
// at most one row, and a row that already exists is kept.

// recordFailedOutcome writes the job's failure rep when its outcome is final.
// final is false while the joined PR is open or unknown: the caller keeps the
// job and asks again. A merged PR is final too, since the merged path writes
// that row. prs reads the task's PRs; nil means an unknown state.
func (a *App) recordFailedOutcome(ctx context.Context, j Job, task, ended string, prs PRReader) (bool, error) {
	if task == "" || outcomeExcluded(j) {
		return true, nil
	}
	state, err := jobPRState(ctx, j, task, prs)
	if err != nil {
		return false, err
	}
	switch state {
	case "merged":
		return true, nil
	case "open", "unknown":
		return false, nil
	}
	store, err := a.benchStore()
	if err != nil {
		return false, err
	}
	var rec map[string]any
	if jobStore, err := a.jobStore(); err == nil {
		rec, _ = jobStore.Get(ctx, j.ID)
	}
	receipt := j.Receipt
	profile := firstNonempty(j.Profile, str(receipt, "profile"))
	effort := firstNonempty(j.Effort, str(receipt, "effort"))
	model := firstNonempty(str(receipt, "model_id"), str(rec, "model"))
	harness := outcomeHarness(receipt, rec)
	var grade, tier string
	if status, err := a.TaskStatus(ctx, task); err == nil {
		grade, tier = outcomeGrade(status.Title)
	}
	if tier == "" {
		tier = firstNonempty(j.T, "T0")
	}
	rounds := outcomeRounds(j)
	notes, _ := json.Marshal(map[string]any{
		"source": "panewire", "schema": 1, "job": j.ID, "task": task,
		"profile": profile, "model": model, "effort": effort, "harness": harness,
		"grade": grade, "tier": tier, "rounds": rounds, "ended": ended,
	})
	completed := 0
	rep := Rep{
		OriginID: stableRepOriginID("panewire-outcome:" + j.ID),
		Profile:  profile, ModelID: model, TaskRef: "hk" + task, Tier: tier,
		Role: outcomeRole(j), Rounds: rounds,
		Completed: &completed, RecordedAt: a.Now().UTC().Format(time.RFC3339Nano),
		Effort: effort, Grade: grade, Notes: string(notes),
	}
	if catalog, err := a.benchCatalog(ctx); err == nil {
		rep.TableGrade = repTableGrade(catalog, profile, effort)
	}
	if _, _, err := store.AddOutcome(ctx, rep); err != nil {
		return false, err
	}
	return true, nil
}

// jobPRState is the state of the PR the job joined: "none" when it joined none,
// else "merged", "open", "closed" (closed unmerged), or "unknown" when the task's
// PR list does not show it or cannot be read.
func jobPRState(ctx context.Context, j Job, task string, prs PRReader) (string, error) {
	if !jobHasJoinedPR(j) {
		return "none", nil
	}
	if prs == nil {
		return "unknown", nil
	}
	changes, err := prs(ctx, task)
	if err != nil {
		return "", err
	}
	pr, head := joinedPR(j)
	state := "unknown"
	for _, change := range changes {
		if !jobChangeMatches(change, pr, head) {
			continue
		}
		switch strings.ToLower(change.State) {
		case "merged":
			return "merged", nil
		case "open":
			state = "open"
		default:
			if state != "open" {
				state = "closed"
			}
		}
	}
	return state, nil
}

// jobChangeMatches reports that a change is one the job itself joined, by head
// or PR number, whatever its state.
func jobChangeMatches(change PRChange, pr, head string) bool {
	if head != "" && change.Head != "" && strings.EqualFold(head, change.Head) {
		return true
	}
	n := prNumber(pr)
	return n > 0 && n == change.Number
}

// outcomeExcluded reports a job spawned to measure a catalog rung. Such a job
// writes no automatic rep, merged or failed.
func outcomeExcluded(j Job) bool {
	return strings.HasPrefix(strings.TrimSpace(str(j.Receipt, "override_reason")), "catalog experiment")
}

// jobCompleted reports that the job's own record completed it.
func jobCompleted(j Job) bool {
	return j.Ended == "job.completed" || j.Outcome == "completed"
}

// failedEndOf names the failure a live job has already ended with, or "" when
// it has none to record: an escalation, or a lost job that is not a quota-stop
// hand-off (that job goes to a successor, not a failure).
func failedEndOf(j Job) string {
	if j.Ended == "job.lost" {
		if _, quota := announcedQuotaStop(j); quota {
			return ""
		}
		return "lost"
	}
	// job.escalate leaves the projection's Terminal and Ended alone, so read the
	// events: the newest escalation counts unless a later spawn, claim, reprompt
	// or other end came after it.
	for i := len(j.Events) - 1; i >= 0; i-- {
		switch j.Events[i].Kind {
		case "job.escalate":
			return "escalated"
		case "job.claim", "job.spawned", "job.reprompted", "job.completed", "job.revoked", "job.lost", "job.handed_off":
			return ""
		}
	}
	return ""
}

// deferredFailure is a failure rep the sweep still owes: the job, its task, and
// the ended reason it will be written with.
type deferredFailure struct {
	job   Job
	task  string
	ended string
}

// closedFailure is the failure rep for a job the pane sweep just closed. A job
// its owner superseded gets one. A completed job with no PR of its own is a
// success and gets none. Any other closed job is reaped.
func (s *TaskPaneSweeper) closedFailure(ctx context.Context, a *App, j Job, task string) {
	ended := "reaped"
	switch {
	case j.Outcome == "superseded":
		ended = "superseded"
	case jobCompleted(j) && !jobHasJoinedPR(j):
		return
	}
	s.attemptFailure(ctx, a, deferredFailure{job: j, task: task, ended: ended})
}

// observeFailure takes a live job that already ended as escalated or lost, so
// the sweep owes its failure rep until it is final. The escalate and lost sites
// may run in another process, so the sweep is the one place that sees them all.
func (s *TaskPaneSweeper) observeFailure(ctx context.Context, a *App, j Job, task string) {
	ended := failedEndOf(j)
	if ended == "" || task == "" {
		return
	}
	if _, pending := s.failures[j.ID]; pending {
		return
	}
	s.attemptFailure(ctx, a, deferredFailure{job: j, task: task, ended: ended})
}

// retryDeferredFailures asks again for every failure rep still open. It runs
// first in each pass, so a job whose PR closed is written even when its pane is
// already gone.
func (s *TaskPaneSweeper) retryDeferredFailures(ctx context.Context, a *App) {
	for _, d := range s.failures {
		s.attemptFailure(ctx, a, d)
	}
}

// attemptFailure writes the rep when it is final, and otherwise keeps it for
// the next pass. A settled job is never written again by the sweep.
func (s *TaskPaneSweeper) attemptFailure(ctx context.Context, a *App, d deferredFailure) {
	id := d.job.ID
	if s.settledFailures[id] {
		delete(s.failures, id)
		return
	}
	final, err := a.recordFailedOutcome(ctx, d.job, d.task, d.ended, s.PR)
	switch {
	case err != nil:
		s.info("failure rep not recorded", "job", id, "reason", err.Error())
		s.keepFailure(d)
	case final:
		if s.settledFailures == nil {
			s.settledFailures = map[string]bool{}
		}
		s.settledFailures[id] = true
		delete(s.failures, id)
	default:
		s.keepFailure(d)
	}
}

func (s *TaskPaneSweeper) keepFailure(d deferredFailure) {
	if s.failures == nil {
		s.failures = map[string]deferredFailure{}
	}
	s.failures[d.job.ID] = d
}

// recordLostFailure writes the failure rep for a job the watcher ended as lost
// (decision unanswered, or its pane gone) when its outcome is already final.
func (a *App) recordLostFailure(ctx context.Context, j Job) {
	if _, err := a.recordFailedOutcome(ctx, j, j.Task, "lost", a.PRs); err != nil {
		a.warn("failure rep not recorded: %v", err)
	}
}
