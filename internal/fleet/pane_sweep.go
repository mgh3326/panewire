package fleet

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// The node's task pane sweep (docs/pane-sweep.md) closes the pane of a worker
// or tester whose bound hk task has ended, or whose job was closed
// (job.revoked, job.completed), once that pane has stayed not working for a
// grace. It exists because a worker stays in its pane after READY, so
// `panewire reap` (terminal jobs only) never reaches it.

// PaneScreenFile is the job-directory file the sweep saves the pane's last
// screen to before it closes the pane; mergewake reap used to leave it.
const PaneScreenFile = "pane-screen.txt"

// DefaultPaneSweepGrace is how long a pane must have stayed not working.
const DefaultPaneSweepGrace = 10 * time.Minute

// TaskStatus is what the sweep reads of one hk task: its state and the
// category that state belongs to.
type TaskStatus struct{ State, Category string }

// TaskStateReader answers the status of one hk task.
type TaskStateReader func(ctx context.Context, task string) (TaskStatus, error)

// NewTaskStateReader reads GET /v1/tasks/<id> with an explicit handoffkeep
// endpoint and credential, as NewJobStore does for job records.
func NewTaskStateReader(baseURL, token, cfID, cfSecret string) (TaskStateReader, error) {
	client, err := newNodeHKClient(baseURL, token, cfID, cfSecret)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, task string) (TaskStatus, error) {
		id, err := strconv.ParseInt(task, 10, 64)
		if err != nil || id <= 0 {
			return TaskStatus{}, errors.New("task id is not numeric")
		}
		var record struct {
			ID       int64  `json:"id"`
			State    string `json:"state"`
			Category string `json:"category"`
		}
		if err := client.get(ctx, "/v1/tasks/"+url.PathEscape(task), &record); err != nil {
			return TaskStatus{}, err
		}
		if record.ID != id || record.State == "" {
			return TaskStatus{}, errors.New("task record is invalid")
		}
		return TaskStatus{State: record.State, Category: record.Category}, nil
	}, nil
}

// The hk task categories that end a job. A state such as deployed or merged
// belongs to one of them, so the sweep follows the category and does not need
// to know every state name.
const (
	taskCategoryCompleted = "completed"
	taskCategoryCanceled  = "canceled"
)

// paneSweepStateFallback is the hk job state a sweep writes for each task
// state that makes a pane eligible when the task record carries no category.
var paneSweepStateFallback = map[string]string{"merged": "completed", "done": "completed", "dropped": "lost"}

// paneSweepEnd says why a pane is swept and what the sweep then writes and
// removes.
type paneSweepEnd struct {
	state     string // the hk job state to write: completed or lost
	reason    string // the job.reaped reason
	taskState string // the hk task state read, "" when none was read
	// merged: the task completed, so its work landed (the worktree is removed
	// only clean and merged). discard: the end is not a merge — the task was
	// canceled, or the job was closed abandoned or superseded — so the
	// worktree is saved and removed whatever its state.
	merged, discard bool
	// byJob: no task ended; the job's own closing record is the only reason,
	// and a revived job cancels the close.
	byJob bool
}

// paneSweepTaskEnd judges a task by its category; the explicit state names
// decide only when the category is absent.
func paneSweepTaskEnd(t TaskStatus) (paneSweepEnd, bool) {
	end := paneSweepEnd{taskState: t.State, reason: "task_" + t.State}
	switch t.Category {
	case taskCategoryCompleted:
		end.state, end.merged = "completed", true
	case taskCategoryCanceled:
		end.state, end.discard = "lost", true
	case "":
		state, ok := paneSweepStateFallback[t.State]
		if !ok {
			return end, false
		}
		end.state, end.merged, end.discard = state, state == "completed", state == "lost"
	default:
		return end, false
	}
	return end, true
}

// paneSweepClosed reports that the job's latest record ended it: a job.revoked
// with any outcome (panewire job close), or a job.completed. A job.lost,
// job.joined or job.handed_off is not one: the hub or another lane owns it.
func paneSweepClosed(j Job) bool {
	return j.Terminal && (j.Ended == "job.revoked" || j.Ended == "job.completed")
}

// paneSweepJobEnd is the end a closed job gives when its task has not ended.
func paneSweepJobEnd(j Job, taskState string) paneSweepEnd {
	end := paneSweepEnd{state: "lost", taskState: taskState, byJob: true}
	switch {
	case j.Outcome != "":
		end.reason = "job_" + j.Outcome
	case j.Ended == "job.completed":
		end.reason = "job_completed"
	default:
		end.reason = "job_revoked"
	}
	if j.Ended == "job.completed" || j.Outcome == "completed" {
		end.state = "completed"
	}
	end.discard = j.Outcome == "abandoned" || j.Outcome == "superseded"
	return end
}

// paneSweepIdleStatus is the herdr agent status of a pane that is not working
// and is not stopped at a prompt. A blocked worker may be waiting on an ESC
// answer, so unlike a reviewer it is never swept (see nodePaneClosableStatus).
var paneSweepIdleStatus = []string{"idle", "done"}

// TaskPaneSweeper holds what the sweep must remember between passes: the first
// time each pane was seen not working. That time is the sweeper's own, so the
// grace counts real idle time, not time since the job spawned.
type TaskPaneSweeper struct {
	Grace     time.Duration
	TaskState TaskStateReader
	// WorkRoot is the node's own work root: the clone it prepares job
	// worktrees from. Empty keeps every worktree, since nothing then proves
	// one was the node's (sweep_worktree.go).
	WorkRoot string
	// Info receives one line per closed pane and one per kept candidate. It
	// gets ids and states only, never screen text.
	Info func(message string, attrs ...any)
	// Exclusive, when set, is called before each close and the function it
	// returns after it, so a close never interleaves with another acting node
	// command (node_command.go nodeCommandMu).
	Exclusive func() (release func())

	idleSince map[string]time.Time
}

func (s *TaskPaneSweeper) info(message string, attrs ...any) {
	if s.Info != nil {
		s.Info(message, attrs...)
	}
}

// paneSweepKey names one pane of one job, so a job that is respawned into a new
// pane starts its idle time again.
func paneSweepKey(j Job) string { return j.ID + "\x00" + j.Pane }

func paneSweepCandidate(j Job) bool {
	return !j.Reaped && !j.Keep && (j.Role == "worker" || j.Role == "tester") && j.Pane != "" && j.Label != ""
}

func paneSweepMatches(j Job, ag Agent) bool {
	return ag.Pane == j.Pane && ag.Name == j.Label && (j.Tab == "" || ag.Tab == j.Tab)
}

// Pass runs one sweep over the jobs under a.Jobs. A pass that cannot read
// herdr changes nothing; one that cannot read a task keeps that job's pane.
func (s *TaskPaneSweeper) Pass(ctx context.Context, a *App) error {
	entries, err := os.ReadDir(a.Jobs)
	if errors.Is(err, os.ErrNotExist) {
		clear(s.idleSince)
		return nil
	}
	if err != nil {
		return err
	}
	var candidates []Job
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		j, err := a.job(entry.Name())
		if err != nil {
			continue
		}
		if paneSweepCandidate(j) {
			candidates = append(candidates, j)
		}
	}
	if len(candidates) == 0 {
		clear(s.idleSince)
		return nil
	}
	agents, err := a.agents(ctx)
	if err != nil {
		// Nothing proved the panes stayed idle through this pass, so the
		// grace starts again once herdr answers.
		clear(s.idleSince)
		s.info("task pane sweep kept every pane", "reason", "herdr-unreadable")
		return nil
	}
	if s.idleSince == nil {
		s.idleSince = map[string]time.Time{}
	}
	now := a.Now()
	live := map[string]bool{}
	for _, j := range candidates {
		key := paneSweepKey(j)
		live[key] = true
		if !s.idle(j, agents) {
			delete(s.idleSince, key)
			continue
		}
		if _, seen := s.idleSince[key]; !seen {
			s.idleSince[key] = now
		}
	}
	for key := range s.idleSince {
		if !live[key] {
			delete(s.idleSince, key)
		}
	}
	statuses := map[string]paneSweepTaskRead{}
	parents := map[string]string{}
	for _, j := range candidates {
		closed := paneSweepClosed(j)
		task := j.Task
		if task == "" {
			// A fix author spawned with --parent-job is judged on its parent's task.
			task = s.inheritedTask(ctx, a, j, parents)
		}
		var status TaskStatus
		switch {
		case task == "":
			if !closed {
				s.info("task pane kept", "job", j.ID, "pane", j.Pane, "reason", "no_task")
				continue
			}
		default:
			read, known := statuses[task]
			if !known {
				read = s.readTask(ctx, task)
				statuses[task] = read
			}
			if read.err != nil && !closed {
				s.info("task pane kept", "job", j.ID, "task", task, "pane", j.Pane, "reason", "task-read-failed")
				continue
			}
			status = read.status
		}
		// A task that ended decides the end state; a closed job whose task is
		// still live, or unreadable, ends by its own record.
		end, eligible := paneSweepTaskEnd(status)
		if !eligible {
			if !closed {
				continue
			}
			end = paneSweepJobEnd(j, status.State)
		}
		since, idle := s.idleSince[paneSweepKey(j)]
		switch {
		case !idle:
			s.info("task pane kept", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", s.notIdleReason(j, agents))
		case now.Sub(since) < s.Grace:
			s.info("task pane kept", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", "grace", "idle_seconds", int(now.Sub(since).Seconds()))
		default:
			if reason := s.close(ctx, a, j, task, end, since, now); reason != "" {
				s.info("task pane kept", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", reason)
			}
		}
	}
	return nil
}

// paneSweepTaskRead is one task read, kept for the pass so a task shared by
// several jobs is read once.
type paneSweepTaskRead struct {
	status TaskStatus
	err    error
}

func (s *TaskPaneSweeper) readTask(ctx context.Context, task string) paneSweepTaskRead {
	if s.TaskState == nil {
		return paneSweepTaskRead{err: errors.New("no task reader")}
	}
	status, err := s.TaskState(ctx, task)
	return paneSweepTaskRead{status: status, err: err}
}

// inheritedTask resolves the task of a job whose claim names none: the task of
// the job it was spawned under (--parent-job), through that job's record on
// this node, else its hk job record's task_id. The parent id is the claim's
// task_inherited_from, else the job's own hk record's parent_job. "" means
// neither resolved, and the pane stays.
func (s *TaskPaneSweeper) inheritedTask(ctx context.Context, a *App, j Job, cache map[string]string) string {
	store, storeErr := a.jobStore()
	parent := j.Inherited
	if parent == "" && storeErr == nil {
		if rec, err := store.Get(ctx, j.ID); err == nil {
			parent = str(rec, "parent_job")
		}
	}
	if parent == "" || parent == j.ID || !validID(parent) {
		return ""
	}
	if task, ok := cache[parent]; ok {
		return task
	}
	task := ""
	if local, err := a.job(parent); err == nil {
		task = local.Task
	}
	if task == "" && storeErr == nil {
		if rec, err := store.Get(ctx, parent); err == nil {
			task = scalar(rec["task_id"])
		}
	}
	cache[parent] = task
	return task
}

// idle reports that the job's pane is listed under the job's own name and is
// not working. A missing pane, a renamed one or an unreadable status is not.
func (s *TaskPaneSweeper) idle(j Job, agents []Agent) bool {
	for _, ag := range agents {
		if ag.Pane == j.Pane {
			return paneSweepMatches(j, ag) && slices.Contains(paneSweepIdleStatus, ag.Status)
		}
	}
	return false
}

func (s *TaskPaneSweeper) notIdleReason(j Job, agents []Agent) string {
	for _, ag := range agents {
		if ag.Pane == j.Pane {
			if !paneSweepMatches(j, ag) {
				return "pane-mismatch"
			}
			return "pane-" + ag.Status
		}
	}
	return "pane-gone"
}

// close is reap's tail for one swept job. It returns a reason when it kept the
// pane, and "" once the pane is closed and the job reaped.
func (s *TaskPaneSweeper) close(ctx context.Context, a *App, j Job, task string, end paneSweepEnd, since, now time.Time) string {
	if s.Exclusive != nil {
		defer s.Exclusive()()
	}
	key := paneSweepKey(j)
	if err := a.savePaneScreen(ctx, j); err != nil {
		return "screen-not-saved"
	}
	// Re-read both durable history and the live pane immediately before the
	// irreversible close, as reap does. A revived or kept job always wins.
	latest, err := a.job(j.ID)
	if err != nil {
		return "job-unreadable"
	}
	if !paneSweepCandidate(latest) || latest.Pane != j.Pane || latest.Label != j.Label || (end.byJob && !paneSweepClosed(latest)) {
		delete(s.idleSince, key)
		return "job-changed"
	}
	if end.byJob {
		// The job may have been closed again, with another outcome, since the
		// pass judged it: the end that is written and cleaned up is the latest.
		end = paneSweepJobEnd(latest, end.taskState)
	}
	fresh, err := a.agents(ctx)
	if err != nil {
		return "herdr-unreadable"
	}
	if !s.idle(latest, fresh) {
		delete(s.idleSince, key)
		return s.notIdleReason(latest, fresh)
	}
	// The usage is read before the close: once herdr stops listing the pane the
	// live session reference is gone, and a receipt that names no session reads
	// no figures. The pane closes before hk is told the job ended, so a failed
	// close never leaves a finished record beside an open pane. A failed record
	// write after the close is a job.record_failed event plus a warning, as in
	// reap. The usage is written after the record, as lifecycle done does.
	usage, haveUsage := a.readJobUsage(ctx, latest, true)
	if err := a.must(ctx, "herdr", "pane", "close", j.Pane); err != nil {
		return "close-failed"
	}
	delete(s.idleSince, key)
	a.markSweptJob(ctx, latest, end.state)
	if haveUsage {
		a.writeJobUsage(ctx, latest.ID, usage)
	}
	if err := a.releaseQuota(ctx, j); err != nil {
		a.warn("quota retained: %v", err)
	}
	if owner, foreign := a.laneOwnedElsewhere(j); foreign {
		a.warn("lane %s retained: it belongs to machine %s and this host is %s", j.HubLane, j.Machine, owner)
	} else if err := a.removeLane(ctx, j); err != nil {
		a.warn("lane retained: %v", err)
	}
	if err := a.event(ctx, j.ID, "job.reaped", map[string]any{"pane_id": j.Pane, "reason": end.reason}); err != nil {
		a.warn("job.reaped not recorded: %v", err)
	}
	s.info("task pane closed", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "idle_seconds", int(now.Sub(since).Seconds()), "reason", end.reason)
	s.sweepWorktree(ctx, a, latest, end)
	return ""
}

// savePaneScreen writes the pane's last screen to <job dir>/pane-screen.txt
// through an os.Root on the job directory, with the mode set before any byte
// is written, as the pane.capture node command does.
func (a *App) savePaneScreen(ctx context.Context, j Job) error {
	r := a.call(ctx, "herdr", "pane", "read", j.Pane)
	if err := check(r, "pane screen"); err != nil {
		return err
	}
	root, err := os.OpenRoot(filepath.Join(a.Jobs, j.ID))
	if err != nil {
		return err
	}
	defer root.Close()
	return durable.ReplaceFileAtomicallyInRoot(root, PaneScreenFile, []byte(r.Out), 0600)
}

// markSweptJob writes the end state to the hk record of a job that never
// reported done: completed for a completed task, lost for a canceled one.
// It goes through putJobRecord, the writer done and watch use. A record that
// is already completed, escalated or lost, or that hk does not hold, is left
// alone; a failed read or write is a warning plus job.record_failed, and the
// sweep goes on, as reap does.
func (a *App) markSweptJob(ctx context.Context, j Job, endState string) {
	store, err := a.jobStore()
	if err != nil {
		a.jobRecordFailed(ctx, j.ID, endState, err)
		return
	}
	rec, err := store.Get(ctx, j.ID)
	if errors.Is(err, errJobNotFound) {
		return
	}
	if err != nil {
		a.jobRecordFailed(ctx, j.ID, endState, err)
		return
	}
	if state := str(rec, "state"); state != "claimed" && state != "spawned" {
		return
	}
	if err := a.putJobRecord(ctx, j.ID, map[string]any{"state": endState}); err != nil {
		a.jobRecordFailed(ctx, j.ID, endState, err)
	}
}
