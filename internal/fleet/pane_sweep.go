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
// or tester whose bound hk task is merged, done or dropped, once that pane has
// stayed not working for a grace. It exists because a worker stays in its pane
// after READY, so `panewire reap` (terminal jobs only) never reaches it.

// PaneScreenFile is the job-directory file the sweep saves the pane's last
// screen to before it closes the pane; mergewake reap used to leave it.
const PaneScreenFile = "pane-screen.txt"

// DefaultPaneSweepGrace is how long a pane must have stayed not working.
const DefaultPaneSweepGrace = 10 * time.Minute

// TaskStateReader answers the state of one hk task.
type TaskStateReader func(ctx context.Context, task string) (string, error)

// NewTaskStateReader reads GET /v1/tasks/<id> with an explicit handoffkeep
// endpoint and credential, as NewJobStore does for job records.
func NewTaskStateReader(baseURL, token, cfID, cfSecret string) (TaskStateReader, error) {
	client, err := newNodeHKClient(baseURL, token, cfID, cfSecret)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, task string) (string, error) {
		id, err := strconv.ParseInt(task, 10, 64)
		if err != nil || id <= 0 {
			return "", errors.New("task id is not numeric")
		}
		var record struct {
			ID    int64  `json:"id"`
			State string `json:"state"`
		}
		if err := client.get(ctx, "/v1/tasks/"+url.PathEscape(task), &record); err != nil {
			return "", err
		}
		if record.ID != id || record.State == "" {
			return "", errors.New("task record is invalid")
		}
		return record.State, nil
	}, nil
}

// paneSweepEndState is the hk job state a sweep writes for each task state
// that makes a pane eligible. A task in any other state keeps its pane.
var paneSweepEndState = map[string]string{"merged": "completed", "done": "completed", "dropped": "lost"}

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
	return !j.Reaped && !j.Keep && (j.Role == "worker" || j.Role == "tester") && j.Task != "" && j.Pane != "" && j.Label != ""
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
	taskStates := map[string]string{}
	taskErrors := map[string]bool{}
	for _, j := range candidates {
		state, known := taskStates[j.Task]
		if !known && !taskErrors[j.Task] {
			if s.TaskState == nil {
				taskErrors[j.Task] = true
			} else if state, err = s.TaskState(ctx, j.Task); err != nil {
				taskErrors[j.Task] = true
			} else {
				taskStates[j.Task] = state
			}
		}
		if taskErrors[j.Task] {
			s.info("task pane kept", "job", j.ID, "task", j.Task, "pane", j.Pane, "reason", "task-read-failed")
			continue
		}
		endState, eligible := paneSweepEndState[state]
		if !eligible {
			continue
		}
		since, idle := s.idleSince[paneSweepKey(j)]
		switch {
		case !idle:
			s.info("task pane kept", "job", j.ID, "task", j.Task, "task_state", state, "pane", j.Pane, "reason", s.notIdleReason(j, agents))
		case now.Sub(since) < s.Grace:
			s.info("task pane kept", "job", j.ID, "task", j.Task, "task_state", state, "pane", j.Pane, "reason", "grace", "idle_seconds", int(now.Sub(since).Seconds()))
		default:
			if reason := s.close(ctx, a, j, state, endState, since, now); reason != "" {
				s.info("task pane kept", "job", j.ID, "task", j.Task, "task_state", state, "pane", j.Pane, "reason", reason)
			}
		}
	}
	return nil
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
func (s *TaskPaneSweeper) close(ctx context.Context, a *App, j Job, taskState, endState string, since, now time.Time) string {
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
	if !paneSweepCandidate(latest) || latest.Pane != j.Pane || latest.Label != j.Label {
		delete(s.idleSince, key)
		return "job-changed"
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
	a.markSweptJob(ctx, latest, endState)
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
	if err := a.event(ctx, j.ID, "job.reaped", map[string]any{"pane_id": j.Pane, "reason": "task_" + taskState}); err != nil {
		a.warn("job.reaped not recorded: %v", err)
	}
	s.info("task pane closed", "job", j.ID, "task", j.Task, "task_state", taskState, "pane", j.Pane, "idle_seconds", int(now.Sub(since).Seconds()))
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
// reported done: completed for a merged or done task, lost for a dropped one.
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
