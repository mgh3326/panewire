package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// The node's task pane sweep (docs/pane-sweep.md) observes ended jobs and idle
// panes. Herdr does not provide an ownership-bound read or close, so a sweep
// may capture only an unattributed local diagnostic and must keep the pane.

// PaneScreenFile is the evidence filename used by the explicit pane.capture
// node command. Sweeps never write it or upload its contents.
const PaneScreenFile = "pane-screen.txt"

// DefaultPaneSweepGrace is how long a pane must have stayed not working.
const DefaultPaneSweepGrace = 10 * time.Minute

// TaskStatus is what the sweep reads of one hk task: its state, category and
// title. The title carries the task's "(grade X, Tn)" marker, which the
// automatic outcome records.
type TaskStatus struct{ State, Category, Title, JobID string }

// TaskStateReader answers the status of one hk task.
type TaskStateReader func(ctx context.Context, task string) (TaskStatus, error)

// NewTaskStateReader reads GET /v1/tasks/<id> with an explicit handoffkeep
// endpoint and credential, as NewJobStore does for job records.
func NewTaskStateReader(baseURL, token, cfID, cfSecret string) (TaskStateReader, error) {
	client, err := newNodeHKClient(baseURL, token, cfID, cfSecret)
	if err != nil {
		return nil, err
	}
	return taskStateReader(client), nil
}

// TaskStatus reads one hk task through the App's handoffkeep client, the one
// ExistingJob reads job records with. A nil TaskState is built on first use.
func (a *App) TaskStatus(ctx context.Context, task string) (TaskStatus, error) {
	if a.TaskState == nil {
		client, err := a.quotaClient()
		if err != nil {
			return TaskStatus{}, err
		}
		a.TaskState = taskStateReader(client)
	}
	return a.TaskState(ctx, task)
}

func taskStateReader(client *hkQuota) TaskStateReader {
	return func(ctx context.Context, task string) (TaskStatus, error) {
		id, err := strconv.ParseInt(task, 10, 64)
		if err != nil || id <= 0 {
			return TaskStatus{}, errors.New("task id is not numeric")
		}
		var record struct {
			ID       int64           `json:"id"`
			State    string          `json:"state"`
			Category string          `json:"category"`
			Title    string          `json:"title"`
			Refs     json.RawMessage `json:"refs"`
		}
		if err := client.get(ctx, "/v1/tasks/"+url.PathEscape(task), &record); err != nil {
			return TaskStatus{}, err
		}
		if record.ID != id || record.State == "" {
			return TaskStatus{}, errors.New("task record is invalid")
		}
		var refs struct {
			JobID string `json:"job_id"`
		}
		_ = json.Unmarshal(record.Refs, &refs)
		return TaskStatus{State: record.State, Category: record.Category, Title: record.Title, JobID: refs.JobID}, nil
	}
}

// PRChange is one PR handoffkeep links to a task: its number, its state and
// the commit it heads.
type PRChange struct {
	Number   int
	State    string
	Head     string
	Repo     string
	OpenedAt string
	MergedAt string
}

// PRReader lists the PRs handoffkeep links to one task. It is what keeps a
// task-derived close from closing a pane whose worktree is on an open PR.
type PRReader func(ctx context.Context, task string) ([]PRChange, error)

// NewPRReader reads GET /v1/changes?task=<id> with an explicit handoffkeep
// endpoint and credential, as NewTaskStateReader does for the task itself.
// That is the source pr status already reads (docs/pr-status.md): a change
// carries the PR's state and head, so no GitHub request is spent.
func NewPRReader(baseURL, token, cfID, cfSecret string) (PRReader, error) {
	client, err := newNodeHKClient(baseURL, token, cfID, cfSecret)
	if err != nil {
		return nil, err
	}
	return prReader(client), nil
}

// PRs lists the PRs hk links to a task through the App's handoffkeep client,
// as TaskStatus reads the task. A nil PR is built on first use; a client that
// cannot be built is an error for this read and is tried again on the next.
func (a *App) PRs(ctx context.Context, task string) ([]PRChange, error) {
	if a.PR == nil {
		client, err := a.quotaClient()
		if err != nil {
			return nil, err
		}
		a.PR = prReader(client)
	}
	return a.PR(ctx, task)
}

func prReader(client *hkQuota) PRReader {
	return func(ctx context.Context, task string) ([]PRChange, error) {
		id, err := strconv.ParseInt(task, 10, 64)
		if err != nil || id <= 0 {
			return nil, errors.New("task id is not numeric")
		}
		var list struct {
			Changes []struct {
				PRNumber int    `json:"pr_number"`
				State    string `json:"state"`
				HeadSHA  string `json:"head_sha"`
				Repo     string `json:"repo"`
				OpenedAt string `json:"opened_at"`
				MergedAt string `json:"merged_at"`
			} `json:"changes"`
		}
		if err := client.get(ctx, "/v1/changes?task="+url.PathEscape(task), &list); err != nil {
			return nil, err
		}
		changes := make([]PRChange, 0, len(list.Changes))
		for _, change := range list.Changes {
			if change.PRNumber <= 0 {
				continue
			}
			changes = append(changes, PRChange{Number: change.PRNumber, State: change.State, Head: change.HeadSHA,
				Repo: change.Repo, OpenedAt: change.OpenedAt, MergedAt: change.MergedAt})
		}
		return changes, nil
	}
}

// The hk task categories that end a job. A state such as deployed or merged
// belongs to one of them, so the sweep follows the category and does not need
// to know every state name.
const (
	taskCategoryCompleted = "completed"
	taskCategoryCanceled  = "canceled"
)

// taskEndedStates are the hk task states that end a task when the record
// carries no category.
var taskEndedStates = []string{"merged", "deployed", "done", "dropped", "canceled"}

// Ended reports whether nobody works the task any more: hk's completed or
// canceled category (merged, deployed, dropped, canceled), or, for a record
// without a category, one of taskEndedStates.
func (t TaskStatus) Ended() bool {
	switch t.Category {
	case taskCategoryCompleted, taskCategoryCanceled:
		return true
	case "":
		return slices.Contains(taskEndedStates, t.State)
	}
	return false
}

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

// paneSweepTaskRole reports that the job's pane is judged by its hk task and
// its open PRs: a worker or a tester. Any other role's pane is judged by the
// job's own end alone. The role is the spawn role when the job has one, since
// the claim records only worker or builder (arbiter/jobs.go).
func paneSweepTaskRole(j Job) bool {
	switch j.QuotaRole() {
	case "worker", "tester":
		return true
	}
	return false
}

// paneSweepClosed reports that the job's latest record ended it: a job.revoked
// with any outcome (panewire job close), a job.completed, or a job.lost. A
// job.lost is not one for a worker or a tester, nor a quota stop: the reviewer
// watcher hands a quota-stopped job to a replacement and keeps the pane until
// that hand-off ends (watch_quota_stop.go). A job.joined or job.handed_off is
// not one: the hub or another lane owns it.
func paneSweepClosed(j Job) bool {
	if !j.Terminal {
		return false
	}
	switch j.Ended {
	case "job.revoked", "job.completed":
		return true
	case "job.lost":
		_, quota := announcedQuotaStop(j)
		return !paneSweepTaskRole(j) && !quota
	}
	return false
}

// paneSweepAwaitsPR reports that the job's own record completed it: the
// worker's job.completed (READY), with no other outcome. Such a worker stays
// its PR's author until the PR merges or its owner releases it (the wake
// method), so its pane waits on an open PR from its worktree. A job.revoked of
// any outcome (panewire job close: abandoned, superseded, or released as
// completed) is the owner giving the job up or releasing it, and is swept
// whatever its PRs say (hk 3383). A reviewer, a tester or a counsel never
// authors the PR it looked at.
func paneSweepAwaitsPR(j Job) bool {
	if !paneSweepClosed(j) || j.Ended != "job.completed" || (j.Outcome != "" && j.Outcome != "completed") {
		return false
	}
	switch firstNonempty(j.SpawnRole, j.Role) {
	case "reviewer", "tester", "counsel":
		return false
	}
	return !strings.HasPrefix(j.Label, "mwr-")
}

// paneSweepWaitsOnPR reports that an open PR from the job's worktree keeps its
// pane: a task-derived end, or a completed author.
func paneSweepWaitsOnPR(j Job) bool { return !paneSweepClosed(j) || paneSweepAwaitsPR(j) }

// paneSweepJobEnd is the end a closed job gives when its task has not ended.
func paneSweepJobEnd(j Job, taskState string) paneSweepEnd {
	end := paneSweepEnd{state: "lost", taskState: taskState, byJob: true}
	switch {
	case j.Outcome != "":
		end.reason = "job_" + j.Outcome
	case j.Ended == "job.completed":
		end.reason = "job_completed"
	case j.Ended == "job.lost":
		end.reason = "job_lost"
	case j.Ended == "job.handed_off":
		end.reason = "job_handed_off"
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
	// PR lists the PRs hk links to a task, so a task-derived end waits while
	// the job's worktree is on an open PR (hk 3401). A nil reader leaves that
	// state unknown, and an unknown state keeps a worktree's pane.
	PR PRReader
	// WorkRoot is the node's own work root: the clone it prepares job
	// worktrees from. Empty keeps every worktree, since nothing then proves
	// one was the node's (sweep_worktree.go).
	WorkRoot string
	// Info receives one line per kept candidate. It gets ids and states only,
	// never screen text.
	Info func(message string, attrs ...any)

	idleSince map[string]time.Time
	// failures holds the failure reps the sweep still owes (their joined PR is
	// open or unread); settledFailures holds the jobs whose rep is final
	// (outcome_failed.go).
	failures        map[string]deferredFailure
	settledFailures map[string]bool
}

func (s *TaskPaneSweeper) info(message string, attrs ...any) {
	if s.Info != nil {
		s.Info(message, attrs...)
	}
}

// paneSweepKey names one pane of one job, so a job that is respawned into a new
// pane starts its idle time again.
func paneSweepKey(j Job) string { return j.ID + "\x00" + j.Pane }

// paneSweepCandidate admits a job whose pane the sweep may close: a spawn role
// from spawnRoles, and either a worker or tester (judged by its task) or a
// job that ended locally. A builder, captain or lead pane is its lane's, not
// the sweep's.
func paneSweepCandidate(j Job) bool {
	return !j.Reaped && !j.Keep && j.Pane != "" && j.Label != "" && slices.Contains(spawnRoles, j.QuotaRole()) &&
		(paneSweepTaskRole(j) || paneSweepClosed(j))
}

// paneSweepMatches reports that herdr's row is the job's pane: the job's pane
// id and tab, and its name. A row whose name was lost matches by the job's
// worktree instead, so a nameless row in another worktree is not the job's.
func paneSweepMatches(j Job, ag Agent) bool {
	if ag.Pane != j.Pane || (j.Tab != "" && ag.Tab != j.Tab) {
		return false
	}
	if ag.Name == j.Label {
		return true
	}
	return ag.Name == "" && sweepSamePath(ag.CWD, str(j.Receipt, "cwd"))
}

// paneRowReason is why herdr's rows do not prove j's pane: "pane-gone" when no
// row has the job's pane id, "pane-mismatch" when the row with that id is not
// the job's (paneSweepMatches), and "" when it is.
func paneRowReason(j Job, agents []Agent) string {
	for _, ag := range agents {
		if ag.Pane == j.Pane {
			if !paneSweepMatches(j, ag) {
				return "pane-mismatch"
			}
			return ""
		}
	}
	return "pane-gone"
}

// Pass runs one sweep over the jobs under a.Jobs. A pass that cannot read
// herdr changes nothing; one that cannot read a task keeps that job's pane.
func (s *TaskPaneSweeper) Pass(ctx context.Context, a *App) error {
	s.retryDeferredFailures(ctx, a)
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
	prs := map[string]paneSweepPRRead{}
	parents := map[string]paneSweepInherited{}
	for _, j := range candidates {
		closed := paneSweepClosed(j)
		var task string
		var taskErr error
		if paneSweepTaskRole(j) {
			task = j.Task
			if task == "" {
				// A fix author spawned with --parent-job is judged on its parent's
				// task. A task that could not be established (taskErr) keeps a
				// completed author's pane below.
				task, taskErr = s.inheritedTask(ctx, a, j, parents)
			}
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
		// The hk change record is the merged signal panewire already polls:
		// record the job's one outcome from it before deciding the pane. Only
		// a job that joined a PR can own the change, and only such a job pays
		// for the read.
		s.recordMergedOutcomes(ctx, a, j, task, status, prs)
		// A live job that already ended as escalated or lost keeps its failure
		// rep owed until it is final (outcome_failed.go).
		s.observeFailure(ctx, a, j, task)
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
			// A task-derived end, and a completed author (paneSweepAwaitsPR),
			// wait on an open PR. A job its owner closed is swept whatever its
			// PRs say (hk 3383).
			if paneSweepTaskRole(j) && paneSweepWaitsOnPR(j) {
				if reason, number := s.paneSweepPRKeep(ctx, a, j, task, taskErr, prs); reason != "" {
					attrs := []any{"job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", reason}
					if number > 0 {
						attrs = append(attrs, "pr", number)
					}
					s.info("task pane kept", attrs...)
					continue
				}
			}
			if reason := s.captureDiagnostic(ctx, a, j); reason != "" {
				s.info("task pane kept", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", reason)
			}
		}
	}
	return nil
}

// recordMergedOutcomes records the job's one outcome from its task's merged
// changes before its pane is judged (docs/pane-sweep.md). Only a job that
// joined a PR can own the change, and only such a job pays for the read.
func (s *TaskPaneSweeper) recordMergedOutcomes(ctx context.Context, a *App, j Job, task string, status TaskStatus, prs map[string]paneSweepPRRead) {
	if task == "" || status.State == "" || s.PR == nil || !jobHasJoinedPR(j) {
		return
	}
	read, known := prs[task]
	if !known {
		read = s.readPRs(ctx, task)
		prs[task] = read
	}
	if read.err != nil {
		return
	}
	for _, change := range read.changes {
		if !strings.EqualFold(change.State, "merged") {
			continue
		}
		if err := a.recordMergedOutcome(ctx, j, task, status, change); err != nil {
			s.info("merged outcome not recorded", "job", j.ID, "pr", change.Number, "reason", err.Error())
		}
	}
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

// paneSweepPRRead is one task's change list, kept for the pass so a task
// shared by several jobs is read once.
type paneSweepPRRead struct {
	changes []PRChange
	err     error
}

func (s *TaskPaneSweeper) readPRs(ctx context.Context, task string) paneSweepPRRead {
	if s.PR == nil {
		return paneSweepPRRead{err: errors.New("no PR reader")}
	}
	changes, err := s.PR(ctx, task)
	return paneSweepPRRead{changes: changes, err: err}
}

// paneSweepPRKeep reports why the job's worktree keeps its close (a
// task-derived end or a completed author), and the open PR's number when one
// is found. "" means no open PR: the close goes ahead. task is the job's task,
// "" when it has none; taskErr says the task could not be established
// (inheritedTask: hk could not be read, or a named parent did not resolve to a
// task). A task that could not be established, a worktree that
// cannot be read, or an unreadable change list keeps the pane, so a failure
// never closes a pane that may still own an open PR. The worktree is read
// before the change list: an empty or merged-only list says nothing about a
// worktree whose head is unknown.
func (s *TaskPaneSweeper) paneSweepPRKeep(ctx context.Context, a *App, j Job, task string, taskErr error, cache map[string]paneSweepPRRead) (reason string, number int) {
	cwd := str(j.Receipt, "cwd")
	if cwd == "" {
		// No worktree: nothing it could have an open PR on.
		return "", 0
	}
	if taskErr != nil {
		return "pr_state_unknown", 0
	}
	if task == "" {
		// No task, and no parent named: hk links PRs to a task, so there is no
		// change list to wait on.
		return "", 0
	}
	heads, ok := paneSweepHeads(ctx, a, cwd)
	if !ok {
		return "pr_state_unknown", 0
	}
	read, known := cache[task]
	if !known {
		read = s.readPRs(ctx, task)
		cache[task] = read
	}
	if read.err != nil {
		return "pr_state_unknown", 0
	}
	if number, open := paneSweepOpenPR(read.changes, heads); open {
		return "open_pr", number
	}
	return "", 0
}

// paneSweepHeads is the commit(s) that identify the job's worktree: HEAD, and
// for an attached worktree the tip of its branch on origin, which is the PR
// head hk records (a change carries no head branch). The tip is read from
// origin itself (ls-remote), not from refs/remotes/origin/<branch>: that ref is
// only as fresh as this clone's last fetch, and a PR head pushed from another
// checkout would be missed. A worktree with no readable HEAD, or an origin
// that cannot be read, leaves the answer unknown; a branch that is not on
// origin is identified by HEAD alone.
func paneSweepHeads(ctx context.Context, a *App, cwd string) ([]string, bool) {
	r := a.call(ctx, "git", "-C", cwd, "rev-parse", "HEAD")
	head := strings.TrimSpace(r.Out)
	if r.Code != 0 || head == "" {
		return nil, false
	}
	heads := []string{head}
	if b := a.call(ctx, "git", "-C", cwd, "branch", "--show-current"); b.Code == 0 {
		if branch := strings.TrimSpace(b.Out); branch != "" {
			t := a.call(ctx, "git", "-C", cwd, "ls-remote", "--exit-code", "origin", "refs/heads/"+branch)
			switch t.Code {
			case 0:
				tip, _, _ := strings.Cut(strings.TrimSpace(t.Out), "\t")
				if tip = strings.TrimSpace(tip); tip != "" && tip != head {
					heads = append(heads, tip)
				}
			case 2:
				// --exit-code: origin has no such branch.
			default:
				return nil, false
			}
		}
	}
	return heads, true
}

// paneSweepOpenPR is the number of an open change whose head is one of the
// worktree's commits.
func paneSweepOpenPR(changes []PRChange, heads []string) (int, bool) {
	for _, change := range changes {
		if !strings.EqualFold(change.State, "open") {
			continue
		}
		changeHead := strings.TrimSpace(change.Head)
		for _, head := range heads {
			if head != "" && strings.EqualFold(changeHead, head) {
				return change.Number, true
			}
		}
	}
	return 0, false
}

// paneSweepInherited is one parent job's task, kept for the pass so a parent
// shared by several fix authors is read once.
type paneSweepInherited struct {
	task string
	err  error
}

// errParentTaskUnresolved says the job names a parent whose task no answer
// names yet: neither the parent's record on this node nor its hk record names
// one, or hk holds no record of the parent. An hk record can lag its job
// (resolveParent waits on such a lag), so this is not a confirmed absence: the
// next pass reads the parent again.
var errParentTaskUnresolved = errors.New("parent job's task not resolved")

// inheritedTask resolves the task of a job whose claim names none: the task of
// the job it was spawned under (--parent-job), through that job's record on
// this node, else its hk job record's task_id. The parent id is the claim's
// task_inherited_from, else the job's own hk record's parent_job.
//
// "" with a nil error is a confirmed absence, and only one answer gives it: no
// parent is named anywhere (the claim names none, and hk's record of the job
// names none or hk holds no record of it). A named parent must resolve to a
// task; one that does not (hk holds no record of it, its records name no task,
// or the name is not a job id) is errParentTaskUnresolved. Any other error
// means hk, which the answer needed, could not be built or read. A live job
// keeps its pane either way (no_task); a completed author is kept only on an
// error (paneSweepPRKeep).
func (s *TaskPaneSweeper) inheritedTask(ctx context.Context, a *App, j Job, cache map[string]paneSweepInherited) (string, error) {
	store, storeErr := a.jobStore()
	parent := j.Inherited
	if parent == "" {
		if storeErr != nil {
			return "", storeErr
		}
		rec, err := store.Get(ctx, j.ID)
		switch {
		case err == nil:
			parent = str(rec, "parent_job")
		case !errors.Is(err, errJobNotFound):
			return "", err
		}
	}
	if parent == "" {
		return "", nil
	}
	if parent == j.ID || !validID(parent) {
		return "", errParentTaskUnresolved
	}
	if read, ok := cache[parent]; ok {
		return read.task, read.err
	}
	var read paneSweepInherited
	if local, err := a.job(parent); err == nil {
		read.task = local.Task
	}
	if read.task == "" {
		if storeErr != nil {
			read.err = storeErr
		} else if rec, err := store.Get(ctx, parent); err == nil {
			read.task = scalar(rec["task_id"])
		} else if !errors.Is(err, errJobNotFound) {
			read.err = err
		}
		if read.task == "" && read.err == nil {
			read.err = errParentTaskUnresolved
		}
	}
	cache[parent] = read
	return read.task, read.err
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

// captureDiagnostic reads a pane as an unattributed local observation. It
// never writes job state or closes the pane because herdr cannot bind either
// operation to an authoritative pane owner and incarnation.
func (s *TaskPaneSweeper) captureDiagnostic(ctx context.Context, a *App, j Job) string {
	screen, err := a.capturePaneScreen(ctx, j)
	if err != nil {
		return "diagnostic-unavailable"
	}
	if err := a.spoolPaneDiagnostic(j.Pane, screen); err != nil {
		a.warn("pane diagnostic could not be spooled: %v", err)
		return "diagnostic-not-saved"
	}
	// Pane listings are mutable metadata. They cannot bind the bytes read above
	// to this job, and herdr exposes no ownership-conditional close operation.
	return "ownership-unproven"
}

// capturePaneScreen reads the pane's last screen from herdr. The result must
// remain unattributed because herdr does not bind it to an owner generation.
func (a *App) capturePaneScreen(ctx context.Context, j Job) ([]byte, error) {
	r := a.call(ctx, "herdr", "pane", "read", j.Pane)
	if err := check(r, "pane screen"); err != nil {
		return nil, err
	}
	return []byte(r.Out), nil
}
