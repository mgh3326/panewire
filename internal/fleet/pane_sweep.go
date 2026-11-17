package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
)

// The node's task pane sweep (docs/pane-sweep.md) closes the pane of a job
// that ended: a reviewer or consult on its terminal record, an implementer
// when its task ends or the job is handed over. A pane is the job's own when
// its pane id and the harness session id recorded in job.spawned both match
// what herdr lists at the moment of closing (operator decisions 265, 266,
// 268, hk 4693).

// PaneScreenFile is the job-directory file a swept pane's last screen is also
// kept in when hk can store it only as a cut tail; the explicit pane.capture
// node command writes it too.
const PaneScreenFile = "pane-screen.txt"

// DefaultPaneSweepGrace is how long a pane must have stayed not working.
const DefaultPaneSweepGrace = 10 * time.Minute

// paneSweepOwnerAge is the deadline from a job's recorded end to its pane's
// close: the node that created the pane closes it within the hour whatever
// the pane shows (decision 266). A --keep pane gets a day instead (decision
// 268, paneSweepKeepAge).
const paneSweepOwnerAge = time.Hour

// paneSweepKeepAge is how long a --keep pane stays after its job ended.
const paneSweepKeepAge = 24 * time.Hour

// paneSweepStaleAge is how old a job's end must be for its still-open, not
// working pane to count as stale: the count each node reports to the hub, whose
// alarm means the host is collecting finished panes (hk 4904). A day is well
// past every close deadline the sweep has (an hour, a kept pane's day), so a
// stale pane is always one the sweep failed to close.
const paneSweepStaleAge = 24 * time.Hour

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

// foldSpawnRole folds the names older receipts used into the spawn role: a
// worker or tester is an implementer, a counsel a consult. The default limit
// and the pane sweep both read the folded name.
func foldSpawnRole(role string) string {
	switch role {
	case "worker", "tester":
		return "implementer"
	case "counsel":
		return "consult"
	}
	return role
}

// paneSweepRole is the job's spawn role with the names older receipts used
// folded in (foldSpawnRole).
func paneSweepRole(j Job) string {
	return foldSpawnRole(j.QuotaRole())
}

// paneSweepTaskRole reports that the job's pane is judged by its hk task and
// its open PRs: an implementer. Any other role's pane is judged by the
// job's own end alone. The role is the spawn role when the job has one, since
// the claim records only implementer or builder (arbiter/jobs.go).
func paneSweepTaskRole(j Job) bool {
	return paneSweepRole(j) == "implementer"
}

// paneSweepClosed reports that the job's latest record ended it for the pane
// close (decision 266): a reviewer or consult ends on any terminal record —
// completed, revoked, handed off, or lost, including a quota stop the hub
// hands to a replacement pane — while an implementer ends on a terminal
// record other than a plain loss: its job.lost ends the pane only when it
// announces a quota hand-over (announcedQuotaStop), because the replacement
// works in a new pane. A job.joined is not an end: the hub or another lane
// owns it.
func paneSweepClosed(j Job) bool {
	if !j.Terminal || j.Ended == "job.joined" {
		return false
	}
	if paneSweepTaskRole(j) && j.Ended == "job.lost" {
		_, quota := announcedQuotaStop(j)
		return quota
	}
	return true
}

// paneSweepAwaitsPR reports that the job's own record completed it: the
// implementer's job.completed (READY), with no other outcome. Such an implementer stays
// its PR's author until the PR merges or its owner releases it (the wake
// method), so its pane waits on an open PR from its worktree. A job.revoked of
// any outcome (panewire job close: abandoned, superseded, or released as
// completed) is the owner giving the job up or releasing it, and is swept
// whatever its PRs say (hk 3383). A reviewer or a consult never
// authors the PR it looked at.
func paneSweepAwaitsPR(j Job) bool {
	if !paneSweepClosed(j) || j.Ended != "job.completed" || (j.Outcome != "" && j.Outcome != "completed") {
		return false
	}
	switch firstNonempty(j.SpawnRole, j.Role) {
	case "reviewer", "consult":
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
	// Info receives one line per closed pane and one per kept candidate. It
	// gets ids and states only, never screen text.
	Info func(message string, attrs ...any)
	// Exclusive, when set, is called before each close and the function it
	// returns after it, so a close never interleaves with another acting node
	// command (node_command.go nodeCommandMu).
	Exclusive func() (release func())
	// EndedOpen receives this pass's count of ended-job panes still open past
	// paneSweepStaleAge, which the node reports to the hub for its alarm (hk
	// 4904). It is called once per measured pass, zero included; a pass that
	// could not read herdr reports nothing.
	EndedOpen func(open int)

	// staleOpen is the count the pass is reporting, for its log line.
	staleOpen int

	idleSince map[string]time.Time
	// endedSince is the first pass each pane's end held: the close deadline of
	// an end the job record does not date (a task-derived end) starts there.
	endedSince map[string]time.Time
	// failures holds the failure reps the sweep still owes (their joined PR is
	// open or unread); settledFailures holds the jobs whose rep is final
	// (outcome_failed.go).
	failures        map[string]deferredFailure
	settledFailures map[string]bool
	// textfileWarned holds the textfile publication failures already warned
	// about, by reason, so a daemon logs each one once
	// (publishPaneMetricTextfile).
	textfileWarned map[string]bool
	// reportNoted holds the republish outcomes already logged, by job, report
	// and reason, so a report that cannot be published yet is one line rather
	// than one per pass (report_republish.go).
	reportNoted map[string]bool
	// reportPins holds each report's pin attempts, so a record hk never answers
	// cannot hold the pass's pin budget ahead of other jobs, and reportPass
	// counts this sweeper's republish passes, the unit that back-off measures in
	// (report_republish.go).
	reportPins map[string]reportPinState
	reportPass int
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
// from spawnRoles, and either an implementer (judged by its task) or a
// job that ended locally. A builder, captain or lead pane is its lane's, not
// the sweep's.
func paneSweepCandidate(j Job) bool {
	return !j.Reaped && !j.Keep && j.Pane != "" && j.Label != "" && slices.Contains(spawnRoles, paneSweepRole(j)) &&
		(paneSweepTaskRole(j) || paneSweepClosed(j))
}

// paneSweepKeepCandidate admits a --keep job: an implementer always, so its
// task can be read and the day counted from the task's end; any other role
// only once its own record ended it at least paneSweepKeepAge ago (decision
// 268). Either way the pane closes a day after its end, and an open PR or a
// live task does not hold it.
func paneSweepKeepCandidate(j Job, now time.Time) bool {
	if !j.Keep || j.Reaped || j.Pane == "" || j.Label == "" ||
		!slices.Contains(spawnRoles, paneSweepRole(j)) {
		return false
	}
	if paneSweepTaskRole(j) {
		return true
	}
	return paneSweepClosed(j) && !j.At.IsZero() && now.Sub(j.At) >= paneSweepKeepAge
}

// paneSweepEndAt is the time the job's end took effect for the close
// deadline: the terminal event's time when the job's own record ended it, else
// the first pass that saw the task-derived end (there is no earlier record of
// it). At alone does not prove a closed job — a reviving claim, spawn or
// reprompt clears it — so a live implementer's task end always dates from the
// sweep's first sighting.
func (s *TaskPaneSweeper) paneSweepEndAt(j Job, now time.Time) time.Time {
	if paneSweepClosed(j) && !j.At.IsZero() {
		return j.At
	}
	key := paneSweepKey(j)
	if s.endedSince == nil {
		s.endedSince = map[string]time.Time{}
	}
	if _, seen := s.endedSince[key]; !seen {
		s.endedSince[key] = now
	}
	return s.endedSince[key]
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
	_, err := s.pass(ctx, a, paneSweepOwnerAge)
	return err
}

// pass runs one sweep: the panes of ended jobs whose end lay at least
// ownerAge ago close whatever they show; an idle ended pane whose grace ran
// closes early. It returns the panes it closed.
func (s *TaskPaneSweeper) pass(ctx context.Context, a *App, ownerAge time.Duration) ([]SweptPane, error) {
	s.retryDeferredFailures(ctx, a)
	entries, err := os.ReadDir(a.Jobs)
	if errors.Is(err, os.ErrNotExist) {
		clear(s.idleSince)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	now := a.Now()
	var candidates, abandoned []Job
	for _, entry := range entries {
		if !entry.IsDir() || !validID(entry.Name()) {
			continue
		}
		j, err := a.job(entry.Name())
		if err != nil {
			continue
		}
		switch {
		case paneSweepAbandoned(j):
			abandoned = append(abandoned, j)
		case paneSweepCandidate(j) || paneSweepKeepCandidate(j, now):
			candidates = append(candidates, j)
		}
	}
	counts := map[string]int{}
	defer s.reportEndedPanes(a, counts)
	if len(candidates) == 0 && len(abandoned) == 0 {
		clear(s.idleSince)
		s.reportStalePanes(0)
		return nil, nil
	}
	agents, err := a.agents(ctx)
	if err != nil {
		// Nothing proved the panes stayed idle through this pass, so the
		// grace starts again once herdr answers.
		clear(s.idleSince)
		s.info("task pane sweep kept every pane", "reason", "herdr-unreadable")
		return nil, nil
	}
	// The node ends its own reviewer and consult jobs that never ran `done`
	// before their panes are judged (hk 4747). Such a job is not one of this
	// pass's candidates — it is still live until the end above.
	swept := s.endAbandonedReviews(ctx, a, abandoned, agents, now)
	if s.idleSince == nil {
		s.idleSince = map[string]time.Time{}
	}
	live := map[string]bool{}
	for _, j := range candidates {
		key := paneSweepKey(j)
		live[key] = true
		if j.Keep || !s.idle(j, agents) {
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
	for key := range s.endedSince {
		if !live[key] {
			delete(s.endedSince, key)
		}
	}
	statuses := map[string]paneSweepTaskRead{}
	prs := map[string]paneSweepPRRead{}
	parents := map[string]paneSweepInherited{}
	// excludedPanes holds the panes this pass kept for a reason that is not a
	// failure (another agent's row, a live task, an open PR, the grace) or
	// closed: the host is not "collecting" them, and the alarm must not latch
	// on a pane the sweep keeps on purpose for as long as the reason holds
	// (hk 4904).
	excludedPanes := map[string]bool{}
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
				excludedPanes[j.Pane] = true
				continue
			}
		default:
			read, known := statuses[task]
			if !known {
				read = s.readTask(ctx, task)
				statuses[task] = read
			}
			if read.err != nil && !closed {
				s.info("task pane kept", "job", j.ID, "task", task, "pane", j.Pane, "reason", "task_read_failed")
				if !j.Keep {
					counts["task_read_failed"]++
				}
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
			// A task that is live again has no end to date: forget the first
			// sighting of an earlier end, so a task that ends again starts a
			// fresh deadline instead of reusing the old one.
			delete(s.endedSince, paneSweepKey(j))
			if !closed {
				continue
			}
			end = paneSweepJobEnd(j, status.State)
		}
		key := paneSweepKey(j)
		since, idle := s.idleSince[key]
		// Past the deadline the pane closes whatever it shows; before it, an
		// idle pane may still close once its grace ran. A --keep pane has only
		// the day deadline (decision 268), never the idle early close.
		deadline := ownerAge
		if j.Keep {
			deadline = paneSweepKeepAge
		}
		due := now.Sub(s.paneSweepEndAt(j, now)) >= deadline
		if j.Keep && !due {
			continue
		}
		if !due {
			switch {
			case !idle:
				s.info("task pane kept", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", s.notIdleReason(j, agents))
				continue
			case now.Sub(since) < s.Grace:
				s.info("task pane kept", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", "grace", "idle_seconds", int(now.Sub(since).Seconds()))
				continue
			}
		}
		// A task-derived end, and a completed author (paneSweepAwaitsPR),
		// wait on an open PR — also past the deadline, since the pane is the
		// PR author's. A job its owner closed is swept whatever its PRs say
		// (hk 3383), and a --keep pane never waits.
		if !j.Keep && paneSweepTaskRole(j) && paneSweepWaitsOnPR(j) {
			if reason, number := s.paneSweepPRKeep(ctx, a, j, task, taskErr, prs); reason != "" {
				attrs := []any{"job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", reason}
				if number > 0 {
					attrs = append(attrs, "pr", number)
				}
				s.info("task pane kept", attrs...)
				if due && reason != "open_pr" {
					counts[reason]++
				}
				if !paneSweepCounted(reason) {
					excludedPanes[j.Pane] = true
				}
				continue
			}
		}
		attempt := s.closePane(ctx, a, j, task, end, agents, now)
		if attempt.reason != "" {
			s.info("task pane kept", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", attempt.reason)
			if due || (!j.Keep && paneSweepCounted(attempt.reason)) {
				counts[attempt.reason]++
			}
			// An unowned pane is the close's refusal, not a collection failure:
			// the alarm count leaves it out even when the reason is otherwise
			// diagnostic, and even when only the fresh pre-close listing saw the
			// reassignment (hk 4919).
			if !paneSweepCounted(attempt.reason) || attempt.unowned {
				excludedPanes[j.Pane] = true
			}
		} else {
			s.info("task pane closed", "job", j.ID, "task", task, "task_state", end.taskState, "pane", j.Pane, "reason", attempt.swept.Reason)
			swept = append(swept, attempt.swept)
		}
	}
	// The count is what the host still holds after this pass's closes: a pane
	// closed here is not a pane the host is collecting, and reporting it would
	// raise the hub's alarm over a pane that is already gone (hk 4904).
	for _, pane := range swept {
		excludedPanes[pane.Pane] = true
	}
	s.reportStalePanes(s.stalePanes(candidates, agents, now, ownerAge, excludedPanes))
	return swept, nil
}

// paneSweepCounted reports that a keep reason belongs in the ended-job pane
// count even before the job's close deadline: a reason that is not "still
// inside its bound" is never a final state, so it is counted whenever it
// keeps an admitted candidate.
func paneSweepCounted(reason string) bool {
	switch reason {
	case "open_pr", "grace", "no_task", "job_changed", SweepPaneGone:
		return false
	}
	return !strings.HasPrefix(reason, "pane-")
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

// paneOwnershipProof is why herdr's rows do not prove the pane is the job's own
// (decisions 265, 4904), "" when they do. "pane_unlisted" when no agent row
// has the job's pane id; "session_mismatch" when the receipt recorded a
// harness session id and the row carries another. A receipt that recorded no
// session at all (hk 4904: the spawner could not read the pane's session, or
// the receipt predates the field) proves the pane by the facts `panewire reap`
// uses — the row carries the job's name and tab and is not working — because
// comparing a placeholder with a live row matches nothing and kept every such
// ended pane open for good.
func paneOwnershipProof(j Job, agents []Agent) string {
	for _, ag := range agents {
		if ag.Pane != j.Pane {
			continue
		}
		want := paneRecordedSession(j)
		if want != "" {
			if agentSessionID(ag) != want {
				return "session_mismatch"
			}
			return ""
		}
		if !paneReapProof(j, ag) {
			return "session_unrecorded"
		}
		return ""
	}
	return "pane_unlisted"
}

// paneReapProof reports that herdr's row proves the pane by the facts
// `panewire reap` uses: the job's exact name, the job's tab, and any status but
// `working` (reap closes a `blocked` or `unknown` row too; hygiene.go). It is
// stricter than paneSweepMatches, which accepts a nameless row that sits in the
// job's worktree: with no recorded session the name is the only identity the
// close has, and an unnamed shell in that worktree is not the job's agent.
func paneReapProof(j Job, ag Agent) bool {
	return ag.Name == j.Label && (j.Tab == "" || ag.Tab == j.Tab) && ag.Status != "working"
}

// paneRecordedSession is the harness session id the job's newest spawn
// receipt recorded (payload.harness_session.id), "" when it names no session.
// A spawner that could not read the pane's session records the placeholder
// "unknown" with source "unavailable" (harness_session.go); it is not a
// session, so it proves nothing and the caller falls back to the reap proof.
func paneRecordedSession(j Job) string {
	m, ok := j.Receipt["harness_session"].(map[string]any)
	if !ok || str(m, "source") == "unavailable" {
		return ""
	}
	return str(m, "id")
}

// agentSessionID reads the session id out of a herdr agent row's
// agent_session field: an object carrying "value" in current herdr, a plain
// string in older snapshots.
func agentSessionID(ag Agent) string {
	if len(ag.Session) == 0 {
		return ""
	}
	var v any
	if json.Unmarshal(ag.Session, &v) != nil {
		return ""
	}
	switch s := v.(type) {
	case string:
		return s
	case map[string]any:
		return str(s, "value")
	}
	return ""
}

// paneSweepClose is one close decision's answer: the swept pane once the pane
// is gone (closed here, or already absent from herdr), the reason it was kept
// otherwise, and whether herdr's rows refused to prove the pane is the job's
// own (paneOwnershipProof, either check). The pass keeps a refused pane out of
// the stale count: the close will not take it, so it is not a pane this host
// failed to collect — though the per-reason textfile still names it (hk 4919).
type paneSweepClose struct {
	swept   SweptPane
	reason  string
	unowned bool
}

// paneSweepKept is one close decision that kept the pane for reason.
func paneSweepKept(reason string) paneSweepClose {
	return paneSweepClose{reason: reason}
}

// paneSweepUnproven is one close decision that kept the pane because
// paneOwnershipProof refused to prove it is the job's own: the keep reason is
// the refusal.
func paneSweepUnproven(reason string) paneSweepClose {
	return paneSweepClose{reason: reason, unowned: true}
}

// closePane closes an ended job's pane and reaps what it leaves. The proof is
// checked before the screen read and again on a fresh listing under the acting
// lock immediately before `herdr pane close`, so the screen and the close both
// belong to the pane the job spawned (decision 265). Either refusal is reported
// as an unowned pane: the pass then keeps it out of the stale count, whose
// authority is that same proof (hk 4919).
func (s *TaskPaneSweeper) closePane(ctx context.Context, a *App, j Job, task string, end paneSweepEnd, agents []Agent, now time.Time) paneSweepClose {
	swept := SweptPane{Job: j.ID, Pane: j.Pane, Role: paneSweepRole(j), Reason: end.reason}
	key := paneSweepKey(j)
	if reason := paneOwnershipProof(j, agents); reason != "" {
		if reason == "pane_unlisted" && s.Exclusive != nil {
			defer s.Exclusive()()
		}
		return s.paneUnlisted(ctx, a, j, task, end, swept, reason)
	}
	if err := a.savePaneScreen(ctx, j, task); err != nil {
		return paneSweepKept("screen_save_failed")
	}
	// Reading and uploading the last screen may wait on herdr or handoffkeep.
	// Keep both outside the node-command lock; the job and the pane are
	// re-read below after taking it, immediately before close.
	if s.Exclusive != nil {
		defer s.Exclusive()()
	}
	latest, err := a.job(j.ID)
	if err != nil {
		return paneSweepKept("job_unreadable")
	}
	// A revived or kept job always wins; the close re-judges the latest
	// record rather than the snapshot the pass took.
	if !s.closeStillEnded(ctx, j, latest, task, end) || (paneSweepWaitsOnPR(latest) && !paneSweepWaitsOnPR(j)) {
		delete(s.idleSince, key)
		delete(s.endedSince, key)
		return paneSweepKept("job_changed")
	}
	fresh, err := a.agents(ctx)
	if err != nil {
		return paneSweepKept("herdr_unreadable")
	}
	if reason := paneOwnershipProof(latest, fresh); reason != "" {
		return s.paneUnlisted(ctx, a, j, task, end, swept, reason)
	}
	// The end lock can wait while a watcher re-prompts or a restore claims
	// the job. Revalidate the entire decision and pane proof under that lock.
	current, endUnlock, refusal := a.lockUnchangedEnd(ctx, j)
	if refusal != "" {
		delete(s.idleSince, key)
		delete(s.endedSince, key)
		return paneSweepKept(refusal)
	}
	defer endUnlock()
	if !s.closeStillEnded(ctx, j, current, task, end) || (paneSweepWaitsOnPR(current) && !paneSweepWaitsOnPR(j)) {
		return paneSweepKept("job_changed")
	}
	// The session must be read before the close removes the live reference;
	// it can take time, so the final pane proof follows it.
	session, haveSession := a.readJobSession(ctx, current, true)
	fresh, err = a.agents(ctx)
	if err != nil {
		return paneSweepKept("herdr_unreadable")
	}
	if reason := paneOwnershipProof(current, fresh); reason != "" {
		return s.paneUnlistedHeld(ctx, a, current, swept, reason)
	}
	if err := a.must(ctx, "herdr", "pane", "close", j.Pane); err != nil {
		return paneSweepKept("close_failed")
	}
	s.reapSweptPaneHeld(ctx, a, current, current, task, end, session, haveSession, now)
	return paneSweepClose{swept: swept}
}

// closeStillEnded reports that the job's latest record still carries the end
// the pass judged: a reaped, revived or renamed job, a lost keep flag, or a
// task that reopened answers false and the close keeps the pane as
// job_changed. A task-derived end re-reads the task at close, because a task
// can come back to life between the pass's read and the close.
func (s *TaskPaneSweeper) closeStillEnded(ctx context.Context, j, latest Job, task string, end paneSweepEnd) bool {
	if latest.Reaped || latest.Pane != j.Pane || latest.Label != j.Label {
		return false
	}
	if j.Keep {
		if !latest.Keep {
			return false
		}
	} else if !paneSweepCandidate(latest) {
		return false
	}
	if paneSweepClosed(latest) {
		return true
	}
	if end.byJob || task == "" {
		return false
	}
	read := s.readTask(ctx, task)
	if read.err != nil {
		return false
	}
	_, eligible := paneSweepTaskEnd(read.status)
	return eligible
}

// paneUnlisted answers a proof failure: a pane no agent row lists is gone for
// real only when `herdr pane list` does not carry it either — then the job is
// reaped as if the close ran. Anything else is the keep reason itself, and an
// unowned pane: the stale count must not treat it as a pane this host failed to
// collect (hk 4919).
func (s *TaskPaneSweeper) paneUnlisted(ctx context.Context, a *App, j Job, task string, end paneSweepEnd, swept SweptPane, reason string) paneSweepClose {
	if reason != "pane_unlisted" {
		return paneSweepUnproven(reason)
	}
	current, unlock, refusal := a.lockUnchangedEnd(ctx, j)
	if refusal != "" {
		return paneSweepKept(refusal)
	}
	defer unlock()
	if !s.closeStillEnded(ctx, j, current, task, end) || (paneSweepWaitsOnPR(current) && !paneSweepWaitsOnPR(j)) {
		return paneSweepKept("job_changed")
	}
	agents, err := a.agents(ctx)
	if err != nil {
		return paneSweepKept("herdr_unreadable")
	}
	if proof := paneOwnershipProof(current, agents); proof != "pane_unlisted" {
		return paneSweepKept("job_changed")
	}
	return s.paneUnlistedHeld(ctx, a, current, swept, reason)
}

func (s *TaskPaneSweeper) paneUnlistedHeld(ctx context.Context, a *App, j Job, swept SweptPane, reason string) paneSweepClose {
	if reason != "pane_unlisted" {
		return paneSweepUnproven(reason)
	}
	panes, err := a.panes(ctx)
	if err != nil {
		return paneSweepUnproven(reason)
	}
	for _, p := range panes {
		if p.Pane == j.Pane {
			return paneSweepUnproven(reason)
		}
	}
	s.reapSweptPaneHeld(ctx, a, j, j, "", paneSweepJobEnd(j, ""), jobSession{}, false, a.Now())
	swept.Reason = SweepPaneGone
	return paneSweepClose{swept: swept}
}

// reapSweptPaneHeld is reapSweptPane run by a caller that already holds the
// job's end lock and checked that no hub close owns the end.
func (s *TaskPaneSweeper) reapSweptPaneHeld(ctx context.Context, a *App, latest, j Job, task string, end paneSweepEnd, session jobSession, haveSession bool, now time.Time) {
	key := paneSweepKey(j)
	delete(s.idleSince, key)
	delete(s.endedSince, key)
	a.markSweptJob(ctx, latest, end.state)
	if haveSession {
		a.writeJobSession(ctx, latest, session)
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
	s.sweepWorktree(ctx, a, latest, end)
	s.closedFailure(ctx, a, j, task)
}

// savePaneScreen keeps the pane's last screen before the close. A screen hk
// stores whole is there alone; a screen hk stores only as a cut tail, or
// refuses for its bytes, keeps its whole copy in <job dir>/pane-screen.txt,
// mode 0600, through an os.Root on the job directory, which the job dir's TTL
// removes with the dir. Only a failed pane read, a refused screen's failed
// local write, or an upload hk may still answer is an error: the pane is kept
// and counted screen_save_failed. An hk refusal is permanent for these bytes —
// hk's secret guard names secret_like_content — so it costs one warning and
// the close goes on; the local file is the screen (hk 4904).
func (a *App) savePaneScreen(ctx context.Context, j Job, task string) error {
	r := a.call(ctx, "herdr", "pane", "read", j.Pane)
	if err := check(r, "pane screen"); err != nil {
		return err
	}
	screen := []byte(r.Out)
	_, err := UploadPaneScreen(ctx, j.ID, task, screen)
	switch {
	case err == nil && len(screen) <= PaneScreenDocMaxBytes:
		return nil
	case err != nil && !paneScreenRefused(err):
		a.warn("pane screen upload failed for job %s; pane kept: %v", j.ID, err)
		return err
	case err != nil:
		a.warn("pane screen refused by hk for job %s; the local copy is the screen: %v", j.ID, err)
	}
	root, err := os.OpenRoot(filepath.Join(a.Jobs, j.ID))
	if err != nil {
		return err
	}
	defer root.Close()
	if err := durable.ReplaceFileAtomicallyInRoot(root, PaneScreenFile, screen, 0600); err != nil {
		return err
	}
	return nil
}

// markSweptJob writes the end state to the hk record of a job that never
// reported done: completed for a completed task, lost for a canceled one.
// A record that is already completed, escalated or lost, that hk does not
// hold, or whose job the hub reassigned (Job.Reassigned) is left alone; a failed read or write is a warning plus
// job.record_failed, and the sweep goes on.
func (a *App) markSweptJob(ctx context.Context, j Job, endState string) {
	if j.Reassigned {
		a.warn("job %s swept: the hub reassigned the job; its hk record is left to the new owner", j.ID)
		return
	}
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
	if err := a.putJobRecord(ctx, j.ID, map[string]any{"state": endState, "at": a.Now().UTC()}); err != nil {
		a.jobRecordFailed(ctx, j.ID, endState, err)
	}
}

// capturePaneScreen reads the pane's last screen from herdr.
func (a *App) capturePaneScreen(ctx context.Context, j Job) ([]byte, error) {
	r := a.call(ctx, "herdr", "pane", "read", j.Pane)
	if err := check(r, "pane screen"); err != nil {
		return nil, err
	}
	return []byte(r.Out), nil
}

// paneSweepMetricFile is the node_exporter textfile one pass leaves: the
// count of ended-job panes still open on this node, by keep reason. The host
// label comes from Alloy's instance label, not the file.
const paneSweepMetricFile = "panewire_panes.prom"

// paneSweepTextfileDirEnv names the directory Alloy's textfile collector
// scrapes (deploy/alloy/README.md). On Linux Alloy runs as its own user, which
// cannot read the node's 0600 state-dir copy, so a node daemon whose
// environment sets this publishes the same body there at mode 0644.
const paneSweepTextfileDirEnv = "FLEET_TEXTFILE_DIR"

// paneSweepStaleStatus reports that the row herdr lists for the job's pane
// shows not working, the alarm's own status rule (idle or done). It decides
// nothing else: the count's identity test is paneOwnershipProof, the close's
// predicate, so a row the close would take — a recorded session proves the
// pane, whatever its name or tab says — counts whenever its status is the
// alarm's (hk 4919). paneSweepMatches is not the count's test: the close does
// not consult it past the deadline, and using it here would hide a pane whose
// close failed after the row's tab or name drifted.
func paneSweepStaleStatus(j Job, agents []Agent) bool {
	for _, ag := range agents {
		if ag.Pane == j.Pane {
			return slices.Contains(paneSweepIdleStatus, ag.Status)
		}
	}
	return false
}

// stalePanes counts the panes this node still holds for jobs whose own record
// ended at least paneSweepStaleAge ago: the pass's own candidates — so a job
// the sweep never closes is never counted, and the count can reach zero — that
// this pass did not close or keep for a reason that is not a failure
// (excludedPanes: an open PR the author is still working, a live task, another
// agent's row, a pane the close saw reassigned), whose close deadline has
// passed, whose row herdr shows not working, and whose pane the close's
// ownership proof accepts. The job's own end is paneSweepClosed, the pass's end
// predicate: a terminal job that does not close it (a loss with no quota
// hand-over, a job.joined) is judged by its live task and may be kept
// legitimately, which must not latch the alarm. The deadline is the pass's own
// (ownerAge, or a kept pane's day): a hub may order a longer age than the
// alarm's day, and a pane the sweep is still correctly waiting on is not one it
// failed to collect (hk 4919). Ownership is decided by paneOwnershipProof, the
// one predicate closePane uses for the close, so a pane the close refuses is
// never counted; the status rule is the alarm's own (idle/done). It is the
// number the hub alarms on (hk 4904, hk 4919): a host collecting finished
// panes.
func (s *TaskPaneSweeper) stalePanes(candidates []Job, agents []Agent, now time.Time, ownerAge time.Duration, excludedPanes map[string]bool) int {
	count := 0
	for _, j := range candidates {
		if !paneSweepClosed(j) || j.At.IsZero() || now.Sub(j.At) < paneSweepStaleAge || excludedPanes[j.Pane] {
			continue
		}
		deadline := ownerAge
		if j.Keep {
			deadline = paneSweepKeepAge
		}
		if now.Sub(s.paneSweepEndAt(j, now)) < deadline {
			continue
		}
		if paneSweepStaleStatus(j, agents) && paneOwnershipProof(j, agents) == "" {
			count++
		}
	}
	return count
}

// reportStalePanes records the pass's stale-pane count for its log line and
// hands it to the node, which publishes it on the heartbeat.
func (s *TaskPaneSweeper) reportStalePanes(open int) {
	s.staleOpen = open
	if s.EndedOpen != nil {
		s.EndedOpen(open)
	}
}

// reportEndedPanes writes the pass's kept-pane counts to the node_exporter
// textfile under the node state dir and logs them. A pass that judged no
// candidate still writes, so a stale reason clears. When the node sets
// FLEET_TEXTFILE_DIR the same body is published there too; the two writes are
// independent, so one failing still leaves the other.
func (s *TaskPaneSweeper) reportEndedPanes(a *App, counts map[string]int) {
	var b strings.Builder
	b.WriteString("# HELP panewire_ended_job_panes_open Panes of ended jobs still open on this node, by keep reason.\n")
	b.WriteString("# TYPE panewire_ended_job_panes_open gauge\n")
	total := 0
	for _, reason := range slices.Sorted(maps.Keys(counts)) {
		fmt.Fprintf(&b, "panewire_ended_job_panes_open{reason=%q} %d\n", reason, counts[reason])
		total += counts[reason]
	}
	s.info("ended job panes open", "open", total, "stale", s.staleOpen, "reasons", fmt.Sprint(counts))
	body := []byte(b.String())
	dir := filepath.Join(a.stateDir(), "metrics")
	if err := os.MkdirAll(dir, 0700); err != nil {
		a.warn("pane metric not written: %v", err)
	} else if err := durable.ReplaceFileAtomically(filepath.Join(dir, paneSweepMetricFile), ".panewire_panes-*.tmp", body, 0600); err != nil {
		a.warn("pane metric not written: %v", err)
	}
	s.publishPaneMetricTextfile(a, body)
}

// publishPaneMetricTextfile publishes the pass's body into FLEET_TEXTFILE_DIR,
// where Alloy's textfile collector reads it as its own user. The state-dir copy
// stays the node's own record. The directory must already exist: a typo warns
// instead of being created. A failed publication warns once per reason and
// never fails the pass.
func (s *TaskPaneSweeper) publishPaneMetricTextfile(a *App, body []byte) {
	dir := strings.TrimSpace(os.Getenv(paneSweepTextfileDirEnv))
	if dir == "" {
		return
	}
	if err := durable.ReplaceFileAtomically(filepath.Join(dir, paneSweepMetricFile), ".panewire_panes-*.tmp", body, 0644); err != nil {
		s.warnTextfileOnce(a, err)
	}
}

// warnTextfileOnce logs a textfile publication failure unless that reason was
// already logged by this sweeper, which lives for the daemon's run. The key is
// the cause, not the message: a missing directory's error names a fresh
// temporary file every pass.
func (s *TaskPaneSweeper) warnTextfileOnce(a *App, err error) {
	reason := "write_failed"
	switch {
	case errors.Is(err, fs.ErrNotExist):
		reason = "dir_missing"
	case errors.Is(err, fs.ErrPermission):
		reason = "dir_unwritable"
	}
	if s.textfileWarned == nil {
		s.textfileWarned = map[string]bool{}
	}
	if s.textfileWarned[reason] {
		return
	}
	s.textfileWarned[reason] = true
	a.warn("pane metric not written to %s (%s): %v", paneSweepTextfileDirEnv, reason, err)
}
