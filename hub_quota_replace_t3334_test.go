package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/mgh3326/panewire/internal/fleet"
)

// quotaJobs is an hk fake that keeps each record whole and refuses to rewrite a
// terminal one, as hk does.
type quotaJobs struct {
	mu      sync.Mutex
	records map[string]map[string]any
}

func (q *quotaJobs) Claim(context.Context, string, string, string) error { return nil }
func (q *quotaJobs) Put(_ context.Context, id string, fields map[string]any) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if state := q.records[id]["state"]; state == "completed" || state == "lost" || state == "escalated" {
		return errors.New("job_state_conflict")
	}
	for key, value := range fields {
		q.records[id][key] = value
	}
	return nil
}
func (q *quotaJobs) Get(_ context.Context, id string) (map[string]any, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	record, ok := q.records[id]
	if !ok {
		return nil, fleet.ErrJobNotFound
	}
	out := map[string]any{}
	for key, value := range record {
		out[key] = value
	}
	return out, nil
}

type quotaReplaceFixture struct {
	hub         *HubServer
	node        *websocket.Conn
	jobs        *quotaJobs
	destination *hubAgent
	holder      hubSpawnTask
	job         string
	paneStatus  string
	role        string
}

// quotaHolderRequest is the submission of a stopped job of the given role: task
// 42, review pass 1 for a reviewer, owner "parent", an auto profile.
func quotaHolderRequest(role, job string) fleet.Submission {
	request := durableSpawnRequest()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--task", "42", "--owner", "parent", "--role", role, "--host", "auto", "--report-relative", job + "/report.md"}
	if role == "reviewer" {
		request.Args = append(request.Args, "--review-pass", "1")
	}
	request.Brief = "Review the change"
	request.Repository.Worktree = job + "-checkout"
	request.Repository.Checkout.Dir = job + "-checkout"
	return request
}

func newQuotaReplaceFixture(t *testing.T, role, job string) *quotaReplaceFixture {
	t.Helper()
	return newQuotaReplaceFixtureWith(t, role, job, nil)
}

// newQuotaReplaceFixtureWith lets a test change the stopped job's stored submission.
func newQuotaReplaceFixtureWith(t *testing.T, role, job string, mutate func(*fleet.Submission)) *quotaReplaceFixture {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	fake, client, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"},
		InboxStorePath:  filepath.Join(t.TempDir(), "inbox.sqlite3"),
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		handoffkeep:     client,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = fake
	t.Cleanup(func() { hub.Close() })
	hub.quotaReplacePoll = 5 * time.Millisecond
	hub.quotaMarkBackoff = time.Millisecond
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.mu.Lock()
	hub.nodes["machine-b"] = &hubNodeRecord{agent: destination}
	hub.mu.Unlock()
	jobs := &quotaJobs{records: map[string]map[string]any{job: {"state": "spawned", "pool": "claude"}}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, hub, server, true)
	holderRequest := quotaHolderRequest(role, job)
	if mutate != nil {
		mutate(&holderRequest)
	}
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), holderRequest, job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-a", Pane: roleHeldPane}, time.Now()); err != nil {
		t.Fatal(err)
	}
	held, err = hub.inbox.store.spawnTask(t.Context(), held.Request.RequestID)
	if err != nil || held.Status != "succeeded" {
		t.Fatalf("holder %+v %v", held, err)
	}
	return &quotaReplaceFixture{hub: hub, node: node, jobs: jobs, destination: destination, holder: held, job: job, paneStatus: "idle", role: role}
}

func quotaStopEvent(job string) hubJobEventPayload {
	return hubJobEventPayload{JobID: job, Epoch: 1, Reason: "quota_stop", OwnerLane: "parent"}
}

type quotaReplaceResult struct {
	job string
	ok  bool
}

// run starts the replacement, plays the hub's side of the placement (the
// successor's operation finishing with outcome) and the holder node's side of
// the close, and returns what the replacement reported.
func (f *quotaReplaceFixture) run(t *testing.T, placed hubSpawnOutcome, nodeCommands int) (quotaReplaceResult, []map[string]json.RawMessage) {
	t.Helper()
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job))
		done <- quotaReplaceResult{job, ok}
	}()
	var commands []map[string]json.RawMessage
	serve := func(command map[string]json.RawMessage) {
		commands = append(commands, command)
		switch string(command["op"]) {
		case `"agents.list"`:
			roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: f.paneStatus, Cwd: "/work/project.checkout"}}, true)
		case `"pane.capture"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/" + f.job + "/pane-screen.txt"}})
		case `"pane.close"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodePaneClosed}})
		case `"job.mark_lost"`:
			nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}})
		default:
			t.Fatalf("unexpected node command %s", command["op"])
		}
	}
	// The hub probes the old pane before it places anything.
	serve(nodeCommandRead(t, f.node))
	if f.paneStatus != "idle" {
		select {
		case result := <-done:
			return result, commands
		case <-time.After(3 * time.Second):
			t.Fatal("the replacement did not give up on a busy pane")
		}
	}
	// The hub closes the old pane (capture, close) before it places anything.
	serve(nodeCommandRead(t, f.node))
	serve(nodeCommandRead(t, f.node))
	served := 3
	if f.role != "reviewer" && nodeCommands > 3 {
		// An implementer's record ends (handed_over_to) before the
		// successor is admitted, because the spawn refuses a task still bound
		// to a job that is not lost.
		serve(nodeCommandRead(t, f.node))
		served++
	}
	next, _, _ := quotaReplacementJob(f.job)
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
		return err == nil
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, placed, time.Now()); err != nil {
		t.Fatal(err)
	}
	for i := served; i < nodeCommands; i++ {
		serve(nodeCommandRead(t, f.node))
	}
	select {
	case result := <-done:
		return result, commands
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
	return quotaReplaceResult{}, nil
}

func (f *quotaReplaceFixture) successor(t *testing.T, job string) fleet.Submission {
	t.Helper()
	task, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), job)
	if err != nil {
		t.Fatalf("no spawn for %s: %v", job, err)
	}
	return task.Request
}

func endReasonOf(t *testing.T, command map[string]json.RawMessage) string {
	t.Helper()
	var args nodeJobMarkLostArgs
	if err := json.Unmarshal(command["args"], &args); err != nil {
		t.Fatal(err)
	}
	return args.EndReason
}

const quotaPlaced = "machine-b"

// (a) hk 3334: a reviewer on quota_stop gets a replacement at once, with the
// stopped pool excluded; the old hk record ends with replaced_by as the last
// token; a third stop in the chain gets no replacement.
func TestQuotaStopReviewerIsReplacedAtOnceOnAnotherPool(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-1-p1")
	placed := hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}
	result, commands := f.run(t, placed, 4)
	if !result.ok || result.job != "mwr-1-p1-q1" {
		t.Fatalf("replacement=%+v, want mwr-1-p1-q1", result)
	}
	request := f.successor(t, "mwr-1-p1-q1")
	args := request.Args
	if i := slices.Index(args, "--exclude-pool"); i < 0 || args[i+1] != "claude" {
		t.Fatalf("the replacement must exclude the stopped pool: %v", args)
	}
	for flag, want := range map[string]string{"--job": "mwr-1-p1-q1", "--task": "42", "--role": "reviewer", "--review-pass": "1", "--owner": "parent", "--report-relative": "mwr-1-p1-q1/report.md"} {
		if got := hubSpawnArg(args, flag); got != want {
			t.Fatalf("%s=%q, want %q (%v)", flag, got, want, args)
		}
	}
	if request.Brief != "Review the change" {
		t.Fatalf("the reviewer keeps the same brief, got %q", request.Brief)
	}
	if request.Repository.Worktree != "mwr-1-p1-q1-checkout" || request.Repository.Checkout.Dir != "mwr-1-p1-q1-checkout" {
		t.Fatalf("the reviewer's checkout follows its job: %+v", request.Repository)
	}
	ops := []string{}
	for _, command := range commands {
		ops = append(ops, strings.Trim(string(command["op"]), `"`))
	}
	if !slices.Equal(ops, []string{"agents.list", "pane.capture", "pane.close", "job.mark_lost"}) {
		t.Fatalf("node commands %v", ops)
	}
	// hk end_reason: replaced_by is the last key=value token (mergewake hk 3335).
	if reason := endReasonOf(t, commands[3]); reason != "quota_stop pool=claude replaced_by=mwr-1-p1-q1" {
		t.Fatalf("end_reason=%q", reason)
	}
	eventually(t, 2*time.Second, "the owner's info lane event", func() bool {
		for _, relay := range spawnFailedInjections(f.destination) {
			if strings.Contains(relay.Text, "replaced mwr-1-p1 (claude quota) with mwr-1-p1-q1 on machine-b/sonnet") {
				return true
			}
		}
		return false
	})

	// The replacement stops on quota in turn: one more, then none.
	second := newQuotaReplaceFixture(t, "reviewer", "mwr-1-p1-q1")
	if result, _ := second.run(t, placed, 4); !result.ok || result.job != "mwr-1-p1-q1-q2" {
		t.Fatalf("second replacement=%+v", result)
	}
	third := newQuotaReplaceFixture(t, "reviewer", "mwr-1-p1-q1-q2")
	if job, ok := third.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent("mwr-1-p1-q1-q2")); ok || job != "" {
		t.Fatalf("a third stop got a replacement: %q", job)
	}
	if _, err := third.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-1-p1-q1-q2-q3"); err == nil {
		t.Fatal("a third replacement was submitted")
	}
}

func TestQuotaReplacementJobNames(t *testing.T) {
	for old, want := range map[string]string{"mwr-1-p1": "mwr-1-p1-q1", "mwr-1-p1-q1": "mwr-1-p1-q1-q2", "pw.job_2": "pw.job_2-q1"} {
		got, _, ok := quotaReplacementJob(old)
		if !ok || got != want || !hubJobIDPattern.MatchString(got) {
			t.Fatalf("%s -> %q %v, want %q", old, got, ok, want)
		}
	}
	if _, _, ok := quotaReplacementJob("mwr-1-p1-q1-q2"); ok {
		t.Fatal("a third replacement is allowed")
	}
	if _, _, ok := quotaReplacementJob(strings.Repeat("a", 126)); ok {
		t.Fatal("a successor id past 128 characters is allowed")
	}
}

// When no other pool can take the work the successor is not placed and the hub
// writes nothing: the old pane is closed (so one job holds the task) and the
// node's watcher ends the job after its grace as a plain quota stop.
func TestQuotaStopReviewerWithNoOtherPoolIsNotReplaced(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-2-p1")
	result, commands := f.run(t, hubSpawnOutcome{RC: 81, Stage: "placement", Reason: "no_eligible_host"}, 3)
	if result.ok || len(commands) != 3 {
		t.Fatalf("result=%+v commands=%d", result, len(commands))
	}
	if state := f.jobs.records["mwr-2-p1"]["state"]; state != "spawned" {
		t.Fatalf("the stopped job was changed: %v", state)
	}
}

// A worker the manager handed over is replaced with the same task; its brief
// names the earlier job's report and branch; its record ends handed_over_to.
func TestQuotaStopHandedOverWorkerIsReplaced(t *testing.T) {
	f := newQuotaReplaceFixture(t, "implementer", "pw-task-42")
	placed := hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "devin-swe2"}
	result, commands := f.run(t, placed, 4)
	if !result.ok || result.job != "pw-task-42-q1" {
		t.Fatalf("replacement=%+v", result)
	}
	request := f.successor(t, "pw-task-42-q1")
	if i := slices.Index(request.Args, "--exclude-pool"); i < 0 || request.Args[i+1] != "claude" {
		t.Fatalf("the hand-over must exclude the stopped pool: %v", request.Args)
	}
	if request.Repository.Worktree != "pw-task-42-checkout" || request.Repository.Checkout.Branch != "wk/task" {
		t.Fatalf("a worker continues in the same worktree and branch: %+v", request.Repository)
	}
	if !strings.HasPrefix(request.Brief, "Review the change") || !strings.Contains(request.Brief, "Continue the earlier job pw-task-42") || !strings.Contains(request.Brief, "pw-task-42/report.md") || !strings.Contains(request.Brief, "wk/task") {
		t.Fatalf("brief=%q", request.Brief)
	}
	if reason := endReasonOf(t, commands[3]); reason != "quota_stop pool=claude handed_over_to=pw-task-42-q1" {
		t.Fatalf("end_reason=%q", reason)
	}
}

// A pinned profile cannot move to another pool, and a role other than worker,
// reviewer has no replacement.
func TestQuotaStopRefusesWhatItCannotReplace(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-3-p1")
	if job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-b", quotaStopEvent("mwr-3-p1")); ok || job != "" {
		t.Fatal("an event from another node was honoured")
	}
	if job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent("unknown-job")); ok || job != "" {
		t.Fatal("an unknown job was replaced")
	}
}

// A pane that is working, not idle, is never replaced or closed: nothing is
// placed while the old pane could still be doing the work.
func TestQuotaStopBusyPaneIsNotReplaced(t *testing.T) {
	f := newQuotaReplaceFixture(t, "implementer", "pw-task-43")
	f.paneStatus = "working"
	result, commands := f.run(t, hubSpawnOutcome{}, 1)
	if result.ok || len(commands) != 1 {
		t.Fatalf("result=%+v commands=%d", result, len(commands))
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), "pw-task-43-q1"); err == nil {
		t.Fatal("a successor was placed beside a working pane")
	}
}

// --exclude-pool is on the hub's operator allowlist beside --exclude-host, with
// the same value shape, and is refused for a caller that is not the operator.
func TestHubSpawnAllowlistCarriesExcludePool(t *testing.T) {
	base := []string{"-c", "/work/project", "-p", "/work/brief.md", "--t", "T1", "--task", "42"}
	for name, tc := range map[string]struct {
		value    string
		operator bool
		want     bool
	}{
		"operator one pool":   {"claude", true, true},
		"operator two pools":  {"claude,codex", true, true},
		"operator bad value":  {"claude codex", true, false},
		"operator empty list": {"claude,,codex", true, false},
		"not the operator":    {"claude", false, false},
	} {
		t.Run(name, func(t *testing.T) {
			args := append(slices.Clone(base), "--exclude-pool", tc.value)
			if !tc.operator {
				args = args[4:]
				args = append([]string{"--t", "T1", "--task", "42"}, args[len(args)-2:]...)
			}
			if got := validHubSpawnArgsForMode(args, tc.operator); got != tc.want {
				t.Fatalf("validHubSpawnArgsForMode(%v, %v)=%v, want %v", args, tc.operator, got, tc.want)
			}
		})
	}
}

// The watcher ends an unchanged stopped job after QuotaStopHandOffGrace; the hub
// must be done, one way or the other, before that, or the old hk record is
// written lost without replaced_by while the successor is still being placed.
// The bound is a context deadline on the whole replacement, so it holds whatever
// the phases (hk reads, node commands, retries) add up to.
func TestQuotaReplaceBudgetFitsTheWatcherGrace(t *testing.T) {
	if quotaReplaceBudget+quotaReplaceSlack > fleet.QuotaStopHandOffGrace {
		t.Fatalf("hub budget %v plus relay slack %v is past the watcher grace %v", quotaReplaceBudget, quotaReplaceSlack, fleet.QuotaStopHandOffGrace)
	}
	if quotaReplaceWait >= quotaReplaceBudget {
		t.Fatalf("the wait for placement %v must leave room in the budget %v", quotaReplaceWait, quotaReplaceBudget)
	}
	long := quotaHolderRequest("reviewer", "mwr-9-p1")
	long.DeadlineSeconds = 3600
	next, err := quotaReplacementRequest(long, "mwr-9-p1", "mwr-9-p1-q1", "claude", "reviewer")
	if err != nil || time.Duration(next.DeadlineSeconds)*time.Second != quotaReplaceDeadline {
		t.Fatalf("the successor's spawn deadline is %ds (%v), want %v", next.DeadlineSeconds, err, quotaReplaceDeadline)
	}
}

// The budget is enforced, not just declared: with a node that never answers,
// the replacement gives up when its budget runs out, not after each step's own
// wait (a 5 s probe here).
func TestQuotaReplacementStopsAtItsBudget(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-8-p1")
	f.hub.quotaReplaceBudget = 200 * time.Millisecond
	started := time.Now()
	if job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job)); ok || job != "" {
		t.Fatalf("replaced without a node answer: %q", job)
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Fatalf("the replacement ran %v past a 200 ms budget", took)
	}
}

// A pane that changed after the probe is refused by pane.close: nothing is
// placed, so the replacement can never run beside the original (review finding).
func TestQuotaStopPaneChangedBeforeCloseGetsNoSuccessor(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-5-p1")
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job))
		done <- quotaReplaceResult{job, ok}
	}()
	command := nodeCommandRead(t, f.node)
	roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
	command = nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/x/pane-screen.txt"}})
	command = nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodePaneChanged}})
	select {
	case result := <-done:
		if result.ok {
			t.Fatalf("replaced a job whose pane could not be closed: %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-5-p1-q1"); err == nil {
		t.Fatal("a successor was placed while the old pane stayed open")
	}
}

// The successor pointer is written once, so a successor id that would push
// end_reason past hk's 128 characters is refused before anything is closed or
// placed (review finding).
func TestQuotaStopRefusesASuccessorWhoseEndReasonWouldNotFit(t *testing.T) {
	job := "mwr-" + strings.Repeat("a", 96)
	f := newQuotaReplaceFixture(t, "reviewer", job)
	next, _, ok := quotaReplacementJob(job)
	if !ok || len(next) > 128 {
		t.Fatalf("setup: the successor id %d must be valid on its own", len(next))
	}
	if got, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(job)); ok || got != "" {
		t.Fatalf("replaced with an id whose end_reason cannot be written: %q", got)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), next); err == nil {
		t.Fatal("a successor was placed")
	}
}

// A transient job.mark_lost failure after the successor is placed is retried, so
// the old record still gets its pointer before the watcher's fallback (review
// finding).
func TestQuotaStopRetriesTheStoppedJobRecordWrite(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-6-p1")
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job))
		done <- quotaReplaceResult{job, ok}
	}()
	command := nodeCommandRead(t, f.node)
	roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/x/pane-screen.txt"}})
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodePaneClosed}})
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-6-p1-q1")
		return err == nil
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	first := nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, first, map[string]any{"error": "mark_lost_failed"})
	second := nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, second, map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}})
	select {
	case result := <-done:
		if !result.ok || result.job != "mwr-6-p1-q1" {
			t.Fatalf("result %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
	if got := endReasonOf(t, second); got != "quota_stop pool=claude replaced_by=mwr-6-p1-q1" {
		t.Fatalf("retried end_reason=%q", got)
	}
}

// reconciling is still an active spawn state (a node may complete the successor
// later): the hub keeps waiting through it, and writes the old record's pointer
// once the successor is placed (review finding).
func TestQuotaStopWaitsThroughReconcilingForTheSuccessor(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-7-p1")
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job))
		done <- quotaReplaceResult{job, ok}
	}()
	roleReplyPanes(t, f.node, nodeCommandRead(t, f.node), []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/x/pane-screen.txt"}})
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodePaneClosed}})
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-7-p1-q1")
		return err == nil
	})
	successor, err := f.hub.inbox.store.bindSpawn(t.Context(), successor.Request.RequestID, successor.Attempt, hubSpawnRequest{RequestID: t14RequestID(333401), Machine: "machine-a"})
	if err != nil {
		t.Fatal(err)
	}
	successor = sentSpawnFixture(t, f.hub.inbox.store, successor)
	// The node's answer is unknown for now: the operation is reconciling.
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 75, Stage: "landing", Reason: "unknown"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got, err := f.hub.inbox.store.spawnTask(t.Context(), successor.Request.RequestID); err != nil || got.Status != "reconciling" {
		t.Fatalf("setup: status %q %v", got.Status, err)
	}
	select {
	case result := <-done:
		t.Fatalf("the hub gave up on a reconciling successor: %+v", result)
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got, _ := f.hub.inbox.store.spawnTask(t.Context(), successor.Request.RequestID); got.Status != "succeeded" {
		t.Fatalf("setup: the reconciling operation did not complete: %q", got.Status)
	}
	command := nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}})
	select {
	case result := <-done:
		if !result.ok || endReasonOf(t, command) != "quota_stop pool=claude replaced_by=mwr-7-p1-q1" {
			t.Fatalf("result %+v end_reason %q", result, endReasonOf(t, command))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
}

// When the wait for the successor ends with the operation still undecided
// (reconciling, or past its deadline), a late node result can still make it
// run, and hk cannot repair the old record afterwards. So the hub writes the
// pointer anyway: a pointer to a successor that later fails is a recoverable
// chain, a running successor nobody points to is not (review finding).
func TestQuotaStopUndecidedSuccessorStillGetsItsPointer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-10-p1")
	f.hub.quotaWait = 150 * time.Millisecond
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job))
		done <- quotaReplaceResult{job, ok}
	}()
	roleReplyPanes(t, f.node, nodeCommandRead(t, f.node), []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/x/pane-screen.txt"}})
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodePaneClosed}})
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), "mwr-10-p1-q1")
		return err == nil
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, hubSpawnOutcome{RC: 75, Stage: "landing", Reason: "unknown"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	// It stays reconciling past the wait: the pointer is written regardless.
	command := nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}})
	select {
	case result := <-done:
		if !result.ok || result.job != "mwr-10-p1-q1" || endReasonOf(t, command) != "quota_stop pool=claude replaced_by=mwr-10-p1-q1" {
			t.Fatalf("result %+v end_reason %q", result, endReasonOf(t, command))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
}

// An operation never bound to a node (accepted) can never place a job: a
// reviewer successor still accepted when the wait ends gets no pointer, and the
// stopped job is left to the watcher's plain quota_stop (review finding).
func TestQuotaStopAcceptedSuccessorGetsNoPointer(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-11-p1")
	f.hub.quotaWait = 100 * time.Millisecond
	done := make(chan quotaReplaceResult, 1)
	go func() {
		job, ok := f.hub.replaceQuotaStopped(t.Context(), "machine-a", quotaStopEvent(f.job))
		done <- quotaReplaceResult{job, ok}
	}()
	roleReplyPanes(t, f.node, nodeCommandRead(t, f.node), []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/project.checkout"}}, true)
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/x/pane-screen.txt"}})
	nodeCommandReplyTo(t, f.node, nodeCommandRead(t, f.node), map[string]any{"result": map[string]any{"outcome": nodePaneClosed}})
	select {
	case result := <-done:
		if result.ok {
			t.Fatalf("a successor that never left accepted got a pointer: %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the replacement did not finish")
	}
}

// A reviewer that inherited its builder's task (--parent-job, no --task) is
// replaced with the same arguments (review finding).
func TestQuotaStopReplacesAnInheritedReviewerJob(t *testing.T) {
	f := newQuotaReplaceFixtureWith(t, "reviewer", "pw-test-1", func(r *fleet.Submission) {
		for i, arg := range r.Args {
			if arg == "--task" {
				r.Args[i], r.Args[i+1] = "--parent-job", "builder-1"
			}
		}
		r.Args = append(r.Args, "--author-family", "anthropic", "--builder-job", "builder-1")
	})
	placed := hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}
	result, _ := f.run(t, placed, 4)
	if !result.ok || result.job != "pw-test-1-q1" {
		t.Fatalf("replacement=%+v", result)
	}
	args := f.successor(t, "pw-test-1-q1").Args
	if hubSpawnArg(args, "--parent-job") != "builder-1" || hubSpawnArg(args, "--task") != "" || hubSpawnArg(args, "--exclude-pool") != "claude" {
		t.Fatalf("successor args %v", args)
	}
}

// A hub restarted between claiming a reviewer's successor and writing the old
// record leaves a running successor nobody points to. The reconcile pass finds
// the successor, closes what is left of the old pane and writes replaced_by
// (review finding).
func TestQuotaReplacementIsReconciledAfterAHubRestart(t *testing.T) {
	f := newQuotaReplaceFixture(t, "reviewer", "mwr-12-p1")
	request, err := quotaReplacementRequest(f.holder.Request, "mwr-12-p1", "mwr-12-p1-q1", "claude", "reviewer")
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-quotaReplaceBudget - time.Minute)
	if err := f.hub.inbox.store.releaseCompletedSpawnRole(t.Context(), f.holder.Request.RequestID, "42:reviewer:p1"); err != nil {
		t.Fatal(err)
	}
	task, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, "mwr-12-p1-q1", "parent", old)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "done", Host: quotaPlaced, Pane: "w9:p1", Profile: "sonnet"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	wrote := make(chan int, 1)
	go func() { wrote <- f.hub.reconcileQuotaReplacements(t.Context()) }()
	// The old pane is already gone (the hub closed it before it stopped).
	roleReplyPanes(t, f.node, nodeCommandRead(t, f.node), []nodeAgentRow{{PaneID: "w2:p9", Cwd: "/work/other"}}, true)
	command := nodeCommandRead(t, f.node)
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}})
	select {
	case n := <-wrote:
		if n != 1 || endReasonOf(t, command) != "quota_stop pool=claude replaced_by=mwr-12-p1-q1" {
			t.Fatalf("wrote %d, end_reason %q", n, endReasonOf(t, command))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the reconcile pass did not finish")
	}
}
