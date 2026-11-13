package panewire

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// escalateQuota serves the one catalog read escalate-author makes.
type escalateQuota struct{ rows []fleet.CatalogRow }

type ladderPlanQuota struct{ retryQuota }

func (ladderPlanQuota) Launch(ctx context.Context, request fleet.QuotaRequest) (fleet.Launch, error) {
	launch, err := (retryQuota{}).Launch(ctx, request)
	launch.Pool = "p"
	return launch, err
}

func (q escalateQuota) Launch(context.Context, fleet.QuotaRequest) (fleet.Launch, error) {
	return fleet.Launch{}, nil
}
func (q escalateQuota) Gate(context.Context, fleet.QuotaRequest) (string, error) { return "", nil }
func (q escalateQuota) Catalog(context.Context) ([]fleet.CatalogRow, error)      { return q.rows, nil }
func (q escalateQuota) Pools(context.Context) (map[string]fleet.PoolView, error) {
	return nil, nil
}

type escalateFixture struct {
	hub         *HubServer
	server      *httptest.Server
	node        *websocket.Conn
	jobs        *quotaJobs
	destination *hubAgent
	job         string
	// prepareOutcome is what the holder's node answers worktree.prepare;
	// empty means the worktree is reusable.
	prepareOutcome string
	// paneAbsent makes the holder's node list no pane for the job.
	paneAbsent bool
	// markLostFails makes the holder's node refuse the lost-record write.
	markLostFails     bool
	markLostCompleted bool
	paneCwd           string

	mu       sync.Mutex
	commands []map[string]json.RawMessage
}

func escalateHolderRequest(job string) fleet.Submission {
	request := durableSpawnRequest()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--task", "42", "--owner", "parent", "--role", "implementer", "--host", "auto", "--report-relative", job + "/report.md"}
	request.Brief = "Fix the thing on branch wk/task."
	request.Repository.Worktree = job + "-checkout"
	request.Repository.Checkout.Dir = job + "-checkout"
	return request
}

func newEscalateFixture(t *testing.T, job string) *escalateFixture {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	_, client, stop := newFakeHandoffkeep(t)
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
	t.Cleanup(func() { hub.Close() })
	hub.quotaReplacePoll = 5 * time.Millisecond
	hub.quotaMarkBackoff = time.Millisecond
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.mu.Lock()
	hub.nodes["machine-b"] = &hubNodeRecord{agent: destination}
	hub.mu.Unlock()
	jobs := &quotaJobs{records: map[string]map[string]any{job: {
		"state": "spawned", "profile": "sonnet", "effort": "high", "pool": "claude",
		"family": "anthropic", "model": "sonnet-m", "owner_lane": "parent", "task_id": "42",
	}}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout, TaskState: func(context.Context, string) (fleet.TaskStatus, error) {
			return fleet.TaskStatus{JobID: job + "-l1"}, nil
		}, Quota: escalateQuota{rows: []fleet.CatalogRow{
			{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
			{Profile: "sonnet", Effort: "xhigh", Grade: "A+", Pool: "claude", Gate: "escalation", ModelID: "sonnet-m"},
		}}}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, hub, server, true)
	held, _, err := hub.inbox.store.claimSpawn(t.Context(), escalateHolderRequest(job), job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), held, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-a", Pane: roleHeldPane}, time.Now()); err != nil {
		t.Fatal(err)
	}
	f := &escalateFixture{hub: hub, server: server, node: node, jobs: jobs, destination: destination, job: job}
	hub.ladderPlan = func(context.Context, fleet.Submission) error { return nil }
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.serveNode(ctx, t)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return f
}

func (f *escalateFixture) request() hubEscalateAuthorRequest {
	return hubEscalateAuthorRequest{
		Task: "42", Job: f.job, PR: "mgh3326/panewire#77", Round: 5,
		Findings:    `[{"ref":"r1","severity":"high","path":"x.go","line":3,"title":"t","body":"b"}]`,
		Grade:       "A",
		FromProfile: "sonnet", FromEffort: "high",
		Profile: "sonnet", Effort: "xhigh", Step: "same_model_next_effort",
		ConsultSeat: "codex-sol@high",
	}
}

// serveNode plays the holder node's side of every node command the hub sends.
func (f *escalateFixture) serveNode(ctx context.Context, t *testing.T) {
	for {
		_, payload, err := f.node.Read(ctx)
		if err != nil {
			return
		}
		var command map[string]json.RawMessage
		if json.Unmarshal(payload, &command) != nil || string(command["type"]) != `"node.command"` {
			continue
		}
		f.mu.Lock()
		f.commands = append(f.commands, command)
		f.mu.Unlock()
		var requestID, op string
		_ = json.Unmarshal(command["request_id"], &requestID)
		_ = json.Unmarshal(command["op"], &op)
		message := map[string]any{"type": "node.command.result", "request_id": requestID, "op": op}
		for key, value := range f.reply(op) {
			message[key] = value
		}
		if err := wsjson.Write(ctx, f.node, message); err != nil {
			return
		}
	}
}

func (f *escalateFixture) reply(op string) map[string]any {
	cwd := "/work/" + f.job + "-checkout"
	paneCwd := cwd
	if f.paneCwd != "" {
		paneCwd = f.paneCwd
	}
	switch op {
	case "agents.list":
		panes := []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: paneCwd}}
		if f.paneAbsent {
			panes = []nodeAgentRow{}
		}
		return map[string]any{"result": map[string]any{"agents": []nodeAgentRow{}, "panes": panes, "panes_ok": true}}
	case "worktree.prepare":
		switch f.prepareOutcome {
		case "", nodePrepareReused:
			return map[string]any{"result": nodeWorktreePrepareResult{Outcome: nodePrepareReused, Worktree: &nodeWorktree{Path: cwd, Branch: "wk/task", Head: strings.Repeat("b", 40)}}}
		default:
			return map[string]any{"result": nodeWorktreePrepareResult{Outcome: f.prepareOutcome}}
		}
	case "worktree.inspect":
		check := "ok"
		switch f.prepareOutcome {
		case nodeOutcomeDirty:
			check = "dirty"
		case nodeRemoveLocked:
			check = "locked"
		case nodePrepareBranchElsewhere:
			check = "unpushed"
		case nodeOutcomeGitFailed:
			check = "unreadable"
		}
		return map[string]any{"result": nodeWorktreeInspectResult{Worktree: &nodeWorktree{Path: paneCwd, Branch: "wk/task", Head: strings.Repeat("b", 40), OriginURL: "https://github.com/example/project.git"}, Check: check}}
	case "pane.capture":
		return map[string]any{"result": map[string]any{"outcome": nodeCaptureScreen, "saved": "/jobs/" + f.job + "/pane-screen.txt"}}
	case "pane.close":
		return map[string]any{"result": map[string]any{"outcome": nodePaneClosed}}
	case "job.mark_lost":
		if f.markLostFails {
			return map[string]any{"error": "mark_lost_failed"}
		}
		if f.markLostCompleted {
			f.jobs.mu.Lock()
			f.jobs.records[f.job]["state"] = "completed"
			f.jobs.mu.Unlock()
			return map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostCompleted}}
		}
		return map[string]any{"result": map[string]any{"outcome": nodeJobMarkLostLost}}
	}
	return map[string]any{"error": "invalid_args"}
}

// ops are the node commands the hub sent since the fixture's last call.
func (f *escalateFixture) take() []map[string]json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	taken := f.commands
	f.commands = nil
	return taken
}

func commandOps(commands []map[string]json.RawMessage) []string {
	ops := []string{}
	for _, command := range commands {
		ops = append(ops, strings.Trim(string(command["op"]), `"`))
	}
	return ops
}

// escalate runs one escalateAuthor call. A non-nil placed completes the
// successor's spawn with that outcome once the hub has claimed it; nil leaves
// it alone (a refusal before the claim, or a replay of a step already placed).
// It returns the answer and the node commands the call made.
func (f *escalateFixture) escalate(t *testing.T, placed *hubSpawnOutcome) (hubEscalateAuthorResult, []map[string]json.RawMessage) {
	t.Helper()
	f.take()
	type answer struct {
		result hubEscalateAuthorResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := f.hub.escalateAuthor(context.Background(), f.request())
		done <- answer{result, err}
	}()
	if placed != nil {
		next, _, _ := escalateAuthorJob(f.job)
		var successor hubSpawnTask
		eventually(t, 3*time.Second, "the successor to be claimed", func() bool {
			var err error
			successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
			return err == nil && successor.Status != "staged"
		})
		if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, *placed, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case answer := <-done:
		if answer.err != nil {
			t.Fatal(answer.err)
		}
		return answer.result, f.take()
	case <-time.After(5 * time.Second):
		t.Fatal("escalate-author did not finish")
	}
	return hubEscalateAuthorResult{}, nil
}

// endHolder is what hk holds once the holder's node wrote the lost record.
func (f *escalateFixture) endHolder(reason string) {
	f.jobs.mu.Lock()
	defer f.jobs.mu.Unlock()
	f.jobs.records[f.job]["state"] = "lost"
	f.jobs.records[f.job]["end_reason"] = reason
}

func (f *escalateFixture) infoTexts() []string {
	var texts []string
	for _, relay := range spawnFailedInjections(f.destination) {
		texts = append(texts, relay.Text)
	}
	return texts
}

func (f *escalateFixture) awaitInfo(t *testing.T, want ...string) {
	t.Helper()
	var seen []string
	eventually(t, 2*time.Second, "the design lane's info event", func() bool {
		seen = append(seen, f.infoTexts()...)
		for _, text := range seen {
			if !slices.ContainsFunc(want, func(w string) bool { return !strings.Contains(text, w) }) {
				return true
			}
		}
		return false
	})
}

var placedAnywhere = hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-b", Pane: "w9:p1", Profile: "sonnet"}

// A round-5 signal starts the author again one ladder step up on the same
// worktree: the successor pins the new rung, names the old job as its parent,
// keeps the checkout and writes one design-lane info.
func TestEscalateAuthorStartsTheNextRungOnTheSameWorktree(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-42")
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-42-l1" || result.Rung != "sonnet@xhigh" {
		t.Fatalf("result=%+v", result)
	}
	successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), "pw-task-42-l1")
	if err != nil {
		t.Fatal(err)
	}
	request := successor.Request
	for flag, want := range map[string]string{
		"--job": "pw-task-42-l1", "-l": "pw-task-42-l1", "-m": "sonnet", "--effort": "xhigh",
		"--parent-job": "pw-task-42", "--report-relative": "pw-task-42-l1/report.md", "--containment": "hk:42",
	} {
		if got := hubSpawnArg(request.Args, flag); got != want {
			t.Fatalf("%s=%q, want %q (%v)", flag, got, want, request.Args)
		}
	}
	if slices.Contains(request.Args, "--task") {
		t.Fatalf("an inherited successor must not name --task: %v", request.Args)
	}
	if reason := hubSpawnArg(request.Args, "--override-reason"); !strings.HasPrefix(reason, "ladder step sonnet@high -> sonnet@xhigh") {
		t.Fatalf("override reason: %q", reason)
	}
	if request.Repository.Checkout == nil || request.Repository.Checkout.Dir != "pw-task-42-checkout" || request.Repository.Worktree != "pw-task-42-checkout" {
		t.Fatalf("the successor must keep the checkout: %+v", request.Repository)
	}
	for _, want := range []string{"Fix the thing on branch wk/task.", `"ref":"r1"`, "codex-sol@high"} {
		if !strings.Contains(request.Brief, want) {
			t.Fatalf("the brief must carry %q:\n%s", want, request.Brief)
		}
	}
	if ops := commandOps(commands); !slices.Equal(ops, []string{"agents.list", "worktree.inspect", "worktree.inspect", "pane.capture", "pane.close", "job.mark_lost"}) {
		t.Fatalf("node commands %v", ops)
	}
	if reason := endReasonOf(t, commands[5]); reason != "ladder step round=5 replaced_by=pw-task-42-l1" {
		t.Fatalf("end_reason=%q", reason)
	}
	f.awaitInfo(t, "ladder step pw-task-42 -> pw-task-42-l1 on sonnet@xhigh")
}

// The successor pins the live checkout rather than the older submission SHA.
func TestEscalateAuthorSuccessorCheckoutPinsTheVerifiedHead(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-47")
	placed := placedAnywhere
	if result, _ := f.escalate(t, &placed); result.Code != "" {
		t.Fatalf("result=%+v", result)
	}
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), "pw-task-47")
	if err != nil {
		t.Fatal(err)
	}
	if holder.Request.Repository.Checkout.Sha == "" {
		t.Fatal("the replaced job's own submission must keep its pinned sha")
	}
	successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), "pw-task-47-l1")
	if err != nil {
		t.Fatal(err)
	}
	checkout := successor.Request.Repository.Checkout
	if checkout == nil || checkout.Mode != "branch" || checkout.Branch != "wk/task" || checkout.Sha != strings.Repeat("b", 40) {
		t.Fatalf("the successor pins the verified branch head: %+v", checkout)
	}
}

func TestEscalateAuthorInspectsWorktreeWithoutPreparingBeforeClose(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-48")
	placed := placedAnywhere
	_, commands := f.escalate(t, &placed)
	ops := commandOps(commands)
	if slices.Contains(ops, "worktree.prepare") || !slices.Equal(ops, []string{"agents.list", "worktree.inspect", "worktree.inspect", "pane.capture", "pane.close", "job.mark_lost"}) {
		t.Fatalf("unexpected operations: %v", ops)
	}
	var inspect nodeWorktreeInspectArgs
	if err := json.Unmarshal(commands[2]["args"], &inspect); err != nil || !inspect.VerifyBranch {
		t.Fatalf("preflight=%+v err=%v", inspect, err)
	}
}

// A rung no account admits is a quota_refused refusal: the old pane is closed
// and the old record ends lost, no successor runs, and both the answer and the
// design lane's info say the author is gone.
func TestEscalateAuthorRefusesWhenTheRungIsNotAdmitted(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-43")
	placed := hubSpawnOutcome{RC: 77, Stage: "placement", Reason: "NO_ROLE_CANDIDATE: quota exhausted"}
	result, commands := f.escalate(t, &placed)
	if result.Code != "quota_refused" || !result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	if len(commands) != 6 {
		t.Fatalf("the pane is closed and the record marked lost: %v", commandOps(commands))
	}
	if reason := endReasonOf(t, commands[5]); reason != "ladder step round=5 replaced_by=pw-task-43-l1" {
		t.Fatalf("end_reason=%q", reason)
	}
	f.awaitInfo(t, "quota_refused", "no author runs on this PR now")
}

// A refusal before the pane close leaves the author running and says the PR
// goes on to its next round.
func TestEscalateAuthorRefusalBeforeTheCloseLeavesTheAuthor(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-49")
	f.prepareOutcome = nodeOutcomeDirty
	result, commands := f.escalate(t, nil)
	if result.Code != "worktree_dirty" || result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	f.awaitInfo(t, "worktree_dirty", "the PR goes on to the next round")
	if ops := commandOps(commands); !slices.Equal(ops, []string{"agents.list", "worktree.inspect", "worktree.inspect"}) {
		t.Fatalf("nothing past the worktree preparation may run: %v", ops)
	}
	if state := f.jobs.records["pw-task-49"]["state"]; state != "spawned" {
		t.Fatalf("a refused step must not end the job: %v", state)
	}
	_, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || job != f.job {
		t.Fatalf("reservation not restored: job=%q err=%v", job, err)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("refused staged successor remains")
	}
}

// The preflight names what keeps the worktree from being reused: a locked
// worktree and a branch with commits origin lacks are refused with the pane
// still open, like a dirty one.
func TestEscalateAuthorWorktreeRefusalCodes(t *testing.T) {
	for outcome, code := range map[string]string{
		nodeRemoveLocked:           "worktree_locked",
		nodePrepareBranchElsewhere: "worktree_unpushed",
		nodeOutcomeGitFailed:       "worktree_unreadable",
	} {
		f := newEscalateFixture(t, "pw-task-50")
		f.prepareOutcome = outcome
		result, commands := f.escalate(t, nil)
		if result.Code != code || result.AuthorEnded {
			t.Fatalf("%s: result=%+v", outcome, result)
		}
		if ops := commandOps(commands); !slices.Equal(ops, []string{"agents.list", "worktree.inspect", "worktree.inspect"}) {
			t.Fatalf("%s: the pane must stay open: %v", outcome, ops)
		}
	}
}

// A rung the catalog no longer has is a no_rung refusal, and nothing is
// closed: the request is stale, not the worker's fault.
func TestEscalateAuthorRefusesAStaleRungWithoutTouchingThePane(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-44")
	request := f.request()
	request.Profile, request.Effort = "opus", "max"
	result, err := f.hub.escalateAuthor(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Code != "no_rung" || result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	if state := f.jobs.records["pw-task-44"]["state"]; state != "spawned" {
		t.Fatalf("a stale request must not touch the job: %v", state)
	}
	if ops := commandOps(f.take()); !slices.Equal(ops, []string{"agents.list"}) {
		t.Fatalf("a stale request must only probe the pane: %v", ops)
	}
}

// A lost answer is replayed by the caller: after a step ended the old record
// the same (job, round) finds its own successor instead of refusing the dead
// record, whether the successor placed or was refused.
func TestEscalateAuthorReplayFindsTheSuccessor(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-51")
	placed := placedAnywhere
	first, _ := f.escalate(t, &placed)
	f.endHolder(fleet.LadderStepEndReason(5, "pw-task-51-l1"))
	again, commands := f.escalate(t, nil)
	if again != first || again.Job != "pw-task-51-l1" || again.Rung != "sonnet@xhigh" {
		t.Fatalf("a replay must answer the same step: first=%+v again=%+v", first, again)
	}
	if len(commands) != 0 {
		t.Fatalf("a replay of a placed step touches nothing: %v", commandOps(commands))
	}

	g := newEscalateFixture(t, "pw-task-52")
	refused := hubSpawnOutcome{RC: 77, Stage: "placement", Reason: "NO_ROLE_CANDIDATE: quota exhausted"}
	firstRefusal, _ := g.escalate(t, &refused)
	g.endHolder(fleet.LadderStepEndReason(5, "pw-task-52-l1"))
	againRefusal, commands := g.escalate(t, nil)
	if againRefusal != firstRefusal || againRefusal.Code != "quota_refused" || !againRefusal.AuthorEnded || len(commands) != 0 {
		t.Fatalf("a replay of a refused step answers its refusal: first=%+v again=%+v commands=%v", firstRefusal, againRefusal, commandOps(commands))
	}
}

// A step that stopped between the record write and the claim (a hub restart)
// is finished by the replay: the pane is already closed and the record already
// ended by this step, so only the successor is claimed.
func TestEscalateAuthorReplayClaimsTheSuccessorOfAnEndedRecord(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-53")
	f.markLostFails = true
	if result, _ := f.escalate(t, nil); result.Code != "job_not_marked" {
		t.Fatalf("stage: %+v", result)
	}
	f.markLostFails = false
	f.endHolder(fleet.LadderStepEndReason(5, "pw-task-53-l1"))
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-53-l1" || result.Rung != "sonnet@xhigh" {
		t.Fatalf("result=%+v", result)
	}
	if len(commands) != 0 {
		t.Fatalf("the pane is closed and the record ended already: %v", commandOps(commands))
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), "pw-task-53-l1"); err != nil {
		t.Fatalf("the successor must be claimed: %v", err)
	}
}

// A record some other path ended is still not a live author.
func TestEscalateAuthorRefusesAnAuthorLostForAnotherReason(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-54")
	f.endHolder("quota_stop pool=claude replaced_by=pw-task-54-q1")
	result, commands := f.escalate(t, nil)
	if result.Code != "job_not_live" || len(commands) != 0 {
		t.Fatalf("result=%+v commands=%v", result, commandOps(commands))
	}
}

// A lost-record write that fails after the pane closed ends the author in
// practice. The retry sees the pane gone and the record still live, writes the
// record and places the successor.
func TestEscalateAuthorRetryAfterAFailedRecordWrite(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-55")
	f.markLostFails = true
	result, _ := f.escalate(t, nil)
	if result.Code != "job_not_marked" || !result.AuthorEnded {
		t.Fatalf("result=%+v", result)
	}
	f.markLostFails, f.paneAbsent = false, true
	placed := placedAnywhere
	result, commands := f.escalate(t, &placed)
	if result.Code != "" || result.Job != "pw-task-55-l1" {
		t.Fatalf("the retry must place the successor: %+v", result)
	}
	if ops := commandOps(commands); !slices.Equal(ops, []string{"agents.list", "job.mark_lost"}) {
		t.Fatalf("an absent pane needs no close: %v", ops)
	}
}

func TestEscalateAuthorJobNames(t *testing.T) {
	if job, _, ok := escalateAuthorJob("pw-task-42"); !ok || job != "pw-task-42-l1" {
		t.Fatalf("job=%q ok=%v", job, ok)
	}
	if job, _, ok := escalateAuthorJob("pw-task-42-q1"); !ok || job != "pw-task-42-q1-l1" {
		t.Fatalf("a quota-stop successor is a job like any other: job=%q ok=%v", job, ok)
	}
	// One ladder step per job chain, also after a quota-stop replacement of
	// the ladder's own successor.
	for _, old := range []string{"pw-task-42-l1", "pw-task-42-l1-q1", "pw-task-42-l1-q1-q2", "pw-task-42-q1-l1-q1"} {
		if _, _, ok := escalateAuthorJob(old); ok {
			t.Fatalf("%s: a second ladder step is not allowed", old)
		}
	}
}

// An experiment arm names the canon the job was submitted with; the successor
// pins another model, so the arm must not survive onto a canon it no longer
// names. ValidateSubmission is what the claim applies.
func TestEscalateAuthorSubmissionSettlesTheExperimentArm(t *testing.T) {
	request := hubEscalateAuthorRequest{Task: "42", Job: "pw-task-56", PR: "o/r#1", Round: 5, Findings: "[]", Grade: "A", FromProfile: "auto", Profile: "sonnet", Effort: "xhigh", Step: "same_model_next_effort"}
	for arm, want := range map[string]string{"auto@": "", "auto@high": "", "sonnet@xhigh": "sonnet@xhigh", "sonnet@": "sonnet@", "": ""} {
		old := escalateHolderRequest("pw-task-56")
		old.E6Arm = arm
		if arm == "sonnet@xhigh" || arm == "sonnet@" {
			old.Args = slices.Clone(old.Args)
			old.Args[slices.Index(old.Args, "-m")+1] = "sonnet"
		}
		next, err := escalateAuthorSubmission(old, request, "pw-task-56-l1")
		if err != nil {
			t.Fatal(err)
		}
		if next.E6Arm != want {
			t.Fatalf("arm %q: successor carries %q, want %q", arm, next.E6Arm, want)
		}
		if _, err := fleet.ValidateSubmission(next); err != nil {
			t.Fatalf("arm %q: the claim would refuse the successor: %v", arm, err)
		}
	}
}

// The request shape is validated before the hub touches anything.
func TestEscalateAuthorRequestRefusal(t *testing.T) {
	good := hubEscalateAuthorRequest{Task: "42", Job: "job-a", PR: "o/r#1", Round: 5, Findings: "[]", Grade: "A"}
	if reason := good.refusal(); reason != "" {
		t.Fatalf("valid request refused: %s", reason)
	}
	for name, mutate := range map[string]func(*hubEscalateAuthorRequest){
		"task":     func(r *hubEscalateAuthorRequest) { r.Task = "x" },
		"job":      func(r *hubEscalateAuthorRequest) { r.Job = "bad job" },
		"pr":       func(r *hubEscalateAuthorRequest) { r.PR = "nope" },
		"round":    func(r *hubEscalateAuthorRequest) { r.Round = 0 },
		"findings": func(r *hubEscalateAuthorRequest) { r.Findings = "{}" },
		"grade":    func(r *hubEscalateAuthorRequest) { r.Grade = "" },
	} {
		request := good
		mutate(&request)
		if reason := request.refusal(); reason == "" {
			t.Fatalf("%s: invalid request accepted", name)
		}
	}
}

// The route takes the operator credential only, and validates before it
// touches anything.
func TestEscalateAuthorRouteAuthorization(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-45")
	body := map[string]any{"task": "42", "job": "pw-task-45", "pr": "o/r#1", "round": 5, "findings": "[]", "grade": "A", "from_profile": "sonnet", "profile": "sonnet", "effort": "xhigh", "step": "same_model_next_effort"}
	status, _, _ := t14HTTP(t, "POST", f.server.URL+"/v1/jobs/escalate-author", "node-token", body)
	if status != 401 {
		t.Fatalf("a node credential must not escalate: %d", status)
	}
	bad := map[string]any{"task": "x"}
	status, _, raw := t14HTTP(t, "POST", f.server.URL+"/v1/jobs/escalate-author", t14OperatorToken, bad)
	if status != 400 || !strings.Contains(string(raw), "invalid_request") {
		t.Fatalf("an invalid request must be refused before anything runs: %d %s", status, raw)
	}
}

func TestEscalateAuthorDistinctRefusalsReachDesignLane(t *testing.T) {
	f := newEscalateFixture(t, "author-refusals")
	request := f.request()
	if _, err := f.hub.escalateAuthorRefusal(t.Context(), request, "parent", "worktree_dirty", "dirty", false); err != nil {
		t.Fatal(err)
	}
	f.awaitInfo(t, "worktree_dirty")
	if _, err := f.hub.escalateAuthorRefusal(t.Context(), request, "parent", "quota_refused", "quota", true); err != nil {
		t.Fatal(err)
	}
	f.awaitInfo(t, "quota_refused", "no author runs")
}

func TestEscalateAuthorPendingReplayDoesNotReportSuccess(t *testing.T) {
	f := newEscalateFixture(t, "author-pending")
	next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
	if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), next, f.job+"-l1", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	f.endHolder(fleet.LadderStepEndReason(5, f.job+"-l1"))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	result, err := f.hub.escalateAuthor(ctx, f.request())
	if err != nil || result.Code != "pending" || result.Job != "" {
		t.Fatalf("pending placement reported success: %+v", result)
	}
}

func TestEscalateAuthorTransfersWorkerReservation(t *testing.T) {
	f := newEscalateFixture(t, "author-reservation")
	placed := placedAnywhere
	result, _ := f.escalate(t, &placed)
	if result.Job == "" {
		t.Fatalf("successor did not place: %+v", result)
	}
	_, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || job != result.Job {
		t.Fatalf("worker reservation = %q, %v; want %q", job, err, result.Job)
	}
}

func TestEscalateAuthorEndedReplayWithoutRungReportsEnded(t *testing.T) {
	f := newEscalateFixture(t, "author-ended-no-rung")
	f.endHolder(fleet.LadderStepEndReason(5, f.job+"-l1"))
	request := f.request()
	request.Profile, request.Step = "", ""
	result, err := f.hub.escalateAuthor(t.Context(), request)
	if err != nil || result.Code != "job_not_marked" || !result.AuthorEnded {
		t.Fatalf("ended replay: result=%+v err=%v", result, err)
	}
}

func TestEscalateAuthorPinsHolderHost(t *testing.T) {
	f := newEscalateFixture(t, "author-host")
	placed := placedAnywhere
	result, _ := f.escalate(t, &placed)
	if result.Job == "" {
		t.Fatalf("result=%+v", result)
	}
	successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), result.Job)
	if err != nil {
		t.Fatal(err)
	}
	if host := hubSpawnArg(successor.Request.Args, "--host"); host != "machine-a" {
		t.Fatalf("successor host=%q; want holder machine-a", host)
	}
}

func TestEscalateAuthorPinsVerifiedCheckoutHead(t *testing.T) {
	f := newEscalateFixture(t, "author-head")
	placed := placedAnywhere
	result, _ := f.escalate(t, &placed)
	if result.Job == "" {
		t.Fatalf("result=%+v", result)
	}
	successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), result.Job)
	if err != nil {
		t.Fatal(err)
	}
	if got := successor.Request.Repository.Checkout.Sha; got != strings.Repeat("b", 40) {
		t.Fatalf("successor checkout head=%q", got)
	}
}

func TestEscalateAuthorRefusesPaneInAnotherWorktree(t *testing.T) {
	f := newEscalateFixture(t, "author-other-cwd")
	f.paneCwd = "/work/another-worktree"
	result, commands := f.escalate(t, nil)
	if result.Code != "worktree_unreadable" || result.AuthorEnded {
		t.Fatalf("result=%+v commands=%v", result, commandOps(commands))
	}
	if slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("closed pane in another worktree: %v", commandOps(commands))
	}
}

func TestEscalateAuthorPersistsSuccessorBeforeClosingPane(t *testing.T) {
	f := newEscalateFixture(t, "author-durable")
	f.markLostFails = true
	result, _ := f.escalate(t, nil)
	if result.Code != "job_not_marked" {
		t.Fatalf("result=%+v", result)
	}
	staged, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
	if err != nil {
		t.Fatalf("successor was not persisted before ending the author: %v", err)
	}
	if staged.Status != "staged" || staged.Request.Repository.Checkout.Sha != strings.Repeat("b", 40) {
		t.Fatalf("staged successor=%+v", staged)
	}
}

func TestEscalateAuthorResumesStagedSuccessorWithoutCatalogRung(t *testing.T) {
	f := newEscalateFixture(t, "author-staged-replay")
	f.markLostFails = true
	first, _ := f.escalate(t, nil)
	if first.Code != "job_not_marked" {
		t.Fatalf("first=%+v", first)
	}
	f.endHolder(fleet.LadderStepEndReason(5, f.job+"-l1"))
	request := f.request()
	request.Profile, request.Step = "", ""
	type answer struct {
		result hubEscalateAuthorResult
		err    error
	}
	done := make(chan answer, 1)
	go func() { result, err := f.hub.escalateAuthor(t.Context(), request); done <- answer{result, err} }()
	var successor hubSpawnTask
	eventually(t, 3*time.Second, "staged successor activation", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
		return err == nil && successor.Status == "accepted"
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, placedAnywhere, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.result.Job != f.job+"-l1" || got.result.Rung != "sonnet@xhigh" {
			t.Fatalf("replay=%+v err=%v", got.result, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replay did not finish")
	}
}

func TestEscalateAuthorDoesNotReportSuccessWithoutTaskBinding(t *testing.T) {
	f := newEscalateFixture(t, "author-unbound")
	original := f.hub.spawnNewApp
	f.hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app, err := original(out, errout)
		app.TaskState = func(context.Context, string) (fleet.TaskStatus, error) { return fleet.TaskStatus{JobID: f.job}, nil }
		app.LadderRebind = func(context.Context, string, string, string, string) error { return fmt.Errorf("rebind refused") }
		return app, err
	}
	placed := placedAnywhere
	type answer struct {
		result hubEscalateAuthorResult
		err    error
	}
	done := make(chan answer, 1)
	go func() { result, err := f.hub.escalateAuthor(t.Context(), f.request()); done <- answer{result, err} }()
	var successor hubSpawnTask
	eventually(t, 3*time.Second, "successor activation", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
		return err == nil && successor.Status == "accepted"
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, placed, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.result.Code != "rebind_unconfirmed" || got.result.Job != "" {
			t.Fatalf("unbound successor reported success: %+v", got.result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("escalation did not finish")
	}
}

func TestEscalateAuthorExpiresStageOnlyWithAuthorPresent(t *testing.T) {
	f := newEscalateFixture(t, "author-expired")
	next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
	if err != nil {
		t.Fatal(err)
	}
	setEscalateAuthorHost(&next, "machine-a")
	next.Repository.Checkout.Sha = strings.Repeat("b", 40)
	next.Repository.Checkout.VerifyReuse = true
	next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.hub.inbox.store.claimSpawnReplacing(t.Context(), next, f.job+"-l1", "parent", time.Now().Add(-10*time.Minute), holder.Request.RequestID, "42:implementer", "staged"); err != nil {
		t.Fatal(err)
	}
	result, commands := f.escalate(t, nil)
	if result.Code != "staged_expired" || result.AuthorEnded || slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("result=%+v commands=%v", result, commandOps(commands))
	}
	_, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || job != f.job {
		t.Fatalf("reservation job=%q err=%v", job, err)
	}
}

func TestExpiredStagedLadderReconcilerPreservesAbsentAuthor(t *testing.T) {
	for _, present := range []bool{true, false} {
		t.Run(fmt.Sprint(present), func(t *testing.T) {
			f := newEscalateFixture(t, "author-expired-reconcile")
			next, err := escalateAuthorSubmission(escalateHolderRequest(f.job), f.request(), f.job+"-l1")
			if err != nil {
				t.Fatal(err)
			}
			next.LadderStep = &fleet.LadderStepIdentity{Parent: f.job, Round: 5, PR: f.request().PR, Profile: "sonnet", Effort: "xhigh"}
			holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.hub.inbox.store.claimSpawnReplacing(t.Context(), next, f.job+"-l1", "parent", time.Now().Add(-10*time.Minute), holder.Request.RequestID, "42:implementer", "staged"); err != nil {
				t.Fatal(err)
			}
			f.paneAbsent = !present
			f.hub.expireStagedLadderSteps(t.Context())
			_, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
			want := f.job + "-l1"
			if present {
				want = f.job
			}
			if err != nil || job != want {
				t.Fatalf("reservation job=%q want=%q err=%v", job, want, err)
			}
		})
	}
}

func TestEscalateAuthorRefusesUnadmittedSuccessorBeforeClose(t *testing.T) {
	f := newEscalateFixture(t, "author-plan-refused")
	f.hub.ladderPlan = func(context.Context, fleet.Submission) error { return fmt.Errorf("quota refused") }
	result, commands := f.escalate(t, nil)
	if result.Code != "quota_refused" || result.AuthorEnded || slices.Contains(commandOps(commands), "pane.close") {
		t.Fatalf("result=%+v commands=%v", result, commandOps(commands))
	}
	_, job, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer")
	if err != nil || job != f.job {
		t.Fatalf("reservation job=%q err=%v", job, err)
	}
}

func TestLadderPlacementPlanAdmitsInheritedSuccessor(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	t14NamedNode(t, h, server, "desktop", true, true)
	planHK(t)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = ladderPlanQuota{}
		app.JobStore = &quotaJobs{records: map[string]map[string]any{"author-plan": {"state": "spawned", "task_id": "42", "profile": "sonnet", "family": "anthropic", "owner_lane": "parent"}}}
		return app, nil
	}
	request := hubEscalateAuthorRequest{Job: "author-plan", Task: "42", PR: "mgh3326/panewire#77", Round: 5, Profile: "sonnet", Effort: "high", FromProfile: "sonnet", FromEffort: "medium", Findings: "[]"}
	next, err := escalateAuthorSubmission(escalateHolderRequest("author-plan"), request, "author-plan-l1")
	if err != nil {
		t.Fatal(err)
	}
	setEscalateAuthorHost(&next, "desktop")
	if err := h.planLadderSuccessor(t.Context(), next); err != nil {
		t.Fatalf("inherited successor plan: %v", err)
	}
}

func TestEscalateAuthorRejectsMalformedJSON(t *testing.T) {
	f := newEscalateFixture(t, "author-malformed")
	for _, body := range []string{`{`, `{"unknown":1}`} {
		req := httptest.NewRequest("POST", "/v1/jobs/escalate-author", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		response := httptest.NewRecorder()
		f.hub.Handler().ServeHTTP(response, req)
		if response.Code != 400 || !strings.Contains(response.Body.String(), "invalid_request") {
			t.Fatalf("body=%q status=%d response=%q", body, response.Code, response.Body.String())
		}
	}
}

func TestEscalateAuthorRecordsSelectionQuotaRefusal(t *testing.T) {
	f := newEscalateFixture(t, "author-selection-refused")
	request := f.request()
	request.Profile, request.Step, request.SelectionRefused = "", "", true
	result, err := f.hub.escalateAuthor(t.Context(), request)
	if err != nil || result.Code != "quota_refused" || result.AuthorEnded {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	f.awaitInfo(t, "quota_refused")
	if slices.Contains(commandOps(f.take()), "pane.close") {
		t.Fatal("closed author pane")
	}
}

func TestEscalateAuthorStagedReplayIgnoresChangedCatalogRung(t *testing.T) {
	f := newEscalateFixture(t, "author-rung-changed")
	f.markLostFails = true
	if result, _ := f.escalate(t, nil); result.Code != "job_not_marked" {
		t.Fatalf("stage=%+v", result)
	}
	f.markLostFails, f.paneAbsent = false, true
	request := f.request()
	request.Profile, request.Effort = "opus", "high"
	type answer struct {
		result hubEscalateAuthorResult
		err    error
	}
	done := make(chan answer, 1)
	go func() { result, err := f.hub.escalateAuthor(t.Context(), request); done <- answer{result, err} }()
	var successor hubSpawnTask
	eventually(t, 3*time.Second, "successor activation", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
		return err == nil && successor.Status == "accepted"
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, placedAnywhere, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-done:
		if got.err != nil || got.result.Rung != "sonnet@xhigh" {
			t.Fatalf("replay=%+v err=%v", got.result, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("replay timed out")
	}
}

func TestEscalateAuthorDropsStageForIndependentlyTerminalParent(t *testing.T) {
	for _, state := range []string{"lost", "completed", "escalated"} {
		t.Run(state, func(t *testing.T) {
			f := newEscalateFixture(t, "author-independent-end")
			f.markLostFails = true
			if result, _ := f.escalate(t, nil); result.Code != "job_not_marked" {
				t.Fatalf("stage=%+v", result)
			}
			f.jobs.mu.Lock()
			f.jobs.records[f.job]["state"] = state
			f.jobs.records[f.job]["end_reason"] = "quota_stop replaced_by=other"
			f.jobs.mu.Unlock()
			result, _ := f.escalate(t, nil)
			if result.Code != "parent_ended" {
				t.Fatalf("result=%+v", result)
			}
			if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
				t.Fatal("staged successor remains")
			}
			if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
				t.Fatal("terminal parent's staged role remains reserved")
			}
		})
	}
}

func TestExpiredLadderStageRecoversTerminalPredecessor(t *testing.T) {
	for _, ownStep := range []bool{false, true} {
		t.Run(fmt.Sprint(ownStep), func(t *testing.T) {
			f := newEscalateFixture(t, "author-expired-terminal")
			f.markLostFails = true
			if result, _ := f.escalate(t, nil); result.Code != "job_not_marked" {
				t.Fatalf("stage=%+v", result)
			}
			reason := "quota_stop replaced_by=other"
			if ownStep {
				reason = fleet.LadderStepEndReason(5, f.job+"-l1")
			}
			f.endHolder(reason)
			f.hub.now = func() time.Time { return time.Now().Add(10 * time.Minute) }
			f.hub.expireStagedLadderSteps(t.Context())
			successor, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1")
			if ownStep {
				if err != nil || successor.Status != "accepted" {
					t.Fatalf("successor=%+v err=%v", successor, err)
				}
			} else if err == nil {
				t.Fatalf("independent terminal stage remains: %+v", successor)
			}
		})
	}
}

func TestEscalateAuthorUnavailableSelectionWithoutStageDoesNotClose(t *testing.T) {
	f := newEscalateFixture(t, "author-no-catalog")
	request := f.request()
	request.Profile = ""
	request.Step = ""
	request.SelectionUnavailable = true
	_, err := f.hub.escalateAuthor(t.Context(), request)
	if err == nil || slices.Contains(commandOps(f.take()), "pane.close") {
		t.Fatalf("err=%v", err)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("unexpected stage")
	}
}

func TestEscalateAuthorDropsStageWhenParentCompletesDuringClose(t *testing.T) {
	f := newEscalateFixture(t, "author-completes-during-close")
	f.markLostCompleted = true
	result, _ := f.escalate(t, nil)
	if result.Code != "parent_ended" {
		t.Fatalf("result=%+v", result)
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("staged successor remains")
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
		t.Fatal("staged role remains reserved")
	}
}

func TestEscalateAuthorRefusesLegacyWorkerWithOwnerESC(t *testing.T) {
	f := newEscalateFixture(t, "legacy-author")
	holder, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(holder.Request.Args); i++ {
		if holder.Request.Args[i] == "--role" {
			holder.Request.Args[i+1] = "worker"
		}
	}
	raw, err := json.Marshal(holder.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.hub.inbox.store.db.ExecContext(t.Context(), f.hub.inbox.store.q(`UPDATE spawn_operations SET payload=? WHERE request_id=?`), string(raw), holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.hub.inbox.store.db.ExecContext(t.Context(), f.hub.inbox.store.q(`UPDATE spawn_reservations SET task_role=? WHERE request_id=?`), "42:worker", holder.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	result, commands := f.escalate(t, nil)
	if result.Code != "legacy_worker_submission" || result.AuthorEnded || len(commands) != 0 {
		t.Fatalf("result=%+v commands=%v", result, commandOps(commands))
	}
	if _, err := f.hub.inbox.store.latestSpawnForJob(t.Context(), f.job+"-l1"); err == nil {
		t.Fatal("legacy refusal created a successor")
	}
	if _, _, _, err := f.hub.inbox.store.spawnRoleOwner(t.Context(), "42:implementer"); err == nil {
		t.Fatal("legacy refusal created an implementer reservation")
	}
	f.awaitInfo(t, "ESC", "legacy_worker_submission")
	items, err := f.hub.inbox.store.list("parent", false)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(items, func(item laneInboxItem) bool {
		return item.Event.NotifyType == "decision" && strings.Contains(item.Event.Text, "legacy_worker_submission")
	}) {
		t.Fatalf("owner did not receive a decision event: %+v", items)
	}
}
