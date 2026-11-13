package panewire

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/mgh3326/panewire/internal/fleet"
)

// escalateQuota serves the one catalog read escalate-author makes.
type escalateQuota struct{ rows []fleet.CatalogRow }

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
	node        *websocket.Conn
	jobs        *quotaJobs
	destination *hubAgent
	job         string
}

func escalateHolderRequest(job string) fleet.Submission {
	request := durableSpawnRequest()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--task", "42", "--owner", "parent", "--role", "worker", "--host", "auto", "--report-relative", job + "/report.md"}
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
		return &fleet.App{JobStore: jobs, Out: out, Err: errout, Quota: escalateQuota{rows: []fleet.CatalogRow{
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
	return &escalateFixture{hub: hub, node: node, jobs: jobs, destination: destination, job: job}
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

// run plays the holder node's side of the close and the successor's placement,
// and returns what escalateAuthor reported plus the node commands it sent.
func (f *escalateFixture) run(t *testing.T, placed hubSpawnOutcome) (hubEscalateAuthorResult, []map[string]json.RawMessage) {
	t.Helper()
	type answer struct {
		result hubEscalateAuthorResult
		err    error
	}
	done := make(chan answer, 1)
	go func() {
		result, err := f.hub.escalateAuthor(context.Background(), f.request())
		done <- answer{result, err}
	}()
	var commands []map[string]json.RawMessage
	serve := func(command map[string]json.RawMessage) {
		commands = append(commands, command)
		switch string(command["op"]) {
		case `"agents.list"`:
			roleReplyPanes(t, f.node, command, []nodeAgentRow{{PaneID: roleHeldPane, Name: f.job, AgentStatus: "idle", Cwd: "/work/" + f.job + "-checkout"}}, true)
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
	serve(nodeCommandRead(t, f.node))
	serve(nodeCommandRead(t, f.node))
	serve(nodeCommandRead(t, f.node))
	serve(nodeCommandRead(t, f.node))
	next, _, _ := escalateAuthorJob(f.job)
	var successor hubSpawnTask
	eventually(t, 2*time.Second, "the successor to be claimed", func() bool {
		var err error
		successor, err = f.hub.inbox.store.latestSpawnForJob(t.Context(), next)
		return err == nil
	})
	if _, err := f.hub.inbox.store.completeSpawn(t.Context(), successor, placed, time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case answer := <-done:
		return answer.result, commands
	case <-time.After(3 * time.Second):
		t.Fatal("escalate-author did not finish")
	}
	return hubEscalateAuthorResult{}, nil
}

// A round-5 signal starts the author again one ladder step up on the same
// worktree: the successor pins the new rung, names the old job as its parent,
// keeps the checkout and writes one design-lane info.
func TestEscalateAuthorStartsTheNextRungOnTheSameWorktree(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-42")
	result, commands := f.run(t, hubSpawnOutcome{RC: 0, Stage: "done", Host: "machine-b", Pane: "w9:p1", Profile: "sonnet"})
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
	ops := []string{}
	for _, command := range commands {
		ops = append(ops, strings.Trim(string(command["op"]), `"`))
	}
	if !slices.Equal(ops, []string{"agents.list", "pane.capture", "pane.close", "job.mark_lost"}) {
		t.Fatalf("node commands %v", ops)
	}
	if reason := endReasonOf(t, commands[3]); reason != "ladder step round=5 replaced_by=pw-task-42-l1" {
		t.Fatalf("end_reason=%q", reason)
	}
	eventually(t, 2*time.Second, "the design lane's info event", func() bool {
		for _, relay := range spawnFailedInjections(f.destination) {
			if strings.Contains(relay.Text, "ladder step pw-task-42 -> pw-task-42-l1 on sonnet@xhigh") {
				return true
			}
		}
		return false
	})
}

// A rung no account admits is a quota_refused refusal: the old pane is closed
// and the old record ends lost, but no successor runs and the design lane
// still gets its one info.
func TestEscalateAuthorRefusesWhenTheRungIsNotAdmitted(t *testing.T) {
	f := newEscalateFixture(t, "pw-task-43")
	result, commands := f.run(t, hubSpawnOutcome{RC: 77, Stage: "placement", Reason: "NO_ROLE_CANDIDATE: quota exhausted"})
	if result.Code != "quota_refused" {
		t.Fatalf("result=%+v", result)
	}
	if len(commands) != 4 {
		t.Fatalf("the pane is closed and the record marked lost: %v", commands)
	}
	if reason := endReasonOf(t, commands[3]); reason != "ladder step round=5 replaced_by=pw-task-43-l1" {
		t.Fatalf("end_reason=%q", reason)
	}
	eventually(t, 2*time.Second, "the refusal's info event", func() bool {
		for _, relay := range spawnFailedInjections(f.destination) {
			if strings.Contains(relay.Text, "quota_refused") && strings.Contains(relay.Text, "the PR goes on to the next round") {
				return true
			}
		}
		return false
	})
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
	if result.Code != "no_rung" {
		t.Fatalf("result=%+v", result)
	}
	if state := f.jobs.records["pw-task-44"]["state"]; state != "spawned" {
		t.Fatalf("a stale request must not touch the job: %v", state)
	}
}

func TestEscalateAuthorJobNames(t *testing.T) {
	if job, _, ok := escalateAuthorJob("pw-task-42"); !ok || job != "pw-task-42-l1" {
		t.Fatalf("job=%q ok=%v", job, ok)
	}
	if _, _, ok := escalateAuthorJob("pw-task-42-l1"); ok {
		t.Fatal("a second ladder step is not allowed")
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
