package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

func autoSpawnRequest() fleet.Submission {
	request := durableSpawnRequest()
	for i := 0; i+1 < len(request.Args); i++ {
		if request.Args[i] == "--host" {
			request.Args[i+1] = "auto"
			return request
		}
	}
	request.Args = append(request.Args, "--host", "auto")
	return request
}

func nodeRefusal(requestID, code string, retryable bool) map[string]any {
	return map[string]any{"type": "job.spawn.result", "request_id": requestID, "rc": 2, "job_id": "", "pane": "", "stdout_tail": "stderr: Fix: x", "error": code, "stage": "placement", "retryable": retryable}
}

// boundedContext makes a lost node result fail the test instead of hanging it.
func boundedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func refusalNodeRequest(request fleet.Submission, id int, machine string) fleet.HubSpawnRequest {
	return fleet.HubSpawnRequest{RequestID: t14RequestID(id), Machine: machine, CWDKey: "project", Brief: fleet.HubBrief{Doc: "briefs/test-job.md"}, Checkout: request.Repository.Checkout, Args: []string{"--job", "test-job", "-m", "sonnet", "-l", "test-job", "--role", "implementer"}, WaitSeconds: 60}
}

// A refusal the node gives before it touches anything must not end an auto
// placement: the candidate loop has to reach the next node. Before this, the
// first refusal bound the operation and every later candidate saw it pending.
func TestAutoPlacementFallsThroughUnmappedNode(t *testing.T) {
	for _, refusal := range []string{"cwd_unmapped"} {
		t.Run(refusal, func(t *testing.T) {
			h := asyncSpawnHub(t)
			var logs bytes.Buffer
			h.logger = slog.New(slog.NewTextHandler(&logs, nil))
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			transport := &directSpawnTransport{h: h, task: task}
			type answer struct {
				reply fleet.HubSpawnReply
				err   error
			}
			first := make(chan answer, 1)
			go func() {
				reply, err := transport.Spawn(boundedContext(t), refusalNodeRequest(request, 901, "machine-a"))
				first <- answer{reply, err}
			}()
			sent := t14ReadNodeSpawn(t, node)
			if err = wsjson.Write(t.Context(), node, nodeRefusal(sent.RequestID, refusal, true)); err != nil {
				t.Fatal(err)
			}
			got := <-first
			if got.err != nil || got.reply.Error != refusal {
				t.Fatalf("placement did not see the refusal: %+v", got)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Status != "accepted" || saved.Node != nil {
				t.Fatalf("refusal ended the operation: status=%s node=%+v", saved.Status, saved.Node)
			}
			if line := logs.String(); !strings.Contains(line, request.RequestID) || !strings.Contains(line, "host=machine-a") || !strings.Contains(line, "profile=sonnet") || !strings.Contains(line, refusal) {
				t.Fatalf("refusal missing from the hub log: %q", line)
			}
			// The next candidate binds and runs under the same operation.
			second := make(chan answer, 1)
			go func() {
				reply, err := transport.Spawn(boundedContext(t), refusalNodeRequest(request, 902, "machine-a"))
				second <- answer{reply, err}
			}()
			sent = t14ReadNodeSpawn(t, node)
			if sent.RequestID != t14RequestID(902) {
				t.Fatalf("second candidate sent %s", sent.RequestID)
			}
			if err = wsjson.Write(t.Context(), node, nodeRefusal(sent.RequestID, "checkout_refused:dirty", false)); err != nil {
				t.Fatal(err)
			}
			<-second
			saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != "failed" || saved.Node == nil || saved.Node.RequestID != t14RequestID(902) {
				t.Fatalf("a refusal after side effects must stay terminal: %+v %v", saved, err)
			}
		})
	}
}

// These stay the operation's result; only an unmapped repository moves on.
func TestAutoPlacementOtherNodeRefusalsStayTerminal(t *testing.T) {
	for _, refusal := range []string{"brief_doc_unavailable", "spawn_disabled"} {
		t.Run(refusal, func(t *testing.T) {
			h := asyncSpawnHub(t)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			transport := &directSpawnTransport{h: h, task: task}
			done := make(chan error, 1)
			go func() {
				_, err := transport.Spawn(boundedContext(t), refusalNodeRequest(request, 906, "machine-a"))
				done <- err
			}()
			sent := t14ReadNodeSpawn(t, node)
			if err = wsjson.Write(t.Context(), node, nodeRefusal(sent.RequestID, refusal, true)); err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != "retryable" || saved.Outcome.Host != "machine-a" {
				t.Fatalf("saved=%+v err=%v", saved, err)
			}
		})
	}
}

// The desk's cause for brief_doc_unavailable: spawn.json without handoffkeep_env.
func TestBriefDocRefusalSaysWhyItCouldNotFetch(t *testing.T) {
	for name, test := range map[string]struct{ config, doc, want string }{
		"no credential file": {`{"cwd_map":{"repo-a":"/work/a"},"workspace":"w","herdr_session":"s","enabled":true}`, "briefs/job.md", "no handoffkeep_env"},
		"outside prefix":     {`{"cwd_map":{"repo-a":"/work/a"},"workspace":"w","herdr_session":"s","enabled":true}`, "other/job.md", "outside this node's brief_doc_prefix"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "spawn.json")
			if err := os.WriteFile(path, []byte(test.config), 0600); err != nil {
				t.Fatal(err)
			}
			client := &HubClient{spawnConfigPath: path}
			result := client.runHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(907), CWDKey: "repo-a", BriefDoc: test.doc, WaitSeconds: 1})
			if result.Error != "brief_doc_unavailable" {
				t.Fatalf("result=%+v", result)
			}
			if reason := spawnOutcomeReason(result.Error, spawnResultRefusal(result.StdoutTail)); !strings.Contains(reason, test.want) {
				t.Fatalf("reason=%q tail=%q", reason, result.StdoutTail)
			}
		})
	}
}

// A pinned host names a node the operator chose: its refusal is the answer.
func TestPinnedHostRefusalStaysTerminal(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := durableSpawnRequest()
	for i := 0; i+1 < len(request.Args); i++ {
		if request.Args[i] == "--host" {
			request.Args[i+1] = "machine-a"
		}
	}
	if hubSpawnArg(request.Args, "--host") != "machine-a" {
		request.Args = append(request.Args, "--host", "machine-a")
	}
	request.Args = append(request.Args, "--containment", "hk:12")
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	done := make(chan error, 1)
	go func() {
		_, err := transport.Spawn(boundedContext(t), refusalNodeRequest(request, 903, "machine-a"))
		done <- err
	}()
	sent := t14ReadNodeSpawn(t, node)
	if err = wsjson.Write(t.Context(), node, nodeRefusal(sent.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "retryable" || saved.Outcome.Host != "machine-a" || saved.Outcome.Profile != "sonnet" {
		t.Fatalf("saved=%+v err=%v", saved, err)
	}
}

// Recovery has no candidate loop to return to, so it records the refusal.
func TestRecoveredDispatchRefusalStaysTerminal(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	spec := *request.Repository.Checkout
	raw, _ := json.Marshal(spec)
	var checkout hubSpawnCheckout
	if err = json.Unmarshal(raw, &checkout); err != nil {
		t.Fatal(err)
	}
	dispatch := hubSpawnRequest{RequestID: t14RequestID(904), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Checkout: &checkout, Args: []string{"--job", "test-job", "-m", "sonnet"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, dispatch); err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	done := make(chan error, 1)
	go func() { _, err := transport.dispatch(boundedContext(t), dispatch); done <- err }()
	sent := t14ReadNodeSpawn(t, node)
	if err = wsjson.Write(t.Context(), node, nodeRefusal(sent.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Status != "retryable" {
		t.Fatalf("saved=%+v err=%v", saved, err)
	}
}

// A replay of an auto request that a node refused for its own configuration
// must place again, not pin the one node known to refuse it.
func TestReplayOfNodeRefusalIsNotPinnedToThatNode(t *testing.T) {
	for reason, want := range map[string]bool{
		"cwd_unmapped":                 true,
		"cwd_unmapped: detail":         true,
		"brief_doc_unavailable":        false,
		"spawn_disabled: rule":         false,
		"spawn_failed: pane uncertain": false,
		"checkout_refused:dirty":       true,
	} {
		if got := spawnRefusalTriesNextHost(reason); got != want {
			t.Errorf("%q: got %v", reason, got)
		}
	}
}

func TestSpawnFailedNotificationNamesHostAndProfile(t *testing.T) {
	text := spawnNotificationText("job", "request", hubSpawnOutcome{RC: 2, Stage: "placement", Reason: "cwd_unmapped", Host: "node-b", Profile: "sonnet"})
	if !strings.Contains(text, "host=node-b") || !strings.Contains(text, "profile=sonnet") {
		t.Fatalf("text=%q", text)
	}
	text = spawnNotificationText("job", "request", hubSpawnOutcome{RC: 79, Stage: "placement", Reason: "configuration"})
	if !strings.Contains(text, "host=none") || !strings.Contains(text, "profile=none") {
		t.Fatalf("unplaced text=%q", text)
	}
	text = spawnNotificationText("job", "request", hubSpawnOutcome{RC: 0, Host: "node-b", Pane: "w:p1", Model: "m", Profile: "sonnet"})
	if !strings.Contains(text, "host=node-b") || !strings.Contains(text, "profile=sonnet") {
		t.Fatalf("spawned text=%q", text)
	}
}

func TestHubLogsSpawnRefusalWithRequestID(t *testing.T) {
	h := asyncSpawnHub(t)
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, nil))
	request := autoSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h.recordSpawnTrace(task, hubSpawnOutcome{RC: 2, Stage: "placement", Reason: "cwd_unmapped", Host: "node-b", Profile: "sonnet"})
	line := logs.String()
	for _, want := range []string{request.RequestID, "job=test-job", "host=node-b", "profile=sonnet", "rc=2", "cwd_unmapped"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log %q lacks %q", line, want)
		}
	}
	if strings.Contains(line, request.Brief) {
		t.Fatalf("log carries the brief: %q", line)
	}
}

// hk cannot hold spawn_retryable (a result needs state=completed), so the hub's
// own durable operation decides whether a fresh request may reuse a job id.
func TestFreshRequestRetriesAfterHubRecordedCleanup(t *testing.T) {
	submit := func(h *HubServer, request fleet.Submission) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(request)
		r := httptest.NewRequest("POST", "/v1/spawns", bytes.NewReader(raw))
		r.Header.Set("Authorization", "Bearer "+t14OperatorToken)
		w := httptest.NewRecorder()
		h.Handler().ServeHTTP(w, r)
		return w
	}
	hub := func(t *testing.T) *HubServer {
		h := asyncSpawnHub(t)
		h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
			return &fleet.App{JobStore: spawnJobsFixture{record: map[string]any{"state": "claimed", "host": "machine-a"}}, Out: out, Err: errout}, nil
		}
		return h
	}
	fresh := func(host string) fleet.Submission {
		request := durableSpawnRequest()
		request.RequestID = t14RequestID(905)
		request.Args = append([]string(nil), request.Args...)
		found := false
		for i := 0; i+1 < len(request.Args); i++ {
			if request.Args[i] == "--host" {
				request.Args[i+1] = host
				found = true
			}
		}
		if !found {
			request.Args = append(request.Args, "--host", host)
		}
		request.Args = append(request.Args, "--containment", "hk:12")
		if !hasOverrideReason(request.Args) {
			request.Args = append(request.Args, "--override-reason", "verified-cleanup-retry")
		}
		return request
	}
	t.Run("cleanup verified", func(t *testing.T) {
		h := hub(t)
		prior := durableSpawnRequest()
		task, _, err := h.inbox.store.claimSpawn(t.Context(), prior, "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 76, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Retryable: true}, time.Now()); err != nil {
			t.Fatal(err)
		}
		if w := submit(h, fresh("machine-a")); w.Code != 202 {
			t.Fatalf("verified cleanup refused: %d %s", w.Code, w.Body)
		}
	})
	t.Run("wrong host", func(t *testing.T) {
		h := hub(t)
		task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 76, Stage: "landing", Reason: "spawn_failed", Host: "machine-a", Retryable: true}, time.Now()); err != nil {
			t.Fatal(err)
		}
		if w := submit(h, fresh("machine-b")); w.Code != 409 || !strings.Contains(w.Body.String(), "retry_host_required") {
			t.Fatalf("host binding lost: %d %s", w.Code, w.Body)
		}
	})
	t.Run("cleanup unverified", func(t *testing.T) {
		h := hub(t)
		task, _, err := h.inbox.store.claimSpawn(t.Context(), durableSpawnRequest(), "test-job", "parent", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = h.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 76, Stage: "landing", Reason: "spawn_failed", Host: "machine-a"}, time.Now()); err != nil {
			t.Fatal(err)
		}
		if w := submit(h, fresh("machine-a")); w.Code != 409 {
			t.Fatalf("uncertain pane reclaimed: %d %s", w.Code, w.Body)
		}
	})
	t.Run("no hub operation", func(t *testing.T) {
		if w := submit(hub(t), fresh("machine-a")); w.Code != 409 {
			t.Fatalf("claim without hub evidence accepted: %d %s", w.Code, w.Body)
		}
	})
}

func hasOverrideReason(args []string) bool { return hubSpawnArg(args, "--override-reason") != "" }

func TestLatestSpawnForJobReadsNewestOperation(t *testing.T) {
	h := asyncSpawnHub(t)
	if _, err := h.inbox.store.latestSpawnForJob(t.Context(), "absent"); err == nil {
		t.Fatal("absent job returned an operation")
	} else if errors.Is(err, errSpawnClaimed) {
		t.Fatalf("err=%v", err)
	}
}
