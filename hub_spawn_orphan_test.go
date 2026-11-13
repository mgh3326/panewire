package panewire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/getsentry/sentry-go"

	"github.com/mgh3326/panewire/internal/fleet"
)

// strandedSpawn is a hub holding task 42 role worker with a request bound to
// machine-a whose deadline passed a minute ago (deadline_exceeded), the state
// a node restart mid-landing leaves (hk 3423).
type strandedSpawn struct {
	hub    *HubServer
	server *httptest.Server
	jobs   *roleJobs
	task   hubSpawnTask
	events *fleetEventRecorder
}

type fleetEventRecorder struct {
	mu     sync.Mutex
	events []fleetEvent
}

func (r *fleetEventRecorder) write(_ time.Time, ev fleetEvent) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
	return true
}

func (r *fleetEventRecorder) all() []fleetEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]fleetEvent(nil), r.events...)
}

func newStrandedSpawn(t *testing.T) *strandedSpawn {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	jobs := &roleJobs{records: map[string]map[string]any{"test-job": {"state": "claimed"}}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	events := &fleetEventRecorder{}
	hub.fleetEvents = events.write
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now().Add(-11*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	spec := *request.Repository.Checkout
	node := hubSpawnRequest{RequestID: t14RequestID(4231), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Checkout: &hubSpawnCheckout{Origin: spec.Origin, Mode: spec.Mode, Branch: spec.Branch, Sha: spec.Sha, Dir: spec.Dir}, Args: []string{"--job", "test-job", "-l", "test-job", "-m", "codex-sol", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "worker"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = hub.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node); err != nil {
		t.Fatal(err)
	}
	if _, err = hub.inbox.store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if task, err = hub.inbox.store.spawnTask(t.Context(), request.RequestID); err != nil || task.Status != "deadline_exceeded" {
		t.Fatalf("stranded fixture %+v %v", task, err)
	}
	return &strandedSpawn{hub: hub, server: server, jobs: jobs, task: task, events: events}
}

// run executes one coordinator pass over the stranded request in the
// background; wait blocks until it returns.
func (s *strandedSpawn) run(t *testing.T) (wait func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); s.hub.executeSpawnTask(t.Context(), s.task) }()
	return func() {
		t.Helper()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("coordinator pass did not return")
		}
	}
}

func (s *strandedSpawn) status(t *testing.T) hubSpawnTask {
	t.Helper()
	task, err := s.hub.inbox.store.spawnTask(t.Context(), s.task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// claimNext submits the same task and role under a new job id straight to the
// store, which is the claim a reclaim-free resubmission makes.
func (s *strandedSpawn) claimNext(t *testing.T) error {
	t.Helper()
	_, _, err := s.hub.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", time.Now())
	return err
}

// answerSpawn reads the hub's late-result replay and answers it with error.
func answerSpawn(t *testing.T, node *websocket.Conn, failure string) hubOutboundMessage {
	t.Helper()
	replay := t14ReadNodeSpawn(t, node)
	if replay.RequestID != t14RequestID(4231) {
		t.Fatalf("replay request=%s", replay.RequestID)
	}
	t14WriteSpawnResult(t, node, replay.RequestID, 75, "", "", "", failure)
	return replay
}

func TestStrandedSpawnUnknownWithoutPaneFailsAndReleases(t *testing.T) {
	s := newStrandedSpawn(t)
	node := t14Node(t, s.hub, s.server, true)
	wait := s.run(t)
	answerSpawn(t, node, "unknown")
	probe := nodeCommandRead(t, node)
	if string(probe["op"]) != `"agents.list"` {
		t.Fatalf("probe op=%s", probe["op"])
	}
	roleReplyPanes(t, node, probe, []nodeAgentRow{{PaneID: "w2:p9", Name: "another-job"}}, true)
	args := roleMarkLostArgs(t, roleReplyMarkLost(t, node, nodeJobMarkLostLost))
	if args["job"] != "test-job" || args["label"] != "test-job" || args["end_reason"] != spawnOrphanedReason {
		t.Fatalf("mark lost args=%v", args)
	}
	wait()
	got := s.status(t)
	if got.Status != "failed" || got.Outcome.Reason != spawnOrphanedReason || got.Outcome.Retryable {
		t.Fatalf("stranded request not finished: %+v", got)
	}
	if err := s.claimNext(t); err != nil {
		t.Fatalf("reservation still held: %v", err)
	}
	var result string
	if err := s.hub.inbox.store.db.QueryRow(`SELECT result FROM spawn_notifications WHERE event_id=?`, got.Request.RequestID+":1:failed").Scan(&result); err != nil || !strings.Contains(result, spawnOrphanedReason) {
		t.Fatalf("no SPAWN_FAILED outbox row: %q %v", result, err)
	}
}

func TestStrandedSpawnLateSuccessStillWins(t *testing.T) {
	s := newStrandedSpawn(t)
	node := t14Node(t, s.hub, s.server, true)
	wait := s.run(t)
	replay := t14ReadNodeSpawn(t, node)
	receipt := fleet.ExecutionReceipt{Job: "test-job", Host: "machine-a", Pane: "pane-1", Profile: "codex-sol", Model: "model-fixture", Pool: "codex", AccountKey: "codex:0123456789abcdef", Role: "worker", CWD: "/work/project.test-job", Label: "test-job", Kind: "codex"}
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": replay.RequestID, "rc": 0, "job_id": "test-job", "pane": "pane-1", "stdout_tail": "OK pane=pane-1 job=test-job", "receipt": receipt, "stage": "landing"}); err != nil {
		t.Fatal(err)
	}
	read := nodeCommandRead(t, node)
	if string(read["op"]) != `"pane.read"` {
		t.Fatalf("late success op=%s", read["op"])
	}
	nodeCommandReplyTo(t, node, read, map[string]any{"result": PaneReadResult{Outcome: nodePaneReadOK, AgentStatus: "working", Composer: nodeComposerUnknown}})
	wait()
	if got := s.status(t); got.Status != "succeeded" || got.Outcome.Pane != "pane-1" || got.Outcome.Pool != "codex" || got.Outcome.AccountKey != "codex:0123456789abcdef" {
		t.Fatalf("late success lost or its placement record dropped: %+v", got)
	}
	if writes := s.jobs.writes(); len(writes) != 0 {
		t.Fatalf("hk written on success: %v", writes)
	}
}

func TestStrandedSpawnStaysPendingWithoutProof(t *testing.T) {
	assertHeld := func(t *testing.T, s *strandedSpawn) {
		t.Helper()
		if got := s.status(t); got.Status != "deadline_exceeded" {
			t.Fatalf("status=%s", got.Status)
		}
		if err := s.claimNext(t); !errors.Is(err, errSpawnClaimed) {
			t.Fatalf("reservation released: %v", err)
		}
		if writes := s.jobs.writes(); len(writes) != 0 {
			t.Fatalf("hk written: %v", writes)
		}
	}
	t.Run("node disconnected", func(t *testing.T) {
		s := newStrandedSpawn(t)
		s.run(t)()
		assertHeld(t, s)
	})
	t.Run("node still executing answers reconciling", func(t *testing.T) {
		s := newStrandedSpawn(t)
		node := t14Node(t, s.hub, s.server, true)
		wait := s.run(t)
		answerSpawn(t, node, "reconciling")
		wait()
		assertHeld(t, s)
	})
	t.Run("labelled pane listed", func(t *testing.T) {
		s := newStrandedSpawn(t)
		node := t14Node(t, s.hub, s.server, true)
		wait := s.run(t)
		answerSpawn(t, node, "unknown")
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w9:p1", Name: "test-job"}}, true)
		wait()
		assertHeld(t, s)
	})
	t.Run("herdr cannot list panes", func(t *testing.T) {
		s := newStrandedSpawn(t)
		node := t14Node(t, s.hub, s.server, true)
		wait := s.run(t)
		answerSpawn(t, node, "unknown")
		roleReplyPanes(t, node, nodeCommandRead(t, node), nil, false)
		wait()
		assertHeld(t, s)
	})
	t.Run("node refuses the lost write", func(t *testing.T) {
		s := newStrandedSpawn(t)
		node := t14Node(t, s.hub, s.server, true)
		wait := s.run(t)
		answerSpawn(t, node, "unknown")
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{}, true)
		roleReplyMarkLost(t, node, nodeJobMarkLostNewerClaim)
		wait()
		assertHeld(t, s)
	})
}

func TestHubSpawnAbandon(t *testing.T) {
	abandon := func(s *strandedSpawn, token string) <-chan t14HTTPResult {
		return t14HTTPAsync(t, http.MethodPost, s.server.URL+"/v1/spawns/"+s.task.Request.RequestID+"/abandon", token, map[string]string{"reason": "node restarted mid-landing"})
	}
	t.Run("requires the operator credential", func(t *testing.T) {
		s := newStrandedSpawn(t)
		if result := t14Result(t, abandon(s, "node-token")); result.status != http.StatusUnauthorized {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
	})
	t.Run("live labelled pane refuses", func(t *testing.T) {
		s := newStrandedSpawn(t)
		node := t14Node(t, s.hub, s.server, true)
		response := abandon(s, t14OperatorToken)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{{PaneID: "w9:p1", Name: "test-job"}}, true)
		result := t14Result(t, response)
		var body map[string]string
		_ = json.Unmarshal(result.body, &body)
		if result.status != http.StatusConflict || body["error"] != "spawn_abandon_refused" || body["reason"] != "pane_present:w9:p1" {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		if got := s.status(t); got.Status != "deadline_exceeded" {
			t.Fatalf("status=%s", got.Status)
		}
		if err := s.claimNext(t); !errors.Is(err, errSpawnClaimed) {
			t.Fatalf("reservation released: %v", err)
		}
	})
	t.Run("no labelled pane releases", func(t *testing.T) {
		s := newStrandedSpawn(t)
		sink := captureHubLog(s.hub)
		node := t14Node(t, s.hub, s.server, true)
		response := abandon(s, t14OperatorToken)
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{}, true)
		args := roleMarkLostArgs(t, roleReplyMarkLost(t, node, nodeJobMarkLostLost))
		if args["end_reason"] != spawnAbandonedReason {
			t.Fatalf("mark lost args=%v", args)
		}
		if result := t14Result(t, response); result.status != http.StatusOK {
			t.Fatalf("status=%d body=%s", result.status, result.body)
		}
		got := s.status(t)
		if got.Status != "failed" || got.Outcome.Reason != spawnAbandonedReason+": node restarted mid-landing" {
			t.Fatalf("abandoned request %+v", got)
		}
		if err := s.claimNext(t); err != nil {
			t.Fatalf("reservation still held: %v", err)
		}
		if log := sink.String(); !strings.Contains(log, "hub spawn abandon") || !strings.Contains(log, "node restarted mid-landing") {
			t.Fatalf("hub log=%s", log)
		}
		var result string
		if err := s.hub.inbox.store.db.QueryRow(`SELECT result FROM spawn_notifications WHERE event_id=?`, got.Request.RequestID+":1:failed").Scan(&result); err != nil || !strings.Contains(result, "node restarted mid-landing") {
			t.Fatalf("spawn event lacks the reason: %q %v", result, err)
		}
		// A finished request is not abandoned twice.
		if result := t14Result(t, abandon(s, t14OperatorToken)); result.status != http.StatusConflict || !strings.Contains(string(result.body), "spawn_not_pending") {
			t.Fatalf("second abandon status=%d body=%s", result.status, result.body)
		}
	})
}

func TestStrandedSpawnWarnsOnceWhenStuck(t *testing.T) {
	s := newStrandedSpawn(t)
	transport := &spawnSentryTransport{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@example.invalid/1", Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	s.hub.spawnSentry = client
	stuck := func() (fingerprints []string) {
		transport.mu.Lock()
		defer transport.mu.Unlock()
		for _, event := range transport.events {
			fingerprints = append(fingerprints, strings.Join(event.Fingerprint, ":"))
		}
		return fingerprints
	}
	s.hub.now = func() time.Time { return s.task.Deadline.Add(29 * time.Minute) }
	s.run(t)()
	if got := stuck(); len(got) != 0 || len(s.events.all()) != 0 {
		t.Fatalf("warned before 30 minutes: %v %v", got, s.events.all())
	}
	s.hub.now = func() time.Time { return s.task.Deadline.Add(31 * time.Minute) }
	// A hub that stopped after writing the marker but before warning: the
	// pending marker must not swallow the warning.
	if _, pending, err := s.hub.inbox.store.pendingSpawnStuck(t.Context(), s.task, s.hub.now()); err != nil || !pending {
		t.Fatalf("pending marker %v %v", pending, err)
	}
	s.run(t)()
	s.run(t)()
	if got := stuck(); len(got) != 1 || got[0] != "spawn-ownership-stuck" {
		t.Fatalf("sentry fingerprints=%v", got)
	}
	events := s.events.all()
	if len(events) != 1 || events[0].Kind != "spawn" || events[0].Outcome != "stuck" || events[0].Job != "test-job" || events[0].RequestID != s.task.Request.RequestID || events[0].Reason != "deadline_exceeded" {
		t.Fatalf("fleet events=%+v", events)
	}
	if got := s.status(t); got.Status != "deadline_exceeded" {
		t.Fatalf("warning changed the request: %s", got.Status)
	}
}

// A journal entry left executing by a node restart answers unknown, so the hub
// can tell it from a request this process is still running (reconciling).
func TestNodeSpawnReplayAnswersUnknownAfterRestart(t *testing.T) {
	results := make(chan hubSpawnResult, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			var result hubSpawnResult
			if wsjson.Read(r.Context(), conn, &result) != nil {
				return
			}
			results <- result
		}
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	peer := &hubClientConnection{connection: conn}
	next := func() hubSpawnResult {
		t.Helper()
		select {
		case result := <-results:
			return result
		case <-time.After(5 * time.Second):
			t.Fatal("node sent no job.spawn.result")
		}
		return hubSpawnResult{}
	}
	root := t.TempDir()
	started, release := make(chan struct{}), make(chan struct{})
	client := &HubClient{nodeHerdr: func(context.Context, ...string) ([]byte, error) { return nil, errors.New("herdr down") }, spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: root, fleetEvents: (&fleetEventRecorder{}).write, spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		close(started)
		<-release
		return t14EngineOK()
	}}

	// The previous process claimed this request and died before completing it.
	orphan := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(4232), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"--job", "job-a"}, WaitSeconds: 60}
	previous, err := openNodeSpawnJournal(root)
	if err != nil {
		t.Fatal(err)
	}
	if claimed, _, err := previous.claim(t.Context(), orphan); err != nil || !claimed {
		t.Fatalf("claim %v %v", claimed, err)
	}
	previous.db.Close()
	client.handleHubSpawn(t.Context(), peer, orphan)
	if got := next(); got.Error != "unknown" || got.RC != 75 || got.RequestID != orphan.RequestID {
		t.Fatalf("orphaned replay answered %+v", got)
	}

	live := orphan
	live.RequestID = t14RequestID(4233)
	go client.handleHubSpawn(t.Context(), peer, live)
	<-started
	client.handleHubSpawn(t.Context(), peer, live)
	if got := next(); got.Error != "reconciling" || got.RequestID != live.RequestID {
		t.Fatalf("live replay answered %+v", got)
	}
	close(release)
	if got := next(); got.Error != "" || got.RC != 0 || got.RequestID != live.RequestID {
		t.Fatalf("live execution answered %+v", got)
	}
	// A completed entry replays its result whatever this process runs.
	client.handleHubSpawn(t.Context(), peer, live)
	if got := next(); got.Error != "" || got.RC != 0 {
		t.Fatalf("completed replay answered %+v", got)
	}
	raw, _ := json.Marshal(spawnResultWire(hubSpawnResult{RequestID: orphan.RequestID, RC: 75, Error: "unknown"}, false))
	if !strings.Contains(string(raw), `"error":"timeout"`) {
		t.Fatalf("legacy hub cannot decode unknown: %s", raw)
	}
}

func TestHubSpawnAbandonCLI(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(tokenPath, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+t14OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_HUB_URL", "")
	t.Setenv("PANEWIRE_HUB_TOKEN_ENV", "")
	t.Setenv("PANEWIRE_HUB_CF_ENV", "")
	t.Run("usage", func(t *testing.T) {
		for _, args := range [][]string{nil, {"cancel"}, {"abandon", "--reason", "x"}, {"abandon", "not-a-request", "--reason", "x"}, {"abandon", t14RequestID(1)}, {"abandon", t14RequestID(1), "--reason", "two\nlines"}} {
			var stdout, stderr bytes.Buffer
			if code := runHubSpawnCLI(args, &stdout, &stderr, hubCLIDeps{}); code != ExitUsage || !strings.Contains(stderr.String(), "panewire hub spawn abandon REQUEST_ID") {
				t.Fatalf("args=%q code=%d stderr=%s", args, code, stderr.String())
			}
		}
	})
	t.Run("abandons through the hub", func(t *testing.T) {
		s := newStrandedSpawn(t)
		node := t14Node(t, s.hub, s.server, true)
		done := make(chan int, 1)
		var stdout, stderr bytes.Buffer
		go func() {
			done <- runHubSpawnCLI([]string{"abandon", s.task.Request.RequestID, "--reason", "node restarted mid-landing", "--hub-url", s.server.URL, "--hub-token-env", tokenPath}, &stdout, &stderr, hubCLIDeps{HTTPClient: s.server.Client(), AllowInsecureForTests: true})
		}()
		roleReplyPanes(t, node, nodeCommandRead(t, node), []nodeAgentRow{}, true)
		roleReplyMarkLost(t, node, nodeJobMarkLostLost)
		if code := <-done; code != ExitOK || !strings.Contains(stdout.String(), "ABANDONED request="+s.task.Request.RequestID+" job=test-job status=failed") {
			t.Fatalf("code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
		stdout.Reset()
		stderr.Reset()
		if code := runHubSpawnCLI([]string{"abandon", s.task.Request.RequestID, "--reason", "again", "--hub-url", s.server.URL, "--hub-token-env", tokenPath}, &stdout, &stderr, hubCLIDeps{HTTPClient: s.server.Client(), AllowInsecureForTests: true}); code != ExitConditionInvalid || !strings.Contains(stderr.String(), "error=spawn_not_pending status=failed") {
			t.Fatalf("second abandon code=%d stderr=%s", code, stderr.String())
		}
	})
}
