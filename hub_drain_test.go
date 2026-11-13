package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

// Tests for the hub's SIGTERM answer. Contract: docs/hub-drain.md.

func assertHubDraining(t *testing.T, status int, header http.Header, body []byte) {
	t.Helper()
	var decoded map[string]any
	_ = json.Unmarshal(body, &decoded)
	if status != http.StatusServiceUnavailable || decoded["error"] != "hub_draining" || decoded["retry"] != true || header.Get("Retry-After") == "" {
		t.Fatalf("want the retryable drain answer, got %d %v %s", status, header, body)
	}
}

// drainHubWithStore is an async-spawn hub whose clock and inbox file the test owns.
func drainHubWithStore(t *testing.T, path string, now func() time.Time) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: spawnJobsFixture{}, Out: out, Err: errout}, nil
	}
	return h
}

func TestDrainFinishesRequestInFlightAndRefusesNewOnes(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, false)
	inFlight := t14HTTPAsync(t, http.MethodPost, nodeCommandURL(server, "machine-a"), t14OperatorToken, nodeCommandCloseBody)
	command := nodeCommandRead(t, node)

	hub.BeginDrain()

	status, header, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/nodes", t14OperatorToken, nil)
	assertHubDraining(t, status, header, body)
	if status, _, body = t14HTTP(t, http.MethodGet, server.URL+"/healthz", "", nil); status != http.StatusOK {
		t.Fatalf("healthz is pinned and must stay answerable while draining: %d %s", status, body)
	}
	short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if hub.AwaitDrained(short) {
		t.Fatal("the drain reported idle with a request still in flight")
	}

	nodeCommandReplyTo(t, node, command, map[string]any{"result": map[string]any{"outcome": "closed"}})
	result := t14Result(t, inFlight)
	if result.status != http.StatusOK {
		t.Fatalf("the request in flight did not complete: %d %s", result.status, result.body)
	}
	long, cancelLong := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelLong()
	if !hub.AwaitDrained(long) {
		t.Fatal("the drain never went idle after the request finished")
	}
}

func TestDrainEndsLongPollWithRetryAnswer(t *testing.T) {
	h := drainHubWithStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), time.Now)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	poll := t14HTTPAsync(t, http.MethodGet, server.URL+"/v1/inbox/wait?lane=lane-a&timeout=30s", t14OperatorToken, nil)
	// The poll has taken its waiter slot once the semaphore shows it.
	eventually(t, 2*time.Second, "the long poll to block", func() bool { return len(h.inbox.waiters) == 1 })

	started := time.Now()
	h.BeginDrain()
	result := t14Result(t, poll)
	assertHubDraining(t, result.status, result.header, result.body)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("the long poll outlived the drain notice by %s", elapsed)
	}
	status, header, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/inbox/wait?lane=lane-a&timeout=30s", t14OperatorToken, nil)
	assertHubDraining(t, status, header, body)
}

func TestDrainRefusesNewSpawnAndTheRestartedHubAcceptsIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	old := drainHubWithStore(t, path, time.Now)
	oldServer := httptest.NewServer(old.Handler())
	defer oldServer.Close()
	request := durableSpawnRequest()

	old.BeginDrain()
	status, header, body := t14HTTP(t, http.MethodPost, oldServer.URL+"/v1/spawns", t14OperatorToken, request)
	assertHubDraining(t, status, header, body)
	if task, err := old.inbox.store.spawnTask(t.Context(), request.RequestID); err == nil && task.Request.RequestID != "" {
		t.Fatalf("a spawn refused during the drain was recorded: %+v", task)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := drainHubWithStore(t, path, time.Now)
	restartedServer := httptest.NewServer(restarted.Handler())
	defer restartedServer.Close()
	status, _, body = t14HTTP(t, http.MethodPost, restartedServer.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted || !strings.Contains(string(body), "durable_hub_reservation") {
		t.Fatalf("the restarted hub did not accept the retried spawn: %d %s", status, body)
	}
}

func TestDrainFinishesSpawnDispatchInFlightAndStartsNoNewOne(t *testing.T) {
	h := drainHubWithStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), time.Now)
	first := durableSpawnRequest()
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), first, "test-job", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int32
	started := make(chan struct{}, 4)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorkers := func() { releaseOnce.Do(func() { close(release) }) }
	// A failed assertion must not leave the worker blocked: Close waits for it.
	t.Cleanup(releaseWorkers)
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		executions.Add(1)
		started <- struct{}{}
		<-release
		return nil, errors.New("fixture configuration refusal")
	}
	h.startSpawnCoordinator(t.Context())
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("the dispatch did not start")
	}

	h.BeginDrain()
	short, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if h.AwaitDrained(short) {
		t.Fatal("the drain reported idle with a spawn dispatch in flight")
	}
	// A second spawn becomes pending after the drain began. Nothing more may
	// start in the window below: the durable row waits for the next hub.
	second := durableSpawnRequest()
	second.RequestID = "d676220b-e763-4af1-aefd-000000000002"
	second.Args = append([]string(nil), first.Args...)
	for index, arg := range second.Args {
		// A task and role may hold one live spawn, so the second differs there too.
		switch arg {
		case "test-job":
			second.Args[index] = "test-job-2"
		case "42":
			second.Args[index] = "43"
		}
	}
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), second, "test-job-2", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	select {
	case h.spawnKick <- struct{}{}:
	default:
	}
	// This window proves nothing more happens; it is not waiting for an outcome.
	time.Sleep(100 * time.Millisecond)
	if got := executions.Load(); got != 1 {
		t.Fatalf("a draining hub started another dispatch: %d executions", got)
	}

	releaseWorkers()
	long, cancelLong := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelLong()
	if !h.AwaitDrained(long) {
		t.Fatal("the drain never went idle after the dispatch finished")
	}
	task, err := h.inbox.store.spawnTask(t.Context(), first.RequestID)
	if err != nil || task.Status == "accepted" || task.Status == "dispatching" {
		t.Fatalf("the dispatch in flight did not run to its outcome: %+v %v", task, err)
	}
	pending, err := h.inbox.store.spawnTask(t.Context(), second.RequestID)
	if err != nil || pending.Status != "accepted" {
		t.Fatalf("the spawn queued during the drain must stay pending for the next hub: %+v %v", pending, err)
	}
}

func TestDrainTellsConnectedNodesToRedialThenClosesTheirSockets(t *testing.T) {
	hub, server := t14Hub(t)
	node := t14Node(t, hub, server, false)

	type outcome struct {
		sawRedial bool
		err       error
	}
	read := make(chan outcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		var result outcome
		for {
			_, payload, err := node.Read(ctx)
			if err != nil {
				result.err = err
				read <- result
				return
			}
			if message, valid := parseHubOutbound(payload); valid && message.Type == "redial" {
				result.sawRedial = true
			}
		}
	}()
	if !hub.Drain(time.Second) {
		t.Fatal("an idle hub did not drain inside its budget")
	}
	result := <-read
	if websocket.CloseStatus(result.err) != websocket.StatusGoingAway {
		t.Fatalf("the socket must close going-away after the redial message, got %v", result.err)
	}
	if !result.sawRedial {
		t.Fatal("the node was never sent a redial message before its socket closed")
	}
	// New dials are refused with the retryable answer until the process exits.
	headers := http.Header{}
	headers.Set(hubMachineIDHeader, "machine-a")
	headers.Set(hubAuthorizationHeader, "Bearer node-token")
	_, response, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a node dialling a draining hub must get 503: %v %v", response, err)
	}
}

func TestStaleNodeSocketWakesSpawnWaitersAtOnce(t *testing.T) {
	var offset atomic.Int64
	now := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	h := drainHubWithStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), now)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	spec := *request.Repository.Checkout
	dispatch := hubSpawnRequest{RequestID: t14RequestID(881), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Checkout: &hubSpawnCheckout{Origin: spec.Origin, Mode: spec.Mode, Branch: spec.Branch, Sha: spec.Sha, Dir: spec.Dir}, Args: []string{"--job", "test-job", "-l", "test-job", "-m", "codex-sol", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "worker"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, dispatch, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	transport := &directSpawnTransport{h: h, task: task}
	// The deadline is far away: only the stale sweep can end this wait early.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := transport.dispatch(ctx, dispatch); done <- err }()
	t14ReadNodeSpawn(t, node)

	offset.Store(int64(2 * h.staleAfter))
	h.Sweep()
	select {
	case err := <-done:
		var hubErr *fleet.HubError
		if !errors.As(err, &hubErr) || hubErr.Kind != "hub_spawn_pending" {
			t.Fatalf("the woken waiter must leave the operation pending for a retry: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the spawn waiter stayed blocked on a stale node socket")
	}
}

func TestServeHubDrainsOnSignalBeforeExiting(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + listener.Addr().String()
	signalled, signal := context.WithCancel(t.Context())
	exited := make(chan int, 1)
	go func() { exited <- serveHub(signalled, hub, []net.Listener{listener}, 5*time.Second) }()

	headers := http.Header{}
	headers.Set(hubMachineIDHeader, "machine-a")
	headers.Set(hubAuthorizationHeader, "Bearer node-token")
	node, _, err := websocket.Dial(t.Context(), "ws"+strings.TrimPrefix(base, "http")+"/v1/agent", &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.CloseNow() })
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "hello", "machine_id": "machine-a", "version": "t14", "accepting": false}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Second, "the node to register", func() bool {
		nodes := hub.Nodes()
		return len(nodes) == 1
	})
	inFlight := t14HTTPAsync(t, http.MethodPost, base+"/v1/nodes/machine-a/command", t14OperatorToken, nodeCommandCloseBody)
	command := nodeCommandRead(t, node)

	signal() // SIGTERM
	eventually(t, 2*time.Second, "the hub to start draining", hub.Draining)
	status, header, body := t14HTTP(t, http.MethodGet, base+"/v1/nodes", t14OperatorToken, nil)
	assertHubDraining(t, status, header, body)
	select {
	case code := <-exited:
		t.Fatalf("the hub exited with a request still in flight: %d", code)
	default:
	}

	nodeCommandReplyTo(t, node, command, map[string]any{"result": map[string]any{"outcome": "closed"}})
	// A live node reads its socket, so it answers the hub's close frame.
	go func() {
		for {
			if _, _, err := node.Read(t.Context()); err != nil {
				return
			}
		}
	}()
	if result := t14Result(t, inFlight); result.status != http.StatusOK {
		t.Fatalf("the request in flight did not complete: %d %s", result.status, result.body)
	}
	select {
	case code := <-exited:
		if code != ExitOK {
			t.Fatalf("exit code %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the hub did not exit after its work finished")
	}
}
