package panewire

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

func admissionMessage(id int, durable bool) hubOutboundMessage {
	message := hubOutboundMessage{RequestID: t14RequestID(id), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"--job", "job-a"}, WaitSeconds: 10}
	if durable {
		message.OperatorAuthorized = true
		message.DeadlineMS = time.Now().Add(10 * time.Second).UnixMilli()
	}
	return message
}

func admissionPeer(t *testing.T) (*hubClientConnection, <-chan hubSpawnResult) {
	t.Helper()
	results := make(chan hubSpawnResult, 20)
	server := httptest.NewServer(httpHandlerCollectingResults(results))
	t.Cleanup(server.Close)
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return &hubClientConnection{connection: conn}, results
}

func admissionResult(t *testing.T, results <-chan hubSpawnResult, timeout time.Duration) hubSpawnResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(timeout):
		t.Fatal("no spawn result while the unrelated spawn held admission")
		return hubSpawnResult{}
	}
}

func admissionInflight(client *HubClient) int {
	client.work.mu.Lock()
	defer client.work.mu.Unlock()
	return client.work.inflight
}

func admissionJournalStatus(t *testing.T, client *HubClient, requestID string) string {
	t.Helper()
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	defer client.releaseSpawnJournal()
	var status string
	err = journal.db.QueryRowContext(t.Context(), `SELECT status FROM spawn_inbox WHERE request_id=?`, requestID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return status
}

// A waiter must expire at its own deadline even if an unrelated launch never
// releases the node. Neither durable nor legacy traffic may start it later.
func TestNodeSpawnAdmissionDeadlineLeavesNoRow_t4579(t *testing.T) {
	for _, tc := range []struct {
		name              string
		durable           bool
		shortLocalTimeout bool
	}{{"durable", true, false}, {"legacy", false, false}, {"short-local-budget", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var runs atomic.Int32
			client := &HubClient{spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: t.TempDir(), fleetEvents: (&fleetEventRecorder{}).write}
			client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
				if runs.Add(1) == 1 {
					close(started)
					<-release
				}
				return t14EngineOK()
			}
			peer, results := admissionPeer(t)
			ctx, cancel := context.WithCancel(t.Context())
			var workers sync.WaitGroup
			t.Cleanup(func() { cancel(); releaseOnce.Do(func() { close(release) }); workers.Wait() })
			start := func(message hubOutboundMessage) {
				workers.Add(1)
				go func() { defer workers.Done(); client.handleHubSpawn(ctx, peer, message) }()
			}
			first, waiter := admissionMessage(4579001, true), admissionMessage(4579002, tc.durable)
			if tc.durable && !tc.shortLocalTimeout {
				waiter.DeadlineMS = time.Now().Add(500 * time.Millisecond).UnixMilli()
			} else {
				waiter.WaitSeconds = 1
			}
			start(first)
			<-started
			start(waiter)
			got := admissionResult(t, results, 2500*time.Millisecond)
			if got.RequestID != waiter.RequestID || got.RC != 124 {
				t.Fatalf("expired waiter answered %+v", got)
			}
			if tc.durable && !tc.shortLocalTimeout && (got.Error != "deadline_not_started" || !got.Retryable) || (!tc.durable || tc.shortLocalTimeout) && (got.Error != "timeout" || got.Retryable) {
				t.Fatalf("expired %s waiter answered %+v", tc.name, got)
			}
			if status := admissionJournalStatus(t, client, waiter.RequestID); status != "" {
				t.Fatalf("expired waiter left journal status %q", status)
			}
			releaseOnce.Do(func() { close(release) })
			workers.Wait()
			if runs.Load() != 1 {
				t.Fatalf("expired waiter launched after admission opened: engine runs=%d", runs.Load())
			}
			if got := admissionResult(t, results, time.Second); got.RequestID != first.RequestID || got.RC != 0 {
				t.Fatalf("first spawn answered %+v", got)
			}
			if tc.durable && !tc.shortLocalTimeout {
				client.handleHubSpawn(ctx, peer, waiter)
				if got := admissionResult(t, results, time.Second); got.Error != "deadline_not_started" || got.RC != 124 {
					t.Fatalf("expired replay answered %+v", got)
				}
				if status := admissionJournalStatus(t, client, waiter.RequestID); status != "" || runs.Load() != 1 {
					t.Fatalf("expired replay left status %q or launched: runs=%d", status, runs.Load())
				}
			}
		})
	}
}

// Cancelling the daemon while one request waits must leave only the claimed
// request for restart settlement. The unclaimed request can run on replay.
func TestNodeSpawnAdmissionRestartReplaysWaiter_t4579(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	root, config := t.TempDir(), t14SpawnConfig(t, t.TempDir())
	old := &HubClient{spawnConfigPath: config, spawnJournalDir: root, fleetEvents: (&fleetEventRecorder{}).write}
	old.spawnEngine = func(ctx context.Context, _, _, _, _ string, _ []string) (*hubSpawnOutput, fleet.Execution) {
		startedOnce.Do(func() { close(started) })
		<-ctx.Done()
		return &hubSpawnOutput{}, fleet.Execution{RC: 124}
	}
	peer, results := admissionPeer(t)
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	first, waiter := admissionMessage(4579011, true), admissionMessage(4579012, true)
	for _, message := range []hubOutboundMessage{first, waiter} {
		workers.Add(1)
		go func() { defer workers.Done(); old.handleHubSpawn(ctx, peer, message) }()
		if message.RequestID == first.RequestID {
			<-started
		}
	}
	eventually(t, time.Second, "both requests to enter the node", func() bool { return admissionInflight(old) == 2 })
	// Give a wrongly claimed waiter a window to appear in the durable journal.
	waitUntil(200*time.Millisecond, func() bool { return admissionJournalStatus(t, old, waiter.RequestID) != "" })
	cancel()
	workers.Wait()
	firstStatus, waiterStatus := admissionJournalStatus(t, old, first.RequestID), admissionJournalStatus(t, old, waiter.RequestID)

	var replayRuns atomic.Int32
	restarted := &HubClient{spawnConfigPath: config, spawnJournalDir: root, fleetEvents: (&fleetEventRecorder{}).write,
		nodeHerdr: func(context.Context, ...string) ([]byte, error) { return nil, errors.New("herdr unavailable") },
		spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
			replayRuns.Add(1)
			return t14EngineOK()
		}}
	restarted.handleHubSpawn(t.Context(), peer, waiter)
	got := admissionResult(t, results, time.Second)
	if firstStatus != "executing" || waiterStatus != "" || got.RequestID != waiter.RequestID || got.RC != 0 || replayRuns.Load() != 1 {
		t.Fatalf("restart: first status=%q waiter status=%q replay=%+v runs=%d", firstStatus, waiterStatus, got, replayRuns.Load())
	}
}

// Both copies arrived before admission. Once one owns execution, the other
// must learn that fact without waiting for that execution to finish.
func TestNodeSpawnAdmissionWakesQueuedDuplicates_t4579(t *testing.T) {
	firstStarted, secondStarted := make(chan struct{}), make(chan struct{})
	prepared := make(chan struct{})
	firstRelease, secondRelease := make(chan struct{}), make(chan struct{})
	var firstOnce, secondOnce sync.Once
	var runs atomic.Int32
	client := &HubClient{spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: t.TempDir(), fleetEvents: (&fleetEventRecorder{}).write}
	preparedPath := t.TempDir()
	client.spawnPrepare = func(context.Context, nodeWorktreePrepareArgs) (any, string) {
		close(prepared)
		return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: preparedPath}}, ""
	}
	client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		switch runs.Add(1) {
		case 1:
			close(firstStarted)
			<-firstRelease
		case 2:
			close(secondStarted)
			<-secondRelease
		}
		return t14EngineOK()
	}
	peer, results := admissionPeer(t)
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		firstOnce.Do(func() { close(firstRelease) })
		secondOnce.Do(func() { close(secondRelease) })
		workers.Wait()
	})
	start := func(message hubOutboundMessage) {
		workers.Add(1)
		go func() { defer workers.Done(); client.handleHubSpawn(ctx, peer, message) }()
	}
	first, second := admissionMessage(4579021, true), admissionMessage(4579022, true)
	second.Checkout = t3482Checkout()
	start(first)
	<-firstStarted
	start(second)
	start(second)
	eventually(t, time.Second, "both duplicate waiters or an early result", func() bool {
		return admissionInflight(client) == 3 || len(results) != 0
	})
	select {
	case result := <-results:
		t.Fatalf("unclaimed duplicate was reported while admission was occupied: %+v", result)
	case <-time.After(200 * time.Millisecond): // proves no duplicate claims the occupied gate
	}
	select {
	case <-prepared:
		t.Fatal("checkout began while the first spawn still held admission")
	default:
	}
	conflict := second
	conflict.BriefInline = "different"
	start(conflict)
	if got := admissionResult(t, results, time.Second); got.RequestID != second.RequestID || got.Error != "journal_conflict" {
		t.Fatalf("changed-payload waiting duplicate answered %+v", got)
	}
	firstOnce.Do(func() { close(firstRelease) })
	<-prepared
	<-secondStarted
	var sawFirst, sawDuplicate bool
	for !sawFirst || !sawDuplicate {
		got := admissionResult(t, results, time.Second)
		switch {
		case got.RequestID == first.RequestID && got.RC == 0:
			sawFirst = true
		case got.RequestID == second.RequestID && got.Error == "reconciling":
			sawDuplicate = true
		default:
			t.Fatalf("unexpected result while second spawn runs: %+v", got)
		}
	}
	start(first) // a cached duplicate also bypasses the occupied gate
	if got := admissionResult(t, results, time.Second); got.RequestID != first.RequestID || got.RC != 0 {
		t.Fatalf("cached duplicate answered %+v", got)
	}
	start(conflict)
	if got := admissionResult(t, results, time.Second); got.RequestID != second.RequestID || got.Error != "journal_conflict" {
		t.Fatalf("changed-payload duplicate answered %+v", got)
	}
	secondOnce.Do(func() { close(secondRelease) })
	workers.Wait()
	if got := admissionResult(t, results, time.Second); got.RequestID != second.RequestID || got.RC != 0 || runs.Load() != 2 {
		t.Fatalf("second spawn answered %+v; engine runs=%d", got, runs.Load())
	}
}

// A hub reconnect replays the same UUID while the original socket's handler
// is still waiting. Both connections share the node's one admission gate.
func TestNodeSpawnAdmissionSurvivesHubReconnect_t4579(t *testing.T) {
	connections := make(chan *websocket.Conn, 2)
	results := make(chan hubSpawnResult, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		connections <- conn
		for {
			var message struct {
				Type string `json:"type"`
				hubSpawnResult
			}
			if wsjson.Read(r.Context(), conn, &message) != nil {
				return
			}
			if message.Type == "job.spawn.result" {
				results <- message.hubSpawnResult
			}
		}
	}))
	defer server.Close()
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true,
		SpawnConfigPath: t14SpawnConfig(t, t.TempDir()), SpawnJournalDir: t.TempDir(), PingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	client.fleetEvents = (&fleetEventRecorder{}).write
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var runs atomic.Int32
	client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		if runs.Add(1) == 1 {
			close(started)
			<-release
		}
		return t14EngineOK()
	}
	ctx, cancel := context.WithCancel(t.Context())
	var sessions sync.WaitGroup
	t.Cleanup(func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		sessions.Wait()
		if !waitUntil(5*time.Second, func() bool { return admissionInflight(client) == 0 }) {
			t.Error("node spawn handlers did not finish during cleanup")
		}
	})
	connect := func() (*websocket.Conn, <-chan error) {
		node, _, err := websocket.Dial(ctx, t14WSURL(server.URL, ""), nil)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		sessions.Add(1)
		go func() { defer sessions.Done(); done <- client.serve(ctx, node); _ = node.CloseNow() }()
		return <-connections, done
	}
	send := func(conn *websocket.Conn, message hubOutboundMessage) {
		t.Helper()
		frame := map[string]any{"type": "job.spawn", "request_id": message.RequestID, "cwd_key": message.CWDKey,
			"brief": map[string]any{"inline": message.BriefInline}, "args": message.Args, "wait_seconds": message.WaitSeconds,
			"deadline_ms": message.DeadlineMS, "operator_authorized": true}
		if err := wsjson.Write(ctx, conn, frame); err != nil {
			t.Fatal(err)
		}
	}
	first, waiter := admissionMessage(4579031, true), admissionMessage(4579032, true)
	oldHub, oldSession := connect()
	send(oldHub, first)
	<-started
	send(oldHub, waiter)
	eventually(t, time.Second, "original waiter to reach the node", func() bool { return admissionInflight(client) == 2 })
	// Give an incorrectly claimed waiter time to appear before the disconnect.
	waitUntil(200*time.Millisecond, func() bool { return admissionJournalStatus(t, client, waiter.RequestID) != "" })
	_ = oldHub.CloseNow()
	select {
	case <-oldSession:
	case <-time.After(time.Second):
		t.Fatal("node did not observe the old hub disconnect")
	}
	newHub, _ := connect()
	send(newHub, waiter)
	eventually(t, time.Second, "replayed waiter or an early result", func() bool {
		return admissionInflight(client) == 3 || len(results) != 0
	})
	select {
	case result := <-results:
		t.Fatalf("reconnect reported an unclaimed waiter as running: %+v", result)
	case <-time.After(200 * time.Millisecond): // proves no result precedes admission
	}
	if status := admissionJournalStatus(t, client, waiter.RequestID); status != "" {
		t.Fatalf("reconnected waiter already has journal status %q", status)
	}
	releaseOnce.Do(func() { close(release) })
	eventually(t, 2*time.Second, "both original and replay to settle", func() bool { return admissionInflight(client) == 0 })
	send(newHub, waiter)
	for {
		got := admissionResult(t, results, time.Second)
		if got.RequestID != waiter.RequestID {
			t.Fatalf("unexpected reconnect result %+v", got)
		}
		if got.RC == 0 {
			break
		}
		if got.Error != "reconciling" {
			t.Fatalf("replayed waiter answered %+v", got)
		}
	}
	eventually(t, 2*time.Second, "final replay handler to finish", func() bool { return admissionInflight(client) == 0 })
	if runs.Load() != 2 {
		t.Fatalf("reconnect ran %d engines; want one per UUID", runs.Load())
	}
}
