package panewire

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A spawn the node never started must settle as such, not stay "ownership
// unresolved" (hk 4379). The node answers rc 124 stage=claim
// deadline_not_started when it first sees a request after its deadline, which
// also means it holds no journal row for it (runClaimedHubSpawn). The hub waits
// a bounded time for that answer after its own landing wait ran out, and a node
// row that is executing or silent keeps ownership as before.

// notStartedSpawn is a bound, dispatching request on a connected node.
type notStartedSpawn struct {
	hub    *HubServer
	node   *websocket.Conn
	task   hubSpawnTask
	server *httptest.Server
}

func newNotStartedSpawn(t *testing.T) *notStartedSpawn {
	t.Helper()
	hub := asyncSpawnHub(t)
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	node := t14Node(t, hub, server, true)
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	spec := *request.Repository.Checkout
	dispatch := hubSpawnRequest{RequestID: t14RequestID(4379), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Checkout: &hubSpawnCheckout{Origin: spec.Origin, Mode: spec.Mode, Branch: spec.Branch, Sha: spec.Sha, Dir: spec.Dir}, Args: []string{"--job", "test-job", "-l", "test-job", "-m", "codex-sol", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "worker"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	if task, err = hub.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, dispatch, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return &notStartedSpawn{hub: hub, node: node, task: task, server: server}
}

// dispatchOnce runs one dispatch whose landing wait ends after wait, with the
// production probe window, and returns its outcome on a channel.
func (s *notStartedSpawn) dispatchOnce(t *testing.T, wait time.Duration) <-chan error {
	t.Helper()
	transport := &directSpawnTransport{h: s.hub, task: s.task, probeWait: 2 * time.Second}
	ctx, cancel := context.WithTimeout(t.Context(), wait)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { _, err := transport.dispatch(ctx, *s.task.Node); done <- err }()
	return done
}

func (s *notStartedSpawn) answer(t *testing.T, fields map[string]any) {
	t.Helper()
	message := map[string]any{"type": "job.spawn.result", "request_id": t14RequestID(4379), "rc": 124, "job_id": "test-job", "pane": "", "stdout_tail": ""}
	for key, value := range fields {
		message[key] = value
	}
	if err := wsjson.Write(t.Context(), s.node, message); err != nil {
		t.Fatal(err)
	}
}

func (s *notStartedSpawn) answerNotStarted(t *testing.T) {
	t.Helper()
	s.answer(t, map[string]any{"stage": "claim", "error": "deadline_not_started", "retryable": true})
}

func (s *notStartedSpawn) wait(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not return")
		return nil
	}
}

func (s *notStartedSpawn) saved(t *testing.T) hubSpawnTask {
	t.Helper()
	task, err := s.hub.inbox.store.spawnTask(t.Context(), s.task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// requireSettledNotStarted is the contract of acceptance 1(a) and 1(b): the
// request ends retryable with the node's reason (never the landing timeout or
// rc 75), says what to do, and frees its (task, role) slot for the replay.
func (s *notStartedSpawn) requireSettledNotStarted(t *testing.T) {
	t.Helper()
	got := s.saved(t)
	if got.Status != "retryable" || !got.Outcome.Retryable || got.Outcome.Reason != "deadline_not_started" || got.Outcome.RC != 124 || got.Outcome.Stage != "claim" || got.Outcome.Fix != spawnNotStartedFix {
		t.Fatalf("not-started answer did not settle the request: status=%s outcome=%+v", got.Status, got.Outcome)
	}
	if _, _, err := s.hub.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", time.Now()); err != nil {
		t.Fatalf("reservation still held: %v", err)
	}
}

// 1(a): the hub's wait ran out, then the node answers the original frame that
// it received late. The dispatch that gave up still settles the request.
func TestLateNotStartedAnswerSettlesRequest_t4379(t *testing.T) {
	s := newNotStartedSpawn(t)
	done := s.dispatchOnce(t, 50*time.Millisecond)
	t14ReadNodeSpawn(t, s.node)
	// Sleep: the landing wait (50 ms) must be over before the node answers, so
	// the answer is late by construction.
	time.Sleep(150 * time.Millisecond)
	s.answerNotStarted(t)
	if err := s.wait(t, done); err != nil {
		t.Fatalf("settled dispatch returned %v", err)
	}
	s.requireSettledNotStarted(t)
}

// 1(b): the node never received the frame, so it holds no journal row. It
// answers only the hub's replay after the wait, and that settles the request.
func TestTimeoutWithoutNodeRowSettlesAsNotStarted_t4379(t *testing.T) {
	s := newNotStartedSpawn(t)
	done := s.dispatchOnce(t, 50*time.Millisecond)
	first := t14ReadNodeSpawn(t, s.node)
	replay := t14ReadNodeSpawn(t, s.node)
	if replay.RequestID != first.RequestID || replay.DeadlineMS != first.DeadlineMS {
		t.Fatalf("probe is not a replay of the same request: %+v vs %+v", first, replay)
	}
	s.answerNotStarted(t)
	if err := s.wait(t, done); err != nil {
		t.Fatalf("settled dispatch returned %v", err)
	}
	s.requireSettledNotStarted(t)
}

// 1(c): a node row that is executing, restarted or unreadable, or no answer at
// all, keeps ownership exactly as the landing timeout always did.
func TestTimeoutKeepsOwnershipWithoutNotStartedProof_t4379(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields map[string]any // nil: the node stays silent
	}{
		{"executing row", map[string]any{"rc": 75, "error": "reconciling"}},
		{"stale executing row", map[string]any{"rc": 75, "error": "unknown"}},
		{"journal unavailable", map[string]any{"rc": 75, "error": "journal_unavailable"}},
		{"silence", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newNotStartedSpawn(t)
			done := s.dispatchOnce(t, 50*time.Millisecond)
			t14ReadNodeSpawn(t, s.node)
			t14ReadNodeSpawn(t, s.node)
			if tc.fields != nil {
				s.answer(t, tc.fields)
			}
			var hubErr *fleet.HubError
			if err := s.wait(t, done); !errors.As(err, &hubErr) || hubErr.Kind != "hub_spawn_pending" {
				t.Fatalf("dispatch returned %v, want hub_spawn_pending", err)
			}
			got := s.saved(t)
			if got.Status != "deadline_exceeded" || got.Outcome.Reason != "timeout" || got.Outcome.RC != 124 || got.Outcome.Retryable {
				t.Fatalf("ownership not kept: status=%s outcome=%+v", got.Status, got.Outcome)
			}
			if _, _, err := s.hub.inbox.store.claimSpawn(t.Context(), roleSecondRequest(), "test-job-r2", "parent", time.Now()); !errors.Is(err, errSpawnClaimed) {
				t.Fatalf("reservation released without proof: %v", err)
			}
		})
	}
}

// The coordinator's later replay pass settles a request that stayed
// deadline_exceeded until the node's answer arrived.
func TestReplayPassSettlesNotStartedRequest_t4379(t *testing.T) {
	s := newStrandedSpawn(t)
	node := t14Node(t, s.hub, s.server, true)
	wait := s.run(t)
	replay := t14ReadNodeSpawn(t, node)
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": replay.RequestID, "rc": 124, "job_id": "test-job", "pane": "", "stdout_tail": "", "stage": "claim", "error": "deadline_not_started", "retryable": true}); err != nil {
		t.Fatal(err)
	}
	wait()
	got := s.status(t)
	if got.Status != "retryable" || got.Outcome.Reason != "deadline_not_started" || got.Outcome.RC == 75 {
		t.Fatalf("late not-started answer did not settle the request: %+v", got)
	}
	if err := s.claimNext(t); err != nil {
		t.Fatalf("reservation still held: %v", err)
	}
}

// 1: while the request's own deadline still has room (the hub's clock is
// behind the node's), an auto placement hands the request to the next host
// instead of ending it.
func TestNotStartedAnswerMovesAutoPlacementToNextHost_t4379(t *testing.T) {
	s := newNotStartedSpawn(t)
	transport := &directSpawnTransport{h: s.hub, task: s.task, placing: true, bound: true}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type reply struct {
		reply fleet.HubSpawnReply
		err   error
	}
	done := make(chan reply, 1)
	go func() { r, err := transport.dispatch(ctx, *s.task.Node); done <- reply{r, err} }()
	t14ReadNodeSpawn(t, s.node)
	s.answerNotStarted(t)
	select {
	case got := <-done:
		if got.err != nil || !got.reply.NextHost || got.reply.RC != 124 {
			t.Fatalf("placement did not move on: %+v %v", got.reply, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not return")
	}
	if got := s.saved(t); got.Status != "accepted" || got.Node != nil {
		t.Fatalf("request stayed bound to the host that never started it: status=%s node=%+v", got.Status, got.Node)
	}
}

// The node's side of 1(b) and 1(c): a replay after the deadline of a request
// with no journal row answers not-started without calling the engine, and a
// request with an executing row never does.
func TestNodeReplayAnswersNotStartedOnlyWithoutJournalRow_t4379(t *testing.T) {
	results := make(chan hubSpawnResult, 4)
	server := httptest.NewServer(httpHandlerCollectingResults(results))
	defer server.Close()
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	peer := &hubClientConnection{connection: conn}
	engineCalls := 0
	// herdr cannot be read, so the restart settle (hub_spawn_settle.go) decides nothing.
	client := &HubClient{nodeHerdr: func(context.Context, ...string) ([]byte, error) { return nil, errors.New("herdr down") }, spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: t.TempDir(), fleetEvents: (&fleetEventRecorder{}).write, spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		engineCalls++
		return t14EngineOK()
	}}
	expired := time.Now().Add(-time.Minute).UnixMilli()
	missing := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(4380), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"--job", "job-a"}, WaitSeconds: 600, DeadlineMS: expired}
	client.handleHubSpawn(t.Context(), peer, missing)
	got := nextSpawnResult(t, results)
	if got.RC != 124 || got.Stage != "claim" || got.Error != "deadline_not_started" || !got.Retryable || engineCalls != 0 {
		t.Fatalf("no-row replay answered %+v (engine calls %d)", got, engineCalls)
	}

	// An executing row, left by a process that died mid-spawn, must never read
	// as not started even though the deadline has passed.
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	executing := missing
	executing.RequestID = t14RequestID(4381)
	if claimed, _, err := journal.claim(t.Context(), executing); err != nil || !claimed {
		t.Fatalf("claim %v %v", claimed, err)
	}
	client.releaseSpawnJournal()
	client.handleHubSpawn(t.Context(), peer, executing)
	if got := nextSpawnResult(t, results); got.Error != "unknown" || got.RC != 75 {
		t.Fatalf("executing-row replay answered %+v", got)
	}
}

// httpHandlerCollectingResults accepts one websocket and forwards every
// job.spawn.result the node writes to results.
func httpHandlerCollectingResults(results chan<- hubSpawnResult) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})
}

func nextSpawnResult(t *testing.T, results <-chan hubSpawnResult) hubSpawnResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("node sent no job.spawn.result")
		return hubSpawnResult{}
	}
}
