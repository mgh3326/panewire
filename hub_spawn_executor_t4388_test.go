package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Per-node spawn execution (hk 4388): placement only binds, each node runs at
// most spawn_in_flight spawns, its queue runs task priority then age, and a
// host refusal goes back to placement at once with the host excluded.

type executorFixture struct {
	hub    *HubServer
	server *httptest.Server
	nodes  map[string]*websocket.Conn
	frames map[string]chan hubOutboundMessage
	base   time.Time
}

func newExecutorFixture(t *testing.T, machines ...string) *executorFixture {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	tokens := map[string]string{"operator": t14OperatorToken}
	for _, machine := range machines {
		tokens[machine] = "node-token"
	}
	h, err := NewHubServer(HubServerConfig{Tokens: tokens, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: spawnJobsFixture{}, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(h.Handler())
	t.Cleanup(server.Close)
	f := &executorFixture{hub: h, server: server, nodes: map[string]*websocket.Conn{}, frames: map[string]chan hubOutboundMessage{}, base: time.Now()}
	for _, machine := range machines {
		conn := t14NamedNode(t, h, server, machine, true, true)
		frames := make(chan hubOutboundMessage, 64)
		go func() {
			for {
				_, payload, err := conn.Read(context.Background())
				if err != nil {
					return
				}
				if message, valid := parseHubOutbound(payload); valid && message.Type == "job.spawn" {
					frames <- message
				}
			}
		}()
		f.nodes[machine], f.frames[machine] = conn, frames
	}
	return f
}

// policy writes a placement policy file with the given spawn settings.
func (f *executorFixture) policy(t *testing.T, edit func(*PlacementPolicy)) {
	t.Helper()
	policy := DefaultPlacementPolicy()
	edit(&policy)
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "placement.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	f.hub.mu.Lock()
	f.hub.placementPolicyPath = path
	f.hub.mu.Unlock()
}

// request is a distinct submission i (its own job, task and request id).
func executorRequest(i int) fleet.Submission {
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(43880 + i)
	request.Args = append([]string(nil), request.Args...)
	for j, v := range request.Args {
		switch v {
		case "test-job":
			request.Args[j] = fmt.Sprintf("job-%d", i)
		case "42":
			request.Args[j] = fmt.Sprint(1000 + i)
		}
	}
	return request
}

// claim accepts request i, i seconds after the fixture's base time, with the
// queue rank of an hk priority (0: no task priority).
func (f *executorFixture) claim(t *testing.T, i, priority int) hubSpawnTask {
	t.Helper()
	request := executorRequest(i)
	task, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, fmt.Sprintf("job-%d", i), "parent", f.base.Add(time.Duration(i)*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.hub.inbox.store.setSpawnRank(t.Context(), request.RequestID, spawnPriorityRank(priority, priority > 0)); err != nil {
		t.Fatal(err)
	}
	return task
}

// bound accepts request i and binds it to machine, as placement does.
func (f *executorFixture) bound(t *testing.T, i, priority int, machine string) hubSpawnTask {
	t.Helper()
	task := f.claim(t, i, priority)
	job := fmt.Sprintf("job-%d", i)
	node := hubSpawnRequest{RequestID: t14RequestID(53880 + i), Machine: machine, CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", job, "-l", job, "-m", "codex-sol", "--role", "worker"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	task, err := f.hub.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, node)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// next waits for machine's next job.spawn frame.
func (f *executorFixture) next(t *testing.T, machine string) hubOutboundMessage {
	t.Helper()
	select {
	case message := <-f.frames[machine]:
		return message
	case <-time.After(3 * time.Second):
		t.Fatalf("no job.spawn reached %s", machine)
		return hubOutboundMessage{}
	}
}

// none proves machine gets no further frame within window.
func (f *executorFixture) none(t *testing.T, machine string, window time.Duration) {
	t.Helper()
	select {
	case message := <-f.frames[machine]:
		t.Fatalf("%s got %s beyond its spawn_in_flight", machine, message.RequestID)
	case <-time.After(window):
	}
}

// refuse answers a frame with a refusal that settles the request.
func (f *executorFixture) refuse(t *testing.T, machine, requestID string) {
	t.Helper()
	if err := wsjson.Write(t.Context(), f.nodes[machine], nodeRefusal(requestID, "brief_doc_unavailable", false)); err != nil {
		t.Fatal(err)
	}
}

func (f *executorFixture) start(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	f.hub.startSpawnCoordinator(ctx)
}

// (a) A node that does not answer holds only its own queue: the other node's
// requests, newer than all of the slow node's, still land.
func TestSlowNodeHoldsOnlyItsOwnQueue_t4388(t *testing.T) {
	f := newExecutorFixture(t, "slow", "fast")
	for i := range 10 {
		f.bound(t, i, 0, "slow")
	}
	f.bound(t, 10, 0, "fast")
	f.bound(t, 11, 0, "fast")
	f.start(t)
	for range 2 {
		f.next(t, "fast")
	}
	for range defaultSpawnInFlight {
		f.next(t, "slow")
	}
	// Window: proves the slow node gets nothing beyond its own limit.
	f.none(t, "slow", 300*time.Millisecond)
}

// (b) 10 requests over 4 nodes start at once, each node bounded by N: the
// node with 4 starts 3 and queues 1.
func TestBurstStartsEveryNodeUpToItsLimit_t4388(t *testing.T) {
	machines := []string{"n1", "n2", "n3", "n4"}
	f := newExecutorFixture(t, machines...)
	for i := range 4 {
		f.bound(t, i, 0, "n1")
	}
	for i := 4; i < 10; i++ {
		f.bound(t, i, 0, machines[1+(i-4)%3])
	}
	f.start(t)
	var first []hubOutboundMessage
	for range defaultSpawnInFlight {
		first = append(first, f.next(t, "n1"))
	}
	for _, machine := range machines[1:] {
		f.next(t, machine)
		f.next(t, machine)
	}
	// Window: the fourth n1 request must wait for a slot.
	f.none(t, "n1", 300*time.Millisecond)
	f.refuse(t, "n1", first[0].RequestID)
	f.next(t, "n1")
}

// (c) On a node with one slot, priority-1 work queued after priority-3 work
// starts first.
func TestQueueRunsPriorityBeforeAge_t4388(t *testing.T) {
	f := newExecutorFixture(t, "n1")
	f.policy(t, func(p *PlacementPolicy) { p.SpawnInFlightHosts = map[string]int{"n1": 1} })
	running := f.bound(t, 0, 3, "n1")
	f.bound(t, 1, 3, "n1")
	f.start(t)
	if got := f.next(t, "n1"); got.RequestID != running.Node.RequestID {
		t.Fatalf("first frame %s, want the oldest %s", got.RequestID, running.Node.RequestID)
	}
	urgent := f.bound(t, 2, 1, "n1")
	f.hub.kickSpawns()
	eventually(t, 3*time.Second, "the priority-1 request to queue", func() bool {
		return f.hub.spawnQueue.depth("n1") == 3
	})
	f.refuse(t, "n1", running.Node.RequestID)
	if got := f.next(t, "n1"); got.RequestID != urgent.Node.RequestID {
		t.Fatalf("after the running request the node got %s, want the priority-1 %s", got.RequestID, urgent.Node.RequestID)
	}
}

// (d) A host refusal returns the request to placement at once with that
// host+account excluded, and placement sends it to the next host before the
// coordinator's 2 s scan would.
func TestHostRefusalMovesToNextHostAtOnce_t4388(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	f := newExecutorFixture(t, "desktop", "machine-a")
	f.hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Source = bothHostsSource{retrySource{now: time.Unix(1_800_000_000, 0)}}
		return app, nil
	}
	request := autoSpawnRequest()
	if _, _, err := f.hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	f.start(t)
	var refused string
	var sent hubOutboundMessage
	select {
	case sent = <-f.frames["desktop"]:
		refused = "desktop"
	case sent = <-f.frames["machine-a"]:
		refused = "machine-a"
	case <-time.After(5 * time.Second):
		t.Fatal("placement sent the request to no node")
	}
	other := map[string]string{"desktop": "machine-a", "machine-a": "desktop"}[refused]
	answered := time.Now()
	if err := wsjson.Write(t.Context(), f.nodes[refused], nodeRefusal(sent.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	var next hubOutboundMessage
	select {
	case next = <-f.frames[other]:
	case <-time.After(3 * time.Second):
		saved, err := f.hub.inbox.store.spawnTask(t.Context(), request.RequestID)
		t.Fatalf("no job.spawn reached %s: %s %+v %+v %v", other, saved.Status, saved.Outcome, saved.Excluded, err)
	}
	if elapsed := time.Since(answered); elapsed >= 1500*time.Millisecond {
		t.Fatalf("next host waited %v after the refusal", elapsed)
	}
	saved, err := f.hub.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Node == nil || saved.Node.RequestID != next.RequestID || saved.Node.Machine != other {
		t.Fatalf("the request is not bound to the next host: %+v %v", saved, err)
	}
	if len(saved.Excluded) != 1 || saved.Excluded[0].Host != refused || saved.Excluded[0].Profile != hubSpawnArg(sent.Args, "-m") || saved.Excluded[0].Reason != "cwd_unmapped" {
		t.Fatalf("the refused host+account is not persisted as excluded: %+v", saved.Excluded)
	}
}

// bothHostsSource sees the one account on both hosts, so either can run it.
type bothHostsSource struct{ retrySource }

func (s bothHostsSource) Presence(context.Context) ([]fleet.PresenceRecord, error) {
	return []fleet.PresenceRecord{{HostID: "desktop", AccountKey: "p:0000000000000001", LastSeen: s.now}, {HostID: "machine-a", AccountKey: "p:0000000000000001", LastSeen: s.now}}, nil
}

// (e) A request bound and queued on a node counts toward its pool's cap
// before the node runs it.
func TestPoolCapCountsBoundQueuedRequests_t4388(t *testing.T) {
	f := newExecutorFixture(t, "n1")
	f.policy(t, func(p *PlacementPolicy) { p.SpawnInFlightHosts = map[string]int{"n1": 1} })
	f.bound(t, 0, 0, "n1")
	queued := f.bound(t, 1, 0, "n1")
	f.start(t)
	f.next(t, "n1")
	eventually(t, 3*time.Second, "the second request to queue", func() bool {
		return f.hub.spawnQueue.depth("n1") == 2
	})
	counts, err := f.hub.liveSpawnProfiles(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if counts["codex-sol"] != 2 {
		t.Fatalf("pool counts %v, want both the running and the queued request (%s)", counts, queued.Request.RequestID)
	}
}

// A node that disconnects returns its queued, never-sent requests to placement.
func TestNodeDisconnectUnbindsQueuedRequests_t4388(t *testing.T) {
	f := newExecutorFixture(t, "n1")
	task := f.claim(t, 0, 0)
	node := hubSpawnRequest{RequestID: t14RequestID(63880), Machine: "n1", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "job-0", "-m", "codex-sol"}, WaitSeconds: 600}
	task, err := f.hub.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, node)
	if err != nil {
		t.Fatal(err)
	}
	q := &f.hub.spawnQueue
	q.claim(task.Request.RequestID)
	q.mu.Lock()
	q.nodeLocked("n1").inFlight = defaultSpawnInFlight // no slot: it stays queued
	q.nodeLocked("n1").queued = []spawnQueueItem{{task: task, fresh: true}}
	q.mu.Unlock()
	f.hub.unbindQueuedSpawns("n1")
	saved, err := f.hub.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil || len(saved.Excluded) != 0 {
		t.Fatalf("queued request was not returned to placement: %+v %v", saved, err)
	}
	if !q.claim(task.Request.RequestID) {
		t.Fatal("the unbound request is still held by the executor")
	}
}

// An operator "off" override taken while a fresh request waits in its node's
// queue returns it to placement instead of sending it (review r4560281308).
func TestQueuedSpawnHonorsAcceptingOff_t4388(t *testing.T) {
	f := newExecutorFixture(t, "n1")
	task := f.bound(t, 0, 0, "n1")
	f.hub.mu.Lock()
	f.hub.relayControl.acceptingOverride = map[string]acceptingOverride{"n1": {Mode: "off"}}
	f.hub.mu.Unlock()
	if !f.hub.spawnQueue.claim(task.Request.RequestID) {
		t.Fatal("claim")
	}
	f.hub.enqueueBoundSpawn(t.Context(), task, true)
	eventually(t, 3*time.Second, "request unbound", func() bool {
		current, err := f.hub.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
		return err == nil && current.Node == nil
	})
	f.none(t, "n1", 200*time.Millisecond)
}

// A request an abandon has fenced is never sent, even when a slot on its
// node opens (review r4560281313).
func TestFencedSpawnIsNeverSent_t4388(t *testing.T) {
	f := newExecutorFixture(t, "n1")
	f.policy(t, func(p *PlacementPolicy) { p.SpawnInFlight = 1 })
	first := f.bound(t, 0, 0, "n1")
	second := f.bound(t, 1, 0, "n1")
	f.start(t)
	frame := f.next(t, "n1")
	if frame.RequestID != first.Node.RequestID {
		t.Fatalf("first frame %s", frame.RequestID)
	}
	if !f.hub.spawnQueue.fence(second.Request.RequestID) {
		t.Fatal("fence refused a queued request")
	}
	if f.hub.spawnQueue.beginSend(second.Request.RequestID) {
		t.Fatal("a fenced request may be sent")
	}
	f.refuse(t, "n1", frame.RequestID)
	// Window: proves the open slot does not send the fenced request.
	f.none(t, "n1", 300*time.Millisecond)
}

// Rows already owned by a worker cannot hide later pending rows from the
// coordinator's scan (review r4560282735).
func TestPendingScanReachesEveryRow_t4388(t *testing.T) {
	f := newExecutorFixture(t)
	for i := range 300 {
		f.claim(t, i, 0)
	}
	pending, err := f.hub.inbox.store.pendingSpawns(t.Context(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 300 {
		t.Fatalf("scan returned %d of 300 pending rows", len(pending))
	}
}

// Queue load changes the decision, not only the scores: a winner with a deep
// queue gives way to an idle host (review r4559870175).
func TestQueueDepthMovesPlacementDecision_t4388(t *testing.T) {
	policy := DefaultPlacementPolicy()
	policy.LocalMachine = "n1"
	host := func(machine string, score float64) PlacementCandidate {
		return PlacementCandidate{Machine: machine, Score: score, Connected: true, MetricsKnown: true, MemoryKnown: true, accepting: true}
	}
	result := PlacementResult{Decision: "n1", Source: "hub-only", Candidates: []PlacementCandidate{host("n1", 100), host("n2", 95)}}
	idle := chargeSpawnQueue(policy, result, func(string) int { return 0 })
	if idle.Decision != "n1" {
		t.Fatalf("idle decision %q, want n1", idle.Decision)
	}
	for _, depth := range []int{3, policy.MaxActiveJobs} {
		busy := chargeSpawnQueue(policy, result, func(machine string) int {
			if machine == "n1" {
				return depth
			}
			return 0
		})
		if busy.Decision != "n2" {
			t.Fatalf("decision stayed on %q with %d spawns on n1: %+v", busy.Decision, depth, busy)
		}
	}
}
