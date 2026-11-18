package panewire

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/panewire/internal/fleet"
)

// supersedeFixture is a hub with a parent job on machine-a (real websocket
// node) and the parent owner lane routed to machine-b (destination agent for
// info events). The hk job store is a quotaJobs fake so superseded writes are
// observable.
type supersedeFixture struct {
	hub         *HubServer
	server      *httptest.Server
	node        *websocket.Conn
	destination *hubAgent
	jobs        *quotaJobs
	logs        *pw2LockedBuffer
	parent      string
	successor   string
}

func newSupersedeFixture(t *testing.T) *supersedeFixture {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	_, relayClient, stop := newFakeHandoffkeep(t)
	t.Cleanup(stop)
	var logs pw2LockedBuffer
	hub, err := NewHubServer(HubServerConfig{
		Tokens:          map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"},
		InboxStorePath:  filepath.Join(t.TempDir(), "inbox.sqlite3"),
		ReportRelayPath: r20LanesFile(t, `{"lanes":{"parent":{"machine":"machine-b","pane":"w1:p1"}}}`),
		Logger:          slog.New(slog.NewTextHandler(&logs, nil)),
		handoffkeep:     relayClient,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hub.Close() })
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8), persisted: make(chan hubRelayPersistedEvent, 8)}
	hub.mu.Lock()
	hub.nodes["machine-b"] = &hubNodeRecord{agent: destination}
	hub.mu.Unlock()
	jobs := &quotaJobs{records: map[string]map[string]any{}}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: jobs, Out: out, Err: errout}, nil
	}
	server := httptest.NewServer(hub.Handler())
	t.Cleanup(server.Close)
	node := t14NamedNodeToken(t, hub, server, "machine-a", "node-token", false, true)
	return &supersedeFixture{hub: hub, server: server, node: node, destination: destination, jobs: jobs, logs: &logs, parent: "parent-job", successor: "successor-job"}
}

func (f *supersedeFixture) seedParent(t *testing.T, state string) {
	t.Helper()
	f.hub.jobs[f.parent] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: f.parent, OwnerLane: "parent", Pane: "w1:p1"}, Node: "machine-a", LastSeen: time.Now().UTC()}
	f.jobs.mu.Lock()
	f.jobs.records[f.parent] = map[string]any{"state": state}
	f.jobs.mu.Unlock()
}

// serveReap answers one job.reap on the parent node and returns what the hub
// asked for.
func serveReap(t *testing.T, f *supersedeFixture, outcome, pane string) map[string]json.RawMessage {
	t.Helper()
	command := nodeCommandRead(t, f.node)
	if string(command["op"]) != `"job.reap"` {
		t.Fatalf("node received %v, want job.reap", command)
	}
	nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": outcome, "pane": pane}})
	return command
}

func reapForceOf(t *testing.T, command map[string]json.RawMessage) bool {
	t.Helper()
	var args nodeJobReapArgs
	if err := json.Unmarshal(command["args"], &args); err != nil {
		t.Fatal(err)
	}
	return args.Force
}

func tryNodeCommand(conn *websocket.Conn) (map[string]json.RawMessage, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for {
		_, payload, err := conn.Read(ctx)
		if err != nil {
			return nil, false
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(payload, &fields) != nil {
			continue
		}
		if string(fields["type"]) == `"node.command"` {
			return fields, true
		}
	}
}

func supersededOf(t *testing.T, f *supersedeFixture) (string, bool) {
	t.Helper()
	f.jobs.mu.Lock()
	defer f.jobs.mu.Unlock()
	rec, ok := f.jobs.records[f.parent]
	if !ok {
		return "", false
	}
	v, _ := rec["superseded"].(string)
	return v, v != ""
}

func supersedeInfos(destination *hubAgent) []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case relay := <-destination.relays:
			if relay.Kind == "lane.event" && strings.Contains(relay.Text, "superseded by") {
				out = append(out, relay)
			}
		default:
			return out
		}
	}
}

// TestSupersedeClosesIdleParentOnLanding is acceptance: an idle parent is
// closed when the successor lands, its worktree survives, and the owner hears
// one info.
func TestSupersedeClosesIdleParentOnLanding(t *testing.T) {
	f := newSupersedeFixture(t)
	f.seedParent(t, "spawned")
	worktree := filepath.Join(t.TempDir(), "worktree")
	if err := os.MkdirAll(worktree, 0755); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { f.hub.supersedeParent(f.parent, f.successor, "parent", "machine-a"); close(done) }()
	command := serveReap(t, f, nodeJobReapClosed, "w1:p1")
	if reapForceOf(t, command) {
		t.Fatalf("idle close must not use force: %v", command["args"])
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supersede did not finish")
	}
	// No extra node work: only the one reap.
	if extra, ok := tryNodeCommand(f.node); ok {
		t.Fatalf("extra node command after close: %v", extra)
	}
	if got, ok := supersededOf(t, f); !ok || got != f.successor {
		t.Fatalf("job.superseded=%q ok=%v, want %q", got, ok, f.successor)
	}
	infos := supersedeInfos(f.destination)
	if len(infos) != 1 || !strings.Contains(infos[0].Text, "[panewire] closed parent-job on machine-a: superseded by successor-job") {
		t.Fatalf("info events=%+v", infos)
	}
	if _, err := os.Stat(worktree); err != nil {
		t.Fatalf("the close must never remove the worktree: %v", err)
	}
	f.jobs.mu.Lock()
	state := f.jobs.records[f.parent]["state"]
	f.jobs.mu.Unlock()
	if state != "spawned" {
		t.Fatalf("the close must not end the hk record (state=%v): the successor carries the task", state)
	}
	if _, deferred := f.hub.supersededParents[f.parent]; deferred {
		t.Fatalf("a closed parent must not stay deferred")
	}
}

// TestSupersedeDefersWorkingParentUntilSweep: a working parent is recorded at
// landing and closed at the next sweep after it stops. Force is never used on
// a working pane.
func TestSupersedeDefersWorkingParentUntilSweep(t *testing.T) {
	f := newSupersedeFixture(t)
	f.seedParent(t, "spawned")
	done := make(chan struct{})
	go func() { f.hub.supersedeParent(f.parent, f.successor, "parent", "machine-a"); close(done) }()
	command := serveReap(t, f, nodeJobReapJobWorking, "w1:p1")
	if reapForceOf(t, command) {
		t.Fatalf("working probe must not use force")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supersede did not finish")
	}
	// Recorded but not closed: no info yet, still deferred.
	if got, ok := supersededOf(t, f); !ok || got != f.successor {
		t.Fatalf("working parent must be recorded superseded: %q %v", got, ok)
	}
	if infos := supersedeInfos(f.destination); len(infos) != 0 {
		t.Fatalf("no info before the close: %+v", infos)
	}
	f.hub.mu.Lock()
	_, deferred := f.hub.supersededParents[f.parent]
	f.hub.mu.Unlock()
	if !deferred {
		t.Fatal("a working parent must wait for the sweep")
	}
	// The sweep closes it once idle.
	sweepDone := make(chan struct{})
	go func() { f.hub.sweepSupersededParents(); close(sweepDone) }()
	command = serveReap(t, f, nodeJobReapClosed, "w1:p1")
	if reapForceOf(t, command) {
		t.Fatalf("idle sweep close must not use force")
	}
	select {
	case <-sweepDone:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not finish")
	}
	infos := supersedeInfos(f.destination)
	if len(infos) != 1 {
		t.Fatalf("sweep close owes one info: %+v", infos)
	}
}

// TestSupersedeBlockedParentClosesWithForce: a pane stopped at a prompt closes
// at landing, proven blocked first and only then forced.
func TestSupersedeBlockedParentClosesWithForce(t *testing.T) {
	f := newSupersedeFixture(t)
	f.seedParent(t, "spawned")
	done := make(chan struct{})
	go func() { f.hub.supersedeParent(f.parent, f.successor, "parent", "machine-a"); close(done) }()
	first := serveReap(t, f, nodeJobReapPaneBlocked, "w1:p1")
	if reapForceOf(t, first) {
		t.Fatalf("blocked must be proven without force first")
	}
	second := serveReap(t, f, nodeJobReapClosed, "w1:p1")
	if !reapForceOf(t, second) {
		t.Fatalf("blocked close uses force after the proof: %v", second["args"])
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supersede did not finish")
	}
	if infos := supersedeInfos(f.destination); len(infos) != 1 {
		t.Fatalf("blocked close owes one info: %+v", infos)
	}
}

// TestSupersedeNeverClosesKeepParent: --keep is never closed and never recorded.
func TestSupersedeNeverClosesKeepParent(t *testing.T) {
	f := newSupersedeFixture(t)
	f.seedParent(t, "spawned")
	done := make(chan struct{})
	go func() { f.hub.supersedeParent(f.parent, f.successor, "parent", "machine-a"); close(done) }()
	command := serveReap(t, f, nodeJobReapProtected, "w1:p1")
	if reapForceOf(t, command) {
		t.Fatalf("keep probe must not use force")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("supersede did not finish")
	}
	if extra, ok := tryNodeCommand(f.node); ok {
		t.Fatalf("a keep parent must see no second command: %v", extra)
	}
	if _, ok := supersededOf(t, f); ok {
		t.Fatal("a keep parent must never be recorded superseded")
	}
	if infos := supersedeInfos(f.destination); len(infos) != 0 {
		t.Fatalf("a keep parent sends no info: %+v", infos)
	}
}

// TestSupersedeMissingParentIsNoop: an unknown or ended parent records nothing
// and sends nothing.
func TestSupersedeMissingParentIsNoop(t *testing.T) {
	f := newSupersedeFixture(t)
	f.hub.supersedeParent("never-existed", f.successor, "parent", "machine-a")
	if extra, ok := tryNodeCommand(f.node); ok {
		t.Fatalf("unknown parent must not reach the node: %v", extra)
	}
	f.seedParent(t, "spawned")
	f.hub.jobs[f.parent].Completed = true
	f.hub.supersedeParent(f.parent, f.successor, "parent", "machine-a")
	if extra, ok := tryNodeCommand(f.node); ok {
		t.Fatalf("ended parent must not reach the node: %v", extra)
	}
	if _, ok := supersededOf(t, f); ok {
		t.Fatal("an ended parent records nothing")
	}
}

// TestSupersedeCloseFailureKeepsSpawnAndWarnsOnce: the successor lands even
// when the close cannot, and the failure is one warn.
func TestSupersedeCloseFailureKeepsSpawnAndWarnsOnce(t *testing.T) {
	f := newSupersedeFixture(t)
	f.seedParent(t, "spawned")
	// Parent on a machine with no connection: the reap cannot be sent.
	f.hub.mu.Lock()
	f.hub.jobs[f.parent].Node = "machine-offline"
	f.hub.mu.Unlock()
	request := durableSpawnRequest()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", f.successor, "--job", f.successor, "--t", "T1", "--parent-job", f.parent, "--owner", "parent", "--role", "implementer", "--host", "auto", "--report-relative", f.successor + "/report.md"}
	task, _, err := f.hub.inbox.store.claimSpawn(context.Background(), request, f.successor, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	changed, err := f.hub.completeSpawn(context.Background(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p9"})
	if err != nil || !changed {
		t.Fatalf("successor spawn must land: changed=%v err=%v", changed, err)
	}
	eventually(t, 3*time.Second, "the failed close to warn once", func() bool {
		return strings.Count(f.logs.String(), "successor close failed") == 1
	})
	saved, err := f.hub.inbox.store.spawnTask(context.Background(), request.RequestID)
	if err != nil || saved.Status != "succeeded" {
		t.Fatalf("failed close must not touch the successor: %+v %v", saved, err)
	}
}

// TestSupersedeLandingHookClosesIdleParent: the succeeded spawn operation
// itself drives the close, end to end.
func TestSupersedeLandingHookClosesIdleParent(t *testing.T) {
	f := newSupersedeFixture(t)
	f.seedParent(t, "spawned")
	request := durableSpawnRequest()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", f.successor, "--job", f.successor, "--t", "T1", "--parent-job", f.parent, "--owner", "parent", "--role", "implementer", "--host", "auto", "--report-relative", f.successor + "/report.md"}
	task, _, err := f.hub.inbox.store.claimSpawn(context.Background(), request, f.successor, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan map[string]json.RawMessage, 1)
	go func() {
		command := nodeCommandRead(t, f.node)
		nodeCommandReplyTo(t, f.node, command, map[string]any{"result": map[string]any{"outcome": "closed", "pane": "w1:p1"}})
		served <- command
	}()
	changed, err := f.hub.completeSpawn(context.Background(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", Pane: "w9:p9"})
	if err != nil || !changed {
		t.Fatalf("successor spawn must land: changed=%v err=%v", changed, err)
	}
	select {
	case command := <-served:
		if reapForceOf(t, command) {
			t.Fatalf("landing close must not use force")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("landing did not close the parent")
	}
	eventually(t, 3*time.Second, "the landing close to record and notify", func() bool {
		got, ok := supersededOf(t, f)
		return ok && got == f.successor && len(supersedeInfos(f.destination)) == 1
	})
}
