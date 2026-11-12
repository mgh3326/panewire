package panewire

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// drainClient is a node whose spawn engine blocks until release is closed.
func drainClient(t *testing.T, started chan<- struct{}, release <-chan struct{}) *HubClient {
	t.Helper()
	return &HubClient{
		spawnConfigPath: t14SpawnConfig(t, t.TempDir()), spawnJournalDir: t.TempDir(), fleetEvents: (&fleetEventRecorder{}).write,
		spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
			started <- struct{}{}
			<-release
			return t14EngineOK()
		},
	}
}

// A stop during an in-flight spawn (launch, brief, result upload) waits for it:
// the result reaches the hub and the journal row is completed before Drain
// returns.
func TestStopDrainCompletesInFlightSpawnUpload(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	client := drainClient(t, started, release)
	peer, next := settlePeer(t)
	message := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(7101), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"--job", "job-a"}, WaitSeconds: 60}
	go client.handleHubSpawn(t.Context(), peer, message)
	<-started

	drained := make(chan int, 1)
	go func() { drained <- client.Drain(t.Context()) }()
	select {
	case left := <-drained:
		t.Fatalf("Drain returned with %d operations in flight", left)
	case <-time.After(50 * time.Millisecond): // proves nothing more happens while the spawn runs
	}
	close(release)
	if left := <-drained; left != 0 {
		t.Fatalf("Drain left %d operations", left)
	}
	if got := next(); got.RC != 0 || got.RequestID != message.RequestID {
		t.Fatalf("result uploaded after drain: %+v", got)
	}
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	defer client.releaseSpawnJournal()
	if stored, err := journal.resultOf(t.Context(), message.RequestID); err != nil || stored == nil || stored.RC != 0 {
		t.Fatalf("journal %+v %v", stored, err)
	}
}

// Once the stop has begun, a new spawn is not started: it is left for the hub
// to replay to the restarted node, and no journal row is claimed for it.
func TestStopDrainRefusesNewSpawn(t *testing.T) {
	started := make(chan struct{}, 1)
	var engineRuns atomic.Int32
	client := drainClient(t, started, closedChannel())
	client.spawnEngine = func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
		engineRuns.Add(1)
		return t14EngineOK()
	}
	peer, _ := settlePeer(t)
	if left := client.Drain(t.Context()); left != 0 {
		t.Fatalf("idle drain left %d", left)
	}
	message := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(7102), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"--job", "job-a"}, WaitSeconds: 60}
	client.handleHubSpawn(t.Context(), peer, message)
	journal, err := client.acquireSpawnJournal()
	if err != nil {
		t.Fatal(err)
	}
	defer client.releaseSpawnJournal()
	if rows, _ := journal.executing(t.Context()); len(rows) != 0 || engineRuns.Load() != 0 {
		t.Fatalf("a spawn started during the stop: rows=%v engine runs=%d", rows, engineRuns.Load())
	}
}

// The wait is bounded: work that does not finish is left to the settle pass
// after the restart.
func TestStopDrainIsBounded(t *testing.T) {
	started, release := make(chan struct{}, 1), make(chan struct{})
	defer close(release)
	client := drainClient(t, started, release)
	client.stopDrain = 100 * time.Millisecond
	peer, _ := settlePeer(t)
	message := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(7103), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"--job", "job-a"}, WaitSeconds: 60}
	go client.handleHubSpawn(t.Context(), peer, message)
	<-started
	begun := time.Now()
	if left := client.Drain(t.Context()); left != 1 {
		t.Fatalf("Drain left %d, want 1", left)
	}
	if took := time.Since(begun); took < 100*time.Millisecond || took > 5*time.Second {
		t.Fatalf("Drain took %s for a 100ms bound", took)
	}
}

// A relay injection and a node command count as work too, and an idle node
// stops at once.
func TestStopDrainWaitsForInjectionAndNodeCommand(t *testing.T) {
	client := &HubClient{}
	if begun := time.Now(); client.Drain(t.Context()) != 0 || time.Since(begun) > time.Second {
		t.Fatal("idle node did not stop at once")
	}
	client = &HubClient{}
	end, ok := client.beginWork(false)
	if !ok {
		t.Fatal("work refused")
	}
	done := make(chan int, 1)
	go func() { done <- client.Drain(t.Context()) }()
	select {
	case <-done:
		t.Fatal("Drain did not wait for the operation")
	case <-time.After(30 * time.Millisecond): // proves nothing more happens while it runs
	}
	end()
	if left := <-done; left != 0 {
		t.Fatalf("left %d", left)
	}
}

func closedChannel() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

// Daemon.Stop drains before it cancels the node's loops: work in flight keeps
// a live context until it ends.
func TestDaemonStopDrainsBeforeCancelling(t *testing.T) {
	client := &HubClient{}
	var cancelled atomic.Bool
	d := &Daemon{cfg: Config{Hub: HubDaemonConfig{Client: client}}, cancel: func() { cancelled.Store(true) }}
	end, _ := client.beginWork(false)
	stopped := make(chan error, 1)
	go func() { stopped <- d.Stop() }()
	time.Sleep(50 * time.Millisecond) // proves the loops are not cancelled while work is in flight
	if cancelled.Load() {
		t.Fatal("Stop cancelled the node while work was in flight")
	}
	end()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if !cancelled.Load() {
		t.Fatal("Stop did not cancel the node after the drain")
	}
}
