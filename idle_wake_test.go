package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type idleWakeTestRig struct {
	t        *testing.T
	store    *Store
	manager  *idleWakeManager
	now      time.Time
	requests []idleWakeRouteRequest
	emitted  []hubScannedRelayEvent
}

func newIdleWakeTestRig(t *testing.T, settle time.Duration) *idleWakeTestRig {
	t.Helper()
	rig := &idleWakeTestRig{t: t, store: NewMemoryStore(t), now: time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)}
	manager, err := newIdleWakeManager(rig.store, t.TempDir(), settle, func(request idleWakeRouteRequest) bool {
		rig.requests = append(rig.requests, request)
		return true
	}, func(event hubScannedRelayEvent) bool {
		rig.emitted = append(rig.emitted, event)
		return true
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	manager.now = func() time.Time { return rig.now }
	rig.manager = manager
	t.Cleanup(func() { _ = rig.store.Close() })
	return rig
}

func (rig *idleWakeTestRig) observe(status string, revision int64, at time.Time) {
	rig.t.Helper()
	if err := rig.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: status, Revision: revision}, at); err != nil {
		rig.t.Fatal(err)
	}
}

func (rig *idleWakeTestRig) route(index int, lane string) {
	rig.t.Helper()
	request := rig.requests[index]
	rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Eligible: true, Lane: lane, Text: idleWakeRouteText(request, idleWakeOwnerResolution{Lane: lane})})
}

func countIdleWakeFiles(t *testing.T, root string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "events-lane"))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestIdleWakeSchemaGuardHelperProcess(t *testing.T) {
	if len(os.Args) < 3 || os.Args[len(os.Args)-2] != "idle-wake-schema-helper" {
		return
	}
	counter := os.Args[len(os.Args)-1]
	file, err := os.OpenFile(counter, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(2)
	}
	if _, err = file.WriteString("probe\n"); err != nil || file.Close() != nil {
		os.Exit(2)
	}
	_, _ = os.Stdout.WriteString(`{}`)
	os.Exit(0)
}

func TestIdleWakeUnavailableEventsCapabilityUsesBoundedProbeBackoff(t *testing.T) {
	socketRoot, err := os.MkdirTemp("/tmp", "pw-iw-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	listener, err := net.Listen("unix", filepath.Join(socketRoot, "herdr.sock"))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			_ = connection.Close()
		}
	}()

	store := NewMemoryStore(t)
	defer store.Close()
	counter := filepath.Join(t.TempDir(), "schema-probes")
	command := []string{os.Args[0], "-test.run=^TestIdleWakeSchemaGuardHelperProcess$", "--", "idle-wake-schema-helper", counter}
	daemon := NewDaemon(Config{Store: store, HerdrSocket: listener.Addr().String(), SchemaCommand: command, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	// A non-nil manager is the condition that keeps the observation loop alive
	// while the startup schema guard says events are unavailable.
	daemon.idleWake = &idleWakeManager{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		daemon.eventLoop(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	probeCount := func() int {
		contents, readErr := os.ReadFile(counter)
		if os.IsNotExist(readErr) {
			return 0
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		return len(strings.Fields(string(contents)))
	}
	time.Sleep(350 * time.Millisecond)
	if got := probeCount(); got != 0 {
		t.Fatalf("capability probes before initial backoff=%d", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for probeCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := probeCount(); got != 1 {
		t.Fatalf("capability probes after initial backoff=%d", got)
	}
	time.Sleep(350 * time.Millisecond)
	if got := probeCount(); got != 1 {
		t.Fatalf("capability probe repeated on socket retry cadence: %d", got)
	}
	backoff := daemonCapabilityRetryInitial
	for _, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		backoff = nextDaemonCapabilityBackoff(backoff)
		if backoff != want {
			t.Fatalf("capability backoff=%s, want %s", backoff, want)
		}
	}
}

func TestIdleWakeWorkingToIdleOrDoneSettlesForSixtySeconds(t *testing.T) {
	for _, target := range []string{"idle", "done"} {
		t.Run(target, func(t *testing.T) {
			rig := newIdleWakeTestRig(t, defaultIdleWakeSettle)
			transition := rig.now.Add(time.Second)
			rig.observe("working", 10, rig.now)
			rig.observe(target, 11, transition)
			rig.manager.Tick(context.Background(), transition.Add(defaultIdleWakeSettle-time.Millisecond))
			if len(rig.requests) != 0 {
				t.Fatalf("route requests before settle=%d", len(rig.requests))
			}
			rig.now = transition.Add(defaultIdleWakeSettle)
			rig.manager.Tick(context.Background(), rig.now)
			if len(rig.requests) != 1 || rig.requests[0].State != target || rig.requests[0].StateChangeSeq != 2 {
				t.Fatalf("settled request=%+v", rig.requests)
			}
			rig.route(0, "owner-lane")
			if len(rig.emitted) != 1 || rig.emitted[0].OwnerLane != "owner-lane" || rig.emitted[0].EventID != rig.requests[0].EventID {
				t.Fatalf("emitted=%+v", rig.emitted)
			}
		})
	}
}

func TestIdleWakeFlapCancelsPendingAndLaterSequenceWakesOnce(t *testing.T) {
	rig := newIdleWakeTestRig(t, defaultIdleWakeSettle)
	rig.observe("working", 1, rig.now)
	firstIdle := rig.now.Add(time.Second)
	rig.observe("idle", 2, firstIdle)
	rig.observe("working", 3, firstIdle.Add(30*time.Second))
	rig.manager.Tick(context.Background(), firstIdle.Add(2*defaultIdleWakeSettle))
	if len(rig.requests) != 0 {
		t.Fatalf("flapped transition requested route: %+v", rig.requests)
	}
	secondIdle := firstIdle.Add(2*defaultIdleWakeSettle + time.Second)
	rig.observe("idle", 4, secondIdle)
	rig.now = secondIdle.Add(defaultIdleWakeSettle)
	rig.manager.Tick(context.Background(), rig.now)
	if len(rig.requests) != 1 || rig.requests[0].StateChangeSeq != 4 {
		t.Fatalf("stable second transition=%+v", rig.requests)
	}
	rig.route(0, "owner-lane")
	rig.manager.Tick(context.Background(), rig.now.Add(time.Hour))
	if len(rig.emitted) != 1 {
		t.Fatalf("same sequence emitted %d times", len(rig.emitted))
	}
}

func TestIdleWakeSameSequenceIsStableAndNewSequenceAddsOne(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	rig.observe("working", 10, rig.now)
	rig.observe("idle", 11, rig.now.Add(time.Second))
	rig.now = rig.now.Add(2 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	rig.route(0, "owner-lane")
	firstID := rig.emitted[0].EventID

	// Duplicate event and snapshot observations do not create a new local
	// state-change sequence, even when upstream revision advances.
	rig.observe("idle", 11, rig.now.Add(time.Second))
	rig.observe("idle", 12, rig.now.Add(2*time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(time.Hour))
	if len(rig.emitted) != 1 {
		t.Fatalf("same state/sequence emitted %d events", len(rig.emitted))
	}

	rig.observe("working", 13, rig.now.Add(2*time.Hour))
	secondIdle := rig.now.Add(2*time.Hour + time.Second)
	rig.observe("idle", 14, secondIdle)
	rig.now = secondIdle.Add(time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	if len(rig.requests) != 2 || rig.requests[1].EventID == firstID || rig.requests[1].StateChangeSeq != 4 {
		t.Fatalf("new sequence requests=%+v", rig.requests)
	}
	rig.route(1, "owner-lane")
	if len(rig.emitted) != 2 {
		t.Fatalf("new sequence emitted total=%d", len(rig.emitted))
	}
}

func TestIdleWakeConflictingSameUpstreamRevisionFailsClosed(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	rig.observe("working", 10, rig.now)
	// A conflicting duplicate revision cannot prove a new state change.
	rig.observe("idle", 10, rig.now.Add(time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(time.Minute))
	if len(rig.requests) != 0 {
		t.Fatalf("same upstream revision invented a wake: %+v", rig.requests)
	}
	rig.observe("idle", 11, rig.now.Add(time.Minute+time.Second))
	rig.manager.Tick(context.Background(), rig.now.Add(time.Minute+2*time.Second))
	if len(rig.requests) != 1 || rig.requests[0].StateChangeSeq != 2 {
		t.Fatalf("next genuine revision did not wake once: %+v", rig.requests)
	}
}

func TestIdleWakeInvalidOptionalMetadataIsDroppedNotExecuted(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	working := HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "bad\nworkspace", Label: "bad\x00label", Status: "working", Revision: 1}
	if err := rig.manager.Observe(context.Background(), working, rig.now); err != nil {
		t.Fatal(err)
	}
	idle := working
	idle.Status, idle.Revision = "idle", 2
	if err := rig.manager.Observe(context.Background(), idle, rig.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(2*time.Second))
	if len(rig.requests) != 1 || rig.requests[0].WorkspaceID != "" || rig.requests[0].Label != "" {
		t.Fatalf("unsafe optional metadata crossed protocol: %+v", rig.requests)
	}
}

func TestIdleWakeNodeRestartRecoversSettledCandidateAndMaterializedEvent(t *testing.T) {
	dir := t.TempDir()
	dbPath, inbox := filepath.Join(dir, "node.sqlite3"), filepath.Join(dir, "inbox")
	now := time.Date(2026, 9, 9, 1, 0, 0, 0, time.UTC)
	store, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	var firstRequests []idleWakeRouteRequest
	first, err := newIdleWakeManager(store, inbox, time.Second, func(request idleWakeRouteRequest) bool {
		firstRequests = append(firstRequests, request)
		return true
	}, func(hubScannedRelayEvent) bool { return true }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	first.now = func() time.Time { return now }
	if err := first.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-r", Status: "working", Revision: 20}, now); err != nil {
		t.Fatal(err)
	}
	if err := first.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-r", Status: "idle", Revision: 21}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	first.Tick(context.Background(), now.Add(2*time.Second))
	if len(firstRequests) != 1 {
		t.Fatalf("first settled requests=%d", len(firstRequests))
	}
	namespace, err := store.idleWakeNamespace(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var replayRequests []idleWakeRouteRequest
	var replayEvents []hubScannedRelayEvent
	restarted, err := newIdleWakeManager(reopened, inbox, time.Second, func(request idleWakeRouteRequest) bool {
		replayRequests = append(replayRequests, request)
		return true
	}, func(event hubScannedRelayEvent) bool {
		replayEvents = append(replayEvents, event)
		return true
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	restarted.now = func() time.Time { return now.Add(time.Minute) }
	restarted.RetrySettled(context.Background(), now.Add(time.Minute))
	if len(replayRequests) != 1 || replayRequests[0].EventID != firstRequests[0].EventID {
		t.Fatalf("restart route requests=%+v first=%+v", replayRequests, firstRequests)
	}
	if got, err := reopened.idleWakeNamespace(context.Background()); err != nil || got != namespace {
		t.Fatalf("sequence namespace after restart=%q want=%q err=%v", got, namespace, err)
	}
	restarted.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: replayRequests[0].EventID, Pane: replayRequests[0].Pane, Eligible: true, Lane: "owner-lane", Text: idleWakeRouteText(replayRequests[0], idleWakeOwnerResolution{Lane: "owner-lane"})})
	if len(replayEvents) != 1 || countIdleWakeFiles(t, inbox) != 1 {
		t.Fatalf("restart events=%d files=%d", len(replayEvents), countIdleWakeFiles(t, inbox))
	}
	// Reapplying a delayed duplicate route response cannot create another file.
	restarted.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: replayRequests[0].EventID, Pane: replayRequests[0].Pane, Eligible: true, Lane: "other-lane", Text: "changed"})
	if len(replayEvents) != 1 || countIdleWakeFiles(t, inbox) != 1 {
		t.Fatalf("duplicate route changed durable event: events=%d files=%d", len(replayEvents), countIdleWakeFiles(t, inbox))
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	finalStore, err := OpenStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer finalStore.Close()
	var finalRequests []idleWakeRouteRequest
	var finalEvents []hubScannedRelayEvent
	finalManager, err := newIdleWakeManager(finalStore, inbox, time.Second, func(request idleWakeRouteRequest) bool {
		finalRequests = append(finalRequests, request)
		return true
	}, func(event hubScannedRelayEvent) bool {
		finalEvents = append(finalEvents, event)
		return true
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	finalManager.RetrySettled(context.Background(), now.Add(2*time.Hour))
	if len(finalRequests) != 0 || len(finalEvents) != 0 || countIdleWakeFiles(t, inbox) != 1 {
		t.Fatalf("post-materialization restart requests=%d events=%d files=%d", len(finalRequests), len(finalEvents), countIdleWakeFiles(t, inbox))
	}
}

func TestIdleWakeRecoveryRetryCannotSettleUnverifiedCandidate(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Minute)
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	afterDue := rig.now.Add(2 * time.Minute)
	rig.manager.RetrySettled(context.Background(), afterDue)
	if len(rig.requests) != 0 {
		t.Fatalf("recovery-only scan advanced settle: %+v", rig.requests)
	}
	// A normal tick follows a current herdr observation and may cross the due
	// boundary using the persisted continuously-non-working state.
	rig.observe("idle", 3, afterDue)
	rig.manager.Tick(context.Background(), afterDue)
	if len(rig.requests) != 1 {
		t.Fatalf("current observation did not release due candidate: %+v", rig.requests)
	}
}

func TestIdleWakePaneMissingFromSnapshotCancelsSettle(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Minute)
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	if err := rig.manager.ObserveSnapshot(context.Background(), nil, rig.now.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(2*time.Minute))
	if len(rig.requests) != 0 {
		t.Fatalf("missing pane completed settle: %+v", rig.requests)
	}
	if err := rig.manager.ObserveSnapshot(context.Background(), []HerdrAgentState{{PaneID: "workspace:pane-1", Status: "working", Revision: 3}}, rig.now.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := rig.manager.ObserveSnapshot(context.Background(), []HerdrAgentState{{PaneID: "workspace:pane-1", Status: "idle", Revision: 4}}, rig.now.Add(3*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(4*time.Minute+time.Second))
	if len(rig.requests) != 1 || rig.requests[0].StateChangeSeq != 5 {
		t.Fatalf("reappeared pane sequence=%+v", rig.requests)
	}
}

func TestIdleWakeHerdrRevisionNamespaceLossFailsClosedWithoutDuplicate(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	rig.observe("working", 40, rig.now)
	rig.observe("idle", 41, rig.now.Add(time.Second))
	rig.now = rig.now.Add(2 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	rig.route(0, "owner-lane")

	// An authoritative snapshot moving backwards identifies a herdr restart.
	// Same-status recovery does not invent a transition or duplicate a wake.
	if err := rig.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker-synthetic", Status: "idle", Revision: 1, Authoritative: true}, rig.now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(time.Minute))
	if len(rig.emitted) != 1 || len(rig.requests) != 1 {
		t.Fatalf("revision reset duplicated settled wake: requests=%d emitted=%d", len(rig.requests), len(rig.emitted))
	}

	// The next observed working->idle change is in the node journal's same
	// namespace but has a genuinely new local state_change_seq.
	if err := rig.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "working", Revision: 2, Authoritative: true}, rig.now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := rig.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "idle", Revision: 3}, rig.now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	rig.now = rig.now.Add(2*time.Minute + 2*time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	if len(rig.requests) != 2 || rig.requests[1].EventID == rig.requests[0].EventID {
		t.Fatalf("post-restart new sequence=%+v", rig.requests)
	}

	// A reset before settle cancels continuity and produces zero route request.
	other := newIdleWakeTestRig(t, time.Minute)
	other.observe("working", 90, other.now)
	other.observe("idle", 91, other.now.Add(time.Second))
	if err := other.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "idle", Revision: 1, Authoritative: true}, other.now.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	other.manager.Tick(context.Background(), other.now.Add(2*time.Minute))
	if len(other.requests) != 0 {
		t.Fatalf("unproven cross-namespace settle requested route: %+v", other.requests)
	}

	// Revision disappearing entirely is also one namespace loss, not a reset
	// on every later zero-revision snapshot. The node-local sequence remains
	// authoritative after that boundary.
	lost := newIdleWakeTestRig(t, time.Second)
	lost.observe("working", 10, lost.now)
	lost.observe("idle", 11, lost.now.Add(time.Second))
	lost.manager.Tick(context.Background(), lost.now.Add(2*time.Second))
	lost.route(0, "owner-lane")
	if err := lost.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "idle", Revision: 0, Authoritative: true}, lost.now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := lost.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "working", Revision: 0, Authoritative: true}, lost.now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := lost.manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "idle", Revision: 0, Authoritative: true}, lost.now.Add(time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	lost.manager.Tick(context.Background(), lost.now.Add(time.Minute+2*time.Second))
	if len(lost.emitted) != 1 || len(lost.requests) != 2 || lost.requests[1].StateChangeSeq != 4 {
		t.Fatalf("lost revision namespace requests=%+v emitted=%d", lost.requests, len(lost.emitted))
	}
	lost.route(1, "owner-lane")
	if len(lost.emitted) != 2 {
		t.Fatalf("post-loss new transition emitted=%d", len(lost.emitted))
	}
}

func TestIdleWakeAssignedRouteIsFirstWriterAndFileFailureRetries(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	rig.observe("working", 1, rig.now)
	rig.observe("done", 2, rig.now.Add(time.Second))
	rig.now = rig.now.Add(2 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	failures := 0
	originalWrite := rig.manager.writeRecord
	rig.manager.writeRecord = func(root string, record emitRecord) (string, error) {
		if failures == 0 {
			failures++
			return "", errors.New("synthetic write failure")
		}
		return originalWrite(root, record)
	}
	request := rig.requests[0]
	first := hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Eligible: true, Lane: "owner-a", Text: idleWakeRouteText(request, idleWakeOwnerResolution{Lane: "owner-a"})}
	rig.manager.ApplyRoute(context.Background(), first)
	if len(rig.emitted) != 0 {
		t.Fatal("failed durable file write still enqueued a lane event")
	}
	rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Eligible: true, Lane: "owner-b", Text: "changed"})
	rig.manager.Tick(context.Background(), rig.now.Add(time.Second))
	if len(rig.emitted) != 1 || rig.emitted[0].OwnerLane != "owner-a" {
		t.Fatalf("retry did not retain first route: %+v", rig.emitted)
	}
}

func TestIdleWakeInternalFileRecoveryReusesOnlyExactFirstWriter(t *testing.T) {
	root := t.TempDir()
	record := emitRecord{Type: "lane.event", Epoch: 1, CreatedAt: "2026-09-09T00:00:00Z", OwnerLane: "owner", PaneID: "workspace:pane-1", EventID: idleWakeEventID(strings.Repeat("e", 32), "workspace:pane-1", 2), Text: `{"kind":"idle-wake"}`}
	first, err := ensureLaneEmitRecord(root, record)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ensureLaneEmitRecord(root, record)
	if err != nil || second != first || countIdleWakeFiles(t, root) != 1 {
		t.Fatalf("exact recovery first=%q second=%q files=%d err=%v", first, second, countIdleWakeFiles(t, root), err)
	}
	conflict := record
	conflict.Text = `{"kind":"changed"}`
	if _, err := ensureLaneEmitRecord(root, conflict); !errors.Is(err, errEmitDuplicateOutboxKey) {
		t.Fatalf("conflicting first-writer record err=%v", err)
	}
}

func TestIdleWakeOwnerResolutionUsesLaneParentThenActiveJobFallback(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"worker-lane":{"machine":"host-a","pane":"workspace:pane-1","parent":"owner-a"},"owner-a":{"machine":"host-a","pane":"workspace:pane-owner"},"owner-b":{"machine":"host-a","pane":"workspace:pane-b"}}}`, nil, nil)
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", activeJobs: map[string]HubActiveJob{
		"job-fallback": {JobID: "job-fallback", AgentLabel: "worker-fallback", Epoch: 1, OwnerLane: "owner-b", Pane: "workspace:pane-job"},
		"job-direct":   {JobID: "job-direct", AgentLabel: "worker-conflict", Epoch: 1, OwnerLane: "owner-b", Pane: "workspace:pane-1"},
		"job-owner":    {JobID: "job-owner", AgentLabel: "owner-conflict", Epoch: 1, OwnerLane: "owner-b", Pane: "workspace:pane-owner"},
	}}
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:pane-1"); got.Lane != "owner-a" || got.JobID != "" {
		t.Fatalf("lane parent owner=%+v", got)
	}
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:pane-job"); got.Lane != "owner-b" || got.JobID != "job-fallback" || got.Label != "worker-fallback" {
		t.Fatalf("active-job fallback=%+v", got)
	}
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:pane-owner"); got.Reason != idleWakeReasonParentless {
		t.Fatalf("parentless lane=%+v", got)
	}
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:unknown"); got.Reason != idleWakeReasonUnknown {
		t.Fatalf("unknown route=%+v", got)
	}
}

func TestIdleWakeActiveJobFallbackComesFromClaimAndSpawnScanner(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "jobs", "job-active", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Format(time.RFC3339)
	if err := os.WriteFile(filepath.Join(events, "00001-job.claimed.json"), []byte(`{"type":"job.claimed","created_at":"`+stamp+`","agent_label":"worker-active","owner_lane":"owner","epoch":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00002-job.spawned.json"), []byte(`{"type":"job.spawned","created_at":"`+stamp+`","pane_id":"workspace:pane-active"}`), 0600); err != nil {
		t.Fatal(err)
	}
	active := scanHubActiveJobsWithPanes(context.Background(), root, func(context.Context) (map[string]bool, error) {
		return map[string]bool{"workspace:pane-active": true}, nil
	})
	if len(active) != 1 || active[0].OwnerLane != "owner" || active[0].Pane != "workspace:pane-active" {
		t.Fatalf("active scanner evidence=%+v", active)
	}
	hub := r20Hub(t, `{"lanes":{"owner":{"machine":"host-a","pane":"workspace:owner"}}}`, nil, nil)
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", activeJobs: map[string]HubActiveJob{}}
	hub.observeActiveJobs("host-a", active, time.Now().UTC())
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:pane-active"); got.Lane != "owner" || got.JobID != "job-active" {
		t.Fatalf("scanner-backed fallback=%+v", got)
	}
}

func TestIdleWakeOwnerResolutionFailsClosedOnAmbiguity(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"lane-a":{"machine":"host-a","pane":"workspace:shared","parent":"owner"},"lane-b":{"machine":"host-a","pane":"workspace:shared","parent":"owner"},"owner":{"machine":"host-a","pane":"workspace:owner"}}}`, nil, nil)
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", activeJobs: map[string]HubActiveJob{}}
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:shared"); got.Reason != idleWakeReasonAmbiguous || got.Lane != "" {
		t.Fatalf("ambiguous direct routes=%+v", got)
	}
	hub = r20Hub(t, `{"lanes":{"owner":{"machine":"host-a","pane":"workspace:owner"}}}`, nil, nil)
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", activeJobs: map[string]HubActiveJob{
		"job-a": {JobID: "job-a", AgentLabel: "worker-a", Epoch: 1, OwnerLane: "owner", Pane: "workspace:shared"},
		"job-b": {JobID: "job-b", AgentLabel: "worker-b", Epoch: 1, OwnerLane: "owner", Pane: "workspace:shared"},
	}}
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:shared"); got.Reason != idleWakeReasonAmbiguous || got.Lane != "" {
		t.Fatalf("ambiguous active jobs=%+v", got)
	}
	hub = r20Hub(t, `{"lanes":{"worker":{"machine":"host-a","pane":"workspace:pane-1","parent":"-owner"},"-owner":{"machine":"host-a","pane":"workspace:owner"}}}`, nil, nil)
	if got := hub.resolveIdleWakeOwner("host-a", "workspace:pane-1"); got.Reason != idleWakeReasonInvalid || got.Lane != "" {
		t.Fatalf("lane outside direct-event vocabulary=%+v", got)
	}
}

func TestIdleWakeUnknownAndParentlessDecisionsProduceZeroDelivery(t *testing.T) {
	for _, reason := range []string{idleWakeReasonUnknown, idleWakeReasonParentless, idleWakeReasonAmbiguous, idleWakeReasonInvalid} {
		t.Run(reason, func(t *testing.T) {
			rig := newIdleWakeTestRig(t, time.Second)
			rig.observe("working", 1, rig.now)
			rig.observe("idle", 2, rig.now.Add(time.Second))
			rig.now = rig.now.Add(2 * time.Second)
			rig.manager.Tick(context.Background(), rig.now)
			request := rig.requests[0]
			rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Reason: reason})
			rig.manager.Tick(context.Background(), rig.now.Add(time.Hour))
			if len(rig.emitted) != 0 || countIdleWakeFiles(t, rig.manager.inboxRoot) != 0 || len(rig.requests) != 1 {
				t.Fatalf("suppressed decision delivered: requests=%d emitted=%d files=%d", len(rig.requests), len(rig.emitted), countIdleWakeFiles(t, rig.manager.inboxRoot))
			}
		})
	}
}

func TestIdleWakeRouteReassignmentDuringSettleUsesCurrentParent(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"worker":{"machine":"host-a","pane":"workspace:pane-1","parent":"owner-old"},"owner-old":{"machine":"host-a","pane":"workspace:old"},"owner-new":{"machine":"host-a","pane":"workspace:new"}}}`, nil, nil)
	rig := newIdleWakeTestRig(t, time.Minute)
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	contents := `{"lanes":{"worker":{"machine":"host-a","pane":"workspace:pane-1","parent":"owner-new"},"owner-old":{"machine":"host-a","pane":"workspace:old"},"owner-new":{"machine":"host-a","pane":"workspace:new"}}}`
	if err := os.WriteFile(hub.reportRelayPath, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	rig.manager.Tick(context.Background(), rig.now.Add(time.Minute+time.Second))
	if len(rig.requests) != 1 {
		t.Fatalf("settled requests=%+v", rig.requests)
	}
	request := rig.requests[0]
	decision := idleWakeRouteDecision(request, hub.resolveIdleWakeOwner("host-a", request.Pane))
	if !decision.Eligible || decision.Lane != "owner-new" {
		t.Fatalf("post-settle route decision=%+v", decision)
	}
	rig.manager.ApplyRoute(context.Background(), decision)
	if len(rig.emitted) != 1 || rig.emitted[0].OwnerLane != "owner-new" {
		t.Fatalf("reassigned route delivery=%+v", rig.emitted)
	}
}

func TestIdleWakeProtocolRejectsUnknownFieldsAndMismatchedSequence(t *testing.T) {
	request := idleWakeRouteRequest{EventID: idleWakeEventID(strings.Repeat("c", 32), "workspace:pane-1", 7), Pane: "workspace:pane-1", WorkspaceID: "workspace", Label: "worker", State: "done", ChangedAt: "2026-09-09T00:00:00Z", StateChangeSeq: 7}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, ok := decodeIdleWakeRouteRequest(payload); !ok || decoded.EventID != request.EventID {
		t.Fatalf("valid request decoded=%+v ok=%t", decoded, ok)
	}
	var object map[string]any
	if json.Unmarshal(payload, &object) != nil {
		t.Fatal("valid request was not JSON")
	}
	object["state_change_seq"] = float64(8)
	mismatched, _ := json.Marshal(object)
	if _, ok := decodeIdleWakeRouteRequest(mismatched); ok {
		t.Fatal("event id with a mismatched state_change_seq was accepted")
	}
	object["state_change_seq"] = float64(7)
	object["future"] = true
	unknown, _ := json.Marshal(object)
	if _, ok := decodeIdleWakeRouteRequest(unknown); ok {
		t.Fatal("unknown route-request field was accepted")
	}

	decision := idleWakeRouteDecision(request, idleWakeOwnerResolution{Lane: "owner", JobID: "job-synthetic"})
	wire, _ := json.Marshal(decision)
	parsed, ok := parseHubOutbound(wire)
	if !ok || !parsed.Eligible || parsed.IdleWakeEventID != request.EventID || parsed.Lane != "owner" || parsed.JobID != "job-synthetic" {
		t.Fatalf("route decision parsed=%+v ok=%t wire=%s", parsed, ok, wire)
	}

	expanding := request
	expanding.Label = strings.Repeat("<", 512)
	text := idleWakeRouteText(expanding, idleWakeOwnerResolution{Lane: "owner"})
	if len(text) > laneEventTextLimit || !json.Valid([]byte(text)) {
		t.Fatalf("bounded wake text len=%d valid_json=%t", len(text), json.Valid([]byte(text)))
	}
}

func TestIdleWakeHubProtocolQueuesCurrentRouteDecision(t *testing.T) {
	hub := r20Hub(t, `{"lanes":{"worker":{"machine":"host-a","pane":"workspace:pane-1","parent":"owner"},"owner":{"machine":"host-a","pane":"workspace:owner"}}}`, nil, nil)
	agent := &hubAgent{idleRoutes: make(chan hubIdleWakeRouteEvent, 1)}
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", state: "connected", agent: agent, activeJobs: map[string]HubActiveJob{}}
	request := idleWakeRouteRequest{EventID: idleWakeEventID(strings.Repeat("d", 32), "workspace:pane-1", 2), Pane: "workspace:pane-1", State: "idle", ChangedAt: "2026-09-09T00:00:00Z", StateChangeSeq: 2}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(hubClientWireEvent(hubClientEvent{Kind: "idle-wake.route.request", Payload: payload}))
	if err != nil {
		t.Fatal(err)
	}
	hub.handleAgentMessage("host-a", "synthetic-remote", agent, wire)
	select {
	case decision := <-agent.idleRoutes:
		if !decision.Eligible || decision.Lane != "owner" || decision.EventID != request.EventID {
			t.Fatalf("queued decision=%+v", decision)
		}
	default:
		t.Fatal("route decision was not queued")
	}
}

func TestIdleWakeRouteWorkerPreservesReceiveOrder(t *testing.T) {
	rig := newIdleWakeTestRig(t, time.Second)
	rig.observe("working", 1, rig.now)
	rig.observe("idle", 2, rig.now.Add(time.Second))
	rig.now = rig.now.Add(2 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	if len(rig.requests) != 1 {
		t.Fatalf("route requests=%d, want 1", len(rig.requests))
	}
	emitted := make(chan hubScannedRelayEvent, 1)
	rig.manager.enqueue = func(event hubScannedRelayEvent) bool {
		emitted <- event
		return true
	}
	request := rig.requests[0]
	first := hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Eligible: true, Lane: "owner-a", Text: idleWakeRouteText(request, idleWakeOwnerResolution{Lane: "owner-a"})}
	second := hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Eligible: true, Lane: "owner-b", Text: idleWakeRouteText(request, idleWakeOwnerResolution{Lane: "owner-b"})}
	routes := make(chan hubIdleWakeRouteEvent, 2)
	// Queue both before starting the sole worker so scheduling cannot reorder
	// the conflicting decisions.
	routes <- first
	routes <- second
	client := &HubClient{warn: func(string) {}}
	client.SetIdleWakeManager(rig.manager)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.applyIdleWakeRoutes(ctx, routes)
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case event := <-emitted:
		if event.OwnerLane != "owner-a" {
			t.Fatalf("first queued route lost receive order: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("queued idle-wake route was not applied")
	}
}

func TestIdleWakeTerminalCandidateRetentionKeepsPendingRetry(t *testing.T) {
	t.Setenv("PANEWIRE_RELAY_OUTBOX_MAX_AGE", "24h")
	rig := newIdleWakeTestRig(t, time.Second)
	base := rig.now

	rig.observe("working", 1, base)
	rig.observe("idle", 2, base.Add(time.Second))
	rig.now = base.Add(2 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	rig.route(0, "owner-a") // assigned and materialized

	rig.observe("working", 3, base.Add(3*time.Second))
	rig.observe("idle", 4, base.Add(4*time.Second))
	rig.now = base.Add(5 * time.Second)
	rig.manager.Tick(context.Background(), rig.now)
	request := rig.requests[1]
	rig.manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Reason: idleWakeReasonUnknown})

	rig.observe("working", 5, base.Add(6*time.Second))
	rig.observe("idle", 6, base.Add(7*time.Second))
	cancelledAt := base.Add(7500 * time.Millisecond)
	rig.observe("working", 7, cancelledAt)          // cancelled before settle
	rig.observe("idle", 8, base.Add(8*time.Second)) // pending retry; never prune

	var total, missingDecisionTime int
	if err := rig.store.db.QueryRow(`SELECT COUNT(*) FROM idle_wake_candidates`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if err := rig.store.db.QueryRow(`SELECT COUNT(*) FROM idle_wake_candidates WHERE decision!='' AND decision_at IS NULL`).Scan(&missingDecisionTime); err != nil {
		t.Fatal(err)
	}
	if total != 4 || missingDecisionTime != 0 {
		t.Fatalf("before retention total=%d terminal_without_time=%d", total, missingDecisionTime)
	}

	pruneAt := cancelledAt.Add(relayOutboxMaxAge() + time.Millisecond)
	rig.manager.RetrySettled(context.Background(), pruneAt)
	var pending, sequence int
	if err := rig.store.db.QueryRow(`SELECT COUNT(*) FROM idle_wake_candidates`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if err := rig.store.db.QueryRow(`SELECT COUNT(*),COALESCE(MAX(state_change_seq),0) FROM idle_wake_candidates WHERE decision=''`).Scan(&pending, &sequence); err != nil {
		t.Fatal(err)
	}
	if total != 1 || pending != 1 || sequence != 8 {
		t.Fatalf("after retention total=%d pending=%d sequence=%d", total, pending, sequence)
	}
}

func TestIdleWakeCLISettleDefaultAndValidation(t *testing.T) {
	plain, code, err := newDaemonForCLI([]string{"--socket", filepath.Join(t.TempDir(), "plain.sock")}, daemonCLIDeps{})
	if err != nil || code != ExitOK || plain.cfg.IdleWakeSettle != defaultIdleWakeSettle {
		t.Fatalf("default settle daemon=%v code=%d err=%v settle=%s", plain, code, err, plain.cfg.IdleWakeSettle)
	}
	root := t.TempDir()
	envPath := filepath.Join(root, "node.env")
	if err := os.WriteFile(envPath, []byte("HUB_MACHINE_ID=host-a\nHUB_TOKEN="+r6NodeAToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--hub-url", "ws://fixture.invalid", "--hub-token-env", envPath, "--hub-jobs-root", root, "--idle-wake-settle", "75s"}
	configured, code, err := newDaemonForCLI(args, daemonCLIDeps{AllowInsecureForTests: true})
	if err != nil || code != ExitOK || configured.cfg.IdleWakeSettle != 75*time.Second {
		t.Fatalf("configured settle daemon=%v code=%d err=%v settle=%s", configured, code, err, configured.cfg.IdleWakeSettle)
	}
	invalid := append(append([]string(nil), args[:len(args)-1]...), "0s")
	if daemon, code, err := newDaemonForCLI(invalid, daemonCLIDeps{AllowInsecureForTests: true}); daemon != nil || code != ExitConditionInvalid || err == nil {
		t.Fatalf("nonpositive settle daemon=%v code=%d err=%v", daemon, code, err)
	}
}

func TestIdleWakeR21PersistenceFailureRetryDuplicateAndNodeRestart(t *testing.T) {
	fake, durable, closeServer := newFakeHandoffkeep(t)
	defer closeServer()
	hub := r20Hub(t, `{"lanes":{"owner":{"machine":"host-a","pane":"workspace:owner"}}}`, durable, nil)
	destination := &hubAgent{relays: make(chan hubRelayInjectEvent, 8)}
	hub.nodes["host-a"] = &hubNodeRecord{machineID: "host-a", agent: destination, activeJobs: map[string]HubActiveJob{}}
	source := &hubAgent{persisted: make(chan hubRelayPersistedEvent, 8)}

	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	now := time.Date(2026, 9, 9, 2, 0, 0, 0, time.UTC)
	node := &HubClient{jobsInboxRoot: inbox, events: make(chan hubClientEvent, 8), completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, relayInflight: map[string]struct{}{}, outbox: store, now: func() time.Time { return now }}
	manager, err := newIdleWakeManager(store, inbox, time.Second, func(idleWakeRouteRequest) bool { return true }, node.EnqueueRelayEvent, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	manager.now = func() time.Time { return now }
	if err := manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "working", Revision: 1}, now); err != nil {
		t.Fatal(err)
	}
	if err := manager.Observe(context.Background(), HerdrAgentState{PaneID: "workspace:pane-1", Status: "idle", Revision: 2}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	manager.Tick(context.Background(), now.Add(2*time.Second))
	candidates, err := store.dueIdleWakeCandidates(context.Background(), now.Add(2*time.Second+idleWakeRouteRetry))
	if err != nil || len(candidates) != 1 {
		t.Fatalf("settled candidates=%+v err=%v", candidates, err)
	}
	request := idleWakeRouteRequestFor(candidates[0])
	manager.ApplyRoute(context.Background(), hubIdleWakeRouteEvent{Type: "idle-wake.route", EventID: request.EventID, Pane: request.Pane, Eligible: true, Lane: "owner", Text: idleWakeRouteText(request, idleWakeOwnerResolution{Lane: "owner"})})
	firstWire := <-node.events
	node.commitRelaySent(firstWire)
	firstPayload, ok := decodeHubLaneEventPayload(firstWire.Payload)
	if !ok {
		t.Fatalf("invalid first lane payload=%s", firstWire.Payload)
	}
	fake.mu.Lock()
	fake.status = http.StatusInternalServerError
	fake.mu.Unlock()
	hub.relayLaneEvent(firstPayload, source)
	if fake.rowCount() != 0 || drainRelays(destination) != 0 || len(drainPersisted(source)) != 0 {
		t.Fatalf("failed persistence spent delivery: rows=%d", fake.rowCount())
	}

	// A new node process reads the same event file/outbox after the retry
	// backoff. The lane and producer id remain the first route decision.
	now = now.Add(2 * relayOutboxBackoff)
	restarted := &HubClient{jobsInboxRoot: inbox, events: make(chan hubClientEvent, 8), completedJobs: map[string]uint64{}, completedReports: map[string]struct{}{}, relayInflight: map[string]struct{}{}, assignedJobs: map[string]uint64{}, outbox: store, now: func() time.Time { return now }}
	replayed := restarted.jobCompletionEvents()
	if len(replayed) != 1 {
		t.Fatalf("restart replay events=%d", len(replayed))
	}
	var replayFlag struct {
		Replay bool `json:"replay"`
	}
	if json.Unmarshal(replayed[0].Payload, &replayFlag) != nil || !replayFlag.Replay {
		t.Fatalf("restart replay flag payload=%s", replayed[0].Payload)
	}
	restarted.commitRelaySent(replayed[0])
	retryPayload, ok := decodeHubLaneEventPayload(replayed[0].Payload)
	if !ok || retryPayload.OwnerLane != "owner" || retryPayload.EventID != firstPayload.EventID {
		t.Fatalf("retry payload=%+v ok=%t", retryPayload, ok)
	}
	fake.mu.Lock()
	fake.status = http.StatusCreated
	fake.mu.Unlock()
	hub.relayLaneEvent(retryPayload, source)
	acks := drainPersisted(source)
	injections := drainRelays(destination)
	if fake.rowCount() != 1 || injections != 1 || len(acks) != 1 {
		t.Fatalf("retry rows=%d injections=%d acks=%d", fake.rowCount(), injections, len(acks))
	}
	restarted.recordRelayPersisted(hubOutboundMessage{Type: acks[0].Type, JobID: acks[0].JobID, Kind: acks[0].Kind, Epoch: acks[0].Epoch, EventID: acks[0].EventID, Lane: acks[0].Lane, ProducerEventID: acks[0].ProducerEventID})
	if pending := restarted.jobCompletionEvents(); len(pending) != 0 {
		t.Fatalf("persisted restart outbox still offered %d events", len(pending))
	}
	hub.relayLaneEvent(retryPayload, source)
	duplicateInjections := drainRelays(destination)
	if fake.rowCount() != 1 || duplicateInjections != 0 {
		t.Fatalf("durable duplicate rows=%d duplicate injections=%d", fake.rowCount(), duplicateInjections)
	}
}

func TestIdleWakeAdversarialLabelRemainsDataAtShellBoundary(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "must-not-exist")
	label := "$(touch " + marker + ");'\"\\payload"
	request := idleWakeRouteRequest{EventID: idleWakeEventID(strings.Repeat("b", 32), "workspace:pane-1", 2), Pane: "workspace:pane-1", Label: label, State: "idle", ChangedAt: "2026-09-09T00:00:00Z", StateChangeSeq: 2}
	if !validIdleWakeRouteRequest(request) {
		t.Fatalf("shell-metacharacter label should remain valid data: %q", label)
	}
	text := idleWakeRouteText(request, idleWakeOwnerResolution{Lane: "owner"})
	if !validLaneEventText(text) || strings.ContainsAny(text, "\r\n\x00") || !strings.Contains(text, "$(touch") {
		t.Fatalf("lane text did not preserve escaped data safely: %q", text)
	}
	script := filepath.Join(dir, "herdr")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncase \"$2\" in prompt) exit 0;; read) exit 0;; *) exit 1;; esac\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if !defaultHubRelayInject(context.Background(), "workspace:owner", text) {
		t.Fatal("synthetic herdr fixture rejected the data argument")
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("label was interpreted by a shell: marker err=%v", err)
	}
}
