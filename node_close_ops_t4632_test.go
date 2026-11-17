package panewire

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/panewire/internal/fleet"
)

// A node not ready for close ops refuses a close op as close_ops_not_ready
// (retry later) and journals nothing; the same node, ready, closes.
func TestCloseOpRefusedUntilCloseOpsReady_t4632(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := closeOpNode(t, inbox, fake)
	ledger := client.nodeHubClose.(*fakeHubClose)
	ledger.notReady = true
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-1")); code != "close_ops_not_ready" {
		t.Fatalf("code=%q, want close_ops_not_ready", code)
	}
	if len(ledger.intents) != 0 || fake.called("pane", "close") != 0 {
		t.Fatalf("intents=%d closes=%d, want none", len(ledger.intents), fake.called("pane", "close"))
	}
	ledger.notReady = false
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
}

// The hello carries close_ops_v1 exactly while the node is ready, and a node
// without a jobs root never advertises it.
func TestHelloAdvertisesCloseOpsOnlyWhenReady_t4632(t *testing.T) {
	ledger := &fakeHubClose{}
	client := &HubClient{jobsInboxRoot: t.TempDir(), nodeHubClose: ledger}
	if caps := client.spawnHelloCapabilities(t.Context()); !slices.Contains(caps, hubCapabilityCloseOps) || !client.closeOpsAdvertised.Load() {
		t.Fatalf("ready node hello %v lacks %s", caps, hubCapabilityCloseOps)
	}
	ledger.notReady = true
	if caps := client.spawnHelloCapabilities(t.Context()); slices.Contains(caps, hubCapabilityCloseOps) || client.closeOpsAdvertised.Load() {
		t.Fatalf("unready node hello %v advertises %s", caps, hubCapabilityCloseOps)
	}
	if caps := (&HubClient{}).spawnHelloCapabilities(t.Context()); slices.Contains(caps, hubCapabilityCloseOps) {
		t.Fatalf("node without a jobs root advertises %s", hubCapabilityCloseOps)
	}
	// The real ledger: no barrier recorded under the jobs root.
	if (&HubClient{jobsInboxRoot: t.TempDir()}).closeOpsReady(t.Context()) {
		t.Fatal("ready without the barrier record")
	}
}

// toggledHubClose is a fakeHubClose whose readiness a test flips while the
// node's connection loop reads it.
type toggledHubClose struct {
	fakeHubClose
	ready atomic.Bool
}

func (f *toggledHubClose) CloseOpsReady(context.Context) bool { return f.ready.Load() }

func helloAdvertisesCloseOps(hello map[string]any) bool {
	capabilities, _ := hello["capabilities"].([]any)
	return slices.Contains(capabilities, any(hubCapabilityCloseOps))
}

// #572 r4575956178, r4575962308: on one long-lived, acknowledged connection
// the node registers again when its readiness changes, both ways, and does
// not repeat the hello while it does not change.
func TestConnectedNodeReRegistersWhenCloseOpsReadinessChanges_t4632(t *testing.T) {
	hub := &helloFallbackHub{}
	server := hub.serve(t)
	ledger := &toggledHubClose{}
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour, helloAckTimeout: time.Minute,
		JobsInboxRoot:       t.TempDir(),
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	client.nodeHubClose = ledger
	client.closeOpsRecheckEvery = 20 * time.Millisecond
	// The re-check runs only on an acknowledged registration; the fake hub
	// answers the first hello's ping, so flipping readiness below follows it.
	connection, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.serve(ctx, connection)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	hellos := func() []map[string]any { h, _ := hub.snapshot(); return h }
	eventually(t, 5*time.Second, "first hello without close_ops_v1", func() bool {
		h := hellos()
		return len(h) == 1 && !helloAdvertisesCloseOps(h[0])
	})
	// Window that proves nothing more happens: readiness unchanged, no repeat.
	time.Sleep(100 * time.Millisecond)
	if n := len(hellos()); n != 1 {
		t.Fatalf("%d hellos while readiness held still, want 1", n)
	}
	ledger.ready.Store(true)
	eventually(t, 5*time.Second, "hello re-registering with close_ops_v1", func() bool {
		h := hellos()
		return len(h) == 2 && helloAdvertisesCloseOps(h[1])
	})
	ledger.ready.Store(false)
	eventually(t, 5*time.Second, "hello re-registering without close_ops_v1", func() bool {
		h := hellos()
		return len(h) == 3 && !helloAdvertisesCloseOps(h[2])
	})
}

// The CLI barrier: refused while an older watcher runs, recorded once none
// does; the record and a fresh scan make the node ready.
func TestNodeCloseOpsEnableCLI_t4632(t *testing.T) {
	inbox := t.TempDir()
	jobs := filepath.Join(inbox, "jobs")
	// An older watcher: it holds the job's watch lock but not the v2 lock.
	release, ok, err := fleet.FenceWatchers(mkdirAll(t, filepath.Join(jobs, "job-old")))
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	t.Cleanup(release)
	old := func(context.Context) ([]byte, error) {
		return []byte("  77 /old/bin/panewire.prev _watch job-old\n"), nil
	}
	clean := func(context.Context) ([]byte, error) { return []byte("  1 /sbin/init\n"), nil }
	var stdout, stderr bytes.Buffer
	if rc := runNodeCloseOpsCLI([]string{"advertise", "--inbox-root", inbox}, &stdout, &stderr, old); rc != ExitConditionInvalid {
		t.Fatalf("advertise before the barrier: rc=%d, want %d", rc, ExitConditionInvalid)
	}
	if rc := runNodeCloseOpsCLI([]string{"enable", "--inbox-root", inbox}, &stdout, &stderr, old); rc != ExitConditionInvalid {
		t.Fatalf("rc=%d stderr=%q, want %d", rc, stderr.String(), ExitConditionInvalid)
	}
	if _, err := os.Stat(filepath.Join(jobs, ".close-ops-enabled")); !os.IsNotExist(err) {
		t.Fatalf("barrier recorded while an older launcher runs: %v", err)
	}
	if rc := runNodeCloseOpsCLI([]string{"enable", "--inbox-root", inbox}, &stdout, &stderr, clean); rc != ExitOK {
		t.Fatalf("rc=%d stderr=%q, want 0", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), "barrier recorded") {
		t.Fatalf("stdout %q", stdout.String())
	}
	if fleet.CloseOpsReady(t.Context(), jobs, clean) {
		t.Fatal("ready after the barrier alone: close_ops_v1 is off by default")
	}
	if rc := runNodeCloseOpsCLI([]string{"advertise", "--inbox-root", inbox}, &stdout, &stderr, clean); rc != ExitOK {
		t.Fatalf("advertise rc=%d stderr=%q", rc, stderr.String())
	}
	if !fleet.CloseOpsReady(t.Context(), jobs, clean) {
		t.Fatal("not ready after the barrier and the advertise step")
	}
	if fleet.CloseOpsReady(t.Context(), jobs, old) {
		t.Fatal("ready while an older watcher runs after the barrier")
	}
	if rc := runNodeCloseOpsCLI([]string{"disable"}, &stdout, &stderr, clean); rc != ExitUsage {
		t.Fatalf("rc=%d for an unknown verb, want usage", rc)
	}
}

// A close on a job no watcher runs for fences the job: the node holds its
// watch lock, so a watcher of any binary started meanwhile exits, until the
// close's settlement ends (#662 review: launchers of every name and kind).
func TestCloseOpFencesAJobWithoutAWatcher_t4632(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	ledger := client.nodeHubClose.(*fakeHubClose)
	ledger.watcher, ledger.state = fleet.HubCloseWatcherNone, fleet.HubClosePending
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
	jobDir := filepath.Join(inbox, "jobs", closeOpJob)
	if _, ok, err := fleet.FenceWatchers(jobDir); err != nil || ok {
		t.Fatalf("watch lock free during an open close: ok=%v err=%v", ok, err)
	}
	ledger.state = fleet.HubCloseSettled
	client.settleHubClose(t.Context(), closeOpJob)
	release, ok, err := fleet.FenceWatchers(jobDir)
	if err != nil || !ok {
		t.Fatalf("watch lock still held after settlement: ok=%v err=%v", ok, err)
	}
	release()
}

// #662 review: a capable hub that acknowledged late (after the capability
// -free fallback) still gets readiness re-registrations.
func TestReadinessReRegistersAfterALateHelloAck_t4632(t *testing.T) {
	hub := &helloFallbackHub{pongDelay: 300 * time.Millisecond}
	server := hub.serve(t)
	ledger := &toggledHubClose{}
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour, helloAckTimeout: 100 * time.Millisecond,
		JobsInboxRoot:       t.TempDir(),
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	client.nodeHubClose = ledger
	client.closeOpsRecheckEvery = 20 * time.Millisecond
	connection, _, err := websocket.Dial(t.Context(), client.endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.serve(ctx, connection)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	// caps hello, capability-free fallback, then the capable hello again.
	eventually(t, 5*time.Second, "capable hello resent after the late ack", func() bool {
		h, _ := hub.snapshot()
		return len(h) == 3
	})
	ledger.ready.Store(true)
	eventually(t, 5*time.Second, "re-registration with close_ops_v1", func() bool {
		h, _ := hub.snapshot()
		return len(h) >= 4 && helloAdvertisesCloseOps(h[len(h)-1])
	})
}

func mkdirAll(t *testing.T, dir string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

// #662 review: close_ops_not_ready is a wire error the hub accepts, so it
// arrives as "not closed, retry later" instead of timing out.
func TestCloseOpsNotReadyIsAWireError_t4632(t *testing.T) {
	if !validNodeCommandError("close_ops_not_ready") {
		t.Fatal("close_ops_not_ready rejected by the hub's result parser")
	}
}

// The node's own ledger reader accepts a hub revocation record (type only).
func TestNodeLedgerReadsAHubRevocationRecord_t4632(t *testing.T) {
	inbox := t.TempDir()
	if err := writeHubRevocation(inbox, hubJobRevokedEvent{Type: "job.revoked", JobID: "job-r", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(filepath.Join(inbox, "jobs", "job-r"))
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	events, err := readNodeJobLedgerEvents(root)
	if err != nil || len(events) != 1 {
		t.Fatalf("events %v, err %v; want the revocation read", events, err)
	}
}

// fenceLedger is a fakeHubClose whose open-intent answer a test sets, and
// whose settlement result is independent of it (a cancelled newer intent
// with an older one still open).
type fenceLedger struct {
	fakeHubClose
	open bool
}

func (f *fenceLedger) FenceState(string) (bool, bool, error) { return f.open, true, nil }

func fenceNode(t *testing.T) (*HubClient, *fenceLedger, string, *[]string) {
	t.Helper()
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	ledger := &fenceLedger{fakeHubClose: fakeHubClose{watcher: fleet.HubCloseWatcherNone}}
	client.nodeHubClose = ledger
	var rearmed []string
	client.fenceRearm = func(job string) { rearmed = append(rearmed, job) }
	return client, ledger, filepath.Join(inbox, "jobs", closeOpJob), &rearmed
}

func watchLockHeld(t *testing.T, dir string) bool {
	t.Helper()
	release, ok, err := fleet.FenceWatchers(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		release()
	}
	return !ok
}

// #662 review round 2: the fence follows the ledger under the end lock.
// A step that cancels a newer intent while an older one is still open keeps
// it; once no intent is open it is released and the live job's watcher is
// re-armed (one that met the fence exited).
func TestFenceFollowsOpenIntents_t4632(t *testing.T) {
	client, ledger, dir, rearmed := fenceNode(t)
	ledger.open, ledger.state = true, fleet.HubCloseCancelled
	client.settleHubClose(t.Context(), closeOpJob)
	if !watchLockHeld(t, dir) || len(ledger.settled) != 1 {
		t.Fatalf("fence held=%v settled=%v; want the fence taken and one step", watchLockHeld(t, dir), ledger.settled)
	}
	client.settleHubClose(t.Context(), closeOpJob)
	if !watchLockHeld(t, dir) {
		t.Fatal("fence dropped while an older intent is open")
	}
	ledger.open = false
	client.settleHubClose(t.Context(), closeOpJob)
	if watchLockHeld(t, dir) {
		t.Fatal("fence kept with no intent open")
	}
	if !slices.Equal(*rearmed, []string{closeOpJob}) {
		t.Fatalf("rearmed %v, want the job", *rearmed)
	}
}

// After a node restart, settlement waits while a watcher that ignores hub
// closes holds the job's watch lock, and takes the fence once it is free.
func TestSettlementWaitsForAnIncompatibleWatcher_t4632(t *testing.T) {
	client, ledger, dir, _ := fenceNode(t)
	ledger.open, ledger.watcher = true, fleet.HubCloseWatcherIncompatible
	release, ok, err := fleet.FenceWatchers(dir) // the old watcher's lock
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	client.settleHubClose(t.Context(), closeOpJob)
	if len(ledger.settled) != 0 {
		t.Fatalf("settled %v while an incompatible watcher runs", ledger.settled)
	}
	release()
	ledger.watcher = fleet.HubCloseWatcherNone
	client.settleHubClose(t.Context(), closeOpJob)
	if len(ledger.settled) != 1 || !watchLockHeld(t, dir) {
		t.Fatalf("settled %v fenced %v after the watcher ended", ledger.settled, watchLockHeld(t, dir))
	}
}

// An accepted operation still resumes once readiness is withdrawn: only new
// closes are held to it.
func TestAcceptedCloseResumesWithoutReadiness_t4632(t *testing.T) {
	inbox := t.TempDir()
	seedNodeCloseOp(t, inbox, "op-1", fleet.CloseOpRegistered)
	client := closeOpNode(t, inbox, idlePaneHerdr())
	client.nodeHubClose.(*fakeHubClose).notReady = true
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-2")); code != "close_ops_not_ready" && code != "close_in_progress" {
		t.Fatalf("a new close while unready: code=%q", code)
	}
}

// #662 review round 3: an intent write that fails keeps the fence while the
// ledger shows any intent open (an older one, or this one made visible
// before its sync failed), and releases it only when none is.
func TestFailedIntentWriteKeepsTheFenceWhileAnIntentIsOpen_t4632(t *testing.T) {
	for _, open := range []bool{true, false} {
		client, ledger, dir, _ := fenceNode(t)
		ledger.open = open
		ledger.record = func(string, fleet.HubCloseIntent) error { return errors.New("sync failed") }
		if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-1")); code != "journal_failed" {
			t.Fatalf("code=%q, want journal_failed", code)
		}
		if held := watchLockHeld(t, dir); held != open {
			t.Fatalf("intent open=%v: fence held=%v", open, held)
		}
	}
}
