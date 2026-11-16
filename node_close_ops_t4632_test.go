package panewire

import (
	"bytes"
	"context"
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

// The CLI barrier: refused while an older launcher runs, recorded once none
// does; the record and a fresh scan make the node ready.
func TestNodeCloseOpsEnableCLI_t4632(t *testing.T) {
	inbox := t.TempDir()
	jobs := filepath.Join(inbox, "jobs")
	old := func(context.Context) ([]byte, error) { return []byte("  77 /old/bin/panewire spawn -p brief\n"), nil }
	clean := func(context.Context) ([]byte, error) { return []byte("  1 /sbin/init\n"), nil }
	var stdout, stderr bytes.Buffer
	if rc := runNodeCloseOpsCLI([]string{"enable", "--inbox-root", inbox}, &stdout, &stderr, old); rc != ExitConditionInvalid {
		t.Fatalf("rc=%d stderr=%q, want %d", rc, stderr.String(), ExitConditionInvalid)
	}
	if _, err := os.Stat(filepath.Join(jobs, ".close-ops-enabled")); !os.IsNotExist(err) {
		t.Fatalf("barrier recorded while an older launcher runs: %v", err)
	}
	if rc := runNodeCloseOpsCLI([]string{"enable", "--inbox-root", inbox}, &stdout, &stderr, clean); rc != ExitOK {
		t.Fatalf("rc=%d stderr=%q, want 0", rc, stderr.String())
	}
	if !strings.Contains(stdout.String(), "close ops enabled") {
		t.Fatalf("stdout %q", stdout.String())
	}
	if !fleet.CloseOpsReady(t.Context(), jobs, clean) {
		t.Fatal("not ready after the barrier")
	}
	if fleet.CloseOpsReady(t.Context(), jobs, old) {
		t.Fatal("ready while an older launcher started after the barrier")
	}
	if rc := runNodeCloseOpsCLI([]string{"disable"}, &stdout, &stderr, clean); rc != ExitUsage {
		t.Fatalf("rc=%d for an unknown verb, want usage", rc)
	}
}
