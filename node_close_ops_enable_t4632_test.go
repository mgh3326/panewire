package panewire

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/mgh3326/panewire/internal/fleet"
)

func closeOpsListing(out string) fleet.ProcessLister {
	return func(context.Context) ([]byte, error) { return []byte(out), nil }
}

// close_ops_v1 is never in the static set; a hello carries it only after the
// recorded barrier step, and only while no old-binary watcher runs.
func TestHelloAdvertisesCloseOpsOnlyBehindTheBarrier_t4632(t *testing.T) {
	if slices.Contains(hubSpawnCapabilities, hubCapabilityCloseOps) {
		t.Fatalf("static capabilities %v advertise %s", hubSpawnCapabilities, hubCapabilityCloseOps)
	}
	inbox := t.TempDir()
	jobs := filepath.Join(inbox, "jobs")
	if err := os.MkdirAll(filepath.Join(jobs, "j1"), 0o700); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{jobsInboxRoot: inbox, closeOpsLister: closeOpsListing("")}
	ctx := context.Background()
	if slices.Contains(client.helloCapabilities(ctx), hubCapabilityCloseOps) {
		t.Fatal("advertised before the barrier step")
	}
	var stdout, stderr bytes.Buffer
	now := func() time.Time { return time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC) }
	if rc := runNodeCloseOpsCLI([]string{"enable", "--inbox-root", inbox}, &stdout, &stderr, now, closeOpsListing("77 panewire _watch j1\n")); rc != ExitPartial {
		t.Fatalf("enable with an old watcher rc=%d stderr=%q, want %d", rc, stderr.String(), ExitPartial)
	}
	if rc := runNodeCloseOpsCLI([]string{"enable", "--inbox-root", inbox}, &stdout, &stderr, now, closeOpsListing("")); rc != ExitOK {
		t.Fatalf("enable rc=%d stderr=%q", rc, stderr.String())
	}
	got := client.helloCapabilities(ctx)
	if !slices.Contains(got, hubCapabilityCloseOps) || !slices.Equal(got[:len(hubSpawnCapabilities)], hubSpawnCapabilities) {
		t.Fatalf("hello after the step = %v, want the static set plus %s", got, hubCapabilityCloseOps)
	}
	client.closeOpsLister = closeOpsListing("77 panewire _watch j1\n")
	if slices.Contains(client.helloCapabilities(ctx), hubCapabilityCloseOps) {
		t.Fatal("advertised while an old-binary watcher runs")
	}
	client.closeOpsLister = func(context.Context) ([]byte, error) { return nil, errors.New("ps failed") }
	if slices.Contains(client.helloCapabilities(ctx), hubCapabilityCloseOps) {
		t.Fatal("advertised on a failed process listing")
	}
	if rc := runNodeCloseOpsCLI([]string{"enable"}, &stdout, &stderr, now, nil); rc != ExitUsage {
		t.Fatalf("enable without --inbox-root rc=%d", rc)
	}
}

// #572 r4576225996, r4576228677: a release whose end-lock wait expired
// answers journal_failed without a second, inline settlement under the
// acting lock; the daemon pass is left to settle.
func TestFailedReleaseSettlesNothingInline_t4632(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	ledger := client.nodeHubClose.(*fakeHubClose)
	unlock, err := fleet.LockJobEnd(filepath.Join(inbox, "jobs", closeOpJob))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	start := time.Now()
	var args nodeCloseOpRefArgs
	args.RequestID, args.Job = "op-9", closeOpJob
	if _, code := client.nodePaneCloseRelease(args); code != "journal_failed" {
		t.Fatalf("release under a held end lock: code=%q, want journal_failed", code)
	}
	if waited := time.Since(start); waited > hubCloseLockBudget+5*time.Second {
		t.Fatalf("release took %v", waited)
	}
	if len(ledger.settled) != 0 {
		t.Fatalf("settled inline after a failed release: %v", ledger.settled)
	}
	// A successful status still settles inline.
	if _, code := client.nodePaneCloseStatus(args); code != "" {
		t.Fatalf("status code=%q", code)
	}
	if len(ledger.settled) != 1 {
		t.Fatalf("settled = %v, want one inline step after a status", ledger.settled)
	}
}

// A readiness change inside a session re-registers: the node re-sends hello
// with close_ops_v1 once the last old-binary watcher ends (hk 4632).
func TestSessionReRegistersWhenCloseOpsReadinessChanges_t4632(t *testing.T) {
	hub := &helloFallbackHub{}
	server := hub.serve(t)
	inbox := t.TempDir()
	jobs := filepath.Join(inbox, "jobs")
	if err := os.MkdirAll(filepath.Join(jobs, "j1"), 0o700); err != nil {
		t.Fatal(err)
	}
	var listing atomic.Value
	listing.Store("77 panewire _watch j1\n")
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "machine-a", Token: "node-token", AllowInsecureForTests: true, PingInterval: time.Hour, helloAckTimeout: 5 * time.Second,
		JobsInboxRoot:       inbox,
		sessionCollector:    func(context.Context) ([]HerdrAgentState, error) { return nil, nil },
		hostLoadCollector:   func(context.Context) (HubHostLoad, error) { return HubHostLoad{}, nil },
		hostMemoryCollector: func(context.Context) (*HubHostMemory, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fleet.EnableCloseOps(context.Background(), jobs, client.nodeVersion(), time.Now(), closeOpsListing("")); err != nil {
		t.Fatal(err)
	}
	client.closeOpsLister = func(context.Context) ([]byte, error) { return []byte(listing.Load().(string)), nil }
	client.closeOpsRecheck = 50 * time.Millisecond
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
	advertises := func(hello map[string]any) bool {
		capabilities, _ := hello["capabilities"].([]any)
		return slices.Contains(capabilities, any(hubCapabilityCloseOps))
	}
	eventually(t, 5*time.Second, "first hello registered", func() bool {
		hellos, registered := hub.snapshot()
		return registered && len(hellos) == 1
	})
	if hellos, _ := hub.snapshot(); advertises(hellos[0]) {
		t.Fatalf("first hello %v advertises close ops with an old watcher live", hellos[0])
	}
	listing.Store("")
	eventually(t, 5*time.Second, "hello re-sent with close_ops_v1", func() bool {
		hellos, _ := hub.snapshot()
		return len(hellos) == 2 && advertises(hellos[1])
	})
}

// #593 review: a new close registration re-checks the barrier; a launcher
// that started after the hello's check refuses it as watcher_incompatible,
// before any intent is recorded.
func TestCloseRegistrationRechecksTheBarrier_t4632(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	client.closeOpsAdmit = nil
	jobs := filepath.Join(inbox, "jobs")
	if err := fleet.EnableCloseOps(context.Background(), jobs, client.nodeVersion(), time.Now(), closeOpsListing("")); err != nil {
		t.Fatal(err)
	}
	client.closeOpsLister = closeOpsListing("4242 /opt/old/panewire spawn --job other\n")
	ledger := client.nodeHubClose.(*fakeHubClose)
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, closeOpArgs("op-1")); code != "watcher_incompatible" || len(ledger.intents) != 0 {
		t.Fatalf("close_op with a launcher live: code=%q intents=%d", code, len(ledger.intents))
	}
	client.closeOpsLister = closeOpsListing("")
	wantCloseOp(t, client, "op-1", fleet.CloseOpClosed, false)
}
