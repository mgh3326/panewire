package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func writeSpawnRoot(t *testing.T, config, root string) {
	t.Helper()
	body, err := json.Marshal(hubSpawnConfig{Enabled: true, WorkRoot: root, Workspace: "worker", HERDRSession: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	if err := replaceFileAtomically(config, ".spawn-root-*", body, 0600); err != nil {
		t.Fatal(err)
	}
}

func rootChangeSpawnFixture(t *testing.T, slots int) (*nodeWorktreeFixture, *HubClient) {
	t.Helper()
	fixture := newNodeWorktreeFixture(t)
	client := fixture.client()
	client.spawnSlotsOverride = slots
	client.spawnJournalDir = t.TempDir()
	client.fleetEvents = (&fleetEventRecorder{}).write
	writeSpawnRoot(t, fixture.config, fixture.workRoot)
	return fixture, client
}

func rootChangeMessage(fixture *nodeWorktreeFixture, id int, job string) hubOutboundMessage {
	message := admissionMessage(id, true)
	message.Args = []string{"--job", job}
	message.Checkout = &hubSpawnCheckout{Origin: fixture.origin, Mode: "detached", Sha: fixture.main, Dir: job}
	return message
}

func TestWorkRootChangeRefusesWhileHeld_t6367(t *testing.T) {
	testWorkRootChangeWhileHeld(t, false)
}

func TestWorkRootChangeReplayEvaluatesAfresh_t6367(t *testing.T) {
	testWorkRootChangeWhileHeld(t, true)
}

func testWorkRootChangeWhileHeld(t *testing.T, replay bool) {
	t.Helper()
	fixture, client := rootChangeSpawnFixture(t, 2)
	jobARoot := filepath.Join(fixture.workRoot, "job-a")
	started, release := make(chan string, 2), make(chan struct{}, 1)
	client.spawnEngine = func(ctx context.Context, cwd fleet.LaunchDir, _, _, _, _ string, _ time.Time, args []string) (*hubSpawnOutput, fleet.Execution) {
		started <- cwd.Path
		if hubSpawnIdentity(args).Job == "job-a" {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return t14EngineOK()
	}
	peer, results, claims := grantPeer(t, client, "grant")
	a := rootChangeMessage(fixture, 636701, "job-a")
	b := rootChangeMessage(fixture, 636702, "job-b")
	b.StartByMS = b.DeadlineMS
	go client.handleHubSpawn(t.Context(), peer, a)
	select {
	case cwd := <-started:
		if cwd != jobARoot {
			t.Fatalf("job-a launched at %q, want %q", cwd, jobARoot)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("job-a did not reach launch, results pending: %d", len(results))
	}
	writeSpawnRoot(t, fixture.config, jobARoot)
	go client.handleHubSpawn(t.Context(), peer, b)
	got := admissionResult(t, results, time.Second)
	if got.RequestID != b.RequestID || got.Error != "checkout_refused:work_root_changed" || got.Stage != "checkout" || got.RC != 81 || !got.Retryable {
		t.Fatalf("job-b result = %+v", got)
	}
	if claims.Load() != 0 {
		t.Fatal("job-b requested a hub grant before refusing")
	}
	if !replay {
		journal, err := client.acquireSpawnJournal()
		if err != nil {
			t.Fatal(err)
		}
		defer client.releaseSpawnJournal()
		if known, cached, err := journal.lookup(t.Context(), b); known || cached != nil || err != nil {
			t.Fatalf("refusal left a journal row: known=%v cached=%+v err=%v", known, cached, err)
		}
	}
	release <- struct{}{}
	if result := nextSpawnResult(t, results); result.RequestID != a.RequestID || result.RC != 0 {
		t.Fatalf("job-a result = %+v", result)
	}
	if replay {
		client.handleHubSpawn(t.Context(), peer, b)
		if result := nextSpawnResult(t, results); result.RequestID != b.RequestID || result.RC != 0 {
			t.Fatalf("replayed job-b result = %+v", result)
		}
		if cwd := <-started; cwd != filepath.Join(jobARoot, "job-b") {
			t.Fatalf("replayed job-b launched at %q", cwd)
		}
	}
}

func TestWorkRootChangeAppliesWhenIdle_t6367(t *testing.T) {
	fixture, client := rootChangeSpawnFixture(t, 2)
	peer, results := admissionPeer(t)
	root := fixture.workRoot
	client.spawnEngine = func(_ context.Context, cwd fleet.LaunchDir, _, _, _, _ string, _ time.Time, args []string) (*hubSpawnOutput, fleet.Execution) {
		if want := filepath.Join(root, hubSpawnIdentity(args).Job); cwd.Path != want {
			t.Errorf("launch cwd = %s, want %s", cwd.Path, want)
		}
		return t14EngineOK()
	}
	client.handleHubSpawn(t.Context(), peer, rootChangeMessage(fixture, 636703, "job-a"))
	if result := nextSpawnResult(t, results); result.RC != 0 {
		t.Fatalf("job-a result = %+v", result)
	}
	root = filepath.Join(root, "another-root")
	writeSpawnRoot(t, fixture.config, root)
	client.handleHubSpawn(t.Context(), peer, rootChangeMessage(fixture, 636704, "job-b"))
	if result := nextSpawnResult(t, results); result.RC != 0 {
		t.Fatalf("job-b result = %+v", result)
	}
}

func TestMissingWorkRootBootstraps_t6367(t *testing.T) {
	fixture, client := rootChangeSpawnFixture(t, 2)
	root := filepath.Join(fixture.root, "missing")
	writeSpawnRoot(t, fixture.config, root)
	client.helloCapabilities([]string{"spawn"})
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("work root before prepare: %v", err)
	}
	client.spawnEngine = func(_ context.Context, cwd fleet.LaunchDir, _, _, _, _ string, _ time.Time, _ []string) (*hubSpawnOutput, fleet.Execution) {
		if cwd.Path != filepath.Join(root, "job-a") {
			t.Errorf("launch cwd = %s", cwd.Path)
		}
		return t14EngineOK()
	}
	peer, results := admissionPeer(t)
	client.handleHubSpawn(t.Context(), peer, rootChangeMessage(fixture, 636705, "job-a"))
	if result := nextSpawnResult(t, results); result.RC != 0 {
		t.Fatalf("job-a result = %+v", result)
	}
	if info, err := os.Stat(root); err != nil || !info.IsDir() {
		t.Fatalf("work root after prepare: %v, %v", info, err)
	}
}
