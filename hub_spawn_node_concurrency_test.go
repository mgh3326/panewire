package panewire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Two spawns on one node overlap outside the exclusive steps (worktree
// prepare, pane create) and never inside them (hk 4388).
func TestNodeSpawnsOverlapOutsideExclusiveSteps(t *testing.T) {
	results := make(chan hubSpawnResult, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	peer := &hubClientConnection{connection: conn}

	var inPrepare, maxPrepare, inPane, maxPane atomic.Int32
	enter := func(in, peak *atomic.Int32) {
		n := in.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		// Simulated step latency, so an overlap inside a step would show.
		time.Sleep(20 * time.Millisecond)
		in.Add(-1)
	}
	var landing sync.WaitGroup
	landing.Add(2)
	bothLanding := make(chan struct{})
	go func() { landing.Wait(); close(bothLanding) }()
	var overlapped atomic.Int32
	client := &HubClient{
		spawnConfigPath: t14SpawnConfig(t, t.TempDir()),
		spawnJournalDir: t.TempDir(),
		fleetEvents:     (&fleetEventRecorder{}).write,
		spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
			enter(&inPrepare, &maxPrepare)
			return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: t.TempDir()}}, ""
		},
		spawnEngine: func(ctx context.Context, _, _, _, _ string, _ []string) (*hubSpawnOutput, fleet.Execution) {
			release := fleet.ExclusiveStep(ctx, fleet.StepPaneCreate)
			enter(&inPane, &maxPane)
			release()
			// The landing wait runs outside every exclusive step: both
			// spawns must be in it at once.
			landing.Done()
			select {
			case <-bothLanding:
				overlapped.Add(1)
			case <-time.After(3 * time.Second):
			}
			return t14EngineOK()
		},
	}
	for i := range 2 {
		checkout := &hubSpawnCheckout{Origin: "https://github.com/acme/repo.git", Mode: "branch", Branch: "feature/a", Dir: "repo-job-" + strconv.Itoa(i)}
		message := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(4388 + i), CWDKey: "repo-a", BriefInline: "brief", Checkout: checkout, Args: []string{"--job", "job-a"}, WaitSeconds: 60}
		go client.handleHubSpawn(t.Context(), peer, message)
	}
	for range 2 {
		select {
		case result := <-results:
			if result.RC != 0 {
				t.Fatalf("spawn result %+v", result)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("node sent no job.spawn.result")
		}
	}
	if overlapped.Load() != 2 {
		t.Fatalf("spawns did not overlap in landing: %d of 2 saw the other", overlapped.Load())
	}
	if maxPrepare.Load() != 1 || maxPane.Load() != 1 {
		t.Fatalf("exclusive steps overlapped: prepare=%d pane=%d", maxPrepare.Load(), maxPane.Load())
	}
}

// Two spawns for the same checkout directory never overlap: the first owns
// the directory until it returns, so the second cannot move it to another
// revision before the first has launched (hk 4388 review).
func TestNodeSpawnsShareNoCheckoutDirBeforeLaunch(t *testing.T) {
	results := make(chan hubSpawnResult, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
	defer server.Close()
	conn, _, err := websocket.Dial(t.Context(), r6WSURL(server.URL, ""), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	peer := &hubClientConnection{connection: conn}
	var inSpawn, peak atomic.Int32
	client := &HubClient{
		spawnConfigPath: t14SpawnConfig(t, t.TempDir()),
		spawnJournalDir: t.TempDir(),
		fleetEvents:     (&fleetEventRecorder{}).write,
		spawnPrepare: func(context.Context, nodeWorktreePrepareArgs) (any, string) {
			n := inSpawn.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: t.TempDir()}}, ""
		},
		spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
			// Simulated launch latency: a second spawn on the same directory
			// would enter prepare inside this window.
			time.Sleep(50 * time.Millisecond)
			inSpawn.Add(-1)
			return t14EngineOK()
		},
	}
	for i := range 2 {
		checkout := &hubSpawnCheckout{Origin: "https://github.com/acme/repo.git", Mode: "branch", Branch: "feature/a", Sha: strconv.Itoa(i), Dir: "repo-job"}
		message := hubOutboundMessage{OperatorAuthorized: true, RequestID: t14RequestID(4390 + i), CWDKey: "repo-a", BriefInline: "brief", Checkout: checkout, Args: []string{"--job", "job-" + strconv.Itoa(i)}, WaitSeconds: 60}
		go client.handleHubSpawn(t.Context(), peer, message)
	}
	for range 2 {
		select {
		case result := <-results:
			if result.RC != 0 {
				t.Fatalf("spawn result %+v", result)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("node sent no job.spawn.result")
		}
	}
	if peak.Load() != 1 {
		t.Fatalf("two spawns held one checkout directory at once: %d", peak.Load())
	}
}

// Two names for one worktree, a directory and a symlink to it, take one
// checkout lock (review r4560281302).
func TestCheckoutDirKeyResolvesAliases(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "real"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "real"), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"work_root":"`+root+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config}
	if a, b := client.checkoutDirKey("alias"), client.checkoutDirKey("real"); a != b {
		t.Fatalf("alias key %q, real key %q", a, b)
	}
	if client.checkoutDirKey("other") == client.checkoutDirKey("real") {
		t.Fatal("distinct directories share a key")
	}
}

// The key of a checkout under a symlinked work root is the same before the
// directory exists and after (review r4561554283).
func TestCheckoutDirKeyStableAcrossCreation(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "work-link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"work_root":"`+link+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config}
	before := client.checkoutDirKey("job-dir")
	if err := os.Mkdir(filepath.Join(real, "job-dir"), 0700); err != nil {
		t.Fatal(err)
	}
	if after := client.checkoutDirKey("job-dir"); before != after {
		t.Fatalf("key before creation %q, after %q", before, after)
	}
}

// A symlink whose target does not exist yet keys like its target, before and
// after the target is created (review r4564003838).
func TestCheckoutDirKeyResolvesDanglingAlias(t *testing.T) {
	root := t.TempDir()
	if err := os.Symlink(filepath.Join(root, "real"), filepath.Join(root, "alias")); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"work_root":"`+root+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config}
	alias, real := client.checkoutDirKey("alias"), client.checkoutDirKey("real")
	if alias != real {
		t.Fatalf("dangling alias key %q, target key %q", alias, real)
	}
	if err := os.Mkdir(filepath.Join(root, "real"), 0700); err != nil {
		t.Fatal(err)
	}
	if after := client.checkoutDirKey("alias"); after != real {
		t.Fatalf("alias key changed on creation: %q, want %q", after, real)
	}
}

// A spawn in a mapped directory inside a worktree takes that worktree's
// checkout lock (review r4564003850).
func TestMappedCWDKeysToItsWorktree(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "job-dir")
	if err := os.MkdirAll(filepath.Join(worktree, "sub"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, ".git"), []byte("gitdir: x\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"work_root":"`+root+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config}
	if got, want := mappedCWDKey(filepath.Join(worktree, "sub")), client.checkoutDirKey("job-dir"); got != want {
		t.Fatalf("mapped cwd key %q, checkout key %q", got, want)
	}
}

// The node enforces its own spawn_in_flight, whatever the hub sends: a hub
// restart or reconnect cannot run more (review r4565217696, r4565161138).
// A spawn that cannot start before its deadline answers not started.
func TestNodeEnforcesItsSpawnInFlight(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	config := filepath.Join(t.TempDir(), "spawn.json")
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	if err := os.WriteFile(config, []byte(`{"cwd_map":{"repo-a":"`+a+`","repo-b":"`+b+`"},"workspace":"worker","herdr_session":"worker","enabled":true,"spawn_in_flight":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	var running, peak atomic.Int32
	release := make(chan struct{})
	client := &HubClient{
		spawnConfigPath: config,
		spawnJournalDir: t.TempDir(),
		fleetEvents:     (&fleetEventRecorder{}).write,
		spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
			n := running.Add(1)
			for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
			}
			<-release
			running.Add(-1)
			return t14EngineOK()
		},
	}
	first := make(chan hubSpawnResult, 1)
	go func() {
		first <- client.runClaimedHubSpawn(t.Context(), hubOutboundMessage{RequestID: t14RequestID(4410), CWDKey: "repo-a", BriefInline: "brief", Args: []string{"--job", "job-a"}, WaitSeconds: 60})
	}()
	eventually(t, 3*time.Second, "first spawn running", func() bool { return running.Load() == 1 })
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	second := client.runClaimedHubSpawn(ctx, hubOutboundMessage{RequestID: t14RequestID(4411), CWDKey: "repo-b", BriefInline: "brief", Args: []string{"--job", "job-b"}, WaitSeconds: 60})
	if second.Error != "deadline_not_started" || !second.Retryable {
		t.Fatalf("second spawn past the node limit: %+v", second)
	}
	close(release)
	if result := <-first; result.RC != 0 {
		t.Fatalf("first spawn %+v", result)
	}
	if peak.Load() != 1 {
		t.Fatalf("node ran %d spawns with spawn_in_flight 1", peak.Load())
	}
}

// A spawn waiting for a checkout lock gives up at its deadline instead of
// holding its slot until the owner returns (review r4565217739).
func TestCheckoutLockWaitEndsAtDeadline(t *testing.T) {
	client := &HubClient{}
	unlock, err := client.lockSpawnStepContext(t.Context(), "checkout-dir:/w/x")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.lockSpawnStepContext(ctx, "checkout-dir:/w/x"); err == nil {
		t.Fatal("a held checkout lock was taken twice")
	}
}

// A mapped spawn with an execution lane keys the lane directory the launcher
// runs in (review r4565161185).
func TestMappedCWDKeyFollowsExecutionLane(t *testing.T) {
	root := t.TempDir()
	lane := filepath.Join(root, "live")
	if err := os.MkdirAll(lane, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(lane, ".git"), []byte("gitdir: x\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, want := mappedCWDKey(fleet.ExecutionLaneDir(root, "live")), nodeStablePath(lane); got != want {
		t.Fatalf("lane key %q, want %q", got, want)
	}
}

// ".." in a symlink target applies after the symlinks before it resolve, as
// the filesystem does (review r4565161207, r4565217718).
func TestStablePathResolvesLinksBeforeDotDot(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "real", "sub"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "real", "job"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real/sub", filepath.Join(root, "jump")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("jump/../job", filepath.Join(root, "a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real/job", filepath.Join(root, "b")); err != nil {
		t.Fatal(err)
	}
	physical, err := filepath.EvalSymlinks(filepath.Join(root, "a"))
	if err != nil {
		t.Fatal(err)
	}
	if a, b := nodeStablePath(filepath.Join(root, "a")), nodeStablePath(filepath.Join(root, "b")); a != b || a != physical {
		t.Fatalf("a=%q b=%q physical=%q", a, b, physical)
	}
}
