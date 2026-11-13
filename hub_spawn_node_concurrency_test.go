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
