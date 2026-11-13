package panewire

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	checkout := &hubSpawnCheckout{Origin: "https://github.com/acme/repo.git", Mode: "branch", Branch: "feature/a", Dir: "repo-job"}
	for i := range 2 {
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
