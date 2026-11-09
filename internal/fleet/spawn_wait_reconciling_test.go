package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// spawnStatusSequence serves one status per read, repeating the last one, and
// counts the reads.
func spawnStatusSequence(t *testing.T, replies ...map[string]any) (hubHTTP, *atomic.Int32) {
	t.Helper()
	var reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := min(int(reads.Add(1))-1, len(replies)-1)
		json.NewEncoder(w).Encode(replies[i])
	}))
	t.Cleanup(server.Close)
	return hubHTTP{base: server.URL, token: "fixture", client: server.Client(), statusPoll: 1}, &reads
}

// A reconciling operation can still land before its deadline, so --wait keeps
// polling until the status becomes terminal.
func TestWaitSpawnKeepsPollingThroughReconciling(t *testing.T) {
	c, reads := spawnStatusSequence(t,
		map[string]any{"status": "reconciling"},
		map[string]any{"status": "reconciling"},
		map[string]any{"status": "succeeded", "result": map[string]any{"rc": 0, "host": "node-b", "profile": "sonnet", "pane": "w:p9"}})
	attempt, err := c.WaitSpawn(t.Context(), statusTestOperation)
	if err != nil || attempt.Host != "node-b" || attempt.Pane != "w:p9" {
		t.Fatalf("attempt=%+v err=%v", attempt, err)
	}
	if got := reads.Load(); got != 3 {
		t.Fatalf("status reads=%d, want 3", got)
	}
}

// A reconciling operation that then passes its deadline keeps the unresolved
// ownership exit.
func TestWaitSpawnReconcilingThenDeadlineIsUnresolved(t *testing.T) {
	c, _ := spawnStatusSequence(t,
		map[string]any{"status": "reconciling"},
		map[string]any{"status": "deadline_exceeded", "result": map[string]any{"rc": 124, "reason": "timeout"}})
	_, err := c.WaitSpawn(t.Context(), statusTestOperation)
	if ErrorCode(err) != exitStateConflict || !strings.Contains(err.Error(), "ownership is unresolved") {
		t.Fatalf("err=%v code=%d", err, ErrorCode(err))
	}
}

// Without a terminal status, --wait waits until its own context ends.
func TestWaitSpawnReconcilingWaitsUntilContextEnds(t *testing.T) {
	c, reads := spawnStatusSequence(t, map[string]any{"status": "reconciling"})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := c.WaitSpawn(ctx, statusTestOperation); err == nil {
		t.Fatal("reconciling returned without a terminal status")
	}
	if reads.Load() < 2 {
		t.Fatalf("status reads=%d, want polling while reconciling", reads.Load())
	}
}
