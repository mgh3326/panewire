package panewire

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

type ownerForbiddenClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *ownerForbiddenClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ownerForbiddenClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type ownerForbiddenLog struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (w *ownerForbiddenLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *ownerForbiddenLog) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// ownerForbiddenHK answers every owner PUT with refusal and counts the PUTs.
type ownerForbiddenHK struct {
	*ownerFenceHK
	refusal error
	mu      sync.Mutex
	puts    int
}

func (s *ownerForbiddenHK) Put(context.Context, string, map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.puts++
	return s.refusal
}

func (s *ownerForbiddenHK) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.puts
}

func ownerForbiddenHub(t *testing.T, store fleet.JobStore, clock *ownerForbiddenClock, logs *ownerForbiddenLog) *HubServer {
	t.Helper()
	hub, err := NewHubServer(HubServerConfig{
		Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "desk-token"},
		Logger: slog.New(slog.NewTextHandler(logs, nil)),
		Now:    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Close() })
	hub.ownerTransferDir = t.TempDir()
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "forbidden-job", Node: "machine-a", Epoch: 1}, false); err != nil {
		t.Fatal(err)
	}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	return hub
}

func ownerForbiddenRecord() *ownerFenceHK {
	return &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "", "owner_epoch": float64(0)}}
}

// heartbeatOwnerSeed is one heartbeat's schedule call, waited out so its
// effects (the PUT, the warning, the refusal memory) are visible to the test.
func heartbeatOwnerSeed(t *testing.T, hub *HubServer, assignment hubOwnerAssignment) {
	t.Helper()
	hub.scheduleOwnerSeed(assignment)
	waitOwnerWork(t, hub, assignment.Job, true)
}

func TestOwnerHostForbiddenBacksOffPerGrant_t6166(t *testing.T) {
	clock := &ownerForbiddenClock{now: time.Date(2026, 11, 24, 2, 0, 0, 0, time.UTC)}
	logs := &ownerForbiddenLog{}
	store := &ownerForbiddenHK{ownerFenceHK: ownerForbiddenRecord(), refusal: &fleet.BackendError{Status: http.StatusForbidden, Code: "job_host_forbidden"}}
	hub := ownerForbiddenHub(t, store, clock, logs)
	assignment := hubOwnerAssignment{Job: "forbidden-job", Node: "machine-a", Epoch: 1}

	for range 4 {
		heartbeatOwnerSeed(t, hub, assignment)
	}
	if got := store.putCount(); got != 1 {
		t.Fatalf("refused grant during backoff: PUTs=%d, want 1", got)
	}
	if got := strings.Count(logs.String(), "handoffkeep owner adoption failed"); got != 1 {
		t.Fatalf("warnings during backoff: %d, want 1", got)
	}
	if !strings.Contains(logs.String(), "code=job_host_forbidden") {
		t.Fatalf("warning lacks the hk refusal code: %s", logs.String())
	}

	clock.Advance(hubOwnerForbiddenBackoff - time.Second)
	heartbeatOwnerSeed(t, hub, assignment)
	if got := store.putCount(); got != 1 {
		t.Fatalf("grant retried before backoff passed: PUTs=%d, want 1", got)
	}

	clock.Advance(time.Second)
	heartbeatOwnerSeed(t, hub, assignment)
	if got := store.putCount(); got != 2 {
		t.Fatalf("grant not retried once backoff passed: PUTs=%d, want 2", got)
	}
	heartbeatOwnerSeed(t, hub, assignment)
	if got := store.putCount(); got != 2 {
		t.Fatalf("second refusal not backed off: PUTs=%d, want 2", got)
	}
}

func TestOwnerServerErrorIsRetriedEachHeartbeat_t6166(t *testing.T) {
	clock := &ownerForbiddenClock{now: time.Date(2026, 11, 24, 2, 0, 0, 0, time.UTC)}
	logs := &ownerForbiddenLog{}
	store := &ownerForbiddenHK{ownerFenceHK: ownerForbiddenRecord(), refusal: &fleet.BackendError{Status: http.StatusInternalServerError}}
	hub := ownerForbiddenHub(t, store, clock, logs)
	assignment := hubOwnerAssignment{Job: "forbidden-job", Node: "machine-a", Epoch: 1}

	for range 3 {
		heartbeatOwnerSeed(t, hub, assignment)
	}
	if got := store.putCount(); got != 3 {
		t.Fatalf("5xx owner write not retried on each heartbeat: PUTs=%d, want 3", got)
	}
}

func TestOwnerNewEpochIsTriedWhileOldGrantBacksOff_t6166(t *testing.T) {
	clock := &ownerForbiddenClock{now: time.Date(2026, 11, 24, 2, 0, 0, 0, time.UTC)}
	logs := &ownerForbiddenLog{}
	store := &ownerForbiddenHK{ownerFenceHK: ownerForbiddenRecord(), refusal: &fleet.BackendError{Status: http.StatusForbidden, Code: "job_host_forbidden"}}
	hub := ownerForbiddenHub(t, store, clock, logs)
	heartbeatOwnerSeed(t, hub, hubOwnerAssignment{Job: "forbidden-job", Node: "machine-a", Epoch: 1})
	if got := store.putCount(); got != 1 {
		t.Fatalf("first grant: PUTs=%d, want 1", got)
	}

	next := hubOwnerAssignment{Job: "forbidden-job", Node: "machine-b", Epoch: 2, From: "machine-a"}
	if err := hub.saveOwnerAssignment(next, false); err != nil {
		t.Fatal(err)
	}
	heartbeatOwnerSeed(t, hub, next)
	if got := store.putCount(); got != 2 {
		t.Fatalf("new epoch held back by the old grant's refusal: PUTs=%d, want 2", got)
	}
}
