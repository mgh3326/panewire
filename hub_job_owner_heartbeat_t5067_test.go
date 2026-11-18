package panewire

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

type blockingOwnerRead struct {
	*ownerFenceHK
	started chan struct{}
	release chan struct{}
	reads   atomic.Int32
}

func (s *blockingOwnerRead) Get(ctx context.Context, id string) (map[string]any, error) {
	if s.reads.Add(1) == 1 {
		close(s.started)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.ownerFenceHK.Get(ctx, id)
}

func TestJobOwnerFenceHeartbeatSeedDoesNotBlockOrDuplicate_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	store := &blockingOwnerRead{ownerFenceHK: hk, started: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(store.release) }) })
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	done := make(chan struct{})
	go func() { hub.sendHeartbeatDirectives("machine-a", agent); close(done) }()
	if !waitUntil(time.Second, func() bool {
		select {
		case <-store.started:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("seed never reached HK")
	}
	if !waitUntil(time.Second, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}) {
		t.Fatal("heartbeat waited for the blocked HK seed")
	}
	select {
	case assignment := <-agent.assignments:
		if assignment.JobID != "fenced-job" || assignment.Epoch != 1 {
			t.Fatalf("assignment=%+v", assignment)
		}
	default:
		t.Fatal("heartbeat did not deliver the existing assignment")
	}
	hub.sendHeartbeatDirectives("machine-a", agent)
	if got := store.reads.Load(); got != 1 {
		t.Fatalf("overlapping seed attempts read HK %d times", got)
	}
	release.Do(func() { close(store.release) })
	waitOwnerWork(t, hub, "fenced-job", true)
	hub.sendHeartbeatDirectives("machine-a", agent)
	waitOwnerWork(t, hub, "fenced-job", true)
	if got := store.reads.Load(); got != 1 {
		t.Fatalf("confirmed owner was read again: reads=%d", got)
	}
}

func TestJobOwnerFenceTerminalRecoveryRetiresJournal_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "completed", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	intent := hubOwnerTransfer{Job: "fenced-job", From: "machine-a", To: "machine-b", Epoch: 1}
	if err := hub.saveOwnerTransfer(intent); err != nil {
		t.Fatal(err)
	}
	hub.recoverOwnerTransfer(t.Context(), intent)
	if exists, err := hub.hasOwnerTransfer(intent); err != nil || exists {
		t.Fatalf("terminal transfer journal retained: exists=%v err=%v", exists, err)
	}
}

type failedOnceOwnerRead struct {
	*ownerFenceHK
	reads atomic.Int32
}

func (s *failedOnceOwnerRead) Get(ctx context.Context, id string) (map[string]any, error) {
	if s.reads.Add(1) == 1 {
		return nil, errors.New("temporary HK outage")
	}
	return s.ownerFenceHK.Get(ctx, id)
}

func TestJobOwnerFenceHeartbeatRetriesUnconfirmedSeed_t5067(t *testing.T) {
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(1)}}
	hub, agent := ownerFenceHub(t, hk)
	store := &failedOnceOwnerRead{ownerFenceHK: hk}
	hub.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		return &fleet.App{JobStore: store, Out: out, Err: errout}, nil
	}
	for i, want := range []int32{1, 2, 2} {
		hub.sendHeartbeatDirectives("machine-a", agent)
		waitOwnerWork(t, hub, "fenced-job", true)
		if got := store.reads.Load(); got != want {
			t.Fatalf("heartbeat %d read HK %d times, want %d", i+1, got, want)
		}
	}
}
