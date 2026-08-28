package panewire_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	panewire "github.com/mgh3326/panewire"
)

func TestFileWaitRequiresSettleAndHashesOnce(t *testing.T) {
	store := panewire.NewMemoryStore(t)
	defer store.Close()
	path := filepath.Join(t.TempDir(), "answer.md")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go func() {
		time.Sleep(30 * time.Millisecond)
		_ = os.WriteFile(path, []byte("partial fixture body"), 0600)
		time.Sleep(60 * time.Millisecond)
		_ = os.WriteFile(path, []byte("final fixture body"), 0600)
	}()
	result, err := panewire.WaitFile(ctx, store, path, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte("final fixture body"))
	if result.Digest != hex.EncodeToString(want[:]) || result.DigestReads != 1 {
		t.Fatalf("result=%+v", result)
	}
	if store.ContainsPayload("final fixture body") {
		t.Fatal("file body must not be persisted")
	}
}

func TestFileWaitTimeoutUsesExitCodeThree(t *testing.T) {
	store := panewire.NewMemoryStore(t)
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := panewire.WaitFile(ctx, store, filepath.Join(t.TempDir(), "missing"), 0)
	if panewire.ExitCode(err) != panewire.ExitTimeout {
		t.Fatalf("err=%v code=%d", err, panewire.ExitCode(err))
	}
}

func TestAgentWaitSnapshotsThenResetsSettleOnFlap(t *testing.T) {
	fixture := newHerdrFixture(t, fixtureSchema(true, true))
	defer fixture.Close()
	fixture.On("agent.list", func() any {
		return map[string]any{"type": "agent_list", "agents": []any{map[string]any{"agent": "build", "pane_id": "p1", "workspace_id": "w1", "agent_status": "working", "revision": 10}}}
	})
	fixture.On("events.subscribe", func() any {
		go func() {
			fixture.Event(map[string]any{"event": map[string]any{"type": "pane_agent_status_changed", "pane_id": "p1", "workspace_id": "w1", "agent_status": "idle"}, "data": map[string]any{"agent_status": "idle"}})
			time.Sleep(40 * time.Millisecond)
			fixture.Event(map[string]any{"event": map[string]any{"type": "pane_agent_status_changed", "pane_id": "p1", "workspace_id": "w1", "agent_status": "working"}, "data": map[string]any{"agent_status": "working"}})
		}()
		return map[string]any{"type": "subscription_started"}
	})
	client, err := panewire.NewHerdrClient(fixture.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result, err := panewire.WaitAgent(context.Background(), client, "build", "working", 80*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "working" || result.SettleResets != 1 {
		t.Fatalf("result=%+v", result)
	}
}
