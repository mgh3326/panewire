package panewire_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	panewire "github.com/mgh3326/panewire"
)

func TestEventLogSubscriptionsAndInboxPersist(t *testing.T) {
	store := panewire.NewMemoryStore(t)
	ctx := context.Background()
	for _, kind := range []string{"pane.agent_status_changed", "pane.output_matched", "pane.scroll_changed"} {
		if err := store.RecordEvent(ctx, panewire.Event{Source: "herdr", Kind: kind, PaneID: "p1", WorkspaceID: "w1", Revision: 7, Payload: json.RawMessage(`{"type":"` + kind + `"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.RecordEvent(ctx, panewire.Event{Source: "inbox", Kind: "inbox.file_created", Path: "/tmp/job.md", Payload: json.RawMessage(`{"size":12}`)}); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordEvent(ctx, panewire.Event{Source: "inbox", Kind: "inbox.file_changed", Path: "/tmp/job.md", Payload: json.RawMessage(`{"size":19}`)}); err != nil {
		t.Fatal(err)
	}
	if got := store.CountEvents(); got != 5 {
		t.Fatalf("event rows=%d, want 5", got)
	}
	if got := store.CountEventKind("pane.output_matched"); got != 1 {
		t.Fatalf("output rows=%d, want 1", got)
	}
	if store.ContainsPayload("private fixture body") {
		t.Fatal("event payload must not contain file contents")
	}

	// Re-opening the same SQLite file must retain the event history.
	path := store.Path()
	store.Close()
	reopened, err := panewire.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.CountEvents(); got != 5 {
		t.Fatalf("persisted event rows=%d, want 5", got)
	}
	_ = time.Now() // keep this fixture explicitly wall-clock independent
}

func TestUnknownHerdrEventAndFieldArePreservedAsWarningMetadata(t *testing.T) {
	ev, ok := panewire.DecodeHerdrEvent([]byte(`{"event":{"type":"pane.future_changed","pane_id":"p1","workspace_id":"w1","revision":8,"future_field":"fixture-only"}}`))
	if !ok || ev.Kind != "pane.future_changed" || string(ev.UnknownFields) == "{}" || !strings.Contains(string(ev.UnknownFields), "future_field") {
		t.Fatalf("event=%+v ok=%v", ev, ok)
	}
}

func TestRealSubscriptionEnvelopeUsesStringEventAndDataObject(t *testing.T) {
	ev, ok := panewire.DecodeHerdrEvent([]byte(`{"event":"pane_agent_status_changed","data":{"pane_id":"p1","workspace_id":"w1","agent_status":"idle","revision":9}}`))
	if !ok || ev.Kind != "pane_agent_status_changed" || ev.PaneID != "p1" || ev.AgentStatus != "idle" || ev.Revision != 9 {
		t.Fatalf("event=%+v ok=%v", ev, ok)
	}
}

func TestIdleWakeSnapshotUsesRealAgentShapeAndTabListLabel(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(true))
	defer fixture.Close()
	fixture.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{
			"agent": "codex", "pane_id": "workspace:pane-1", "workspace_id": "workspace", "tab_id": "workspace:tab-1", "agent_status": "working", "revision": 42,
		}}}
	})
	fixture.On("tab.list", func() any {
		return map[string]any{"tabs": []any{map[string]any{"tab_id": "workspace:tab-1", "workspace_id": "workspace", "label": "worker-synthetic"}}}
	})
	client, err := panewire.NewHerdrClient(fixture.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	states, err := client.AgentStates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0].PaneID != "workspace:pane-1" || states[0].WorkspaceID != "workspace" || states[0].Label != "worker-synthetic" || states[0].Status != "working" || states[0].Revision != 42 || !states[0].Authoritative {
		t.Fatalf("agent states=%+v", states)
	}
}

func TestInboxWatcherRecordsCreateAndChange(t *testing.T) {
	// This fixture exercises the fsnotify watcher specifically. R20 defaults
	// macOS to poll, so the mode is named rather than inherited from GOOS.
	t.Setenv("PANEWIRE_INBOX_WATCH", "fsnotify")
	store := panewire.NewMemoryStore(t)
	defer store.Close()
	root := t.TempDir()
	watcher, err := panewire.NewInboxWatcher(root, store)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = watcher.Run(ctx) }()
	path := filepath.Join(root, "answer.md")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(func() bool { return store.CountEventKind("inbox.file_created") >= 1 }, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("fixture changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := waitFor(func() bool { return store.CountEventKind("inbox.file_changed") >= 1 }, time.Second); err != nil {
		t.Fatal(err)
	}
}

func waitFor(condition func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("condition not observed before timeout")
}
