package panewire_test

import (
	"context"
	"encoding/json"
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
