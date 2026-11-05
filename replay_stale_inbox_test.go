package panewire

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A relay row that stayed undelivered in handoffkeep past the replay window is
// retired at restart, not migrated into the inbox again after the inbox's own
// evidence for it was pruned (hk 2976: 11-02 events reappeared on 11-05).
func TestReplayRetiresStaleRowsBeforeInboxMigration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		age       time.Duration
		wantItems int
		retired   bool
	}{
		{"stale", 72 * time.Hour, 0, true},
		{"fresh", time.Hour, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake, client, closeServer := newFakeHandoffkeep(t)
			t.Cleanup(closeServer)
			path := filepath.Join(t.TempDir(), "inbox.sqlite3")
			now := time.Now().UTC()
			h, _ := quietHub(t, path, "default", client, &now)
			row := handoffkeepRelayEvent{ID: 4242, Kind: "lane.event", Epoch: 1, OwnerLane: "lane-a", EventID: "old-info", Text: "INFO merged", ReceivedAt: now.Add(-tc.age).Format(time.RFC3339Nano)}
			// Retention pruned the inbox evidence.
			if _, err := h.inbox.store.db.Exec(`DELETE FROM lane_inbox`); err != nil {
				t.Fatal(err)
			}
			h.Close()
			h2, _ := quietHub(t, path, "default", client, &now)
			t.Cleanup(func() { _ = h2.Close() })
			h2.replayRelayEvent(row, relayReplaySourceStartup)
			items, err := h2.inbox.store.list("lane-a", false)
			if err != nil {
				t.Fatal(err)
			}
			if len(items) != tc.wantItems {
				t.Fatalf("inbox items=%d want %d: %+v", len(items), tc.wantItems, items)
			}
			retired := false
			fake.mu.Lock()
			for _, call := range fake.calls {
				if call.Method == "POST" && strings.Contains(call.Path, "/4242/delivered") && strings.HasPrefix(asString(call.Body["pane"]), "replay-retired:stale") {
					retired = true
				}
			}
			fake.mu.Unlock()
			if retired != tc.retired {
				t.Fatalf("retired=%v want %v", retired, tc.retired)
			}
		})
	}
}
