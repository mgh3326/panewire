package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// hk 3704 (review on PR 752): the relay answer for a job must fail closed. A
// job whose events cannot be read is owed, not settled, so the at-end and TTL
// gates keep its dir. The inbox index used to skip such a job silently.

func TestJobRelayOwedKeepsWhenTheEventsCannotBeRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a 0700 events directory whatever its mode")
	}
	ctx := context.Background()
	inbox := t.TempDir()
	store := NewMemoryStore(t)
	defer store.Close()
	r20WriteEvent(t, inbox, "t3704-unreadable", "00001-job.completed.json", `{"type":"job.completed","epoch":1,"owner_lane":"lane-a","label":"agent-a","host":"host-a","report_path":"a.md","report_last_line":"done"}`, time.Time{})
	events := filepath.Join(inbox, "jobs", "t3704-unreadable", "events")
	if err := os.Chmod(events, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(events, 0o700) })

	node := r20Node(inbox, store)
	owed, err := node.jobRelayOwed(ctx, "t3704-unreadable")
	if !owed || err == nil {
		t.Fatalf("unreadable events: owed=%t err=%v, want owed with an error", owed, err)
	}
}
