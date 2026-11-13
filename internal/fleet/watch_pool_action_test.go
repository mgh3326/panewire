package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func writePoolActionMarker(t *testing.T, a *App, job string, at time.Time) {
	t.Helper()
	dir := filepath.Join(a.Jobs, job)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"action_id": "0b3f4b1e-0000-4000-8000-000000000001:1", "end_reason": "quota_stop pool=devin", "at": at.UTC().Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, PoolActionFile), body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPoolActionOpenFollowsTheMarker(t *testing.T) {
	a := testApp(t)
	if a.poolActionOpen("j") {
		t.Fatal("a job with no marker read as registered")
	}
	writePoolActionMarker(t, a, "j", a.Now())
	if !a.poolActionOpen("j") {
		t.Fatal("a fresh marker did not read as registered")
	}
	writePoolActionMarker(t, a, "j", a.Now().Add(-poolActionMarkerTTL-time.Minute))
	if a.poolActionOpen("j") {
		t.Fatal("a marker older than its bound still held the watcher")
	}
	if err := os.WriteFile(filepath.Join(a.Jobs, "j", PoolActionFile), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if a.poolActionOpen("j") {
		t.Fatal("an unreadable marker read as registered")
	}
}

// While the hub owns an action for a job, the watcher does not write the plain
// lost record its missing-pane fallback would: that record could never carry the
// quota reason or a hand-over's successor pointer.
func TestWatcherLeavesTheTerminalRecordToARegisteredPoolAction(t *testing.T) {
	for _, test := range []struct {
		name     string
		marker   bool
		wantLost bool
	}{
		{"registered action", true, false},
		{"no marker", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := testApp(t)
			ledgerRunner(t, a) // herdr lists no panes, so the job's pane is missing
			seedJob(t, a, "j", "worker")
			seedLedger(t, a, "j")
			if test.marker {
				writePoolActionMarker(t, a, "j", a.Now())
			}
			sleeps := 0
			a.Sleep = func(ctx context.Context, _ time.Duration) error {
				sleeps++
				if sleeps >= 4 {
					return context.Canceled
				}
				return nil
			}
			_ = a.watch(context.Background(), []string{"j"})
			lost := slices.Contains(eventKindsOf(t, a, "j"), "job.lost")
			if lost != test.wantLost {
				t.Fatalf("job.lost recorded=%v, want %v", lost, test.wantLost)
			}
		})
	}
}
