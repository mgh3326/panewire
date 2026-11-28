package panewire

// hk 7101: the lane binding in force at a receipt is the one first sighted at or
// before the receipt, whatever receipt was bound last. The cases use the rig of
// relay_stale_notice_t6924_test.go.

import (
	"context"
	"path/filepath"
	"testing"
)

// hk 7101 #1: two receipts across a job change arrive out of order. The late
// receipt (job B, t=100) binds B first; the early receipt (job B, t=50) then
// arrives while A was still in force at t=50. The early receipt must bind B at
// t=50, so a row received at t=60 resolves to B. Killed by: comparing the
// receipt's job with the newest binding of the lane instead of the binding in
// force at its own time, which skips the t=50 binding and leaves [50,100) on A.
func TestT7101BindingInForceAtReceiptTimeOutOfOrder(t *testing.T) {
	rig := newT6924Rig(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	store := rig.hub.inbox.store
	ctx := context.Background()
	const lane = "t7101-lane"
	const jobA, jobB = "t7101-job-a", "t7101-job-b"
	if err := store.bindStaleReplayLane(ctx, lane, jobA, 0); err != nil {
		t.Fatal(err)
	}
	// The late receipt arrives first.
	if err := store.bindStaleReplayLane(ctx, lane, jobB, 100); err != nil {
		t.Fatal(err)
	}
	// Then the early receipt, whose time was A's stint.
	if err := store.bindStaleReplayLane(ctx, lane, jobB, 50); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		at   int64
		want string
	}{{at: 25, want: jobA}, {at: 60, want: jobB}, {at: 150, want: jobB}} {
		got, found, err := store.staleReplayJobAt(ctx, lane, c.at)
		if err != nil {
			t.Fatal(err)
		}
		if !found || got != c.want {
			t.Fatalf("job at t=%d is %q (found=%v), want %q: a stint's start was skipped", c.at, got, found, c.want)
		}
	}
}
