package fleet

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// F2 (hk 5063): PendingHubCloseJobs skips placement-only dirs, which never ran
// on this host and have no hub-close intent. Before, they were listed as
// pending because foldJob failed on a ledger with no claim or receipt.
func TestPendingHubCloseJobsSkipsHubPlacementOnly_t5063(t *testing.T) {
	a, _ := hubCloseFixture(t)
	// Seed a hub-side placement-only job directory.
	if err := a.event(context.Background(), "placement-only", "job.placement", map[string]any{
		"source": "auto", "chose": "ncp",
	}); err != nil {
		t.Fatal(err)
	}
	pending, err := PendingHubCloseJobs(a.Jobs)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(pending, "placement-only") {
		t.Fatalf("PendingHubCloseJobs returned placement-only job: %v", pending)
	}

	// A regular job with an open hub-close intent is still listed.
	in := liveIntent(t, a)
	recordHubClose(t, a, in)
	pending, err = PendingHubCloseJobs(a.Jobs)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(pending, []string{"j"}) {
		t.Fatalf("PendingHubCloseJobs = %v, want [j]", pending)
	}
}

// F6 / M3b (hk 5063): a failed end-lock take in lockUnlessHubCloseOwnsEnd is
// treated as owned (fail closed), so generic ends do not touch the job.
func TestGenericEndOwnershipUnderFailedEndLock_t5063(t *testing.T) {
	a, _ := hubCloseFixture(t)
	// Pre-take the end lock.
	unlock, err := LockJobEnd(filepath.Join(a.Jobs, "j"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	owned, rel := a.lockUnlessHubCloseOwnsEnd(ctx, "j")
	if !owned || rel != nil {
		t.Fatalf("lockUnlessHubCloseOwnsEnd = (%v, %t), want (true, nil) on end-lock take failure", owned, rel != nil)
	}
}
