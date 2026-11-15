package panewire

import (
	"testing"
	"time"
)

// The per-account gate reads live placed profiles by account: two launches on
// one account (one through the builder-devin seat), one on another, a paid
// Devin launch and an unattributed free launch (hk 4765). Selection folds the
// profiles through the seat canon; the hub reports them as placed.
func TestLiveProfilesByAccountCountsPlacedProfiles(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acctA = "devin:aaaaaaaaaaaaaaaa"
	const acctB = "devin:bbbbbbbbbbbbbbbb"
	complete := func(n int, job, profile, account string) hubSpawnTask {
		t.Helper()
		task := liveOp(t, h, n, job, "auto", now)
		outcome := hubSpawnOutcome{RC: 0, Stage: "done", Profile: profile, Pool: "devin", AccountKey: account}
		if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("complete %s: %v %v", task.Job, changed, err)
		}
		return task
	}
	free1 := complete(1, "free-1", "devin-swe2", acctA)
	complete(2, "free-2", "devin-swe2-medium", acctA)
	complete(3, "seat", "builder-devin", acctA)
	complete(4, "paid", "devin-ds41", acctA)
	complete(5, "other", "devin-swe2", acctB)
	complete(6, "unattributed", "devin-swe2", "")

	got, err := h.liveProfilesByAccount(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	wantA := map[string]int{"devin-swe2": 1, "devin-swe2-medium": 1, "builder-devin": 1, "devin-ds41": 1}
	if len(got) != 2 || len(got[acctA]) != len(wantA) || len(got[acctB]) != 1 || got[acctB]["devin-swe2"] != 1 {
		t.Fatalf("live profiles = %v, want the attributed launches by account", got)
	}
	for profile, n := range wantA {
		if got[acctA][profile] != n {
			t.Fatalf("live profiles = %v, want %v on A", got, wantA)
		}
	}
	got, err = h.liveProfilesByAccount(t.Context(), free1.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if got[acctA]["devin-swe2"] != 0 || len(got[acctA]) != 3 || len(got) != 2 {
		t.Fatalf("live profiles except self = %v, want the spawn being placed out", got)
	}
}
