package panewire

import (
	"testing"
	"time"
)

// The per-account free-band gate counts live hub spawns placed onto the SWE-2
// free rungs by account: two live launches on one account, one on another, a
// paid Devin launch and an unattributed free launch (hk 4765).
func TestLiveFreeBandByAccountCountsPlacedFreeLaunches(t *testing.T) {
	h := asyncSpawnHub(t)
	now := time.Now()
	const acctA = "devin:aaaaaaaaaaaaaaaa"
	const acctB = "devin:bbbbbbbbbbbbbbbb"
	complete := func(task hubSpawnTask, profile, account string) {
		t.Helper()
		outcome := hubSpawnOutcome{RC: 0, Stage: "done", Profile: profile, Pool: "devin", AccountKey: account}
		if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
			t.Fatalf("complete %s: %v %v", task.Job, changed, err)
		}
	}
	free1 := liveOp(t, h, 1, "free-1", "auto", now)
	complete(free1, "devin-swe2", acctA)
	complete(liveOp(t, h, 2, "free-2", "auto", now), "devin-swe2-medium", acctA)
	complete(liveOp(t, h, 3, "paid", "auto", now), "devin-ds41", acctA)
	complete(liveOp(t, h, 4, "other", "auto", now), "devin-swe2", acctB)
	complete(liveOp(t, h, 5, "unattributed", "auto", now), "devin-swe2", "")

	got, err := h.liveFreeBandByAccount(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if got[acctA] != 2 || got[acctB] != 1 || len(got) != 2 {
		t.Fatalf("live free band = %v, want two launches on A and one on B", got)
	}
	got, err = h.liveFreeBandByAccount(t.Context(), free1.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if got[acctA] != 1 || got[acctB] != 1 || len(got) != 2 {
		t.Fatalf("live free band except self = %v, want the spawn being placed out", got)
	}
}
