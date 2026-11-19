package fleet

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/arbiter"
	"github.com/mgh3326/panewire/internal/jobowner"
)

func recordQuota(t *testing.T, a *App, job, pool string, epoch uint64) {
	t.Helper()
	if _, err := a.ledger().RecordQuotaPool(t.Context(), arbiter.QuotaRun{Job: job, Profile: "fixture", Gate: arbiter.Gate{Pool: pool}, Epoch: epoch}); err != nil {
		t.Fatalf("record %s/%s epoch %d: %v", job, pool, epoch, err)
	}
}

func wantPools(t *testing.T, a *App, job string, want ...string) {
	t.Helper()
	_, pools := ledgerState(t, a, job)
	if !slices.Equal(pools, want) {
		t.Fatalf("quota pools=%v, want %v", pools, want)
	}
}

func noAmbiguityWarning(t *testing.T, a *App) {
	t.Helper()
	if warning := a.Err.(*bytes.Buffer).String(); strings.Contains(warning, "retained") {
		t.Fatalf("a quota row was retained and logged: %q", warning)
	}
}

// moveA1B2A3 is the job's trip through the hub: A holds epoch 1, B takes it at
// 2, and A is granted it again at 3.
func moveA1B2A3(t *testing.T, a *App) {
	t.Helper()
	if err := jobowner.Revoke(a.Jobs, "j", 2); err != nil {
		t.Fatal(err)
	}
	if err := jobowner.Assign(a.Jobs, "j", 3); err != nil {
		t.Fatal(err)
	}
}

// Both attempts hold a row of their own (different pools). Each release takes
// its own and leaves the other's, whichever ends first.
func TestQuotaEpochReleaseTakesOnlyTheAttemptsOwnRow_t5294(t *testing.T) {
	for _, firstEpoch := range []uint64{1, 3} {
		t.Run("epoch "+string(rune('0'+firstEpoch))+" ends first", func(t *testing.T) {
			a := testApp(t)
			ledgerRunner(t, a)
			seedJob(t, a, "j", "implementer")
			seedLedgerQuota(t, a, "j", 1) // A1: pool "example"
			moveA1B2A3(t, a)
			recordQuota(t, a, "j", "other", 3) // A3
			own := map[uint64]string{1: "example", 3: "other"}
			secondEpoch := 4 - firstEpoch
			if err := a.releaseQuota(t.Context(), Job{ID: "j", OwnerEpoch: firstEpoch}); err != nil {
				t.Fatal(err)
			}
			wantPools(t, a, "j", own[secondEpoch])
			if err := a.releaseQuota(t.Context(), Job{ID: "j", OwnerEpoch: secondEpoch}); err != nil {
				t.Fatal(err)
			}
			wantPools(t, a, "j")
			noAmbiguityWarning(t, a)
		})
	}
}

// The same pool twice: A3's acquire takes over A1's row (the hub revoked A1),
// and A1's late release leaves A3's row alone. Nothing is left once A3 ends.
func TestQuotaEpochLateOldReleaseLeavesNewAttemptsRow_t5294(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	seedJob(t, a, "j", "implementer")
	seedLedgerQuota(t, a, "j", 1)
	moveA1B2A3(t, a)
	recordQuota(t, a, "j", "example", 3)
	if err := a.releaseQuota(t.Context(), Job{ID: "j", OwnerEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	wantPools(t, a, "j", "example")
	if err := a.releaseQuota(t.Context(), Job{ID: "j", OwnerEpoch: 3}); err != nil {
		t.Fatal(err)
	}
	wantPools(t, a, "j")
	if st, _ := ledgerState(t, a, "j"); st != "released" {
		t.Fatalf("job state %s, want released", st)
	}
	noAmbiguityWarning(t, a)
}

// The old attempt releases before the new one acquires: the usual order.
func TestQuotaEpochOldReleaseThenNewAcquire_t5294(t *testing.T) {
	a := testApp(t)
	ledgerRunner(t, a)
	seedJob(t, a, "j", "implementer")
	seedLedgerQuota(t, a, "j", 1)
	moveA1B2A3(t, a)
	if err := a.releaseQuota(t.Context(), Job{ID: "j", OwnerEpoch: 1}); err != nil {
		t.Fatal(err)
	}
	wantPools(t, a, "j")
	recordQuota(t, a, "j", "example", 3)
	if err := a.releaseQuota(t.Context(), Job{ID: "j", OwnerEpoch: 3}); err != nil {
		t.Fatal(err)
	}
	wantPools(t, a, "j")
	noAmbiguityWarning(t, a)
}

// A row recorded before epochs existed is released when the job's current
// attempt has ended here, and never because a grant happens to match.
func TestQuotaEpochLessRowReleasedOnlyByTerminalEnd_t5294(t *testing.T) {
	legacyJob := func(t *testing.T) *App {
		a := testApp(t)
		ledgerRunner(t, a)
		seedJob(t, a, "j", "implementer")
		seedLedgerQuota(t, a, "j", 0)
		return a
	}
	snapshot := func(t *testing.T, a *App) Job {
		j, err := a.job("j")
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	t.Run("the current attempt's end releases it whatever the grant", func(t *testing.T) {
		for _, grant := range []uint64{1, 3} {
			a := legacyJob(t)
			if grant > 1 {
				if err := jobowner.Assign(a.Jobs, "j", grant); err != nil {
					t.Fatal(err)
				}
			}
			putEvent(t, a, "j", "job.lost", map[string]any{})
			if err := a.releaseQuota(t.Context(), snapshot(t, a)); err != nil {
				t.Fatal(err)
			}
			wantPools(t, a, "j")
		}
	})
	t.Run("a live job's row is not released by a matching grant", func(t *testing.T) {
		a := legacyJob(t)
		if err := a.releaseQuota(t.Context(), snapshot(t, a)); err != nil {
			t.Fatal(err)
		}
		wantPools(t, a, "j", "example")
	})
	t.Run("a hub revocation keeps it even with a matching grant", func(t *testing.T) {
		a := legacyJob(t)
		putEvent(t, a, "j", "job.lost", map[string]any{})
		if err := jobowner.Revoke(a.Jobs, "j", 2); err != nil {
			t.Fatal(err)
		}
		if err := a.releaseQuota(t.Context(), snapshot(t, a)); err != nil {
			t.Fatal(err)
		}
		wantPools(t, a, "j", "example")
	})
	// A1 is revoked and the job returns to A3, whose new spawn clears the
	// revocation. A delayed A1 watcher must not free the row A3 may be using.
	t.Run("a delayed watcher of an older attempt keeps it after the job started again", func(t *testing.T) {
		a := legacyJob(t)
		putEvent(t, a, "j", "job.lost", map[string]any{})
		a1 := snapshot(t, a)
		moveA1B2A3(t, a)
		putEvent(t, a, "j", "job.claim", map[string]any{"owner_lane": "owner", "t_level": "T1", "role": "implementer"})
		putEvent(t, a, "j", "job.spawned", map[string]any{"pane_id": "w:p2", "tab_id": "w:t1", "label": "agent-a", "profile": "cmd-dsflash41", "family": "deepseek"})
		if current := snapshot(t, a); current.Ended != "" || current.Reassigned {
			t.Fatalf("setup: the job did not start again: %+v", current)
		}
		if err := a.releaseQuota(t.Context(), a1); err != nil {
			t.Fatal(err)
		}
		wantPools(t, a, "j", "example")
	})
}

// A spawn records the grant it starts under, and a failed spawn's cleanup
// releases that same row.
func TestQuotaEpochSpawnRecordsTheGrantItStartsUnder_t5294(t *testing.T) {
	for _, grant := range []uint64{0, 3} {
		a, s, _, _ := cmdFixture(t)
		want := uint64(1) // a job never reassigned holds the initial hub epoch
		if grant > 0 {
			if err := jobowner.Assign(a.Jobs, s.Job, grant); err != nil {
				t.Fatal(err)
			}
			want = grant
		}
		if e := a.Spawn(t.Context(), s); e != nil {
			t.Fatal(e)
		}
		epoch, err := a.ledger().QuotaEpoch(t.Context(), s.Job, "example")
		if err != nil || epoch != want {
			t.Fatalf("grant %d: recorded epoch %d err=%v, want %d", grant, epoch, err, want)
		}
	}
}
