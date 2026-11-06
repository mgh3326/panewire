package shared

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// The free-tier tests run the generic flow with a free-tier policy over the
// fake store's one account; devin is the pool that has one.

func freeState(state string, resets *time.Time) *FreeBucket {
	return &FreeBucket{State: state, ResetsAt: resets}
}

func at(d time.Duration) *time.Time { t := testNow.Add(d); return &t }

func localFree(b *FreeBucket) func(time.Time) *FreeBucket {
	return func(time.Time) *FreeBucket { return b }
}

func newFreeFlow(local *FreeBucket) *Flow {
	f := newFlow()
	f.LocalFree = localFree(local)
	return f
}

// freeStored is a stored reading of the free tier alone, as a devin account
// whose paid probe produced nothing publishes it.
func freeStored(measured time.Time, free *FreeBucket) *Observation {
	return &Observation{ID: 9, Kind: KindAuto, MeasuredAt: measured, Buckets: []Bucket{}, FreeBucket: free}
}

func failingMeasurement(err error, calls *int) Measurer {
	return func(context.Context) (quota.Observation, error) {
		*calls++
		return quota.Observation{}, err
	}
}

func TestFreeFreshReadingIsReusedWithoutMeasuring(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = freeStored(testNow.Add(-time.Minute), freeState(FreeAvailable, nil))
	reading, err := newFreeFlow(freeState(FreeAvailable, nil)).Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t))
	if err != nil || reading.Free == nil || reading.Free.State != FreeAvailable || len(reading.Buckets) != 0 {
		t.Fatalf("reading %+v err=%v", reading, err)
	}
	if got := hk.callLog(); strings.Contains(got, "lease") || strings.Contains(got, "observation") {
		t.Fatalf("a fresh reading took the lease or published: %s", got)
	}
}

// A reading without a free bucket is not current for a pool that has one: a
// plain paid reading (another writer's) is measured over.
func TestFreeReadingWithoutFreeBucketIsNotFresh(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = stored(testNow.Add(-time.Minute), 30)
	var measured int
	reading, err := newFreeFlow(freeState(FreeAvailable, nil)).Refresh(t.Context(), hk.client(), testKey, goodMeasurement(&measured))
	if err != nil || measured != 1 || reading.Free == nil || len(reading.Buckets) != 2 {
		t.Fatalf("reading %+v err=%v measured=%d", reading, err, measured)
	}
	if len(hk.pub) != 1 || hk.pub[0]["free_bucket"] == nil {
		t.Fatalf("published %v", hk.pub)
	}
}

// An exceed recorded on this host reaches the store even while the stored
// reading is fresh.
func TestFreeLocalExceedIsPublishedOverAFreshReading(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = freeStored(testNow.Add(-time.Minute), freeState(FreeAvailable, nil))
	var measured int
	reading, err := newFreeFlow(freeState(FreeExhausted, at(3*time.Hour))).Refresh(t.Context(), hk.client(), testKey, failingMeasurement(errors.New("x"), &measured))
	if err != nil || reading.Free == nil || reading.Free.State != FreeExhausted || !reading.Free.ResetsAt.Equal(*at(3 * time.Hour)) {
		t.Fatalf("reading %+v err=%v", reading, err)
	}
	free := hk.pub[0]["free_bucket"].(map[string]any)
	if free["state"] != "exhausted" || free["resets_at"] != "2026-11-05T15:00:00Z" {
		t.Fatalf("published free bucket %v", free)
	}
}

// A host without the exceed must not clear another host's active hold.
func TestFreeMergeKeepsAnotherHostsLaterHold(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = freeStored(testNow.Add(-time.Hour), freeState(FreeExhausted, at(2*time.Hour)))
	var measured int
	reading, err := newFreeFlow(freeState(FreeAvailable, nil)).Refresh(t.Context(), hk.client(), testKey, failingMeasurement(errors.New("x"), &measured))
	if err != nil || reading.Free.State != FreeExhausted || !reading.Free.ResetsAt.Equal(*at(2 * time.Hour)) {
		t.Fatalf("reading %+v err=%v", reading, err)
	}
}

// The free tier does not depend on the paid probe: a failed probe still
// publishes the free state with no paid window, releases the lease, and is not
// an error (pacelever's rule).
func TestFreeFailedPaidMeasurementStillPublishesTheFreeState(t *testing.T) {
	for name, err := range map[string]error{
		"auth":      quota.Fail(quota.ErrKindAuthFailed, "no"),
		"probe":     quota.Fail(quota.ErrKindProbe, "probe timeout stage=no-prompt"),
		"not quota": errors.New("boom"),
	} {
		t.Run(name, func(t *testing.T) {
			hk := newFakeHK(t)
			hk.latest = freeStored(testNow.Add(-time.Hour), freeState(FreeAvailable, nil))
			var measured int
			reading, refreshErr := newFreeFlow(freeState(FreeAvailable, nil)).Refresh(t.Context(), hk.client(), testKey, failingMeasurement(err, &measured))
			if refreshErr != nil || measured != 1 || reading.Free == nil || len(reading.Buckets) != 0 {
				t.Fatalf("reading %+v err=%v measured=%d", reading, refreshErr, measured)
			}
			if len(hk.pub) != 1 || len(hk.pub[0]["buckets"].([]any)) != 0 || hk.pub[0]["free_bucket"].(map[string]any)["state"] != "available" {
				t.Fatalf("published %v", hk.pub)
			}
			if got := hk.callLog(); !strings.Contains(got, "PUT /v1/quota/accounts/"+testKey+"/observation\nDELETE /v1/quota/accounts/"+testKey+"/lease") {
				t.Fatalf("the lease was not released after the publication:\n%s", got)
			}
		})
	}
}

// A rate-limited paid probe backs the account off, and the backoff skips the
// probe but not the free-state publication.
func TestFreeRateLimitBacksOffTheProbeNotThePublication(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = freeStored(testNow.Add(-time.Hour), freeState(FreeAvailable, nil))
	flow := newFreeFlow(freeState(FreeAvailable, nil))
	var measured int
	limited := quota.Fail(quota.ErrKindRateLimited, "usage HTTP 429")
	limited.RetryAfter = time.Minute
	if _, err := flow.Refresh(t.Context(), hk.client(), testKey, failingMeasurement(limited, &measured)); err != nil || measured != 1 {
		t.Fatalf("first refresh err=%v measured=%d", err, measured)
	}
	hk.mu.Lock()
	hk.latest = freeStored(testNow.Add(-time.Hour), freeState(FreeAvailable, nil)) // stale again
	hk.mu.Unlock()
	reading, err := flow.Refresh(t.Context(), hk.client(), testKey, failingMeasurement(errors.New("must not be probed"), &measured))
	if err != nil || measured != 1 || reading.Free == nil {
		t.Fatalf("second refresh %+v err=%v measured=%d", reading, err, measured)
	}
	if len(hk.pub) != 2 {
		t.Fatalf("publications: %d", len(hk.pub))
	}
}

// A paid measurement of another account still refuses to publish.
func TestFreeMeasuredAccountMustMatch(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = freeStored(testNow.Add(-time.Hour), freeState(FreeAvailable, nil))
	wrong := func(context.Context) (quota.Observation, error) {
		o, _ := goodMeasurement(new(int))(context.Background())
		o.AccountFP = "fedcba9876543210"
		return o, nil
	}
	_, err := newFreeFlow(freeState(FreeAvailable, nil)).Refresh(t.Context(), hk.client(), testKey, wrong)
	if quota.KindOf(err) != quota.ErrKindAccountMismatch || len(hk.pub) != 0 {
		t.Fatalf("err=%v published=%v", err, hk.pub)
	}
}

// Lease held elsewhere never blocks past MaxWait, and the host measures and
// publishes nothing meanwhile.
func TestFreeLeaseHeldElsewhereIsWaitedOnOnlyUpToMaxWait(t *testing.T) {
	hk := newFakeHK(t)
	hk.latest = freeStored(testNow.Add(-time.Hour), freeState(FreeAvailable, nil))
	hk.onLease = func(h *fakeHK, n int, holder string) (Lease, bool) {
		return Lease{Holder: "other-host", ExpiresAt: time.Now().Add(10 * time.Minute)}, true
	}
	flow := newFreeFlow(freeState(FreeAvailable, nil))
	flow.MaxWait = 40 * time.Millisecond
	start := time.Now()
	_, err := flow.Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t))
	if quota.KindOf(err) != quota.ErrKindSharedPending {
		t.Fatalf("err = %v, want shared_pending", err)
	}
	// An upper bound only: the lease expires in ten minutes, the bound is 40 ms.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("waited %s past a 40 ms bound", elapsed)
	}
	if len(hk.pub) != 0 || hk.count("DELETE /v1/quota/accounts/"+testKey+"/lease") != 0 {
		t.Fatalf("a host without the lease published or released: %s", hk.callLog())
	}
}

// Without a MaxWait the wait keeps the two-lease-window bound.
func TestMaxWaitDefaultsToTwoLeaseWindows(t *testing.T) {
	f := &Flow{LeaseTTL: 30}
	if got := f.maxWait(); got != 60*time.Second {
		t.Fatalf("default wait %s", got)
	}
	f.MaxWait = time.Second
	if got := f.maxWait(); got != time.Second {
		t.Fatalf("explicit wait %s", got)
	}
}

func TestNormalizeFree(t *testing.T) {
	for _, test := range []struct {
		name  string
		in    *FreeBucket
		state string
		valid bool
	}{
		{"absent", nil, "", true},
		{"available", freeState(FreeAvailable, nil), FreeAvailable, true},
		{"available with a reset", freeState(FreeAvailable, at(time.Hour)), FreeAvailable, false},
		{"unknown", freeState(FreeUnknown, nil), FreeUnknown, true},
		{"exhausted", freeState(FreeExhausted, at(time.Hour)), FreeExhausted, true},
		{"exhausted without a reset", freeState(FreeExhausted, nil), "", false},
		{"exhausted, reset passed", freeState(FreeExhausted, at(-time.Minute)), FreeAvailable, true},
		{"other state", freeState("full", nil), "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, valid := normalizeFree(test.in, testNow)
			state := ""
			if got != nil {
				state = got.State
			}
			if valid != test.valid || state != test.state {
				t.Fatalf("got %v valid=%t, want %q valid=%t", got, valid, test.state, test.valid)
			}
		})
	}
}

func TestFreeNeedsPublish(t *testing.T) {
	for _, test := range []struct {
		name          string
		stored, local *FreeBucket
		want          bool
	}{
		{"nothing stored", nil, freeState(FreeAvailable, nil), true},
		{"both available", freeState(FreeAvailable, nil), freeState(FreeAvailable, nil), false},
		{"local exceed over available", freeState(FreeAvailable, nil), freeState(FreeExhausted, at(time.Hour)), true},
		{"local exceed ends later", freeState(FreeExhausted, at(time.Hour)), freeState(FreeExhausted, at(2*time.Hour)), true},
		{"stored hold ends later", freeState(FreeExhausted, at(2*time.Hour)), freeState(FreeExhausted, at(time.Hour)), false},
		{"local available does not clear a hold", freeState(FreeExhausted, at(time.Hour)), freeState(FreeAvailable, nil), false},
		{"local damaged over available", freeState(FreeAvailable, nil), freeState(FreeUnknown, nil), true},
		{"local damaged over a hold", freeState(FreeExhausted, at(time.Hour)), freeState(FreeUnknown, nil), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := freeNeedsPublish(test.stored, test.local, testNow); got != test.want {
				t.Fatalf("freeNeedsPublish = %t, want %t", got, test.want)
			}
		})
	}
}

// fakeWait is the wait's wall clock and pause on virtual time: Sleep records
// each requested pause and moves the clock by it, so the whole wait runs in no
// time and the bound is exact.
type fakeWait struct {
	now    time.Time
	sleeps []time.Duration
}

func (w *fakeWait) wall() time.Time { return w.now }

func (w *fakeWait) sleep(_ context.Context, d time.Duration) error {
	w.sleeps = append(w.sleeps, d)
	w.now = w.now.Add(d)
	return nil
}

func (w *fakeWait) total() time.Duration {
	var sum time.Duration
	for _, d := range w.sleeps {
		sum += d
	}
	return sum
}

// MaxWait is a hard bound, not a check between pauses: each pause is cut to
// what is left of it, and once it has passed the refresh reads once more and
// gives up without sleeping again. Whatever the poll interval, the total slept
// never exceeds MaxWait.
func TestMaxWaitIsAHardBoundOnTheWholeWait(t *testing.T) {
	for _, test := range []struct {
		name          string
		wait, max     time.Duration
		wantSleeps    []time.Duration
		freeTierFlows bool
	}{
		{"pause cut to the remainder", 2 * time.Second, 5 * time.Second, []time.Duration{2 * time.Second, 2 * time.Second, time.Second}, false},
		{"pause longer than the bound", 10 * time.Second, 3 * time.Second, []time.Duration{3 * time.Second}, false},
		{"bound is a whole number of pauses", time.Second, 3 * time.Second, []time.Duration{time.Second, time.Second, time.Second}, false},
		{"free-tier pool", 2 * time.Second, 5 * time.Second, []time.Duration{2 * time.Second, 2 * time.Second, time.Second}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			hk := newFakeHK(t)
			hk.latest = freeStored(testNow.Add(-time.Hour), freeState(FreeAvailable, nil))
			if !test.freeTierFlows {
				hk.latest = stored(testNow.Add(-time.Hour), 30)
			}
			hk.onLease = func(h *fakeHK, n int, holder string) (Lease, bool) {
				return Lease{Holder: "other-host", ExpiresAt: time.Now().Add(time.Hour)}, true
			}
			clock := &fakeWait{now: time.Now()}
			flow := newFlow()
			if test.freeTierFlows {
				flow = newFreeFlow(freeState(FreeAvailable, nil))
			}
			flow.Wait, flow.MaxWait, flow.Wall, flow.Sleep = test.wait, test.max, clock.wall, clock.sleep
			_, err := flow.Refresh(t.Context(), hk.client(), testKey, mustMeasureNothing(t))
			if quota.KindOf(err) != quota.ErrKindSharedPending {
				t.Fatalf("err = %v, want shared_pending", err)
			}
			if clock.total() > test.max {
				t.Fatalf("slept %s in %v, past the %s bound", clock.total(), clock.sleeps, test.max)
			}
			if len(clock.sleeps) != len(test.wantSleeps) {
				t.Fatalf("pauses %v, want %v", clock.sleeps, test.wantSleeps)
			}
			for i, d := range clock.sleeps {
				if d != test.wantSleeps[i] {
					t.Fatalf("pauses %v, want %v", clock.sleeps, test.wantSleeps)
				}
			}
			// The last read after the bound is a read, not a sleep: it was made.
			if got := hk.latestCalls; got != len(test.wantSleeps)+2 {
				t.Fatalf("%d store reads for %d pauses, want one before, one per pause and one after the bound", got, len(test.wantSleeps))
			}
			if len(hk.pub) != 0 {
				t.Fatal("published although another host held the lease")
			}
		})
	}
}
