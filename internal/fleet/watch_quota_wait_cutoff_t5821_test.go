package fleet

import (
	"context"
	"sync"
	"testing"
	"time"
)

// The manager judges an ack at its bound (quotaWaitAdoptBound, 30 s after the
// decision). The bound is restated here: the root package owns the constant.
const testQuotaWaitBound = 30 * time.Second

// An ack is read under the events lock (hk 6172): the watcher stamps it and writes
// it while it holds that lock, so the manager's read cannot fall between the
// stamp and the INSERT. The watcher is held there by the store's AfterGuard hook.
// The stamp is before the bound, so the manager sees the ack once the watcher's
// lock is released and accepts it.
func TestQuotaWaitAckIsReadUnderTheEventsLock_t5821(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	base := time.Now().UTC().Truncate(time.Second)
	c := installHeldClock(a, base)
	attempt, until := requestWait(t, a, 10*time.Minute)
	bound := base.Add(testQuotaWaitBound)

	stamp := base.Add(time.Second)
	c.set(stamp)
	store := a.ledger()
	held := make(chan struct{})
	release := make(chan struct{})
	var holdOnce, releaseOnce sync.Once
	releaseWatcher := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseWatcher()
	store.AfterGuard = func() {
		holdOnce.Do(func() { close(held) })
		<-release
	}
	a.Arbiter = store

	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	adopted := make(chan struct{})
	go func() {
		defer close(adopted)
		a.adoptQuotaWait(ctx, j, viewLedger(j.Events), time.Time{}, base.Add(time.Minute))
	}()
	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the watcher never reached the stamp")
	}

	// A read that returns while the watcher still holds the lock did not wait for
	// it. The window proves that negative: the manager must not return from its
	// read until the watcher's lock is released.
	type ackRead struct {
		at  time.Time
		err error
	}
	read := make(chan ackRead, 1)
	go func() {
		at, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until)
		read <- ackRead{at, err}
	}()
	select {
	case r := <-read:
		t.Fatalf("the manager read %v while the watcher held the events lock: it did not wait for the stamp and the insert", r)
	case <-time.After(time.Second):
	}

	releaseWatcher()
	got := <-read
	<-adopted
	if got.err != nil {
		t.Fatal(got.err)
	}
	if !got.at.Equal(stamp) {
		t.Fatalf("acknowledged at %s, want the stamp %s", got.at.Format(time.RFC3339Nano), stamp.Format(time.RFC3339Nano))
	}
	if got.at.After(bound) {
		t.Fatalf("an ack stamped at %s is after the bound %s and must be refused", got.at.Format(time.RFC3339Nano), bound.Format(time.RFC3339Nano))
	}
}

// An ack stamped after the manager's read is refused: the manager reads at its
// bound and sees no ack; the watcher then stamps after the bound and the ack is
// written after that read, so it cannot adopt the wait.
func TestQuotaWaitAckStampedAfterTheManagersReadIsRefused_t5821(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	base := time.Now().UTC().Truncate(time.Second)
	c := installHeldClock(a, base)
	attempt, until := requestWait(t, a, 10*time.Minute)
	bound := base.Add(testQuotaWaitBound)

	// The manager's decision at its bound: no ack yet.
	c.set(bound.Add(time.Second))
	if at, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until); err != nil || !at.IsZero() {
		t.Fatalf("the manager read %v (err %v) before any ack was written, want none", at, err)
	}

	// The watcher stamps and writes its ack after that read, past the bound.
	late := bound.Add(2 * time.Second)
	c.set(late)
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a.adoptQuotaWait(ctx, j, viewLedger(j.Events), time.Time{}, base.Add(time.Minute))

	got, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(late) || !got.After(bound) {
		t.Fatalf("the late ack reads at %s; want %s, after the bound %s, so the manager refuses it", got.Format(time.RFC3339Nano), late.Format(time.RFC3339Nano), bound.Format(time.RFC3339Nano))
	}
}
