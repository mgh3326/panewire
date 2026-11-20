package fleet

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The watcher stamps adopted_at under the job's events lock, the moment the ack
// is committed (hk 5815): a wait for the lock must not leave an earlier time on
// the ack than the one it is written at.
func TestQuotaWaitAdoptedAtIsStampedUnderTheEventsLock_t5815(t *testing.T) {
	a, _, _, _ := quotaStopFixture(t, "reviewer", "idle", "I stopped here.\n")
	base := time.Now().UTC().Truncate(time.Second)
	var mu sync.Mutex
	clock := base
	a.Now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	}
	setClock := func(at time.Time) {
		mu.Lock()
		defer mu.Unlock()
		clock = at
	}
	attempt, until := requestWait(t, a, 10*time.Minute)

	// Hold the events lock the way a sequence allocation does, so the adoption
	// queues behind it while the clock moves on.
	lockFile, err := os.OpenFile(filepath.Join(a.Jobs, "j", ".wrk-events.lock"), os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	j, err := a.job("j")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.adoptQuotaWait(ctx, j, viewLedger(j.Events), time.Time{}, base.Add(time.Minute))
	}()
	// Simulated lock wait: the adoption is held behind the lock for this window,
	// so a stamp taken before the lock reads the clock's old value.
	time.Sleep(50 * time.Millisecond)
	committed := base.Add(800 * time.Millisecond)
	setClock(committed)
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	<-done

	got, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(committed) {
		t.Fatalf("acknowledged at %s, want the commit's millisecond %s (the stamp was taken before the events lock)", got.Format(time.RFC3339Nano), committed.Format(time.RFC3339Nano))
	}
}
