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
	c := installHeldClock(a, base)
	attempt, until := requestWait(t, a, 10*time.Minute)

	committed := base.Add(800 * time.Millisecond)
	adoptWhileLockHeld(t, a, c, base.Add(time.Minute), committed)

	got, err := QuotaWaitAckedAt(a.Jobs, "j", attempt, until)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(committed) {
		t.Fatalf("acknowledged at %s, want the commit's millisecond %s (the stamp was taken before the events lock)", got.Format(time.RFC3339Nano), committed.Format(time.RFC3339Nano))
	}
}

// heldClock is the App's clock for a test that must order a clock read against
// the events lock. A read returns the current value; the first read after armRead
// closes the channel armRead returned, after it has captured the value. set moves
// the clock for later reads.
type heldClock struct {
	mu   sync.Mutex
	now  time.Time
	read chan struct{}
}

// installHeldClock makes a's clock a heldClock that starts at start.
func installHeldClock(a *App, start time.Time) *heldClock {
	c := &heldClock{now: start}
	a.Now = func() time.Time {
		c.mu.Lock()
		defer c.mu.Unlock()
		at := c.now
		if c.read != nil {
			close(c.read)
			c.read = nil
		}
		return at
	}
	return c
}

// armRead returns a channel that closes on the next read of the clock.
func (c *heldClock) armRead() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.read = make(chan struct{})
	return c.read
}

// set moves the clock for the reads after it.
func (c *heldClock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
}

// adoptWhileLockHeld runs the watcher's adoption of job "j" while the test holds
// the job's events lock, the lock every sequence allocation takes. The adoption
// reads the clock before it asks for the lock (the first read, which the test
// waits for, so no guard can run yet); the test then sets the clock to commit and
// releases the lock, so the guard reads commit. It returns the deadline and the
// wait adoptQuotaWait returned. Nothing sleeps: the ordering is the lock's.
func adoptWhileLockHeld(t *testing.T, a *App, c *heldClock, deadline, commit time.Time) (time.Time, time.Time) {
	t.Helper()
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
	read := c.armRead()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var dl, held time.Time
	done := make(chan struct{})
	go func() {
		defer close(done)
		dl, held = a.adoptQuotaWait(ctx, j, viewLedger(j.Events), time.Time{}, deadline)
	}()
	select {
	case <-read:
	case <-time.After(10 * time.Second):
		t.Fatal("the adoption never read the clock")
	}
	c.set(commit)
	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	<-done
	return dl, held
}
