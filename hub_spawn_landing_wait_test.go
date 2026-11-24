package panewire

import "testing"

// A landing wait whose test never calls end must still have its Done channel
// closed once the test's cleanups run, so a dispatch goroutine blocked on it
// returns instead of outliving the test (hk 6088).
func TestLandingWaitCleanupClosesDoneWithoutEnd(t *testing.T) {
	var done <-chan struct{}
	t.Run("test fails before end", func(t *testing.T) {
		ctx, _ := newLandingWait(t)
		done = ctx.Done()
	})
	select {
	case <-done:
	default:
		t.Fatal("cleanup left the landing wait open")
	}
}

// end is idempotent: a test may call it and then let cleanup run it again.
func TestLandingWaitEndTwiceAndCleanupDoesNotPanic(t *testing.T) {
	t.Run("end then cleanup", func(t *testing.T) {
		ctx, end := newLandingWait(t)
		end()
		end()
		select {
		case <-ctx.Done():
		default:
			t.Fatal("end did not close the landing wait")
		}
	})
}
