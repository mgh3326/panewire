package panewire

import (
	"io"
	"testing"
	"time"
)

// A stalled store must not prevent unrelated hub work from acquiring mu.
func TestPoolActionStoreWaitDoesNotHoldHubLock(t *testing.T) {
	for _, operation := range []string{"create", "reassign"} {
		t.Run(operation, func(t *testing.T) {
			f, _ := poolActionFixture(t)
			if operation == "create" {
				f.hub.mu.Lock()
				f.hub.jobs[f.job].Orphaned = false
				f.hub.mu.Unlock()
			}
			db := f.hub.inbox.store.db
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			before := db.Stats().WaitCount
			done := make(chan struct{})
			go func() {
				defer close(done)
				if operation == "reassign" {
					_, _ = f.hub.reassignJob(f.job, "machine-b")
					return
				}
				app, err := f.hub.newSpawnApp(io.Discard, io.Discard)
				if err == nil {
					_ = f.hub.createPoolAction(t.Context(), poolExhaustion{Pool: "claude", Cause: poolCauseRetired}, f.holder, app)
				}
			}()
			waiting := waitUntil(2*time.Second, func() bool { return db.Stats().WaitCount > before })
			unlocked := make(chan struct{})
			go func() {
				f.hub.mu.Lock()
				f.hub.mu.Unlock()
				close(unlocked)
			}()
			available := waitUntil(500*time.Millisecond, func() bool {
				select {
				case <-unlocked:
					return true
				default:
					return false
				}
			})
			_ = tx.Rollback()
			<-done
			<-unlocked
			if !waiting || !available {
				t.Fatalf("store waiting=%v; hub lock available=%v", waiting, available)
			}
		})
	}
}
