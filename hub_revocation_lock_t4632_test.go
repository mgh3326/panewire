package panewire

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A revocation cannot be recorded and acknowledged while settlement holds the
// end lock on which its hk decision is based.
func TestHubRevocationWaitsForSettlementEndLock_t4632(t *testing.T) {
	inbox := t.TempDir()
	dir := filepath.Join(inbox, "jobs", "job-r")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	unlock, err := fleet.LockJobEnd(dir)
	if err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- writeHubRevocation(inbox, hubJobRevokedEvent{Type: "job.revoked", JobID: "job-r", Epoch: 2})
	}()
	<-started
	// This window proves no revocation is recorded while settlement owns the lock.
	select {
	case err := <-done:
		t.Fatalf("revocation completed while end lock held: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	unlock()
	locked = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("revocation did not finish after lock release")
	}
}
