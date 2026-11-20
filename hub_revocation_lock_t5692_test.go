package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// T5692: the hub revocation writer waits on a job's .wrk-events.lock only up to
// hubRevocationLockWait. When a holder outlasts that bound, it returns an error
// and writes nothing. The inbound dispatcher then sends no job.revocation.ack,
// so the hub retries, and a stuck lock cannot stall the ordered dispatcher.
func TestHubRevocationGivesUpOnStuckEventsLock_t5692(t *testing.T) {
	prev := hubRevocationLockWait
	hubRevocationLockWait = 300 * time.Millisecond
	t.Cleanup(func() { hubRevocationLockWait = prev })

	inbox := t.TempDir()
	jobDir := filepath.Join(inbox, "jobs", "job-h")
	if err := os.MkdirAll(jobDir, 0o700); err != nil {
		t.Fatal(err)
	}
	holdJobEventsLock(t, jobDir)

	start := time.Now()
	err := writeHubRevocation(inbox, hubJobRevokedEvent{Type: "job.revoked", JobID: "job-h", Epoch: 2})
	took := time.Since(start)
	if err == nil {
		t.Fatal("writeHubRevocation returned nil while the events lock was held: the caller would ack an unwritten revocation")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writeHubRevocation error = %v, want the lock-wait deadline", err)
	}
	if took > 3*time.Second {
		t.Fatalf("writeHubRevocation waited %v, far past its %v bound", took, hubRevocationLockWait)
	}
	revoked, err := filepath.Glob(filepath.Join(jobDir, "events", "*-job.revoked.json"))
	if err != nil || len(revoked) != 0 {
		t.Fatalf("a revocation was written while the lock was held: %v err=%v", revoked, err)
	}
}
