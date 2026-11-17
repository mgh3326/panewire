package panewire

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// A hub revocation is published and acknowledged without waiting on a local
// job lock: the ordered inbound dispatcher must not stall, or drop the
// revocation, while sweep, reap or settlement holds the end lock. The hub, not
// this lock, fences a reassignment (docs/node-commands.md, I1/I2).
func TestHubRevocationIsNotHeldBehindEndLock_t4632(t *testing.T) {
	inbox := t.TempDir()
	dir := filepath.Join(inbox, "jobs", "job-r")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	unlock, err := fleet.LockJobEnd(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	start := time.Now()
	err = writeHubRevocation(inbox, hubJobRevokedEvent{Type: "job.revoked", JobID: "job-r", Epoch: 2})
	took := time.Since(start)
	markers, globErr := filepath.Glob(filepath.Join(dir, "events", "*-job.revoked.json"))
	if err != nil || globErr != nil || len(markers) != 1 || took >= time.Second {
		t.Fatalf("err=%v markers=%v globErr=%v took=%v", err, markers, globErr, took)
	}
}
