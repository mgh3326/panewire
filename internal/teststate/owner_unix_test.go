//go:build unix

package teststate

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestSweepStaleKeepsAnotherUsersStateDir proves the ownership rule: a shared
// temp dir may hold a stale state dir another account created, and a sweep
// must leave it alone. Only root can create one, so a normal run skips.
func TestSweepStaleKeepsAnotherUsersStateDir(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("creating a directory owned by another account needs root")
	}
	base := t.TempDir()
	foreign := filepath.Join(base, StatePrefix+"foreign")
	if err := os.Mkdir(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(foreign, 12345, 12345); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	stale := now.Add(-2 * StaleAfter)
	if err := os.Chtimes(foreign, stale, stale); err != nil {
		t.Fatal(err)
	}
	removed, err := SweepStale(base, now, StaleAfter, StatePrefix)
	if err != nil {
		t.Fatalf("SweepStale: %v", err)
	}
	if len(removed) != 0 {
		t.Fatalf("removed another account's dir: %v", removed)
	}
	if _, err := os.Lstat(foreign); err != nil {
		t.Fatalf("another account's dir must survive: %v", err)
	}
}
