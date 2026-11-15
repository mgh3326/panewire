package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGoToolchainCacheStaysOutOfTestStateDir proves TestMain pins the go
// toolchain env from the launch environment (hk 4733). On Linux GOCACHE
// derives from XDG_CACHE_HOME, and TestMain points that at the state dir, so a
// pin taken after the move put a fresh build cache in every run's state dir:
// roughly 1 GB left behind whenever a test binary died before its cleanup.
func TestGoToolchainCacheStaysOutOfTestStateDir(t *testing.T) {
	if testStateDir == "" {
		t.Fatal("TestMain recorded no state dir")
	}
	for _, key := range []string{"GOCACHE", "GOMODCACHE", "GOPATH"} {
		value := os.Getenv(key)
		if value == "" {
			t.Skipf("%s is not pinned: go is not on PATH, so no test can run it", key)
		}
		if value == testStateDir || strings.HasPrefix(value, testStateDir+string(filepath.Separator)) {
			t.Fatalf("%s=%s is under the test state dir %s", key, value, testStateDir)
		}
	}
}
