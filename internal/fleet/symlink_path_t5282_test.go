package fleet

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hk 5282: macOS keeps every temp dir under /var, which is a symlink to
// /private/var. A symlink at the top level is the OS layout, so a path under
// it is not refused; the close-ops ledger reads depend on that.
func TestNoSymlinksAllowsATopLevelAlias_t5282(t *testing.T) {
	alias := ""
	for _, candidate := range []string{"/var", "/tmp", "/etc", "/bin"} {
		if info, err := os.Lstat(candidate); err == nil && info.Mode()&os.ModeSymlink != 0 {
			alias = candidate
			break
		}
	}
	if alias == "" {
		t.Skip("this host has no top-level symlink to test against (macOS /var, Linux /bin)")
	}
	if err := noSymlinks(filepath.Join(alias, "not-created-by-the-test", "events")); err != nil {
		t.Fatalf("path under top-level alias %s refused: %v", alias, err)
	}
}

// A symlink below the top level is still refused, wherever the temp dir is.
func TestNoSymlinksRefusesADeepSymlink_t5282(t *testing.T) {
	base := t.TempDir()
	real := filepath.Join(base, "real")
	if err := os.Mkdir(real, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if err := noSymlinks(filepath.Join(link, "events")); err == nil || !strings.Contains(err.Error(), "symlink path refused") {
		t.Fatalf("err=%v, want a refused symlink below the top level", err)
	}
}
