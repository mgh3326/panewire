package panewire

import (
	"os"
	"path/filepath"
	"runtime"
)

// fleetInboxRootRel is the shared inbox root under the user's home that a
// Linux node daemon relays from (--hub-jobs-root, docs/jobs-resilience.md);
// job.go's jobs-root rule resolves "$HOME/work/herdr-inbox/jobs" from the
// same place.
const fleetInboxRootRel = "work/herdr-inbox"

// defaultInboxRoot is the inbox root used when neither --inbox-root nor
// PANEWIRE_INBOX_ROOT names one. `panewire emit` and the daemon's own
// fallback both resolve it here, so the two cannot disagree on one host.
func defaultInboxRoot() string {
	home, _ := os.UserHomeDir()
	return defaultInboxRootFor(runtime.GOOS, home)
}

// defaultInboxRootFor is the per-OS rule, parameterised for tests. macOS
// keeps the daemon's data directory beside the default socket — live macOS
// nodes emit there without PANEWIRE_INBOX_ROOT, so it must not move. Every
// other OS uses ~/work/herdr-inbox: "~/Library/Application Support" does not
// exist there, and the old macOS-only default made every emit on a Linux node
// fail with "inbox root mismatch" (docs/r20-relay-persistence.md).
func defaultInboxRootFor(goos, home string) string {
	if goos == "darwin" {
		return filepath.Join(home, "Library", "Application Support", "panewire", "inbox")
	}
	return filepath.Join(home, filepath.FromSlash(fleetInboxRootRel))
}
