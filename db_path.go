package panewire

import (
	"os"
	"path/filepath"
	"runtime"
)

// defaultDBPath is the SQLite file the node daemon opens when --db is absent,
// and `panewire deliveries` reads when --db is absent, so the command and the
// daemon it reads from cannot name different files.
func defaultDBPath() string {
	home, _ := os.UserHomeDir()
	return defaultDBPathFor(runtime.GOOS, home, os.Getenv("XDG_DATA_HOME"))
}

// defaultDBPathFor is the per-OS rule, parameterised for tests. macOS keeps
// the daemon's data directory beside the default socket. Every other OS uses
// the XDG data home ($XDG_DATA_HOME, default ~/.local/share):
// "~/Library/Application Support" does not exist there, so a Linux node read
// its deliveries from a path no daemon ever created (hk 3467).
func defaultDBPathFor(goos, home, dataHome string) string {
	if goos == "darwin" {
		return filepath.Join(home, "Library", "Application Support", "panewire", "panewire.sqlite3")
	}
	if dataHome == "" {
		dataHome = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(dataHome, "panewire", "panewire.sqlite3")
}
