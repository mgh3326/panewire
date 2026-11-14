package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestDefaultStatePathsStayUnderTestHome proves TestMain's isolation end to
// end (hk 4685): the resolvers every daemon and CLI command falls back to
// name state under the temp home, never the launch home. The arbiter guard
// panics if any Store opens under the launch home, so dropping the override
// turns this red.
func TestDefaultStatePathsStayUnderTestHome(t *testing.T) {
	for name, path := range map[string]string{
		"node db":       defaultDBPath(),
		"inbox root":    defaultInboxRoot(),
		"hub inbox db":  defaultHubInboxPath(),
		"spawn journal": filepath.Join(filepath.Dir(defaultDBPath()), "spawn", "spawn-inbox.sqlite3"),
	} {
		if testLaunchHome != "" && (path == testLaunchHome || strings.HasPrefix(path, testLaunchHome+string(os.PathSeparator))) {
			t.Fatalf("%s resolved under the launch home: %s", name, path)
		}
	}
}
