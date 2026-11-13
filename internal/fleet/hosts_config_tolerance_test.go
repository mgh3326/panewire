package fleet

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// An unknown top-level hosts key, whether a scalar or a table, loads and is
// named in the startup warning instead of refusing the whole hosts.toml.
func TestHostsConfigUnknownTopLevelKeyIgnoredAndWarned(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte("spawn = true\n[bogus]\nx = 1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("hosts.toml with unknown top-level keys refused: %v", err)
	}
	if !slices.Equal(c.ignoredTables, []string{"bogus", "spawn"}) {
		t.Fatalf("ignoredTables=%v, want [bogus spawn]", c.ignoredTables)
	}
	var out bytes.Buffer
	warnIgnoredHostsTables(c, path, &out)
	for _, key := range []string{"bogus", "spawn"} {
		if !strings.Contains(out.String(), "unknown hosts key ["+key+"] in "+path) {
			t.Fatalf("warning does not name key %s and file: %q", key, out.String())
		}
	}
}
