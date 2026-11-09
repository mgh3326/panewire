package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Workers on m1b read "never on mbp" as themselves (its computer name is a
// MacBook Pro) and stopped; the preamble names the one no-test host by
// hostname so every worker can tell (hk 3733).
func TestSpawnBriefPreambleStatesTestHostRuleT3733(t *testing.T) {
	a, s, _, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(filepath.Join(a.Jobs, s.Job, "brief-delivered.md"))
	if e != nil {
		t.Fatal(e)
	}
	brief := string(b)
	start := strings.Index(brief, "Host safety (fleet-v1):\n")
	end := strings.Index(brief, "\nLaunch identity:")
	if start < 0 || end < start {
		t.Fatalf("delivered brief has no fleet-v1 block: %q", brief)
	}
	var bullet string
	for _, line := range strings.Split(brief[start:end], "\n") {
		if strings.HasPrefix(line, "- ") && strings.Contains(line, "mbp-server") {
			bullet = line
		}
	}
	if bullet == "" {
		t.Fatalf("fleet-v1 block has no bullet naming mbp-server: %q", brief[start:end])
	}
	// The rule's polarity: mbp-server is forbidden and every other host is
	// told to run the tests itself. Substrings alone would let "never" turn
	// into "always" or the permission drop out unnoticed.
	for _, want := range []string{
		"never run `go test` on mbp-server",
		"run `go test` for the packages you changed directly in your worktree",
	} {
		if !strings.Contains(bullet, want) {
			t.Errorf("host-test bullet lacks the rule %q: %q", want, bullet)
		}
	}
	for _, want := range []string{
		"`hostname -s`",
		"`go test`",
		"robinui-MacBookPro",
		"home-desktop",
		"directly in your worktree",
		"`remote-gotest` exists only on mbp-server",
		"CI",
	} {
		if !strings.Contains(bullet, want) {
			t.Errorf("host-test bullet lacks %q: %q", want, bullet)
		}
	}
}
