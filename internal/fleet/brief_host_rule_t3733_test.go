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
		"`remote-gotest <m1b|desktop> <repo> <ref>",
		"CI",
	} {
		if !strings.Contains(bullet, want) {
			t.Errorf("host-test bullet lacks %q: %q", want, bullet)
		}
	}
}

func TestSpawnBriefPreambleFleetV1RulesT3809(t *testing.T) {
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
	block := brief[start:end]

	// (a) the delivered fleet-v1 block no longer contains "do not receive lane.event"
	// nor "herdr agent prompt", and its Reporting upward bullet contains
	// "whatever harness the parent runs"
	for _, bad := range []string{"do not receive lane.event", "herdr agent prompt"} {
		if strings.Contains(block, bad) {
			t.Errorf("fleet-v1 block still contains %q", bad)
		}
	}
	var reportingBullet string
	for _, line := range strings.Split(block, "\n") {
		if strings.HasPrefix(line, "- Reporting upward") {
			reportingBullet = line
		}
	}
	if reportingBullet == "" {
		t.Fatalf("fleet-v1 block lacks Reporting upward bullet: %q", block)
	}
	if !strings.Contains(reportingBullet, "whatever harness the parent runs") {
		t.Errorf("Reporting upward bullet lacks %q: %q", "whatever harness the parent runs", reportingBullet)
	}

	// (c) the block contains the bullets starting "- Finishing work:",
	// "- Ordinary obstacles", "- Deliberate blockers", and the phrase "need no approval";
	// "Difficulty or size is never a reason to stop" appears exactly once
	for _, prefix := range []string{"- Finishing work:", "- Ordinary obstacles", "- Deliberate blockers"} {
		found := false
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, prefix) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("fleet-v1 block lacks bullet with prefix %q", prefix)
		}
	}
	if !strings.Contains(block, "need no approval") {
		t.Errorf("fleet-v1 block lacks %q", "need no approval")
	}
	diffCount := strings.Count(block, "Difficulty or size is never a reason to stop")
	if diffCount != 1 {
		t.Errorf("expected 'Difficulty or size is never a reason to stop' exactly once, found %d", diffCount)
	}

	// (d) the block's first bullet names it "the single test and host rule"
	var firstBullet string
	for _, line := range strings.Split(block, "\n") {
		if strings.HasPrefix(line, "- ") {
			firstBullet = line
			break
		}
	}
	if !strings.Contains(firstBullet, "the single test and host rule") {
		t.Errorf("first bullet %q lacks 'the single test and host rule'", firstBullet)
	}
}
