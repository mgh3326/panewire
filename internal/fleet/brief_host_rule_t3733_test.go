package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The test rule is the same on every host (operator decision 237, hk 4496):
// mbp-server was a no-test host until 2026-11-13 (hk 3733). The bullet must
// say so and no line of the block may forbid go test on a host.
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
	block := brief[start:end]
	if strings.Contains(block, "never run `go test`") || strings.Contains(block, "remote-gotest") {
		t.Fatalf("fleet-v1 block still forbids go test on a host: %q", block)
	}
	var bullet string
	for _, line := range strings.Split(block, "\n") {
		if strings.HasPrefix(line, "- Test host:") {
			bullet = line
		}
	}
	if bullet == "" {
		t.Fatalf("fleet-v1 block has no test-host bullet: %q", block)
	}
	for _, want := range []string{
		"every host, mbp-server included",
		"run `go test` for the packages you changed directly in your worktree",
		"`go test ./...`",
		"CI",
	} {
		if !strings.Contains(bullet, want) {
			t.Errorf("test-host bullet lacks %q: %q", want, bullet)
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
