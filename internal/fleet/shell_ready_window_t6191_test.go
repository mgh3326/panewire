package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestShellReadyTokenWindowSucceedsAfter20Reads verifies that start polls for the
// shell-ready token until the shell_ready deadline rather than giving up after 20
// reads (hk 6191). With a fake herdr whose pane shows the token only on the 30th
// read, launch succeeds.
func TestShellReadyTokenWindowSucceedsAfter20Reads(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	clock := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	a.Now = func() time.Time { return clock }
	a.Sleep = func(_ context.Context, d time.Duration) error {
		clock = clock.Add(d)
		return nil
	}

	readCount := 0
	token := ""
	started := false
	base := fr.Do
	fr.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "agent start"):
			started = true
		case strings.HasPrefix(args, "pane run") && len(c.Args) > 3 && c.Args[3] == "echo":
			token = c.Args[len(c.Args)-1]
			return Result{}
		case strings.HasPrefix(args, "pane read"):
			if started {
				return Result{Out: ompCapture(t, "screen_opus_high_worktree.txt")}
			}
			readCount++
			if readCount < 30 {
				return Result{Out: "waiting for shell initialization...\n"}
			}
			return Result{Out: token + "\n"}
		}
		return base(c)
	}

	if err := a.Spawn(context.Background(), s); err != nil {
		t.Fatalf("Spawn failed unexpectedly: %v", err)
	}
	if readCount < 30 {
		t.Fatalf("expected at least 30 reads, got %d", readCount)
	}
}

// TestShellReadyTokenWindowFailsAtDeadlineWhenTokenNeverAppears verifies that if
// the token never appears, launch fails with "shell command execution not proven"
// once the shell_ready deadline expires.
func TestShellReadyTokenWindowFailsAtDeadlineWhenTokenNeverAppears(t *testing.T) {
	a, s, fr := ompSpawnFixture(t)
	clock := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	a.Now = func() time.Time { return clock }
	a.Sleep = func(_ context.Context, d time.Duration) error {
		clock = clock.Add(d)
		return nil
	}

	reads := 0
	base := fr.Do
	fr.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "pane run") && len(c.Args) > 3 && c.Args[3] == "echo":
			return Result{}
		case strings.HasPrefix(args, "pane read"):
			reads++
			return Result{Out: "unrelated output\n"}
		}
		return base(c)
	}

	err := a.Spawn(context.Background(), s)
	if err == nil {
		t.Fatal("expected Spawn to fail when token never appears, got nil")
	}
	if !strings.Contains(err.Error(), "shell command execution not proven") {
		t.Fatalf("expected error to contain %q, got %v", "shell command execution not proven", err)
	}
	// Default timeout is 120s; starting after initial shell check (which takes 0 sleeps here),
	// token loop should poll at 1s intervals until deadline.
	if reads < 100 {
		t.Fatalf("expected at least 100 reads before timeout, got %d", reads)
	}
}
