package fleet

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// agySpawnFixture is ompSpawnFixture with an agy seat in a zsh pane: once herdr
// starts the agent, the pane's foreground process is agy.
func agySpawnFixture(t *testing.T) (*App, Spawn, *fakeRunner) {
	return agySpawnFixtureShell(t, "zsh")
}

// agySpawnFixtureShell is agySpawnFixture with the pane's shell named by shell.
func agySpawnFixtureShell(t *testing.T, shell string) (*App, Spawn, *fakeRunner) {
	t.Helper()
	a, s, fr := ompSpawnFixture(t)
	s.Profile = "agy-sonnet46"
	a.Quota = &fakeQuota{LaunchValue: Launch{Profile: "agy-sonnet46", ModelID: "claude-sonnet-4-6", Grade: "S+", Pool: "agy", Gate: "default"}, GateDo: gatePools(map[string]string{"agy-sonnet46": "agy"})}
	base := fr.Do
	started := false
	fr.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "agent start"):
			started = true
		case !started && strings.HasPrefix(args, "pane process-info"):
			return Result{Out: fmt.Sprintf(`{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":1,"argv0":%q}]}}`, shell)}
		case started && strings.HasPrefix(args, "pane process-info"):
			return Result{Out: `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"agy"}]}}`}
		}
		return base(c)
	}
	return a, s, fr
}

// readinessRuns returns the pane run calls that carry the SSH removal, and the
// index of the first agent start call.
func readinessRuns(fr *fakeRunner) (runs []int, startAt int) {
	startAt = -1
	for i, c := range fr.Calls {
		args := strings.Join(c.Args, " ")
		if strings.HasPrefix(args, "pane run w:p1 ") && strings.Contains(args, "SSH_") {
			runs = append(runs, i)
		}
		if strings.HasPrefix(args, "agent start") && startAt < 0 {
			startAt = i
		}
	}
	return runs, startAt
}

// An agy pane's shell removes the SSH variables in the same line that echoes the
// readiness token, before herdr starts agy (hk 5699). The spawner's own
// environment carries them here, and none of it may reach the pane.
func TestAgySpawnRemovesSSHVariablesInTheReadinessLine(t *testing.T) {
	for _, k := range agySSHEnv {
		t.Setenv(k, "fixture")
	}
	a, s, fr := agySpawnFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	runs, startAt := readinessRuns(fr)
	if len(runs) != 1 || startAt < 0 || runs[0] > startAt {
		t.Fatalf("SSH removal calls %v, agent start at %d; want one removal sent before agent start", runs, startAt)
	}
	c := fr.Calls[runs[0]]
	prefix := c.Args[3]
	if !strings.HasPrefix(prefix, "unset SSH_CONNECTION SSH_CLIENT SSH_TTY && ") || !strings.HasSuffix(prefix, " && echo") {
		t.Fatalf("zsh readiness prefix = %q, want the unset and the echo in one line", prefix)
	}
	if len(c.Args) != 5 || !strings.HasPrefix(c.Args[4], "panewire-ready-") {
		t.Fatalf("readiness call args = %q, want the token as the last argument", c.Args)
	}
	for i, call := range fr.Calls {
		if i == runs[0] {
			continue
		}
		if args := strings.Join(call.Args, " "); strings.Contains(args, "SSH_") {
			t.Fatalf("call %q carries an SSH variable outside the readiness line", args)
		}
	}
}

// A fish pane has no unset builtin: the removal is set -e, and the token is
// echoed only when every variable is confirmed absent with set -q.
func TestAgySpawnFishPaneRemovesVariablesInTheReadinessLine(t *testing.T) {
	a, s, fr := agySpawnFixtureShell(t, "fish")
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	runs, startAt := readinessRuns(fr)
	if len(runs) != 1 || runs[0] > startAt {
		t.Fatalf("fish removal calls %v, agent start at %d", runs, startAt)
	}
	prefix := fr.Calls[runs[0]].Args[3]
	for _, want := range []string{"set -e SSH_CONNECTION; ", "set -e SSH_TTY; ", "and not set -q SSH_CLIENT; ", "and echo"} {
		if !strings.Contains(prefix, want) {
			t.Fatalf("fish readiness prefix %q lacks %q", prefix, want)
		}
	}
}

// A pane shell that is neither POSIX nor fish has no removal, so the spawn is
// refused before herdr starts agy rather than launching it with the variables.
func TestAgySpawnRefusesUnsupportedPaneShell(t *testing.T) {
	a, s, fr := agySpawnFixtureShell(t, "nu")
	err := a.Spawn(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), `pane shell "nu"`) {
		t.Fatalf("Spawn error = %v, want an agy refusal naming the nu shell", err)
	}
	if _, startAt := readinessRuns(fr); startAt >= 0 {
		t.Fatalf("agent start ran at call %d on an unsupported shell", startAt)
	}
}

// If the shell swallows the readiness line, the token never appears, so the
// spawn does not treat the variables as removed and does not start agy.
func TestAgySpawnNeverStartsWhenRemovalLineIsSwallowed(t *testing.T) {
	a, s, fr := agySpawnFixture(t)
	base := fr.Do
	fr.Do = func(c Command) Result {
		if strings.HasPrefix(strings.Join(c.Args, " "), "pane run w:p1 ") && strings.Contains(strings.Join(c.Args, " "), "SSH_") {
			return Result{}
		}
		return base(c)
	}
	if err := a.Spawn(context.Background(), s); err == nil {
		t.Fatal("Spawn succeeded although the readiness line never echoed")
	}
	if _, startAt := readinessRuns(fr); startAt >= 0 {
		t.Fatalf("agent start ran at call %d after a swallowed removal", startAt)
	}
}

// The removal is scoped to agy. Every other harness keeps the plain echo.
func TestPaneReadyPrefixIsAgyOnly(t *testing.T) {
	for _, shell := range []string{"zsh", "bash", "fish"} {
		got, err := paneReadyPrefix("agy", shell)
		if err != nil || !strings.Contains(got, "SSH_TTY") || !strings.HasSuffix(got, "echo") {
			t.Fatalf("paneReadyPrefix(agy, %s) = %q, %v; want the SSH removal ending in echo", shell, got, err)
		}
	}
	for _, kind := range []string{"claude", "codex", "omp", "devin", "cmd", "kiro", "opencode"} {
		got, err := paneReadyPrefix(kind, "nu")
		if err != nil || got != "echo" {
			t.Fatalf("paneReadyPrefix(%s, nu) = %q, %v; want the plain echo", kind, got, err)
		}
	}
}

// The GOFLAGS injection is unchanged for an agy pane: the spawner's flags plus
// -trimpath, carried by --env as before.
func TestAgySpawnKeepsGOFLAGSInjection(t *testing.T) {
	t.Setenv("GOFLAGS", "-mod=mod")
	a, s, fr := agySpawnFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	got := paneEnvValues(fr.Calls, "GOFLAGS")
	if len(got) != 1 || got[0] != "GOFLAGS=-mod=mod -trimpath" {
		t.Fatalf("agy pane env GOFLAGS = %v, want exactly GOFLAGS=-mod=mod -trimpath", got)
	}
}
