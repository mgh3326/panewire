package fleet

import (
	"context"
	"slices"
	"strings"
	"testing"
)

// agySpawnFixture is ompSpawnFixture with an agy seat: once herdr starts the
// agent, the pane's foreground process is agy.
func agySpawnFixture(t *testing.T) (*App, Spawn, *fakeRunner) {
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
		case started && strings.HasPrefix(args, "pane process-info"):
			return Result{Out: `{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"agy"}]}}`}
		}
		return base(c)
	}
	return a, s, fr
}

// An agy pane's shell drops the SSH variables before herdr starts agy, so agy
// reads its keyring login rather than a file token (hk 5699). The spawner's own
// environment carries them here and none of it may reach the pane.
func TestAgySpawnUnsetsSSHVariablesBeforeAgentStart(t *testing.T) {
	for _, k := range agySSHEnv {
		t.Setenv(k, "fixture")
	}
	a, s, fr := agySpawnFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	unsetAt, startAt := -1, -1
	for i, c := range fr.Calls {
		args := strings.Join(c.Args, " ")
		if args == "pane run w:p1 unset SSH_CONNECTION SSH_CLIENT SSH_TTY" && unsetAt < 0 {
			unsetAt = i
		}
		if strings.HasPrefix(args, "agent start") && startAt < 0 {
			startAt = i
		}
	}
	if unsetAt < 0 || startAt < 0 || unsetAt > startAt {
		t.Fatalf("agy pane shell unset at call %d, agent start at call %d; want the unset sent before agent start", unsetAt, startAt)
	}
	for i, c := range fr.Calls {
		if i == unsetAt {
			continue
		}
		if args := strings.Join(c.Args, " "); strings.Contains(args, "SSH_") {
			t.Fatalf("call %q carries an SSH variable outside the unset", args)
		}
	}
}

// The removal is scoped to agy. Claude reads these variables to detect an SSH
// session, so every other harness keeps the variables it inherits.
func TestPaneUnsetCommandIsAgyOnly(t *testing.T) {
	want := []string{"unset", "SSH_CONNECTION", "SSH_CLIENT", "SSH_TTY"}
	if got := paneUnsetCommand("agy"); !slices.Equal(got, want) {
		t.Fatalf("paneUnsetCommand(agy) = %v, want %v", got, want)
	}
	for _, kind := range []string{"claude", "codex", "omp", "devin", "cmd", "kiro", "opencode"} {
		if got := paneUnsetCommand(kind); got != nil {
			t.Fatalf("paneUnsetCommand(%s) = %v, want nil", kind, got)
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
