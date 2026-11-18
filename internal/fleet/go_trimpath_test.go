package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoFlagsForPaneKeepsFlagsAndAppendsTrimpath(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"", "-trimpath"},
		{"-mod=mod", "-mod=mod -trimpath"},
		{"-mod=mod -trimpath -count=1", "-mod=mod -trimpath -count=1"},
		{"-trimpath=false", "-trimpath=false -trimpath"},
		{"-trimpath=true", "-trimpath=true"},
		{"-trimpath -trimpath=false", "-trimpath -trimpath=false -trimpath"},
		{"-trimpath=false -trimpath", "-trimpath=false -trimpath"},
		{`"-ldflags=-X 'main.note=a -trimpath b'"`, `"-ldflags=-X 'main.note=a -trimpath b'" -trimpath`},
	} {
		if got := goFlagsForPane(c.in); got != c.want {
			t.Errorf("goFlagsForPane(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// paneEnvValues returns every value of key passed with --env to `herdr tab
// create`.
func paneEnvValues(calls []Command, key string) []string {
	var values []string
	for _, c := range calls {
		if !strings.HasPrefix(strings.Join(c.Args, " "), "tab create") {
			continue
		}
		for i, arg := range c.Args {
			if i > 0 && c.Args[i-1] == "--env" && strings.HasPrefix(arg, key+"=") {
				values = append(values, arg)
			}
		}
	}
	return values
}

// A spawned pane takes -trimpath in GOFLAGS, and the spawner's own flags come
// along: the cache then hits across worktree directories (hk 5160).
func TestSpawnPaneEnvironmentAddsTrimpath(t *testing.T) {
	t.Setenv("GOFLAGS", "-mod=mod")
	a, s, f, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	got := paneEnvValues(f.Calls, "GOFLAGS")
	if len(got) != 1 || got[0] != "GOFLAGS=-mod=mod -trimpath" {
		t.Fatalf("pane env GOFLAGS = %v, want exactly GOFLAGS=-mod=mod -trimpath", got)
	}
}

// Flags saved by `go env -w` still apply: the pane value carries them, because
// a nonempty GOFLAGS in the pane would otherwise hide the saved file value.
func TestSpawnPaneEnvironmentKeepsSavedGOFLAGS(t *testing.T) {
	t.Setenv("GOFLAGS", "")
	t.Setenv("GOENV", writeGOEnv(t, "GOFLAGS=-tags=enterprise\n"))
	a, s, f, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	got := paneEnvValues(f.Calls, "GOFLAGS")
	if len(got) != 1 || got[0] != "GOFLAGS=-tags=enterprise -trimpath" {
		t.Fatalf("pane env GOFLAGS = %v, want exactly GOFLAGS=-tags=enterprise -trimpath", got)
	}
}

// An exported GOFLAGS wins over the saved file value, as in the go command.
func TestSpawnPaneEnvironmentExportedGOFLAGSWins(t *testing.T) {
	t.Setenv("GOFLAGS", "-mod=mod")
	t.Setenv("GOENV", writeGOEnv(t, "GOFLAGS=-tags=enterprise\n"))
	a, s, f, _ := cmdFixture(t)
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	got := paneEnvValues(f.Calls, "GOFLAGS")
	if len(got) != 1 || got[0] != "GOFLAGS=-mod=mod -trimpath" {
		t.Fatalf("pane env GOFLAGS = %v, want exactly GOFLAGS=-mod=mod -trimpath", got)
	}
}

func writeGOEnv(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "go-env")
	if e := os.WriteFile(path, []byte(content), 0600); e != nil {
		t.Fatal(e)
	}
	return path
}

// The switch turns the flag off without touching the pane environment.
func TestSpawnPaneEnvironmentTrimpathOff(t *testing.T) {
	t.Setenv("GOFLAGS", "-mod=mod")
	a, s, f, _ := cmdFixture(t)
	a.Config.Local["go_trimpath"] = false
	if e := a.Spawn(context.Background(), s); e != nil {
		t.Fatal(e)
	}
	if got := paneEnvValues(f.Calls, "GOFLAGS"); len(got) != 0 {
		t.Fatalf("pane env GOFLAGS = %v, want none with the switch off", got)
	}
}

// [local] go_trimpath is a literal boolean: the spawn switch reads it, and a
// string is refused like the other local booleans.
func TestGoTrimpathConfigKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if e := os.WriteFile(path, []byte("[local]\ngo_trimpath = false\n"), 0600); e != nil {
		t.Fatal(e)
	}
	c, e := LoadConfig(path)
	if e != nil {
		t.Fatal(e)
	}
	if boolean(c.Local, "go_trimpath", true) {
		t.Fatal("go_trimpath = false did not load")
	}
	if e := os.WriteFile(path, []byte("[local]\ngo_trimpath = \"no\"\n"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := LoadConfig(path); e == nil {
		t.Fatal("a string go_trimpath was accepted")
	}
}
