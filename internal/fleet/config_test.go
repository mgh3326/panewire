package fleet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigCaps(t *testing.T) {
	for _, host := range []string{"local", "hosts.pi"} {
		t.Run(host, func(t *testing.T) {
			for _, tc := range []struct {
				name, value, wantCaps, wantErr string
			}{
				{name: "known_list", value: `["macos", "linux", "unity", "xcode", "gpu", "heavy-go"]`, wantCaps: "macos,linux,unity,xcode,gpu,heavy-go"},
				{name: "empty_list", value: `[]`},
				{name: "omitted"},
				{name: "true", value: `true`, wantErr: "must be a list of known capability strings, got true"},
				{name: "false", value: `false`, wantErr: "must be a list of known capability strings, got false"},
				{name: "integer", value: `42`, wantErr: "must be a list of known capability strings, got 42"},
				{name: "float", value: `1.5`, wantErr: "must be a list of known capability strings, got 1.5"},
				{name: "string", value: `"linux"`, wantErr: `must be a list of known capability strings, got "linux"`},
				{name: "nested_table", value: `{ linux = true }`, wantErr: `must be a list of known capability strings, got map[string]interface {}{"linux":true}`},
				{name: "unknown_capability", value: `["teleport"]`, wantErr: `contains an invalid capability "teleport"`},
				{name: "uppercase", value: `["Unity"]`, wantErr: `contains an invalid capability "Unity"`},
				{name: "empty_capability", value: `[""]`, wantErr: `contains an invalid capability ""`},
				{name: "mixed_list", value: `["linux", true]`, wantErr: "contains an invalid capability true"},
				{name: "nested_list", value: `[["linux"]]`, wantErr: `contains an invalid capability []interface {}{"linux"}`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					body := "[" + host + "]\n"
					if tc.value != "" {
						body += "caps = " + tc.value + "\n"
					}
					path := filepath.Join(t.TempDir(), "hosts.toml")
					if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
						t.Fatal(err)
					}
					cfg, err := LoadConfig(path)
					if tc.wantErr != "" {
						if exitCode(err) != exitConfig {
							t.Fatalf("caps = %s: want exit 70, got %v", tc.value, err)
						}
						if want := host + ".caps " + tc.wantErr; err.Error() != want {
							t.Fatalf("want error %q, got %q", want, err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					h := cfg.Local
					if host != "local" {
						h = cfg.Hosts["pi"]
					}
					if got := strings.Join(hostCaps(h), ","); got != tc.wantCaps {
						t.Fatalf("want caps %q, got %q", tc.wantCaps, got)
					}
				})
			}
		})
	}
}

func TestConfigSweepRemoveWorktrees(t *testing.T) {
	load := func(t *testing.T, body string) (Config, error) {
		t.Helper()
		path := filepath.Join(t.TempDir(), "hosts.toml")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return LoadConfig(path)
	}
	c, err := load(t, "[sweep]\nremove_worktrees = false\n")
	if err != nil || boolean(c.Sweep, "remove_worktrees", true) != false {
		t.Fatalf("explicit off: %v %v", c.Sweep, err)
	}
	c, err = load(t, "[local]\n")
	if err != nil || boolean(c.Sweep, "remove_worktrees", true) != true {
		t.Fatalf("default on: %v %v", c.Sweep, err)
	}
	if _, err = load(t, "[sweep]\nremove_worktrees = \"no\"\n"); exitCode(err) != exitConfig {
		t.Fatalf("a non-boolean was accepted: %v", err)
	}
}

func TestConfigPathFleetResolution(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("FLEET_HOSTS_CONFIG", "")
	t.Setenv("WRK_HOSTS_CONFIG", "")

	var warn bytes.Buffer
	if got := configPath(&warn); got != filepath.Join(xdg, "fleet", "hosts.toml") || warn.Len() != 0 {
		t.Fatalf("default path = %q warn=%q", got, warn.String())
	}

	t.Setenv("FLEET_HOSTS_CONFIG", "/tmp/explicit-fleet.toml")
	if got := configPath(&warn); got != "/tmp/explicit-fleet.toml" {
		t.Fatalf("FLEET_HOSTS_CONFIG must win: %q", got)
	}
	t.Setenv("FLEET_HOSTS_CONFIG", "")

	// One-release fallback: only the retired wrk path exists.
	old := filepath.Join(xdg, "wrk", "hosts.toml")
	if e := os.MkdirAll(filepath.Dir(old), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(old, []byte("[local]\n"), 0600); e != nil {
		t.Fatal(e)
	}
	warn.Reset()
	if got := configPath(&warn); got != old || !strings.Contains(warn.String(), "fleet") {
		t.Fatalf("retired fallback = %q warn=%q", got, warn.String())
	}

	// The fleet path wins once both exist.
	next := filepath.Join(xdg, "fleet", "hosts.toml")
	if e := os.MkdirAll(filepath.Dir(next), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(next, []byte("[local]\n"), 0600); e != nil {
		t.Fatal(e)
	}
	warn.Reset()
	if got := configPath(&warn); got != next || warn.Len() != 0 {
		t.Fatalf("fleet path must win: %q warn=%q", got, warn.String())
	}

	// The retired env var is ignored while the fleet config exists.
	t.Setenv("WRK_HOSTS_CONFIG", "/tmp/explicit-wrk.toml")
	warn.Reset()
	if got := configPath(&warn); got != next || !strings.Contains(warn.String(), "ignored") {
		t.Fatalf("WRK_HOSTS_CONFIG must not override the fleet config: %q warn=%q", got, warn.String())
	}

	// With the fleet file gone it still loads for one release, with a warning.
	if e := os.Remove(next); e != nil {
		t.Fatal(e)
	}
	warn.Reset()
	if got := configPath(&warn); got != "/tmp/explicit-wrk.toml" || !strings.Contains(warn.String(), "FLEET_HOSTS_CONFIG") {
		t.Fatalf("WRK_HOSTS_CONFIG fallback = %q warn=%q", got, warn.String())
	}
}
