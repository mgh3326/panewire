package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// panewire measures claude and codex itself; every other pool is still
// pacelever's.
func TestRefreshCommandPerPool(t *testing.T) {
	claude := refreshCommand("claude", "pacelever-bin", true)
	if claude.Name != "panewire" || strings.Join(claude.Args, " ") != "quota refresh claude" {
		t.Fatalf("claude refresh = %+v", claude)
	}
	codex := refreshCommand("codex", "pacelever-bin", true)
	if codex.Name != "panewire" || strings.Join(codex.Args, " ") != "quota refresh codex" {
		t.Fatalf("codex refresh = %+v", codex)
	}
	devin := refreshCommand("devin", "pacelever-bin", false)
	if devin.Name != "pacelever-bin" || strings.Join(devin.Args, " ") != "refresh devin" {
		t.Fatalf("devin refresh = %+v", devin)
	}
	if got := remoteRefreshLine("claude", true); got != "panewire quota refresh 'claude'" {
		t.Fatalf("remote claude line %q", got)
	}
	if got := remoteRefreshLine("codex", true); got != "panewire quota refresh 'codex'" {
		t.Fatalf("remote codex line %q", got)
	}
	if got := remoteRefreshLine("devin", false); got != "pacelever refresh 'devin'" {
		t.Fatalf("remote devin line %q", got)
	}
}

// Over ssh the target runs panewire's quota command for claude and codex,
// never pacelever.
func TestMeasureTargetSSHRefreshPerPool(t *testing.T) {
	for pool, want := range map[string]string{"claude": "panewire quota refresh 'claude'", "codex": "panewire quota refresh 'codex'", "devin": "pacelever refresh 'devin'"} {
		var lines []string
		a := &App{Config: Config{Hosts: map[string]map[string]any{"host-b": {}}}}
		a.Runner = &fakeRunner{Do: func(c Command) Result {
			lines = append(lines, c.Name+" "+strings.Join(c.Args, " "))
			return Result{}
		}}
		if err := a.measureTarget(context.Background(), "host-b", pool); err != nil {
			t.Fatalf("%s: %v", pool, err)
		}
		if len(lines) != 1 || !strings.HasPrefix(lines[0], "ssh ") || !strings.HasSuffix(lines[0], " host-b "+want) {
			t.Fatalf("%s: ssh line %q, want it to end with %q", pool, lines, want)
		}
		if (pool == "claude" || pool == "codex") && strings.Contains(lines[0], "pacelever") {
			t.Fatalf("%s measured through pacelever: %q", pool, lines[0])
		}
	}
}

// commandcode has a native/shadow switch: the host's `[quota].native_pools`
// decides, and an unlisted (or unreadable) config leaves it in shadow, so spawns
// keep running pacelever for it. claude and codex ignore the list.
func TestNativeQuotaPoolSwitch(t *testing.T) {
	native := map[string]any{"native_pools": []any{"commandcode"}}
	for _, test := range []struct {
		pool  string
		quota map[string]any
		want  bool
	}{
		{"claude", nil, true},
		{"codex", map[string]any{"native_pools": []any{}}, true},
		{"commandcode", nil, false},
		{"commandcode", map[string]any{}, false},
		{"commandcode", map[string]any{"native_pools": []any{}}, false},
		{"commandcode", map[string]any{"native_pools": "commandcode"}, false},
		{"commandcode", native, true},
		{"devin", native, false},
	} {
		if got := IsNativeQuotaPool(test.pool, test.quota); got != test.want {
			t.Errorf("IsNativeQuotaPool(%q, %v) = %v, want %v", test.pool, test.quota, got, test.want)
		}
	}
	shadow := &App{Config: Config{Quota: map[string]any{}}}
	flipped := &App{Config: Config{Quota: native}}
	if cmd := refreshCommand("commandcode", "pacelever-bin", shadow.nativeQuotaPool("commandcode")); cmd.Name != "pacelever-bin" || strings.Join(cmd.Args, " ") != "refresh commandcode" {
		t.Fatalf("shadow refresh = %+v", cmd)
	}
	if cmd := refreshCommand("commandcode", "pacelever-bin", flipped.nativeQuotaPool("commandcode")); cmd.Name != "panewire" || strings.Join(cmd.Args, " ") != "quota refresh commandcode" {
		t.Fatalf("native refresh = %+v", cmd)
	}
	for app, want := range map[*App]string{shadow: "pacelever refresh 'commandcode'", flipped: "panewire quota refresh 'commandcode'"} {
		var lines []string
		app.Config.Hosts = map[string]map[string]any{"host-b": {}}
		app.Runner = &fakeRunner{Do: func(c Command) Result {
			lines = append(lines, c.Name+" "+strings.Join(c.Args, " "))
			return Result{}
		}}
		if err := app.measureTarget(context.Background(), "host-b", "commandcode"); err != nil {
			t.Fatal(err)
		}
		if len(lines) != 1 || !strings.HasSuffix(lines[0], " host-b "+want) {
			t.Fatalf("ssh line %q, want it to end with %q", lines, want)
		}
	}
}

func writeHostsConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// HostNativeQuotaPool reads the host's config at call time, so the desk's flip
// needs no restart, and fails to shadow.
func TestHostNativeQuotaPoolReadsTheHostConfig(t *testing.T) {
	t.Setenv("FLEET_HOSTS_CONFIG", filepath.Join(t.TempDir(), "absent.toml"))
	if HostNativeQuotaPool("commandcode") {
		t.Fatal("commandcode is native without a config")
	}
	if !HostNativeQuotaPool("claude") || !HostNativeQuotaPool("codex") || HostNativeQuotaPool("devin") {
		t.Fatal("fixed pools changed")
	}
	path := writeHostsConfig(t, "[quota]\nfresh_ttl_s = 300\n")
	t.Setenv("FLEET_HOSTS_CONFIG", path)
	if HostNativeQuotaPool("commandcode") {
		t.Fatal("commandcode is native with no native_pools")
	}
	if err := os.WriteFile(path, []byte("[quota]\nnative_pools = [\"commandcode\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !HostNativeQuotaPool("commandcode") {
		t.Fatal("commandcode stayed in shadow after the flip")
	}
	if err := os.WriteFile(path, []byte("[quota]\nnative_pools = [\"commandcode\", \"devin\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if HostNativeQuotaPool("commandcode") {
		t.Fatal("an invalid config flipped the switch")
	}
}

func TestLoadConfigValidatesNativePools(t *testing.T) {
	for body, ok := range map[string]bool{
		"[quota]\nnative_pools = [\"commandcode\"]\n": true,
		"[quota]\nnative_pools = []\n":                true,
		"[quota]\nnative_pools = \"commandcode\"\n":   false,
		"[quota]\nnative_pools = [\"devin\"]\n":       false,
		"[quota]\nnative_pools = [1]\n":               false,
	} {
		_, err := LoadConfig(writeHostsConfig(t, body))
		if (err == nil) != ok {
			t.Errorf("%q: err = %v, want ok=%v", body, err, ok)
		}
	}
}
