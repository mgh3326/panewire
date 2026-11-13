package fleet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

type fakeRunner struct {
	Calls []Command
	Do    func(Command) Result
}

func (f *fakeRunner) Run(_ context.Context, c Command) Result {
	if c.Name == "arbiter" {
		// The local ledger is in-process (internal/arbiter); panewire must never exec it.
		panic("panewire executed arbiter: " + strings.Join(c.Args, " "))
	}
	f.Calls = append(f.Calls, c)
	var result Result
	if f.Do != nil {
		result = f.Do(c)
	}
	if c.Name == "herdr" && strings.Join(c.Args, " ") == "workspace list" && result.Code == 0 && result.Out == "" {
		result.Out = `{"result":{"workspaces":[{"workspace_id":"workspace","label":"workspace"},{"workspace_id":"target-workspace","label":"target-workspace"},{"workspace_id":"local-workspace","label":"local-workspace"},{"workspace_id":"remote-workspace","label":"remote-workspace"},{"workspace_id":"caller-only-workspace","label":"caller-only-workspace"},{"workspace_id":"workers","label":"workers"}]}}`
	}
	return result
}
func code(err error) int {
	var e *ExitError
	if errors.As(err, &e) {
		return e.Code
	}
	if err != nil {
		return 1
	}
	return 0
}
func TestR15GateRoleDenialRemaps77(t *testing.T) {
	for _, n := range []int{0, 2, 3, 4, 5, 6, 70} {
		t.Run("gate_exit_"+strconv.Itoa(n), func(t *testing.T) {
			f := &fakeRunner{Do: func(Command) Result { return Result{Code: n, Out: `{}`} }}
			q := CLIQuota{Runner: f, Binary: "quota"}
			_, e := q.Gate(context.Background(), QuotaRequest{Profile: "profile", Role: "tester", Effort: "high"})
			want := n
			if n == 5 {
				want = 77
			}
			if code(e) != want {
				t.Fatalf("code=%d want=%d", code(e), want)
			}
		})
	}
}
func TestSeatPolicy(t *testing.T) {
	p, e := LoadPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	for _, tt := range []struct {
		name, profile, role, gate, grade, builder, arm string
		host                                           bool
		want                                           int
	}{
		{"cmd worker default", "cmd-dsflash41", "worker", "default", "B", "", "", true, 0},
		{"cmd shares ds41", "cmd-dsflash41", "tester", "default", "B", "deepseek", "", true, 77},
		{"independent tester", "cmd-dsflash41", "tester", "default", "B", "anthropic", "", true, 0},
		{"missing builder family", "cmd-dsflash41", "tester", "default", "B", "", "", true, 77},
		{"grade too low", "cmd-dsflash41", "worker", "default", "S+", "", "", true, 77},
		{"capability absent", "cmd-dsflash41", "worker", "default", "B", "", "", false, 70},
		{"E6 marker absent", "builder-haiku-high", "worker", "default", "B", "", "", true, 77},
		{"E6 marker exact", "builder-haiku-high", "worker", "default", "B", "", "haiku@high", true, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := p.Profiles[tt.profile]
			host := map[string]any{s.Capability: tt.host}
			l := Launch{ModelID: "provider/model", Effort: "high", Grade: "A+", Gate: tt.gate}
			if got := code(s.Check(l, tt.role, tt.grade, tt.builder, tt.arm, host)); got != tt.want {
				t.Fatalf("got %d want %d", got, tt.want)
			}
		})
	}
}
func TestHostsConfigFailClosed(t *testing.T) {
	for _, input := range []string{"[local]\nspawn = 'true'", "[local]\nspawn=true\nspawn=false", "[hub]\nquota_gate='typo'", "[hub]\nstate='maybe'", "[hub]\nstate_publisher_ttl_s=0", "[hub]\nhub_token_env='TOKEN'", "[local]\ncmd_dsflash41=1"} {
		t.Run(input, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "hosts.toml")
			if e := os.WriteFile(path, []byte(input), 0600); e != nil {
				t.Fatal(e)
			}
			if _, e := LoadConfig(path); code(e) != 70 {
				t.Fatalf("expected 70: %v", e)
			}
		})
	}
}

// TestRetiredHeavyKeysAreIgnored keeps old hosts.toml files loading: the
// removed heavy command's keys are accepted as inert data, never errors.
func TestRetiredHeavyKeysAreIgnored(t *testing.T) {
	input := "[local]\nheavy_max = 2\nheavy_load_wait = true\n[hosts.box]\nheavy = true\n"
	path := filepath.Join(t.TempDir(), "hosts.toml")
	if e := os.WriteFile(path, []byte(input), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := LoadConfig(path); e != nil {
		t.Fatalf("retired heavy keys must load without error: %v", e)
	}
}
func TestR02InstalledCatalogOwnsModelsEffortsAndGrades(t *testing.T) {
	f := &fakeRunner{Do: func(c Command) Result {
		return Result{Out: `{"profile":"p","model_id":"new-model-from-installed-tool","effort":"high","grade":"A+","gate":"default"}`}
	}}
	q := CLIQuota{Runner: f, Binary: "pacelever"}
	l, e := q.Launch(context.Background(), QuotaRequest{Profile: "p", Effort: "high", Role: "worker"})
	if e != nil || l.ModelID != "new-model-from-installed-tool" {
		t.Fatalf("%+v %v", l, e)
	}
	if !strings.Contains(strings.Join(f.Calls[0].Args, " "), "--role worker") {
		t.Fatal("role not forwarded")
	}
}
func TestBriefPolicyHostAndProfileOverrides(t *testing.T) {
	a := testApp(t)
	a.Config.Local = map[string]any{"machine_id": "host-a"}
	path := filepath.Join(t.TempDir(), "brief.json")
	b := `{"version":1,"default":"d","blocks":{"d":"default","h":"host","p":"profile"},"hosts":{"host-a":"h"},"profiles":{"p":"p"}}`
	if e := os.WriteFile(path, []byte(b), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PANEWIRE_BRIEF_POLICY", path)
	for _, tt := range []struct{ profile, want string }{{"x", "host"}, {"p", "profile"}} {
		got, e := a.prepareBrief(Spawn{Profile: tt.profile}, Seat{}, Launch{}, "BODY")
		if e != nil || !strings.Contains(got, tt.want+"\nLaunch identity: family=unknown launch_profile=unknown (use these for the Mergewake-Author-Family and Mergewake-Author-Profile trailers)\n\nBODY") {
			t.Fatalf("%q %v", got, e)
		}
	}
}

// The embedded policy holds only launchable seats, and none of the harnesses
// hk policy/index retired (grok, kimi, pi-*) may come back as a profile.
func TestEmbeddedPolicyHasNoRetiredOrUnlaunchableSeat(t *testing.T) {
	p, e := LoadPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	a := testApp(t)
	retired := regexp.MustCompile(`grok|kimi|^pi-|^pi$`)
	for name, s := range p.Profiles {
		if retired.MatchString(name) || retired.MatchString(s.Kind) || retired.MatchString(s.Canon) {
			t.Errorf("%s: retired seat (kind %s, canon %s)", name, s.Kind, s.Canon)
		}
		if _, _, e := a.launchArgs(s, Launch{ModelID: "provider/model", Effort: "high"}); e != nil && strings.Contains(e.Error(), "unsupported agent kind") {
			t.Errorf("%s: %v", name, e)
		}
	}
}
