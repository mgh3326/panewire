package fleet

import (
	"context"
	"strings"
	"testing"
)

// panewire measures claude and codex itself; every other pool is still
// pacelever's.
func TestRefreshCommandPerPool(t *testing.T) {
	claude := refreshCommand("claude", "pacelever-bin")
	if claude.Name != "panewire" || strings.Join(claude.Args, " ") != "quota refresh claude" {
		t.Fatalf("claude refresh = %+v", claude)
	}
	codex := refreshCommand("codex", "pacelever-bin")
	if codex.Name != "panewire" || strings.Join(codex.Args, " ") != "quota refresh codex" {
		t.Fatalf("codex refresh = %+v", codex)
	}
	devin := refreshCommand("devin", "pacelever-bin")
	if devin.Name != "pacelever-bin" || strings.Join(devin.Args, " ") != "refresh devin" {
		t.Fatalf("devin refresh = %+v", devin)
	}
	if got := remoteRefreshLine("claude"); got != "panewire quota refresh 'claude'" {
		t.Fatalf("remote claude line %q", got)
	}
	if got := remoteRefreshLine("codex"); got != "panewire quota refresh 'codex'" {
		t.Fatalf("remote codex line %q", got)
	}
	if got := remoteRefreshLine("devin"); got != "pacelever refresh 'devin'" {
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
