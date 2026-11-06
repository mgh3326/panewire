package fleet

import (
	"context"
	"strings"
	"testing"
)

// panewire measures claude itself; every other pool is still pacelever's.
func TestRefreshCommandPerPool(t *testing.T) {
	claude := refreshCommand("claude", "pacelever-bin")
	if claude.Name != "panewire" || strings.Join(claude.Args, " ") != "quota refresh claude" {
		t.Fatalf("claude refresh = %+v", claude)
	}
	codex := refreshCommand("codex", "pacelever-bin")
	if codex.Name != "pacelever-bin" || strings.Join(codex.Args, " ") != "refresh codex" {
		t.Fatalf("codex refresh = %+v", codex)
	}
	if got := remoteRefreshLine("claude"); got != "panewire quota refresh 'claude'" {
		t.Fatalf("remote claude line %q", got)
	}
	if got := remoteRefreshLine("codex"); got != "pacelever refresh 'codex'" {
		t.Fatalf("remote codex line %q", got)
	}
}

// Over ssh the target runs panewire's quota command for claude, never
// pacelever.
func TestMeasureTargetSSHRefreshPerPool(t *testing.T) {
	for pool, want := range map[string]string{"claude": "panewire quota refresh 'claude'", "codex": "pacelever refresh 'codex'"} {
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
		if pool == "claude" && strings.Contains(lines[0], "pacelever") {
			t.Fatalf("claude measured through pacelever: %q", lines[0])
		}
	}
}
