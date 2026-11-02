package fleet

import "testing"

func TestProcNameStripsExeSuffix(t *testing.T) {
	for in, want := range map[string]string{
		"/home/u/.local/lib/node_modules/@anthropic-ai/claude-code/bin/claude.exe": "claude",
		"claude":         "claude",
		"/usr/bin/devin": "devin",
		"kimi-code":      "kimi-code",
	} {
		if got := procName(in); got != want {
			t.Fatalf("procName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestProcessNameFallsBackWhenArgv0Missing(t *testing.T) {
	if got := procName(firstNonEmpty("", firstArg([]string{"claude", "--model", "sonnet"}), "claude")); got != "claude" {
		t.Fatalf("argv fallback = %q", got)
	}
	if got := procName(firstNonEmpty("", firstArg(nil), "claude")); got != "claude" {
		t.Fatalf("name fallback = %q", got)
	}
}
