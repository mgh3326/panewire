package panewire

import (
	"strings"
	"testing"
)

// titledClaudeScreen is a Claude Code pane whose session title is drawn into
// the composer's top edge, as on the desk pane (synthetic text).
func titledClaudeScreen(composer string) string {
	return strings.Join([]string{
		"⏺ Earlier assistant output.",
		"",
		"──────────────────────────────────────── Operator desk batch 5 decisions (2) ─",
		"❯ " + composer,
		"──────────────────────────────────────────────────────────────────────────────",
		"  [Opus 5.5] │ desk git:(main)",
		"  ⏵⏵ bypass permissions on (shift+tab to cycle)",
	}, "\n")
}

func TestRelayComposerLocatesTitledClaudeComposer(t *testing.T) {
	for _, tc := range []struct{ typed, want string }{{"", ""}, {"hello there", "hellothere"}} {
		got, ok := relayComposer("claude", titledClaudeScreen(tc.typed))
		if !ok || got != tc.want {
			t.Fatalf("typed %q: composer=%q located=%v, want %q located", tc.typed, got, ok, tc.want)
		}
	}
	region, ok := composerRegion(titledClaudeScreen("queued text"))
	if !ok || !strings.Contains(region, "queued text") {
		t.Fatalf("composerRegion: %q %v", region, ok)
	}
	lines := strings.Split(titledClaudeScreen(""), "\n")
	if top, _, ok := promptComposer("claude", lines); !ok || top != 2 {
		t.Fatalf("promptComposer: top=%d ok=%v", top, ok)
	}
}

func TestClaudeDividerRejectsLookalikes(t *testing.T) {
	for _, line := range []string{
		"──────── a heading that does not end with a rule",
		"text ──────── text ─",
		"─── short title ─",
		"",
	} {
		if isClaudeDividerLine(line) {
			t.Fatalf("%q must not count as a Claude composer edge", line)
		}
	}
	for _, line := range []string{
		"────────────────────",
		"  ──────────────── Some session title (3) ─  ",
	} {
		if !isClaudeDividerLine(line) {
			t.Fatalf("%q must count as a Claude composer edge", line)
		}
	}
}
