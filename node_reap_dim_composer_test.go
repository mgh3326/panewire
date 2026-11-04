package panewire

import (
	"strings"
	"testing"
)

const (
	sgrDim   = "\x1b[2m"
	sgrReset = "\x1b[0m"
)

// Synthetic `herdr pane read --format ansi` composers. A Claude prompt
// suggestion is drawn dim (SGR 2); typed text is not.
func TestNodeComposerStateClaudeDimSuggestion(t *testing.T) {
	cases := []struct {
		name     string
		composer string
		want     string
	}{
		{"all dim suggestion", "❯ " + sgrDim + "Check PR 76 status" + sgrReset, nodeComposerEmpty},
		{"all dim with dim glyph", sgrDim + "❯ accept-escalated, no batch 3 needed" + sgrReset, nodeComposerEmpty},
		{"dim then reset by 22", "❯ " + sgrDim + "Check PR 76 status\x1b[22m  ", nodeComposerEmpty},
		{"dim set with other attributes", "❯ \x1b[1;2mCheck PR 76 status" + sgrReset, nodeComposerEmpty},
		{"dim state carries over the row break", "❯ " + sgrDim + "Check PR 76\n  status" + sgrReset, nodeComposerEmpty},
		{"dim after a colour sequence", "❯ \x1b[38;2;136;136;136m" + sgrDim + "Check PR 76 status" + sgrReset, nodeComposerEmpty},
		{"truecolour 2 is not dim", "❯ \x1b[38;2;136;136;136mCheck PR 76 status" + sgrReset, nodeComposerHasText},
		{"256 colour 2 is not dim", "❯ \x1b[38;5;2mCheck PR 76 status" + sgrReset, nodeComposerHasText},
		{"mixed typed then dim", "❯ fix the " + sgrDim + "tests too" + sgrReset, nodeComposerHasText},
		{"mixed dim then typed", "❯ " + sgrDim + "Check PR " + sgrReset + "76", nodeComposerHasText},
		{"dim reset before text", "❯ " + sgrDim + sgrReset + "Check PR 76 status", nodeComposerHasText},
		{"dim cleared by 22 before text", "❯ " + sgrDim + "x\x1b[22mtyped", nodeComposerHasText},
		{"plain typed", "❯ half a thought", nodeComposerHasText},
		{"plain typed no glyph", "half a thought", nodeComposerHasText},
		{"typed on a later row", "❯ " + sgrDim + "Check PR 76 status" + sgrReset + "\nmore typed", nodeComposerHasText},
		{"plain empty", "❯ ", nodeComposerEmpty},
		{"dim chip-less blank", "❯ " + sgrDim + "   " + sgrReset, nodeComposerEmpty},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			screen := nodeReapScreen(test.composer)
			if got := nodeComposerState("claude", screen); got != test.want {
				t.Fatalf("nodeComposerState = %q, want %q\nscreen: %q", got, test.want, screen)
			}
		})
	}
}

// Dimness decides nothing for the harnesses whose composers are read as text,
// and the ANSI read must not change what they read as.
func TestNodeComposerStateOtherHarnessesIgnoreAnsi(t *testing.T) {
	cases := []struct {
		name   string
		agent  string
		screen string
		want   string
	}{
		{"codex idle", "codex", nodeTailWith(nodeCodexIdleTail, "› "+sgrDim+"Ask Codex to do anything"+sgrReset), nodeComposerEmpty},
		{"codex dim suggestion stays text", "codex", nodeTailWith(nodeCodexIdleTail, "› "+sgrDim+"half a thought"+sgrReset), nodeComposerHasText},
		{"devin idle", "devin", nodeTailWith(nodeDevinIdleTail, "❭ "+sgrDim+"Ask Devin to build features, fix bugs, or work on your code"+sgrReset), nodeComposerEmpty},
		{"devin dim suggestion stays text", "devin", nodeTailWith(nodeDevinIdleTail, "❭ "+sgrDim+"half a thought"+sgrReset), nodeComposerHasText},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := nodeComposerState(test.agent, test.screen); got != test.want {
				t.Fatalf("nodeComposerState = %q, want %q\nscreen: %q", got, test.want, test.screen)
			}
		})
	}
}

func TestNodeScreenPlainDropsEscapes(t *testing.T) {
	raw := "\x1b]0;title\x07\x1b[2K\x1b[1;38;2;1;2;3mhi\x1b[0m \x1b]8;;http://x\x1b\\link\x1b(B\r\nnext\x1b[2"
	if got := nodeScreenPlain(raw); got != "hi link\nnext" {
		t.Fatalf("nodeScreenPlain = %q", got)
	}
}

// pane-read and pane-close both read the screen with attributes, and the tail
// handed back carries none.
func TestNodePaneReadAnsiComposer(t *testing.T) {
	screen := nodeReapScreen("❯ " + sgrDim + "Check PR 76 status" + sgrReset)
	fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeReapRow("w1:p1", "job-a", "claude", false))}, read: screen}
	args := nodeReapReadArgs
	args.TailLines = 3
	result, code := nodeRun(t, &HubClient{nodeHerdr: fake.run}, nodeCommandPaneRead, args)
	if code != "" || result["composer"] != nodeComposerEmpty {
		t.Fatalf("code=%q result=%v", code, result)
	}
	if fake.called("pane", "read", "w1:p1", "--source", "visible", "--format", "ansi") != 1 {
		t.Fatalf("calls=%v", fake.calls)
	}
	if tail, _ := result["screen_tail"].(string); strings.Contains(tail, "\x1b") || !strings.Contains(tail, "Check PR 76 status") {
		t.Fatalf("screen_tail = %q", tail)
	}
}
