package panewire

import (
	"strings"
	"testing"
	"time"
)

// Screen tails captured from finished panes, with the working directory replaced
// and the transcript shortened. A case changes only the composer row and the
// rows above it, so it runs through the layout the harness really draws.
const (
	nodeCodexIdleTail = "  Worked for 7m 22s • 7:33 AM\n" +
		"\n" +
		"\n" +
		"%COMPOSER%\n" +
		"\n" +
		"  GPT-6.1-Sol high · ~/work/<repo> · Read brief and follow instructions · Main [default]\n" +
		"  ? for shortcuts                                                            ⚠ 4 warnings · f2 to view\n"

	nodeDevinIdleTail = " No secrets, migration or irreversible-mutation issues were found in the delta.\n" +
		"\n" +
		" ⏺ ~/work/<repo>\n" +
		"──────────────────────────────────────────────────────────────────────────── Review and Follow the Delivered Coding … ─\n" +
		"%COMPOSER%\n" +
		"───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────\n" +
		"DeepSeek V4.1 Flash High · bypass                                                     Context: 124k / 1.0M tokens (11%)\n"

	nodeCodexPlaceholderRow = "› Ask Codex to do anything"
	nodeDevinPlaceholderRow = "❭ Ask Devin to build features, fix bugs, or work on your code"

	// nodeCodexQueuePreview is codex-rs/tui's pending input preview, which it
	// draws above an empty composer showing the placeholder.
	nodeCodexQueuePreview = "• Queued follow-up inputs\n" +
		"  ↳ a message the agent has not read\n" +
		"    ⇧← edit last queued message\n" +
		"\n"

	// nodeDevinQueueBanner is devin's queue header as captured live (see
	// devinQueueBannerRE), which sits directly above the composer.
	nodeDevinQueueBanner = "○ a message the agent has not read\n" +
		"── 1 queued ── ↑ edit · ↵ send now\n"
)

func nodeTailWith(tail, composer string) string {
	return strings.Replace(tail, "%COMPOSER%", composer, 1)
}

func TestNodeComposerStateIdlePlaceholders(t *testing.T) {
	cases := []struct {
		name     string
		agent    string
		screen   string
		composer string
	}{
		// Idle: the placeholder is the whole composer row.
		{"codex idle", "codex", nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow), nodeComposerEmpty},
		{"codex idle crlf", "codex", strings.ReplaceAll(nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow), "\n", "\r\n"), nodeComposerEmpty},
		{"codex idle padded", "codex", nodeTailWith(nodeCodexIdleTail, "  ›   Ask Codex to do anything   "), nodeComposerEmpty},
		{"devin idle", "devin", nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow), nodeComposerEmpty},
		{"devin idle crlf", "devin", strings.ReplaceAll(nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow), "\n", "\r\n"), nodeComposerEmpty},
		{"devin idle padded", "devin", nodeTailWith(nodeDevinIdleTail, "  ❭   Ask Devin to build features, fix bugs, or work on your code   "), nodeComposerEmpty},

		// The placeholder followed by more text is a draft.
		{"codex placeholder then typed", "codex", nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow+" and fix the tests"), nodeComposerHasText},
		{"codex placeholder then typed row", "codex", nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow+"\n  and fix the tests"), nodeComposerHasText},
		{"devin placeholder then typed", "devin", nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow+" and fix the tests"), nodeComposerHasText},
		{"devin placeholder then typed row", "devin", nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow+"\n  and fix the tests"), nodeComposerHasText},

		// A draft that merely contains the phrase is a draft.
		{"codex typed contains placeholder", "codex", nodeTailWith(nodeCodexIdleTail, "› please Ask Codex to do anything for me"), nodeComposerHasText},
		{"codex typed differs by case", "codex", nodeTailWith(nodeCodexIdleTail, "› ask codex to do anything"), nodeComposerHasText},
		{"devin typed contains placeholder", "devin", nodeTailWith(nodeDevinIdleTail, "❭ please Ask Devin to build features, fix bugs, or work on your code"), nodeComposerHasText},
		{"devin typed differs by spacing", "devin", nodeTailWith(nodeDevinIdleTail, "❭ Ask Devin to build features,fix bugs,or work on your code"), nodeComposerHasText},
		{"codex typed", "codex", nodeTailWith(nodeCodexIdleTail, "› half a thought"), nodeComposerHasText},
		{"devin typed", "devin", nodeTailWith(nodeDevinIdleTail, "❭ half a thought"), nodeComposerHasText},

		// A harness's placeholder means nothing in another harness's composer.
		{"devin placeholder in codex", "codex", nodeTailWith(nodeCodexIdleTail, "› Ask Devin to build features, fix bugs, or work on your code"), nodeComposerHasText},
		{"codex placeholder in devin", "devin", nodeTailWith(nodeDevinIdleTail, "❭ Ask Codex to do anything"), nodeComposerHasText},

		// Queued messages are unsent text behind an otherwise idle composer.
		{"codex queued above placeholder", "codex", nodeTailWith(nodeCodexIdleTail, nodeCodexQueuePreview+nodeCodexPlaceholderRow), nodeComposerHasText},
		{"codex steer above placeholder", "codex", nodeTailWith(nodeCodexIdleTail, "• Messages to be submitted after next tool call\n  ↳ steer\n\n"+nodeCodexPlaceholderRow), nodeComposerHasText},
		{"codex turn-end above placeholder", "codex", nodeTailWith(nodeCodexIdleTail, "• Messages to be submitted at end of turn\n  ↳ steer\n\n"+nodeCodexPlaceholderRow), nodeComposerHasText},
		{"devin queue hint", "devin", nodeTailWith(nodeDevinIdleTail, "❭ Press Enter to send queued messages now"), nodeComposerHasText},
		{"devin queue banner above placeholder", "devin", strings.Replace(nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow), "──────────────────────────────────────────────────────────────────────────── Review", nodeDevinQueueBanner+"──────────────────────────────────────────────────────────────────────────── Review", 1), nodeComposerHasText},

		// These prompts prove nothing about the pane's unsent text.
		{"codex side conversation hides the thread's draft", "codex", nodeTailWith(nodeCodexIdleTail, "› Ask a follow-up question"), nodeComposerHasText},
		{"codex sub-agent view hides the thread's draft", "codex", nodeTailWith(nodeCodexIdleTail, "› Viewing sub-agent — direct input is disabled"), nodeComposerHasText},
		{"codex disabled input", "codex", nodeTailWith(nodeCodexIdleTail, "› Answer the questions to continue."), nodeComposerHasText},
		{"codex retired placeholder is a plausible draft", "codex", nodeTailWith(nodeCodexIdleTail, "› Explain this codebase"), nodeComposerHasText},
		{"devin busy placeholder", "devin", nodeTailWith(nodeDevinIdleTail, "❭ Guide Devin while it works"), nodeComposerHasText},

		// Only the live composer counts: an echo above it does not.
		{"codex echoed placeholder above draft", "codex", nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow+"\n\n• Done.\n\n› half a thought"), nodeComposerHasText},

		// Nothing recognised.
		{"codex unrecognized", "codex", "  Worked for 7m 22s • 7:33 AM\n\n  ? for shortcuts\n", nodeComposerUnknown},
		{"devin unrecognized", "devin", " ⏺ ~/work/<repo>\nAsk Devin to build features, fix bugs, or work on your code\n", nodeComposerUnknown},
		{"placeholder row without a harness", "", nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow), nodeComposerUnknown},
		{"placeholder row of an unknown harness", "mystery", nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow), nodeComposerUnknown},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := nodeComposerState(test.agent, test.screen); got != test.composer {
				t.Fatalf("nodeComposerState = %q, want %q\nscreen:\n%s", got, test.composer, test.screen)
			}
		})
	}
}

// relayComposerReturnSafe decides whether a return keypress may submit what is
// in the composer. Reading the idle placeholder as empty is for closing a pane
// only, so the relay guard keeps treating it as foreign text.
func TestNodeComposerIdlePlaceholderLeavesRelayGuardAlone(t *testing.T) {
	screen := nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow)
	if safe, rule := relayComposerReturnSafe("devin", screen, "text", screen); safe || rule != "composer_foreign" {
		t.Fatalf("relayComposerReturnSafe = %t %q", safe, rule)
	}
}

func TestNodeCommandPaneCloseIdleHarnessFixtures(t *testing.T) {
	cases := []struct {
		name   string
		agent  string
		screen string
		closes int
	}{
		{"idle codex closes", "codex", nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow), 1},
		{"idle devin closes", "devin", nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow), 1},
		{"codex draft keeps the pane", "codex", nodeTailWith(nodeCodexIdleTail, nodeCodexPlaceholderRow+" and fix the tests"), 0},
		{"devin draft keeps the pane", "devin", nodeTailWith(nodeDevinIdleTail, nodeDevinPlaceholderRow+" and fix the tests"), 0},
		{"idle cmd closes", "cmd", t3374Screen(t3374IdleRow), 1},
		{"cmd draft keeps the pane", "cmd", t3374Screen("❯ fix the test"), 0},
		{"cmd draft after the cursor cell keeps the pane", "cmd", t3374Screen("❯ \x1b[7mf" + sgrReset + "ix the test"), 0},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fake := &nodeFakeHerdr{agents: []string{nodeAgentList(nodeReapRow("w1:p1", "job-a", test.agent, false))}, read: test.screen}
			expected := map[string]any{"pane_id": "w1:p1", "name": "job-a", "agent_status": "idle", "cwd": "/work/a"}
			result, code := nodeRun(t, &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond}, nodeCommandPaneClose, expected)
			if code != "" {
				t.Fatal(code)
			}
			want := nodePaneChanged
			if test.closes == 1 {
				want = nodePaneClosed
			}
			if result["outcome"] != want || fake.called("pane", "close", "w1:p1") != test.closes {
				t.Fatalf("result=%v calls=%v", result, fake.calls)
			}
			// A close rests on the composer read before the settle and the one
			// after it; a refused first read stops there.
			reads := 1
			if test.closes == 1 {
				reads = 2
			}
			if got := fake.called("pane", "read", "w1:p1", "--source", "visible"); got != reads {
				t.Fatalf("composer reads = %d, want %d: %v", got, reads, fake.calls)
			}
		})
	}
}
