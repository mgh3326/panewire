package panewire

// hk 3374: an idle cmd pane draws "Ask your question..." in its empty composer
// in the muted foreground its dividers use, with the cursor (reverse video) on
// the first cell. That is not dim, so the dim rule read it as a pending draft
// and every relay event ended "presend:composer_pending". The rule that reads it
// as empty lives beside the dim rule (nodeComposerState), so the relay presend
// and the reap share it.

import (
	"testing"
)

const (
	t3374Muted = "\x1b[38;2;138;148;168m"
	// t3374IdleRow is the idle composer row of the live pane wF:p1GW, byte for
	// byte (hk:doc ticket/2026-11-07/pw-cmd-placeholder).
	t3374IdleRow = "❯ \x1b[0m\x1b[7mA\x1b[0m" + t3374Muted + "sk your question...\x1b[0m"
)

// t3374Screen draws composer between cmd's muted dividers.
func t3374Screen(composer string) string {
	divider := t3374Muted + nodeReapDivider + sgrReset
	return "transcript line\n" + divider + "\n" + composer + "\n" + divider + "\n  footer\n"
}

func TestNodeComposerStateCmdIdlePlaceholder_t3374(t *testing.T) {
	cases := []struct {
		name   string
		screen string
		want   string
	}{
		{"a: idle placeholder, cursor on the first cell", t3374Screen(t3374IdleRow), nodeComposerEmpty},
		{"b: cursor not on the first cell, all muted", t3374Screen("❯ " + t3374Muted + "Ask your question..." + sgrReset), nodeComposerEmpty},
		{"placeholder in dim", t3374Screen("❯ \x1b[7mA" + sgrReset + sgrDim + "sk your question..." + sgrReset), nodeComposerEmpty},
		{"c: draft in the default foreground", t3374Screen("❯ fix the test"), nodeComposerHasText},
		{"draft after the cursor cell", t3374Screen("❯ \x1b[7mf" + sgrReset + "ix the test"), nodeComposerHasText},
		{"typed placeholder words in the default foreground", t3374Screen("❯ Ask your question...\x1b[7m \x1b[0m"), nodeComposerHasText},
		{"placeholder with a default-foreground tail", t3374Screen("❯ \x1b[7mA" + sgrReset + t3374Muted + "sk your" + sgrReset + " question..."), nodeComposerHasText},
		{"cursor on a later cell", t3374Screen("❯ " + t3374Muted + "Ask your " + sgrReset + "\x1b[7mq" + sgrReset + t3374Muted + "uestion..." + sgrReset), nodeComposerHasText},
		{"another muted text is not the placeholder", t3374Screen("❯ " + t3374Muted + "half a thought" + sgrReset), nodeComposerHasText},
		{"placeholder muted in a colour the dividers do not use", t3374Screen("❯ \x1b[38;2;1;2;3mAsk your question..." + sgrReset), nodeComposerHasText},
		{"placeholder with a muted continuation row", t3374Screen(t3374IdleRow + "\n" + t3374Muted + "more" + sgrReset), nodeComposerHasText},
		{"d: dim suggestion", nodeReapScreen("❯ " + sgrDim + "Check PR 76 status" + sgrReset), nodeComposerEmpty},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := nodeComposerState("cmd", test.screen); got != test.want {
				t.Fatalf("nodeComposerState(cmd) = %q, want %q\nscreen: %q", got, test.want, test.screen)
			}
		})
	}
}

// With plain dividers there is no muted colour to compare against, so only dim
// qualifies; and claude's composer never takes the cmd placeholder.
func TestNodeComposerStatePlaceholderIsCmdOnly_t3374(t *testing.T) {
	if got := nodeComposerState("cmd", nodeReapScreen(t3374IdleRow)); got != nodeComposerHasText {
		t.Fatalf("cmd with default-foreground dividers = %q, want has_text: no muted colour to match", got)
	}
	if got := nodeComposerState("claude", t3374Screen(t3374IdleRow)); got != nodeComposerHasText {
		t.Fatalf("claude = %q, want has_text: the placeholder is cmd's", got)
	}
}

// Acceptance 1(a): the production symptom. The idle pane is typed into once and
// the return is pressed once.
func TestCmdIdlePlaceholderIsTypedOnce_t3374(t *testing.T) {
	pane := t3330InstallComposer(t, "")
	pane.flag(t, "placeholder")
	text := t3330RelayText()

	result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", text, nil)

	if result.Outcome != relayInjectDelivered {
		t.Fatalf("result=%+v, want delivered: the placeholder is not a pending draft", result)
	}
	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times, want 1", got)
	}
	if got := pane.calls(t, "pane send-keys"); got != 1 {
		t.Fatalf("pane send-keys called %d times, want exactly 1 return", got)
	}
	if got := pane.submittedCopies(t, text); got != 1 {
		t.Fatalf("submitted copies=%d, want exactly 1", got)
	}
}

// Acceptance 1(c) through the relay: a real draft beside the muted dividers is
// withheld and nothing is written.
func TestCmdDraftBesideMutedDividersIsWithheld_t3374(t *testing.T) {
	pane := t3330InstallComposer(t, "fix the test")
	pane.flag(t, "placeholder")

	result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", t3330RelayText(), nil)

	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "presend:composer_pending" {
		t.Fatalf("result=%+v, want maybe-in-pane with presend:composer_pending", result)
	}
	if got := pane.calls(t, "pane send-text") + pane.calls(t, "pane send-keys"); got != 0 {
		t.Fatalf("the pane was written to %d times, want 0", got)
	}
}
