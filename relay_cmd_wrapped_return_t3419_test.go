package panewire

// hk 3419: production v0.0.156 held a desk answer for a working cmd pane, typed
// it when the pane went done, and then withheld the one return with
// return_withheld:composer_foreign. The text stayed unsent in the composer.
//
// The ticket's suspected cause, a composer reader that does not join the
// wrapped rows, does not reproduce: relayComposer joins every row between the
// dividers and drops all whitespace, so the wrapped fixtures below pass on the
// base commit too. They pin that reading. The second cause the code allowed is
// the gate's single read right after pane send-text, which can catch a long
// text before the harness has drawn all of it. The gate now re-reads such a
// composer for a short window (relayCmdReturnGate).

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const t3419Width = 40

func t3419Event(id int64, body string) string {
	held := relayHeld{EventID: id, Text: body}
	return relayNonce(held) + " " + held.Text
}

// t3419Long is one event long enough to wrap over four composer rows at
// t3419Width. The path token is longer than the width, so one wrap point cuts
// inside a word.
func t3419Long() string {
	return t3419Event(765186, "[operator-desk] ANSWER | lane=pw-m4-reps keep the rebased head, path=docs/relay-harness-delivery.md#cmd-return-gate then READY")
}

// t3419Wrap wraps text the way the cmd composer draws it: the first row after
// the prompt, every later row under a two-space indent, breaking at the last
// space that fits and cutting a word only when it alone is wider than a row.
// The space at a wrap point is not drawn.
func t3419Wrap(text string, width int) []string {
	var rows []string
	for len(text) > width {
		cut := strings.LastIndex(text[:width+1], " ")
		if cut <= 0 {
			rows, text = append(rows, text[:width]), text[width:]
			continue
		}
		rows, text = append(rows, text[:cut]), text[cut+1:]
	}
	rows = append(rows, text)
	for i := range rows {
		if i == 0 {
			rows[i] = "❯ " + rows[i]
		} else {
			rows[i] = "  " + rows[i]
		}
	}
	return rows
}

// t3419Plain and t3419ANSI draw composer rows between cmd's dividers: the
// plain read the return gate takes, and the ANSI read the presend takes, with
// the muted dividers and the reverse-video cursor cell after the text.
func t3419Plain(rows []string) string {
	return "⏺ done\n" + t3330CmdDivider + "\n" + strings.Join(rows, "\n") + "\n" + t3330CmdDivider + "\n  ? for shortcuts\n"
}

func t3419ANSI(rows []string) string {
	divider := t3374Muted + t3330CmdDivider + sgrReset
	styled := append([]string(nil), rows...)
	styled[len(styled)-1] += "\x1b[7m \x1b[0m"
	return "⏺ done\n" + divider + "\n" + strings.Join(styled, "\n") + "\n" + divider + "\n" + t3374Muted + "  ? for shortcuts" + sgrReset + "\n"
}

// Acceptance 1 (a)-(e): the gate's verdict on each composer, read from the
// plain screen and from the ANSI screen, and the presend's verdict on the ANSI
// screen. The presend snapshot is the idle composer the text was typed into.
func TestCmdComposerReturnGateReadsWrappedText_t3419(t *testing.T) {
	long := t3419Long()
	short := t3419Event(765187, "fix the failing check")
	other := t3419Event(765188, "second answer for the same lane")
	idle := t3419Plain([]string{"❯ Ask your question..."})
	if rows := t3419Wrap(long, t3419Width); len(rows) != 4 {
		t.Fatalf("fixture wraps over %d rows, want 4: %q", len(rows), rows)
	}

	cases := []struct {
		name     string
		rows     []string
		text     string
		safe     bool
		rule     string
		presend  string
		gateSkip bool
	}{
		{"a: one long event wrapped over 4 rows", t3419Wrap(long, t3419Width), long, true, "composer_self", "composer_pending", false},
		{"b: the long event, then an operator draft row", append(t3419Wrap(long, t3419Width), "  PASS 로 전환해줘"), long, false, "composer_foreign", "composer_pending", false},
		{"b: an operator draft row, then the long event", append([]string{"❯ PASS 로 전환해줘"}, strings.Split(strings.ReplaceAll(strings.Join(t3419Wrap(long, t3419Width), "\n"), "❯ ", "  "), "\n")...), long, false, "composer_foreign", "composer_pending", false},
		{"c: two events, two nonces", t3419Wrap(long+" "+other, t3419Width), long, false, "composer_foreign", "composer_pending", false},
		{"c: the other event alone", t3419Wrap(other, t3419Width), long, false, "composer_foreign", "composer_pending", false},
		{"d: the idle placeholder", []string{"❯ \x1b[0m\x1b[7mA\x1b[0m" + t3374Muted + "sk your question...\x1b[0m"}, long, false, "", "composer_empty", true},
		{"e: a short one-row event", t3419Wrap(short, t3419Width), short, true, "composer_self", "composer_pending", false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ansi := t3419ANSI(test.rows)
			if test.gateSkip {
				ansi = t3374Screen(test.rows[0])
			}
			if got := relayAnsiComposerState(ansi); got != test.presend {
				t.Fatalf("presend relayAnsiComposerState = %q, want %q\nscreen: %q", got, test.presend, ansi)
			}
			if test.gateSkip {
				return
			}
			for _, screen := range []struct{ source, text string }{{"plain", t3419Plain(test.rows)}, {"ansi", nodeScreenPlain(ansi)}} {
				safe, rule := relayComposerReturnSafe("cmd", screen.text, test.text, idle)
				if safe != test.safe || rule != test.rule {
					t.Fatalf("%s: relayComposerReturnSafe = %t %q, want %t %q\nscreen: %q", screen.source, safe, rule, test.safe, test.rule, screen.text)
				}
			}
		})
	}
}

func (pane *t3330Composer) set(t *testing.T, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(pane.dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Acceptance 1 (a) through the relay: the long event is drawn wrapped over
// four rows, the gate reads it as this inject's own text, the one return is
// pressed and the delivery is proven.
func TestCmdWrappedEventReturnPressedOnce_t3419(t *testing.T) {
	cases := []struct {
		name        string
		lag         string
		placeholder bool
		empty       bool
	}{
		{"drawn at once", "", true, false},
		// The production symptom: the first reads after pane send-text see only
		// the head of the text. Without the gate's re-read this is
		// return_withheld:composer_foreign.
		{"drawn late", "3", true, false},
		// The first reads still show the empty composer the text was typed
		// into. That reads composer_empty, and a return pressed then submits
		// nothing, so the gate re-reads it too (PR 321 review).
		{"drawn late into an empty composer", "3", false, true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			pane := t3330InstallComposer(t, "")
			if test.placeholder {
				pane.flag(t, "placeholder")
			}
			pane.set(t, "wrap", "40")
			if test.lag != "" {
				pane.set(t, "draw_lag", test.lag)
			}
			if test.empty {
				pane.flag(t, "lag_empty")
			}
			text := t3419Long()

			result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", text, nil)

			if result.Outcome != relayInjectDelivered {
				t.Fatalf("result=%+v, want delivered", result)
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
		})
	}
}

// Acceptance 1 (b) through the relay: an operator draft typed after the text
// is withheld at once. The gate does not wait out its window on it, because a
// draft is neither a head of the text nor the presend composer.
func TestCmdWrappedEventWithDraftIsWithheld_t3419(t *testing.T) {
	old := relayCmdDrawWindow
	relayCmdDrawWindow = 10 * time.Second
	t.Cleanup(func() { relayCmdDrawWindow = old })
	pane := t3330InstallComposer(t, "")
	pane.flag(t, "placeholder")
	pane.set(t, "wrap", "40")
	pane.flag(t, "draft_after_text")

	start := time.Now()
	result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", t3419Long(), nil)

	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "return_withheld:composer_foreign" {
		t.Fatalf("result=%+v, want maybe-in-pane with return_withheld:composer_foreign", result)
	}
	if got := pane.calls(t, "pane send-keys"); got != 0 {
		t.Fatalf("pane send-keys called %d times, want 0", got)
	}
	if elapsed := time.Since(start); elapsed >= relayCmdDrawWindow {
		t.Fatalf("withheld after %s: the gate waited out its window on a draft", elapsed)
	}
}

// A composer that never finishes drawing is withheld when the window ends;
// the re-reads never press return.
func TestCmdComposerNeverDrawnIsWithheld_t3419(t *testing.T) {
	old := relayCmdDrawWindow
	relayCmdDrawWindow = 300 * time.Millisecond
	t.Cleanup(func() { relayCmdDrawWindow = old })
	pane := t3330InstallComposer(t, "")
	pane.flag(t, "placeholder")
	pane.set(t, "wrap", "40")
	pane.set(t, "draw_lag", "1000")

	result := defaultHubRelayInjectVerdict(t.Context(), "w1:pW", t3419Long(), nil)

	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "return_withheld:composer_foreign" {
		t.Fatalf("result=%+v, want maybe-in-pane with return_withheld:composer_foreign", result)
	}
	if got := pane.calls(t, "pane send-keys"); got != 0 {
		t.Fatalf("pane send-keys called %d times, want 0", got)
	}
}
