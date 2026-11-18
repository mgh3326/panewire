// Package agybusy decides whether an agy (Antigravity CLI) pane that herdr
// reports idle or done is still inside a turn (hk 5241). herdr's agent_status
// for agy can read done between long tool runs while the agent still works, so
// the screen is the second source, as devinBusy uses it for devin. The rule and
// its evidence are in docs/relay-harness-delivery.md ("agy busy check").
//
// Both the root package (the hub's agent listing) and internal/fleet (the local
// pane judgements) call Status, so the rule is written once.
package agybusy

import (
	"regexp"
	"strings"
)

// Harness is herdr's name for Antigravity CLI in agent rows.
const Harness = "agy"

// VisibleLines is how many lines of the visible screen a caller reads. The
// judgement looks only at the bottom of it: the footer and the spinner rule.
const VisibleLines = 40

// spinnerLines is how many non-empty rows at the bottom of the screen a
// working spinner may sit in; it matches herdr's own agy rule.
const spinnerLines = 12

// footerLines is how many non-empty rows at the bottom hold the footer that
// reads "esc to cancel" directly under the prompt box.
const footerLines = 2

// spinnerRE is a braille spinner row: a glyph, a space, then the activity
// text ("⠋ Running tools", "⠙ Thinking"). U+2800 (blank braille) is excluded.
var spinnerRE = regexp.MustCompile(`^\s*[\x{2801}-\x{28FF}]+\s+\S`)

// Busy names why screen shows agy still in a turn, or returns "" when it shows
// a finished turn. It is conservative about an empty screen: nothing readable
// is not proof of an end of turn, so it returns "empty".
//
// Evidence, in order: the "esc to cancel" footer under the prompt box, then a
// braille spinner row in the bottom rows of the screen.
func Busy(screen string) string {
	lines := nonEmptyLines(screen)
	if len(lines) == 0 {
		return "empty"
	}
	for _, line := range tail(lines, footerLines) {
		if strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "esc to cancel") {
			return "esc_to_cancel"
		}
	}
	for _, line := range tail(lines, spinnerLines) {
		if spinnerRE.MatchString(line) {
			return "spinner"
		}
	}
	return ""
}

// Status returns the agent status a caller should act on for an agent herdr
// reports as harness with status. Only an agy pane that herdr reports idle or
// done is read again: read returns the visible screen (VisibleLines rows). The
// result is "working" when the screen shows a turn in progress, and also when
// the read fails, because an unreadable screen is not proof that agy is
// finished. Every other case returns status unchanged.
func Status(harness, status string, read func() (string, error)) string {
	if !strings.EqualFold(strings.TrimSpace(harness), Harness) {
		return status
	}
	if status != "idle" && status != "done" {
		return status
	}
	screen, err := read()
	if err != nil || Busy(screen) != "" {
		return "working"
	}
	return status
}

func nonEmptyLines(screen string) []string {
	var lines []string
	for _, line := range strings.Split(screen, "\n") {
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func tail(lines []string, n int) []string {
	if len(lines) <= n {
		return lines
	}
	return lines[len(lines)-n:]
}
