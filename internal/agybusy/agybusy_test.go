package agybusy

import (
	"errors"
	"testing"
)

// The screens below are reconstructed from herdr 0.9.5's own agy rule table
// (herdr's agy evidence comment: a working turn draws a braille spinner row
// above the prompt box and "esc to cancel" directly under its bottom border;
// an idle prompt shows "? for shortcuts"). No live mid-tool-run capture was
// available when this was written (hk 5241); docs/relay-harness-delivery.md
// says what that leaves open.
const workingToolRun = ` Antigravity CLI

 > run the test suite for the fleet package

 ⏺ Bash(go test ./internal/fleet/ -count=1)
   ⎿  ok  github.com/mgh3326/panewire/internal/fleet  4.012s
   ⎿  (output truncated)

 ⠙ Running tools · 42s
╭──────────────────────────────────────────────────────────────────────────────╮
│ >                                                                            │
╰──────────────────────────────────────────────────────────────────────────────╯
   esc to cancel
`

const endOfTurn = ` Antigravity CLI

 > run the test suite for the fleet package

 ⏺ The fleet package passes: go test ./internal/fleet/ reports ok.

╭──────────────────────────────────────────────────────────────────────────────╮
│ >                                                                            │
╰──────────────────────────────────────────────────────────────────────────────╯
 ? for shortcuts
`

func TestBusyWorkingToolRunIsBusy(t *testing.T) {
	if got := Busy(workingToolRun); got != "esc_to_cancel" {
		t.Fatalf("Busy(working tool run) = %q, want esc_to_cancel", got)
	}
}

func TestBusySpinnerOnlyIsBusy(t *testing.T) {
	// No footer, only the spinner row above the prompt box.
	screen := " Antigravity CLI\n\n ⠋ Thinking about the diff\n╭──╮\n│ > │\n╰──╯\n"
	if got := Busy(screen); got != "spinner" {
		t.Fatalf("Busy(spinner only) = %q, want spinner", got)
	}
}

func TestBusyEndOfTurnIsIdle(t *testing.T) {
	if got := Busy(endOfTurn); got != "" {
		t.Fatalf("Busy(end of turn) = %q, want idle", got)
	}
}

func TestBusyEmptyOrBlankIsBusy(t *testing.T) {
	for _, screen := range []string{"", "   \n\n  \n"} {
		if got := Busy(screen); got != "empty" {
			t.Fatalf("Busy(%q) = %q, want empty (conservative)", screen, got)
		}
	}
}

func TestBusyIgnoresFooterFarAboveThePrompt(t *testing.T) {
	// "esc to cancel" from an earlier turn, with a finished answer below it,
	// is transcript text, not the footer under the prompt box.
	screen := "   esc to cancel\n answer line one\n answer line two\n answer line three\n? for shortcuts\n"
	if got := Busy(screen); got != "" {
		t.Fatalf("Busy(old footer above answer) = %q, want idle", got)
	}
}

func TestStatusAgyIdleWithBusyScreenIsWorking(t *testing.T) {
	for _, status := range []string{"idle", "done"} {
		got := Status(Harness, status, func() (string, error) { return workingToolRun, nil })
		if got != "working" {
			t.Fatalf("Status(agy, %s, working screen) = %q, want working", status, got)
		}
	}
}

func TestStatusAgyEndOfTurnStaysDone(t *testing.T) {
	got := Status(Harness, "done", func() (string, error) { return endOfTurn, nil })
	if got != "done" {
		t.Fatalf("Status(agy, done, end of turn) = %q, want done", got)
	}
}

func TestStatusAgyUnreadableScreenIsWorking(t *testing.T) {
	// Mutant guard: an unreadable screen must not read as idle.
	got := Status(Harness, "idle", func() (string, error) { return "", errors.New("agent read: pane gone") })
	if got != "working" {
		t.Fatalf("Status(agy, idle, read error) = %q, want working", got)
	}
}

func TestStatusAgyEmptyScreenIsWorking(t *testing.T) {
	got := Status(Harness, "done", func() (string, error) { return "  \n", nil })
	if got != "working" {
		t.Fatalf("Status(agy, done, blank screen) = %q, want working", got)
	}
}

func TestStatusWorkingAndBlockedAreNotRead(t *testing.T) {
	read := func() (string, error) {
		t.Fatal("a status that is not idle or done must not read the screen")
		return "", nil
	}
	for _, status := range []string{"working", "blocked", "unknown", ""} {
		if got := Status(Harness, status, read); got != status {
			t.Fatalf("Status(agy, %q) = %q, want unchanged", status, got)
		}
	}
}

func TestStatusOtherHarnessesAreUnchanged(t *testing.T) {
	// devin's own check lives in the root package; this rule must not touch it.
	read := func() (string, error) {
		t.Fatal("a non-agy harness must not read the screen")
		return "", nil
	}
	for _, harness := range []string{"devin", "claude", "codex", "cmd", ""} {
		if got := Status(harness, "done", read); got != "done" {
			t.Fatalf("Status(%q, done) = %q, want done", harness, got)
		}
	}
}

func TestStatusHarnessNameIsCaseInsensitive(t *testing.T) {
	got := Status(" AGY ", "idle", func() (string, error) { return workingToolRun, nil })
	if got != "working" {
		t.Fatalf("Status(\" AGY \", idle, busy) = %q, want working", got)
	}
}
