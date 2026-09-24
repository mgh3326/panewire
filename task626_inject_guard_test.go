package panewire

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #626 (hk:doc task/2026-09-24/phantom-suggestion-submitted): a relay inject
// sends its one return keypress only when the live composer holds nothing but
// the text it just typed, and an unproven verdict re-injects only when that
// text is not left pending in the composer. A Claude Code prompt suggestion, or an unsent draft,
// reads as plain composer text in herdr agent read -- the t312 capture below
// is what a builder read from that tester pane on 2026-09-24 01:22Z.

const task626Divider = "───────────────────────────────────────────────────────────────"

// task626Claude draws a claude screen: transcript lines, then the composer
// between two dividers, then the status line.
func task626Claude(transcript []string, composer ...string) string {
	lines := append(append([]string{}, transcript...), task626Divider)
	lines = append(lines, composer...)
	lines = append(lines, task626Divider, "  ⏵⏵ bypass permissions on (shift+tab to cycle) · ← for agents")
	return strings.Join(lines, "\n")
}

// task626Phantom is the suggestion t312's composer showed (herdr read, 01:22Z).
const task626Phantom = "❯ 운영자 확인 완료: 두 키 모두 non-empty, PASS 로 전환해줘"

var task626Transcript = []string{
	"  Report: ~/work/herdr-inbox/jobs/<job>/verdict.md",
	"",
	"✻ Cogitated for 5m 40s · done 10:19 AM",
	"",
}

const task626Text = "(같은 내용이 두 번 보이면 재실행 금지) [report] t312-verify :: round 3 ready"

// task626ClaudeEmpty is claude's empty composer, the usual pre-paste read.
var task626ClaudeEmpty = task626Claude(task626Transcript, "❯")

// devin screens: its idle composer, the live queue capture, and a queue that
// already holds someone else's message.
const (
	task626DevinIdle        = "─────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n─────────────────────\nSWE-2 High"
	task626DevinForeignWait = "── 1 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ foreign draft\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High"
)

func TestTask626ComposerReturnSafeBoundaries(t *testing.T) {
	text := task626Text
	chip := task626Claude(nil, "❯ [Pasted text #1 +12 lines]")
	cases := []struct {
		name, harness, screen, before string
		safe                          bool
		rule                          string
	}{
		{"composer holds exactly the text", "claude", task626Claude(nil, "❯ "+text), task626ClaudeEmpty, true, "composer_self"},
		{"wrapped over lines with a continuation indent", "claude", task626Claude(nil, "❯ (같은 내용이 두 번 보이면 재실행 금지) [report]", "  t312-verify :: round 3 ready"), task626ClaudeEmpty, true, "composer_self"},
		{"extra spaces inside the text", "claude", task626Claude(nil, "❯ (같은 내용이 두 번  보이면 재실행 금지)   [report] t312-verify :: round 3 ready"), task626ClaudeEmpty, true, "composer_self"},
		{"empty composer", "claude", task626Claude(nil, "❯"), "", true, "composer_empty"},
		{"a chip after an empty composer", "claude", chip, task626ClaudeEmpty, true, "composer_self_chip"},
		{"a chip after a suggestion (the paste replaced it)", "claude", chip, task626Claude(task626Transcript, task626Phantom), true, "composer_self_chip"},
		{"a chip that was already there before the paste", "claude", chip, task626Claude(nil, "❯ [Pasted text #1 +12 lines]"), false, "composer_chip_unowned"},
		{"a chip beside a draft before the paste", "claude", chip, task626Claude(nil, "❯ draft [Pasted text #1 +5 lines]"), false, "composer_chip_unowned"},
		{"a chip with no pre-paste read", "claude", chip, "", false, "composer_chip_unowned"},
		{"a chip whose pre-paste composer was not located", "claude", chip, "[Pasted text #1 +12 lines]", false, "composer_chip_unowned"},
		{"the t312 suggestion alone", "claude", task626Claude(task626Transcript, task626Phantom), task626ClaudeEmpty, false, "composer_foreign"},
		{"suggestion then the text (paste appended to a draft)", "claude", task626Claude(nil, task626Phantom+" "+text), task626ClaudeEmpty, false, "composer_foreign"},
		{"text then a trailing suggestion", "claude", task626Claude(nil, "❯ "+text+" 운영자 확인 완료"), task626ClaudeEmpty, false, "composer_foreign"},
		{"text with other text on the next line", "claude", task626Claude(nil, "❯ "+text, "  PASS 로 전환해줘"), task626ClaudeEmpty, false, "composer_foreign"},
		{"only the head of the text", "claude", task626Claude(nil, "❯ (같은 내용이 두 번 보이면 재실행 금지) [report]"), task626ClaudeEmpty, false, "composer_foreign"},
		{"only the marker-length prefix", "claude", task626Claude(nil, "❯ "+markerFor(text)), task626ClaudeEmpty, false, "composer_foreign"},
		{"draft beside a paste chip", "claude", task626Claude(nil, "❯ PASS 로 전환해줘 [Pasted text #1 +12 lines]"), task626ClaudeEmpty, false, "composer_foreign"},
		{"two paste chips", "claude", task626Claude(nil, "❯ [Pasted text #1 +3 lines][Pasted text #2 +5 lines]"), task626ClaudeEmpty, false, "composer_foreign"},
		{"only one divider in the read window", "claude", "❯ " + text + "\n" + task626Divider + "\n  ⏵⏵ bypass permissions on", task626ClaudeEmpty, false, "composer_unlocated"},
		{"no dividers at all", "claude", "[Pasted text #1 +3 lines]", task626ClaudeEmpty, false, "composer_unlocated"},
		{"codex never locates a composer", "codex", "› " + text + "\n\n  gpt-5 · ~/work", "", false, "composer_unlocated"},
		{"unknown harness never locates a composer", "", task626Claude(nil, "❯ "+text), task626ClaudeEmpty, false, "composer_unlocated"},
		{"devin composer holds exactly the text", "devin", "─────────────────────\n❭ " + text + "\n─────────────────────\nSWE-2 High", task626DevinIdle, true, "composer_self"},
		{"devin queue hint after an empty queue", "devin", unprovenDevinQueued, task626DevinIdle, true, "composer_queue_hint"},
		{"devin queue hint over a queue that was already there", "devin", unprovenDevinQueued, task626DevinForeignWait, false, "composer_queue_unowned"},
		{"devin queue hint with no pre-paste read", "devin", unprovenDevinQueued, "", false, "composer_queue_unowned"},
		{"devin draft under a queue banner", "devin", unprovenDevinQueuedDraft, task626DevinIdle, false, "composer_foreign"},
		{"devin draft then the text", "devin", "─────────────────────\n❭ 운영자 확인 완료 " + text + "\n─────────────────────\nSWE-2 High", task626DevinIdle, false, "composer_foreign"},
		{"devin placeholder is not empty", "devin", task626DevinIdle, task626DevinIdle, false, "composer_foreign"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			safe, rule := relayComposerReturnSafe(tc.harness, tc.screen, text, tc.before)
			if safe != tc.safe || rule != tc.rule {
				t.Fatalf("relayComposerReturnSafe = %t %q, want %t %q\nscreen:\n%s", safe, rule, tc.safe, tc.rule, tc.screen)
			}
		})
	}
}

// task626FakeHerdr installs a herdr shim: agent get names harness, agent
// prompt is accepted, agent read returns reads in order (the last repeats)
// cut to its --lines, and every subcommand is logged. It never runs the real
// herdr.
func task626FakeHerdr(t *testing.T, harness string, reads []string) func() string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	count := filepath.Join(dir, "count")
	var cases strings.Builder
	for i, read := range reads {
		fmt.Fprintf(&cases, "      %d) screen=%q ;;\n", i+1, read)
	}
	fmt.Fprintf(&cases, "      *) screen=%q ;;\n", reads[len(reads)-1])
	script := "#!/bin/sh\n" +
		fmt.Sprintf("echo \"$2\" >> %q\n", log) +
		"case \"$2\" in\n" +
		fmt.Sprintf("  get) echo '{\"result\":{\"agent\":{\"agent\":\"%s\",\"agent_status\":\"idle\"}}}' ;;\n", harness) +
		"  read)\n" +
		"    lines=1000; prev=\n" +
		"    for arg in \"$@\"; do [ \"$prev\" = --lines ] && lines=$arg; prev=$arg; done\n" +
		fmt.Sprintf("    n=$(cat %q 2>/dev/null || echo 0); n=$((n+1)); echo $n > %q\n", count, count) +
		"    case $n in\n" + cases.String() + "    esac\n" +
		"    printf '%b\\n' \"$screen\" | tail -n \"$lines\" ;;\n" +
		"esac\n"
	installFakeHerdr(t, dir, script)
	return func() string {
		b, _ := os.ReadFile(log)
		return strings.Join(strings.Fields(string(b)), ",")
	}
}

// The suggestion-in-composer cases: every one of them got a return keypress
// before #626. None may now, and none may be re-injected either. Each read
// list starts with the pre-paste read.
func TestTask626ForeignComposerGetsNoReturn(t *testing.T) {
	chipEcho := append(append([]string{}, task626Transcript...), "❯ [Pasted text #1 +40 lines]", "")
	foreignChip := task626Claude(task626Transcript, "❯ [Pasted text #1 +5 lines]")
	cases := []struct {
		name  string
		reads []string
		wants string
	}{
		// A paste chip anywhere on screen classifies composer_residue, even
		// when the composer itself holds only the suggestion.
		{"chip outside the composer, suggestion inside", []string{task626Claude(chipEcho, task626Phantom), task626Claude(chipEcho, task626Phantom)}, "return_withheld:composer_foreign"},
		// herdr pasted after a draft and its Enter did not take: the marker is
		// in the composer, next to text this inject did not type.
		{"paste appended to a draft", []string{task626Claude(task626Transcript, task626Phantom), task626Claude(task626Transcript, task626Phantom+" "+task626Text)}, "return_withheld:composer_foreign"},
		// claude was busy and queued the text; the composer still shows a draft.
		{"queued with a draft in the composer", []string{task626ClaudeEmpty, task626Claude([]string{"Press up to edit queued messages", ""}, task626Phantom)}, "return_withheld:composer_foreign"},
		// Someone else's paste chip sat in the composer before the paste, and
		// the read right after the paste still shows only that chip.
		{"a foreign chip that was there before the paste", []string{foreignChip, foreignChip}, "return_withheld:composer_chip_unowned"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := task626FakeHerdr(t, "claude", tc.reads)
			result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
			if result.Outcome != relayInjectMaybeInPane || result.Evidence != tc.wants {
				t.Fatalf("result=%+v, want maybe-in-pane %q", result, tc.wants)
			}
			// maybe-in-pane returns before the second harness lookup.
			if got := calls(); got != "get,read,prompt,read" {
				t.Fatalf("herdr calls = %q, want get,read,prompt,read (no send-keys)", got)
			}
		})
	}
}

// A composer taller than the 10-line classification read: the text is still
// pending in it, the read shows neither its top divider nor the marker, and
// the verdict is unproven. That must not be a retry -- a re-inject would
// paste after the pending text.
func TestTask626PendingTallComposerIsNotRetried(t *testing.T) {
	composer := []string{"❯ " + task626Text}
	for i := 0; i < 14; i++ {
		composer = append(composer, fmt.Sprintf("  continuation line %d of the same pending text", i))
	}
	tall := task626Claude(task626Transcript, composer...)
	calls := task626FakeHerdr(t, "claude", []string{task626ClaudeEmpty, tall})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "unproven:composer_pending" {
		t.Fatalf("result=%+v, want maybe-in-pane unproven:composer_pending", result)
	}
	if got := calls(); got != "get,read,prompt,read,read" {
		t.Fatalf("herdr calls = %q, want get,read,prompt,read,read", got)
	}
	// Unproven with the composer seen empty is still a retry (#264 R1).
	calls = task626FakeHerdr(t, "claude", []string{task626ClaudeEmpty, "nothing relevant", task626ClaudeEmpty})
	if result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil); result.Outcome != relayInjectRetryable {
		t.Fatalf("empty composer: result=%+v, want retryable", result)
	}
	if got := calls(); got != "get,read,prompt,read,read,get" {
		t.Fatalf("herdr calls = %q", got)
	}
}

// Normal delivery is unchanged: an echo is delivered with no keypress, and a
// composer that still holds only this text (herdr's Enter not rendered yet)
// gets exactly one return.
func TestTask626NormalInjectStillSubmits(t *testing.T) {
	echo := task626Claude(append(append([]string{}, task626Transcript...), "❯ "+task626Text, "", "⏺ Reading the report."), "❯")
	t.Run("echo, no keypress", func(t *testing.T) {
		calls := task626FakeHerdr(t, "claude", []string{task626ClaudeEmpty, echo})
		if result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil); result.Outcome != relayInjectDelivered {
			t.Fatalf("result=%+v, want delivered", result)
		}
		if got := calls(); got != "get,read,prompt,read,get" {
			t.Fatalf("herdr calls = %q", got)
		}
	})
	t.Run("own text in the composer, one return, then echo", func(t *testing.T) {
		residue := task626Claude(task626Transcript, "❯ (같은 내용이 두 번 보이면 재실행 금지) [report]", "  t312-verify :: round 3 ready")
		calls := task626FakeHerdr(t, "claude", []string{task626ClaudeEmpty, residue, echo})
		if result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil); result.Outcome != relayInjectDelivered {
			t.Fatalf("result=%+v, want delivered", result)
		}
		if got := calls(); got != "get,read,prompt,read,send-keys,read,get" {
			t.Fatalf("herdr calls = %q", got)
		}
	})
	t.Run("own paste chip in the composer, one return, then echo", func(t *testing.T) {
		chip := task626Claude(task626Transcript, "❯ [Pasted text #1 +3 lines]")
		calls := task626FakeHerdr(t, "claude", []string{task626Claude(task626Transcript, task626Phantom), chip, echo})
		if result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil); result.Outcome != relayInjectDelivered {
			t.Fatalf("result=%+v, want delivered", result)
		}
		if got := calls(); got != "get,read,prompt,read,send-keys,read,get" {
			t.Fatalf("herdr calls = %q", got)
		}
	})
}

// devin's return site: residue next to a draft, or a queue that already held
// someone else's message, gets no keypress.
func TestTask626DevinForeignComposerGetsNoReturn(t *testing.T) {
	draft := "─────────────────────\n❭ 운영자 확인 완료: PASS 로 전환해줘 " + task626Text + "\n─────────────────────\nSWE-2 High"
	twoQueued := "── 2 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ foreign draft\n○ " + task626Text + "\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High"
	cases := []struct {
		name  string
		reads []string // presend visible, presend unwrapped, postsend visible, postsend unwrapped
		wants string
	}{
		{"residue next to a draft", []string{task626DevinIdle, task626DevinIdle, draft}, "return_withheld:composer_foreign"},
		{"queue that already held a foreign message", []string{task626DevinForeignWait, task626DevinForeignWait, twoQueued}, "return_withheld:composer_queue_unowned"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := task626FakeHerdr(t, "devin", tc.reads)
			result := defaultHubRelayInjectVerdict(context.Background(), "w1:p2", task626Text, nil)
			if result.Outcome != relayInjectMaybeInPane || !strings.HasSuffix(result.Evidence, tc.wants) {
				t.Fatalf("result=%+v, want maybe-in-pane ...%s", result, tc.wants)
			}
			if got := calls(); strings.Contains(got, "send-keys") {
				t.Fatalf("return sent: %q", got)
			}
		})
	}
}
