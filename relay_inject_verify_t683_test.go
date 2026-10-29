package panewire

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #683: node-side claude inject verification reported unproven for text that
// had in fact landed -- the #650 AC3 false negative. The marker had scrolled
// out of the 10-line classification read on a busy pane; busy_relay then
// retried and the pane showed the text up to relayMaxInjectAttempts times.
// Verification now reads the visible window plus the recent-unwrapped
// transcript, accepts the harness queue banner when its nonce shows, and
// treats every undecidable verdict as may-be-in-pane -- never a re-inject.
//
// #687: proof is a per-row nonce token, not the rendered text. These tests
// inject the composed, nonce-bearing text (what deliver() sends) and the
// fake panes echo it the way claude does.

// task683FakeHerdr installs a herdr stand-in whose "agent read" answer is
// chosen by the --source flag: the visible list serves --source visible
// reads and the unwrapped list serves --source recent-unwrapped reads, each
// consumed in order with the last entry repeating. Every invocation is
// appended to a call log, one word per call, so a test can count prompts and
// keypresses from disk.
func task683FakeHerdr(t *testing.T, agent string, visible, unwrapped []string) (dir string, calls func() []string) {
	t.Helper()
	dir = t.TempDir()
	for key, screens := range map[string][]string{"vis": visible, "uw": unwrapped} {
		if len(screens) == 0 {
			screens = []string{""}
		}
		for i, screen := range screens {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%s.%d", key, i+1)), []byte(screen), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, key+".last"), []byte(screens[len(screens)-1]), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var script strings.Builder
	script.WriteString("#!/bin/sh\nd=" + shellQuote(dir) + "\n")
	script.WriteString("echo \"$2\" >> \"$d/calls\"\n")
	script.WriteString("case \"$2\" in\n")
	script.WriteString("get) echo '{\"result\":{\"agent\":{\"agent\":\"" + agent + "\"}}}' ;;\n")
	script.WriteString("read) src=visible; lines=60; prev=\n")
	script.WriteString("  for a in \"$@\"; do [ \"$prev\" = \"--source\" ] && src=$a; [ \"$prev\" = \"--lines\" ] && lines=$a; prev=$a; done\n")
	script.WriteString("  case \"$src\" in recent-unwrapped) key=uw ;; *) key=vis ;; esac\n")
	script.WriteString("  n=$(( $(cat \"$d/$key.idx\" 2>/dev/null || echo 0) + 1 )); echo $n > \"$d/$key.idx\"\n")
	script.WriteString("  f=\"$d/$key.$n\"; [ -f \"$f\" ] || f=\"$d/$key.last\"; tail -n \"$lines\" \"$f\" ;;\n")
	script.WriteString("esac\n")
	installFakeHerdr(t, dir, script.String())
	return dir, func() []string {
		b, err := os.ReadFile(filepath.Join(dir, "calls"))
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return strings.Fields(string(b))
	}
}

func task683Count(calls []string, want string) int {
	n := 0
	for _, call := range calls {
		if call == want {
			n++
		}
	}
	return n
}

// task683Filler draws busy-turn output rows. ⏺ rows sit at column 0 in the
// live transcript (indented rows are wrap continuations or nested detail,
// which merge into the block above them for nonce matching).
func task683Filler(prefix string, n int) []string {
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lines = append(lines, fmt.Sprintf("⏺ %s output line %d of a busy turn", prefix, i))
	}
	return lines
}

// AC1/AC4: the injected text scrolled past the old 10-line read while the
// pane stayed busy. The widened reads still find the nonce -- once inside
// the 60-line visible window, once only inside the 400-line
// recent-unwrapped transcript -- and the inject is delivered with exactly
// one prompt.
func TestTask683ScrolledMarkerIsDeliveredOnce(t *testing.T) {
	text := task687Text(68301, task626Text)
	echo := "❯ " + text
	t.Run("inside the widened visible read", func(t *testing.T) {
		post := task626Claude(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 40)...), "❯")
		postTranscript := strings.Join(append(append([]string{}, task626Transcript...), echo), "\n")
		_, calls := task683FakeHerdr(t, "claude",
			[]string{task626ClaudeEmpty, post},
			[]string{task626TranscriptOnly, postTranscript})
		result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
		if result.Outcome != relayInjectDelivered || result.Evidence != "visible+recent-unwrapped:nonce_echo" {
			t.Fatalf("result=%+v, want delivered visible+recent-unwrapped:nonce_echo", result)
		}
		if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
			t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
		}
	})
	t.Run("only inside the recent-unwrapped transcript", func(t *testing.T) {
		post := task626Claude(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 200)...), "❯")
		postTranscript := strings.Join(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 200)...), "\n")
		_, calls := task683FakeHerdr(t, "claude",
			[]string{task626ClaudeEmpty, post},
			[]string{task626TranscriptOnly, postTranscript})
		result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
		if result.Outcome != relayInjectDelivered || result.Evidence != "recent-unwrapped:nonce_echo" {
			t.Fatalf("result=%+v, want delivered recent-unwrapped:nonce_echo", result)
		}
		if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
			t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
		}
	})
}

// Round-4 directive: there is no presend deliver-without-typing path. A
// hub replay of a row whose nonce already landed pastes it again -- a
// visible duplicate is accepted, because every presend identity rule the
// testers probed produced a silent-loss variant -- and the postsend nonce
// still proves delivery. Mutant: re-adding a presend echo skip turns this
// RED -- prompts drops to 0.
func TestTask683ReplayOfLandedRowTypesAgain(t *testing.T) {
	text := task687Text(68302, task626Text)
	echo := "❯ " + text
	post := task626Claude(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 40)...), "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{post, post},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence != "visible:nonce_echo" {
		t.Fatalf("result=%+v, want delivered on the postsend nonce, never a presend claim", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("a replay must still type exactly once: %q", got)
	}
}

// #683 tester B1: head and tail fragments are not identity -- every
// idle-wake to one lane shares the head, and same-path reports share the
// tail. Under #687 the distinguishing token is the nonce: a presend read
// that shows only a *different* row's echo must not claim this row -- the
// new message is pasted, and its own nonce delivers it.
func TestTask683SharedHeadMarkerIsTyped(t *testing.T) {
	wakeA := task687Text(18057, "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p1\",\"changed_at\":\"2026-09-24T19:35:59.581Z\",\"state_change_seq\":1}")
	wakeB := task687Text(18074, "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p7\",\"changed_at\":\"2026-09-24T19:37:29.581Z\",\"state_change_seq\":9}")
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+wakeA), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+wakeA), "\n")
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+wakeB), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+wakeB), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "wB:pD8", wakeB, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence == "" || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want delivered on the new message's own nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("different message sharing the head fragment was not typed: %q", got)
	}
}

// #683 tester B1': two rounds of one job share the head AND the tail
// fragment -- same label and host give the same head, the same report path
// gives the same tail; only the middle differs. The nonce distinguishes
// them: the earlier round's echo never closes the later round.
func TestTask683SameJobRoundsAreTyped(t *testing.T) {
	roundA := task687Text(68401, "(같은 내용이 두 번 보이면 재실행 금지) [report] b618-decision-requests (home-desktop) :: VERDICT: BLOCKER @3def3e3 -> jobs/618-decision-requests-20260924-1610/report.md")
	roundB := task687Text(68402, "(같은 내용이 두 번 보이면 재실행 금지) [report] b618-decision-requests (home-desktop) :: VERDICT: PASS @de41e7a -> jobs/618-decision-requests-20260924-1610/report.md")
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+roundA), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+roundA), "\n")
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+roundB), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+roundB), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "wB:pD8", roundB, nil)
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the later round typed and delivered on its own nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("later job round sharing both fragments was not typed: %q", got)
	}
}

// #683 tester B1', containment arm: a shorter message whose body is a
// substring inside a longer earlier echo must not be claimed delivered --
// and under #687 its own nonce is the only thing that proves it.
func TestTask683ContainedPrefixIsNotDelivered(t *testing.T) {
	long := task687Text(68501, "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] go ahead with round 3 once the tester PASSes")
	short := task687Text(68502, "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] go ahead")
	pre := task626Claude(append(append([]string{}, task626Transcript...), "❯ "+long), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+long), "\n")
	post := task626Claude(append(append([]string{}, task626Transcript...), "❯ "+short), "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, post})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", short, nil)
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the contained-prefix message typed and delivered on its own nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("message contained inside a longer echo was not typed: %q", got)
	}
}

// Round-4 directive, batch arm: a replay batch whose later member's text
// already echoes on the pane is typed anyway -- presend evidence was the
// silent-loss family -- and the batch's own postsend nonce echo is what
// delivers it.
func TestTask683BatchWithMemberEchoIsTyped(t *testing.T) {
	memberB := "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p1\",\"state_change_seq\":1}"
	memberC := "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p7\",\"state_change_seq\":9}"
	batch := relayBatchText([]relayHeld{{EventID: 68601, Text: memberB}, {EventID: 68602, Text: memberC}}, false, time.Now())
	// The pane already echoes member C's text -- from an earlier solo
	// delivery, so under a different nonce.
	earlierC := task687Text(68001, memberC)
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+earlierC), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+earlierC), "\n")
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+batch), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+batch), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "wB:pD8", batch, []string{memberB, memberC})
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the batch typed and delivered on its own postsend nonce echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("batch was not typed: %q", got)
	}
	if len(result.Proven) != 2 {
		t.Fatalf("batch proven=%v, want both member nonces", result.Proven)
	}
}

// #683 tester M1: claude renders markdown in the drawn echo -- **…** and
// `…` vanish -- so the echo is not byte-equal to the typed text. The nonce
// has no markdown-significant bytes, so a rendered echo still carries it.
func TestTask683MarkdownEchoIsDelivered(t *testing.T) {
	text := task687Text(68721, "(같은 내용이 두 번 보이면 재실행 금지) [report] b672-sgov (home-desktop) :: **Status:** PR #2096 is ready at `696bf48` -> jobs/672-sgov-20260924-2340/report.md")
	rendered := "❯ " + relayNoncesIn(text)[0] + " (같은 내용이 두 번 보이면 재실행 금지) [report] b672-sgov (home-desktop) :: Status: PR #2096 is ready at 696bf48 -> jobs/672-sgov-20260924-2340/report.md"
	post := task626Claude(append(append(append([]string{}, task626Transcript...), rendered), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), rendered), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, post},
		[]string{task626TranscriptOnly, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence != "visible+recent-unwrapped:nonce_echo" {
		t.Fatalf("result=%+v, want delivered visible+recent-unwrapped:nonce_echo on the markdown-rendered echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("markdown text was not typed: %q", got)
	}
}

// #683 tester B1”: a wrapped echo is physical rows -- the prompt row plus
// two-space-indented continuations -- in both read sources. Under #687 a
// shorter message ending at a wrap break inside a longer echo is a foreign
// row: its nonce is absent, so it is typed and proven by its own nonce.
func TestTask683WrapBoundaryPrefixIsTyped(t *testing.T) {
	long := task687Text(68801, "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane and wait for my review before merging")
	short := task687Text(68802, "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane")
	longNonce := relayNoncesIn(long)[0]
	wrapped := []string{
		"❯ " + longNonce + " (같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane",
		"  and wait for my review before merging",
	}
	pre := task626Claude(append(append([]string{}, task626Transcript...), wrapped...), "❯")
	if !relayNoncePresent(promptTranscript("claude", pre), longNonce) {
		t.Fatal("the wrapped echo does not carry its own nonce")
	}
	if relayNoncePresent(promptTranscript("claude", pre), relayNoncesIn(short)[0]) {
		t.Fatal("a different row's nonce matched the wrapped echo")
	}
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+short), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+short), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{task626TranscriptOnly, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", short, nil)
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the wrap-boundary row typed and delivered on its own nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("message ending at a wrap break of a longer echo was not typed: %q", got)
	}
}

// #683 delta tester M2: a wrapped echo in the recent-unwrapped read is
// physical rows too; a nonce hard-broken across the wrap rejoins through
// the block join.
func TestTask683WrappedEchoInUnwrappedIsDelivered(t *testing.T) {
	text := task687Text(68901, "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane and wait for my review before merging")
	nonce := relayNoncesIn(text)[0]
	wrappedPost := strings.Join(append(append([]string{}, task626Transcript...),
		"❯ "+nonce[:4],
		"  "+nonce[4:]+" (같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane",
		"  and wait for my review before merging"), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, task626ClaudeEmpty},
		[]string{task626TranscriptOnly, wrappedPost})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence != "recent-unwrapped:nonce_echo" {
		t.Fatalf("result=%+v, want delivered recent-unwrapped:nonce_echo on the wrap-split nonce", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("calls=%q", got)
	}
}

// #683 delta tester N1: a text that differs from an on-screen echo only by
// literal delimiters is a different message. Under #687 the rows differ by
// nonce regardless; the row whose nonce never lands is may-be-in-pane.
func TestTask683LiteralDelimitersAreIdentity(t *testing.T) {
	onPane := task687Text(68011, "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] wait 5~10 min before retrying max_retries")
	other := task687Text(68012, "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] wait 510 min before retrying maxretries")
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+onPane), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+onPane), "\n")
	if relayNoncePresent(promptTranscript("claude", pre), relayNoncesIn(other)[0]) {
		t.Fatal("the other row's nonce matched the on-pane echo")
	}
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, pre},
		[]string{preTranscript, preTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", other, nil)
	if result.Outcome != relayInjectMaybeInPane || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the different row typed and left may-be-in-pane", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("row differing only by literal delimiters was not typed: %q", got)
	}
}

// #683 tester S1: a queue banner that was already up before the paste
// belongs to an older queue and cannot prove this message joined it. With
// no nonce of this row on the pane either, the verdict is may-be-in-pane.
func TestTask683PreExistingBannerIsNotProof(t *testing.T) {
	text := task687Text(68021, task626Text)
	banner := task626Claude([]string{"Press up to edit queued messages"}, "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{banner, banner},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("result=%+v, want maybe_in_pane on a pre-existing banner", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
	}
}

// #683 tester S3: recent-unwrapped carries the live composer too, so a
// nonce still pending there must not count as transcript echo. Both sources
// get the composer cut before nonces are matched.
func TestTask683ComposerInUnwrappedIsNotEcho(t *testing.T) {
	text := task687Text(68031, task626Text)
	pending := "❯ earlier turn\n" + task626Divider + "\n❯ " + text + "\n" + task626Divider + "\n⏵⏵ bypass permissions"
	echo := "❯ " + text
	post := task626Claude(append(append([]string{}, task626Transcript...), echo), "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, post},
		[]string{pending, post})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectDelivered || !strings.HasPrefix(result.Evidence, "visible") {
		t.Fatalf("result=%+v, want postsend visible nonce_echo (composer text must not count presend)", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("pending composer nonce claimed as echo, prompt skipped: %q", got)
	}
}

// AC1/AC4: the harness's documented queue banner plus the message's nonce
// on the transcript is landed -- the queue submits the message itself when
// the turn ends. One prompt, no keypress, reported delivered with the
// queued reason. #687: the banner alone is not enough (the queued-chip
// rule), so the nonce must show.
func TestTask683QueuedBannerIsLanded(t *testing.T) {
	text := task687Text(68041, task626Text)
	queued := task626Claude([]string{"❯ " + text, "Press up to edit queued messages", ""}, task626Phantom)
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, queued},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectQueued || result.Evidence != "queued_banner" {
		t.Fatalf("result=%+v, want queued queued_banner", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
	}
	// The codex queue wording lands the same way.
	_, calls = task683FakeHerdr(t, "codex",
		[]string{"codex pane\n", "Messages to be submitted after next tool call\n❯ " + text + "\n"},
		nil)
	result = defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectQueued || result.Evidence != "queued_banner" {
		t.Fatalf("codex result=%+v, want queued queued_banner", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("codex herdr calls = %q", got)
	}
}

// AC2/AC4: an undecidable pane -- reads answer, no nonce of this row is
// visible -- stops after one prompt. deliver() reports it unconfirmed and
// drops the local row with reason maybe_in_pane instead of re-arming for a
// retry; the durable hub row stays undelivered for a later replay. Restoring
// re-inject-on-unproven turns this RED: the prompt count grows to
// relayMaxInjectAttempts.
func TestTask683UndecidablePaneIsNotReinjected(t *testing.T) {
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty},
		[]string{task626TranscriptOnly})
	events := make(chan hubClientEvent, 8)
	client := &HubClient{}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().deliver(context.Background(), []relayHeld{{
		Pane: "w1:p1", Lane: "lane-a", EventID: 68301, JobID: "relay-job-68301",
		Text: task626Text, HeldSince: client.relayNow(), DeliverPolicy: "idle",
	}}, false)
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("undecidable verdict re-injected: herdr calls = %q", got)
	}
	var unconfirmed *relayAckPayload
	var dropped *relayDroppedPayload
	heldReports := 0
	for {
		select {
		case event := <-events:
			switch event.Kind {
			case "relay.unconfirmed":
				var ack relayAckPayload
				if err := json.Unmarshal(event.Payload, &ack); err != nil {
					t.Fatal(err)
				}
				unconfirmed = &ack
			case "relay.dropped":
				var drop relayDroppedPayload
				if err := json.Unmarshal(event.Payload, &drop); err != nil {
					t.Fatal(err)
				}
				dropped = &drop
			case "relay.held":
				heldReports++
			}
		default:
			goto drained
		}
	}
drained:
	if unconfirmed == nil || !strings.HasPrefix(unconfirmed.Reason, "maybe_in_pane") {
		t.Fatalf("undecidable inject was not reported unconfirmed: %+v", unconfirmed)
	}
	if dropped == nil || dropped.Reason != "maybe_in_pane" {
		t.Fatalf("undecidable inject was not observably dropped: %+v", dropped)
	}
	if heldReports != 0 {
		t.Fatalf("undecidable inject re-armed a held row %d times", heldReports)
	}
}

// A presend read that fails proves nothing about the pane, so typing blind
// is withheld: a replayed maybe-in-pane row would otherwise paste a second
// copy. Same fail-closed shape as devin's presend:read_failed.
func TestTask683UnreadablePresendNeverTypes(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	script := "#!/bin/sh\necho \"$2\" >> \"" + log + "\"\ncase \"$2\" in\n" +
		"get) echo '{\"result\":{\"agent\":{\"agent\":\"claude\"}}}' ;;\n" +
		"read) exit 1 ;;\n" +
		"esac\n"
	installFakeHerdr(t, dir, script)
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task687Text(68051, task626Text), nil)
	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "presend:unproven:read_failed" {
		t.Fatalf("result=%+v, want maybe-in-pane presend:unproven:read_failed", result)
	}
	b, _ := os.ReadFile(log)
	if strings.Contains(string(b), "prompt") {
		t.Fatalf("typed into an unreadable pane: %s", b)
	}
}

// The composer is located only in the visible read, so a presend with the
// visible read down -- even when recent-unwrapped still answers -- cannot
// rule out a pending paste, and typing blind could paste a second copy on
// top of it. Fail closed, same shape as both reads failing.
func TestTask687VisibleDownPresendNeverTypes(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	script := "#!/bin/sh\necho \"$2\" >> \"" + log + "\"\ncase \"$2\" in\n" +
		"get) echo '{\"result\":{\"agent\":{\"agent\":\"claude\"}}}' ;;\n" +
		"read) case \" $*\" in *recent-unwrapped*) echo '❯ unrelated transcript' ;; *) exit 1 ;; esac ;;\n" +
		"esac\n"
	installFakeHerdr(t, dir, script)
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task687Text(68052, task626Text), nil)
	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "presend:unproven:read_failed" {
		t.Fatalf("result=%+v, want maybe-in-pane presend:unproven:read_failed", result)
	}
	b, _ := os.ReadFile(log)
	if strings.Contains(string(b), "prompt") {
		t.Fatalf("typed with the composer unreadable: %s", b)
	}
}

// A prompt herdr rejected is the one failure that stays retryable: nothing
// was typed, so re-injecting cannot duplicate. deliver() re-arms with
// reason retry, which the hub's decodeRelayHeldPayload must accept (AC3).
func TestTask683RejectedPromptStaysRetryable(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	script := "#!/bin/sh\necho \"$2\" >> \"" + log + "\"\ncase \"$2\" in\n" +
		"get) echo '{\"result\":{\"agent\":{\"agent\":\"claude\"}}}' ;;\n" +
		"prompt) exit 1 ;;\n" +
		"read) printf '%s\\n' '" + task626Divider + "' '❯' '" + task626Divider + "' ;;\n" +
		"esac\n"
	installFakeHerdr(t, dir, script)
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task687Text(68061, task626Text), nil)
	if result.Outcome != relayInjectRetryable || result.Evidence != "prompt_failed" {
		t.Fatalf("result=%+v, want retryable prompt_failed", result)
	}
	// deliver() re-arms the row with reason retry; the hub decoder is the
	// contract AC3 fixed -- a held payload with reason retry must parse.
	payload, err := json.Marshal(relayHeldPayload{
		EventID: 68302, JobID: "relay-job-68302", Pane: "w1:p1", Lane: "lane-a",
		Reason: "retry", Preview: "preview", HeldSince: time.Now().UTC().Format(time.RFC3339Nano),
		DeliverPolicy: "idle",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := decodeRelayHeldPayload(payload); !ok {
		t.Fatalf("hub decoder rejected relay.held reason retry: %s", payload)
	}
}
