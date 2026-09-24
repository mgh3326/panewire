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
// transcript, accepts the harness queue banner as landed, and treats every
// undecidable verdict as may-be-in-pane -- never a re-inject.

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

func task683Filler(prefix string, n int) []string {
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lines = append(lines, fmt.Sprintf("  ⏺ %s output line %d of a busy turn", prefix, i))
	}
	return lines
}

// AC1/AC4: the injected text scrolled past the old 10-line read while the
// pane stayed busy. The widened reads still find it -- once inside the
// 60-line visible window, once only inside the 400-line recent-unwrapped
// transcript -- and the inject is delivered with exactly one prompt. With
// the old single 10-line read this goes unproven, so restoring that read
// turns the test RED.
func TestTask683ScrolledMarkerIsDeliveredOnce(t *testing.T) {
	echo := "❯ " + task626Text
	t.Run("inside the widened visible read", func(t *testing.T) {
		post := task626Claude(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 40)...), "❯")
		postTranscript := strings.Join(append(append([]string{}, task626Transcript...), echo), "\n")
		_, calls := task683FakeHerdr(t, "claude",
			[]string{task626ClaudeEmpty, post},
			[]string{task626TranscriptOnly, postTranscript})
		result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
		if result.Outcome != relayInjectDelivered || result.Evidence != "visible:marker_echo" {
			t.Fatalf("result=%+v, want delivered visible:marker_echo", result)
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
		result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
		if result.Outcome != relayInjectDelivered || result.Evidence != "recent-unwrapped:marker_echo" {
			t.Fatalf("result=%+v, want delivered recent-unwrapped:marker_echo", result)
		}
		if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
			t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
		}
	})
}

// AC2's late-delivery path: a hub replay of a row whose text already landed
// meets the presend check, which sees the echo and delivers without typing
// anything. This is what keeps a may-be-in-pane outcome safe to drop
// locally: the durable row is owed to the lane and the replay finds it.
func TestTask683ReplayOfLandedRowNeverTypes(t *testing.T) {
	echo := "❯ " + task626Text
	post := task626Claude(append(append(append([]string{}, task626Transcript...), echo), task683Filler("post", 40)...), "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{post},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence != "presend:visible:marker_echo" {
		t.Fatalf("result=%+v, want delivered presend:visible:marker_echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 0 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("replay typed into a pane that already has the text: %q", got)
	}
}

// #683 tester B1: head and tail fragments are not identity -- every
// idle-wake to one lane shares the 48-rune head marker, and same-path
// reports share the tail. A presend read that shows only a *different*
// message's echo must not claim this row delivered without typing: the
// new message is pasted, and its own echo delivers it.
func TestTask683SharedHeadMarkerIsTyped(t *testing.T) {
	wakeA := "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p1\",\"changed_at\":\"2026-09-24T19:35:59.581Z\",\"state_change_seq\":1}"
	wakeB := "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p7\",\"changed_at\":\"2026-09-24T19:37:29.581Z\",\"state_change_seq\":9}"
	if devinRelayMarker(wakeA) != devinRelayMarker(wakeB) {
		t.Fatal("fixture drifted: the two wakes must share the head marker")
	}
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+wakeA), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+wakeA), "\n")
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+wakeB), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+wakeB), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "wB:pD8", wakeB, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence == "" || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want delivered on the new message's own echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("different message sharing the head marker was not typed: %q", got)
	}
}

// #683 tester B1': two rounds of one job share the head AND the tail
// marker -- same label and host give the same head, the same report path
// gives the same tail; only the middle differs. Echo identity is the whole
// body ending at a line boundary, so the earlier round's echo must not
// close the later round: it is typed and delivered on its own echo.
func TestTask683SameJobRoundsAreTyped(t *testing.T) {
	roundA := "(같은 내용이 두 번 보이면 재실행 금지) [report] b618-decision-requests (home-desktop) :: VERDICT: BLOCKER @3def3e3 -> jobs/618-decision-requests-20260924-1610/report.md"
	roundB := "(같은 내용이 두 번 보이면 재실행 금지) [report] b618-decision-requests (home-desktop) :: VERDICT: PASS @de41e7a -> jobs/618-decision-requests-20260924-1610/report.md"
	if devinRelayMarker(roundA) != devinRelayMarker(roundB) || devinRelayTailMarker(roundA) != devinRelayTailMarker(roundB) {
		t.Fatal("fixture drifted: same-job rounds must share head and tail markers")
	}
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+roundA), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+roundA), "\n")
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+roundB), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+roundB), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "wB:pD8", roundB, nil)
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the later round typed and delivered on its own echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("later job round sharing both markers was not typed: %q", got)
	}
}

// #683 tester B1', containment arm: a shorter message whose body is a
// substring inside a longer earlier echo must not be claimed delivered --
// the body has to end where a line ends.
func TestTask683ContainedPrefixIsNotDelivered(t *testing.T) {
	long := "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] go ahead with round 3 once the tester PASSes"
	short := "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] go ahead"
	pre := task626Claude(append(append([]string{}, task626Transcript...), "❯ "+long), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+long), "\n")
	post := task626Claude(append(append([]string{}, task626Transcript...), "❯ "+short), "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, post})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", short, nil)
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the contained-prefix message typed and delivered on its own echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("message contained inside a longer echo was not typed: %q", got)
	}
}

// #683 tester B1', batch arm: a replay batch whose later member already has
// an echo on the pane must not claim the batch delivered from that member's
// echo -- the member's presence holds the batch as may-be-in-pane (part of
// it may have landed by another route) and nothing is typed.
func TestTask683BatchWithMemberEchoIsHeldNotClaimed(t *testing.T) {
	memberB := "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p1\",\"state_change_seq\":1}"
	memberC := "(같은 내용이 두 번 보이면 재실행 금지) [event] director-1 :: {\"kind\":\"idle-wake\",\"pane\":\"w16:p7\",\"state_change_seq\":9}"
	batch := "[batch 2건] 1) " + memberB + " 2) " + memberC
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+memberC), task683Filler("later", 30)...), "❯")
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+memberC), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre},
		[]string{preTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "wB:pD8", batch, []string{memberB, memberC})
	if result.Outcome != relayInjectMaybeInPane || !strings.Contains(result.Evidence, "member_echo") {
		t.Fatalf("result=%+v, want presend may-be-in-pane on the member echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 0 {
		t.Fatalf("batch typed although a member's echo is on the pane: %q", got)
	}
}

// #683 tester M1: claude renders markdown in the drawn echo -- **…** and
// `…` vanish -- so the echo is not byte-equal to the typed text. 42% of
// real job.completed texts carry markdown; echo matching normalizes both
// sides so a landed report still reads as delivered.
func TestTask683MarkdownEchoIsDelivered(t *testing.T) {
	text := "(같은 내용이 두 번 보이면 재실행 금지) [report] b672-sgov (home-desktop) :: **Status:** PR #2096 is ready at `696bf48` -> jobs/672-sgov-20260924-2340/report.md"
	rendered := "❯ (같은 내용이 두 번 보이면 재실행 금지) [report] b672-sgov (home-desktop) :: Status: PR #2096 is ready at 696bf48 -> jobs/672-sgov-20260924-2340/report.md"
	pre := task626Claude(append(append(append([]string{}, task626Transcript...), rendered), task683Filler("post", 40)...), "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", text, nil)
	if result.Outcome != relayInjectDelivered || result.Evidence != "presend:visible:marker_echo" {
		t.Fatalf("result=%+v, want delivered presend:visible:marker_echo on the markdown-rendered echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 0 {
		t.Fatalf("landed markdown text re-typed because the echo lost its delimiters: %q", got)
	}
}

// #683 tester B1'': the line boundary that anchors an echo is a *logical*
// line, and the visible read is physical rows. When a longer earlier echo
// word-wraps exactly after a shorter message's body, that body sits at a
// physical line end mid-echo -- indented continuation rows have to be
// joined before matching or the shorter message is claimed delivered
// without typing.
func TestTask683WrapBoundaryPrefixIsTyped(t *testing.T) {
	long := "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane and wait for my review before merging"
	short := "(같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane"
	wrapped := []string{
		"❯ (같은 내용이 두 번 보이면 재실행 금지) [event] b683-inject-verify :: [chat] stop the lane",
		"  and wait for my review before merging",
	}
	pre := task626Claude(append(append([]string{}, task626Transcript...), wrapped...), "❯")
	if !relayEchoContains(promptTranscript("claude", pre), long, true) {
		t.Fatal("the wrapped echo does not match its own text")
	}
	if relayEchoContains(promptTranscript("claude", pre), short, true) {
		t.Fatal("the wrap-break prefix matched inside the longer wrapped echo")
	}
	preTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+long), "\n")
	post := task626Claude(append(append(append([]string{}, task626Transcript...), "❯ "+short), task683Filler("post", 40)...), "❯")
	postTranscript := strings.Join(append(append([]string{}, task626Transcript...), "❯ "+short), "\n")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{pre, post},
		[]string{preTranscript, postTranscript})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", short, nil)
	if result.Outcome != relayInjectDelivered || strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want the wrap-boundary prefix typed and delivered on its own echo", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("message ending at a wrap break of a longer echo was not typed: %q", got)
	}
}

// #683 tester S1: a queue banner that was already up before the paste
// belongs to an older queue and cannot prove this message joined it. With
// no echo of the text either, the verdict is may-be-in-pane -- never a
// delivered claim.
func TestTask683PreExistingBannerIsNotProof(t *testing.T) {
	banner := task626Claude([]string{"Press up to edit queued messages"}, "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{banner, banner},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("result=%+v, want maybe_in_pane on a pre-existing banner", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
	}
}

// #683 tester S3: recent-unwrapped carries the live composer too, so text
// still pending there must not count as transcript echo. Both sources get
// the composer cut before markers are matched.
func TestTask683ComposerInUnwrappedIsNotEcho(t *testing.T) {
	pending := "❯ earlier turn\n" + task626Divider + "\n❯ " + task626Text + "\n" + task626Divider + "\n⏵⏵ bypass permissions"
	echo := "❯ " + task626Text
	post := task626Claude(append(append([]string{}, task626Transcript...), echo), "❯")
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, post},
		[]string{pending, post})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
	if result.Outcome != relayInjectDelivered || !strings.HasPrefix(result.Evidence, "visible:") {
		t.Fatalf("result=%+v, want postsend visible:marker_echo (composer text must not count presend)", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 {
		t.Fatalf("pending composer text claimed as echo, prompt skipped: %q", got)
	}
}

// AC1/AC4: the harness's documented queue banner is landed -- the queue
// submits the message itself when the turn ends. One prompt, no keypress,
// reported delivered with the queued reason.
func TestTask683QueuedBannerIsLanded(t *testing.T) {
	queued := task626Claude([]string{"Press up to edit queued messages", ""}, task626Phantom)
	_, calls := task683FakeHerdr(t, "claude",
		[]string{task626ClaudeEmpty, queued},
		[]string{task626TranscriptOnly})
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
	if result.Outcome != relayInjectQueued || result.Evidence != "queued_banner" {
		t.Fatalf("result=%+v, want queued queued_banner", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("herdr calls = %q, want one prompt and no keypress", got)
	}
	// The codex queue wording lands the same way.
	_, calls = task683FakeHerdr(t, "codex",
		[]string{"codex pane\n", "Messages to be submitted after next tool call\n"},
		nil)
	result = defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
	if result.Outcome != relayInjectQueued || result.Evidence != "queued_banner" {
		t.Fatalf("codex result=%+v, want queued queued_banner", result)
	}
	if got := calls(); task683Count(got, "prompt") != 1 || task683Count(got, "send-keys") != 0 {
		t.Fatalf("codex herdr calls = %q", got)
	}
}

// AC2/AC4: an undecidable pane -- reads answer, nothing about the text is
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
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "presend:unproven:read_failed" {
		t.Fatalf("result=%+v, want maybe-in-pane presend:unproven:read_failed", result)
	}
	b, _ := os.ReadFile(log)
	if strings.Contains(string(b), "prompt") {
		t.Fatalf("typed into an unreadable pane: %s", b)
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
	result := defaultHubRelayInjectVerdict(context.Background(), "w1:p1", task626Text, nil)
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
