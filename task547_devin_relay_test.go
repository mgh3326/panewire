package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// #547: devin submission evidence without duplicate re-injection
// (hk:doc task/2026-09-21/pw62-devin-submission-evidence,
// decision/2026-09-21/task547-esc-relay-contract).

const task547RelayText = "(같은 내용이 두 번 보이면 재실행 금지) [event] b534 :: review the migration plan and report"

// task547QueuedScreen is the shape director-1 observed on the b534 devin pane
// on 2026-09-21 18:3x: two relay messages held in devin's queue, the queue
// header, and the composer hint -- neither message submitted.
func task547QueuedScreen(messages ...string) string {
	var screen strings.Builder
	screen.WriteString("❭ earlier instruction\n⠋ Thinking 12m04s\n")
	for _, message := range messages {
		screen.WriteString("○ " + message + "\n")
	}
	screen.WriteString("── 2 queued ── ↑ edit · ↵ send now\n")
	screen.WriteString("────────────────────────────────────────\n")
	screen.WriteString("❭ Press Enter to send queued messages now\n")
	screen.WriteString("────────────────────────────────────────\n")
	return screen.String()
}

func task547SubmittedScreen(message string) string {
	return "❭ " + message + "\n⠋ Thinking 3s\n" +
		"────────────────────────────────────────\n" +
		"❭ Guide Devin while it works\n" +
		"────────────────────────────────────────\n"
}

const task547IdleScreen = "❭ earlier instruction\ndone.\n" +
	"────────────────────────────────────────\n" +
	"❭ Ask Devin to build features, fix bugs, or work on your code\n" +
	"────────────────────────────────────────\n"

// AC3/AC4②, mutant (a): a devin message that is only in devin's queue must
// classify as queued, never as submitted, even though its queued row carries
// the marker.
func TestTask547DevinQueuedOnlyIsNeverSubmitted(t *testing.T) {
	marker := devinRelayMarker(task547RelayText)
	screen := task547QueuedScreen("(같은 내용이 두 번 보이면 재실행 금지) [event] b534 :: an older queued note", task547RelayText)
	for _, candidate := range []string{marker, markerFor(task547RelayText)} {
		result, rule := classifySubmissionEvidence("devin", screen, candidate)
		if result != "queued" || rule != "devin_queue_banner" {
			t.Fatalf("marker=%q: queued-only devin screen classified %s/%s, want queued/devin_queue_banner", candidate, result, rule)
		}
	}
	// The composer hint alone (queue header scrolled away) is the same state.
	hintOnly := "○ " + task547RelayText + "\n────\n❭ Press Enter to send queued messages now\n────\n"
	if result := classifySubmission("devin", hintOnly, marker); result != "queued" {
		t.Fatalf("composer hint alone classified %s, want queued", result)
	}
}

// AC3: the three devin states stay distinct, and unproven is never promoted.
func TestTask547DevinThreeStates(t *testing.T) {
	marker := devinRelayMarker(task547RelayText)
	cases := []struct {
		name, screen, want, rule string
	}{
		{"submitted: echo in transcript, queue gone, turn running", task547SubmittedScreen(task547RelayText), "marker_observed", "marker_echo"},
		{"queued only", task547QueuedScreen(task547RelayText), "queued", "devin_queue_banner"},
		{"unknown: nothing about this message on screen", task547IdleScreen, "unproven", "none"},
		{"residue: message still in the composer", "────\n❭ " + task547RelayText + "\n────\n", "composer_residue", "composer_divider"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			result, rule := classifySubmissionEvidence("devin", tc.screen, marker)
			if result != tc.want || rule != tc.rule {
				t.Fatalf("got %s/%s, want %s/%s", result, rule, tc.want, tc.rule)
			}
		})
	}
}

// A wrapped visible screen can split the marker across lines; devin matching
// ignores whitespace so the split does not turn a submission into unproven.
func TestTask547DevinMarkerSurvivesLineWrap(t *testing.T) {
	marker := devinRelayMarker(task547RelayText)
	wrapped := "❭ [event] b534 :: review the mig\n  ration plan and report\n────\n❭ Guide Devin while it works\n────\n"
	if result := classifySubmission("devin", wrapped, marker); result != "marker_observed" {
		t.Fatalf("wrapped echo classified %s, want marker_observed", result)
	}
}

// Relay texts share a fixed prefix; markerFor's 24 runes are all boilerplate
// and would match any earlier relay message. The devin marker skips it.
func TestTask547DevinRelayMarkerSkipsBoilerplate(t *testing.T) {
	cases := map[string]string{
		task547RelayText: "[event] b534 :: review the migration plan and re",
		"[대기 만료 12분] " + task547RelayText:                            "[event] b534 :: review the migration plan and re",
		"[batch 2건] 1) [대기 만료 3분] " + task547RelayText + " 2) other": "[event] b534 :: review the migration plan and re",
	}
	for text, want := range cases {
		if got := devinRelayMarker(text); got != want {
			t.Fatalf("devinRelayMarker(%q)=%q, want %q", text, got, want)
		}
	}
	if markerFor(task547RelayText) != markerFor("(같은 내용이 두 번 보이면 재실행 금지) [event] other :: x") {
		t.Fatal("premise changed: markerFor no longer collides on relay boilerplate")
	}
}

// task547FakeDevin is a herdr stand-in for the devin relay path. Screens are
// chosen by phase (before prompt, after prompt, after the return keypress)
// and by read source. Every call is logged as "cmd" or "read:<source>".
type task547FakeDevin struct {
	before, afterSend, afterReturn map[string]string // source -> screen ("10" is the claude/codex read)
	failRead                       string            // phase whose reads fail
	failPrompt                     bool              // herdr rejects the prompt
	getBefore, getAfter            string            // agent reported before / after the prompt (default devin)
	status                         string            // agent_status reported by get (default idle; "-" omits it)
}

func (f task547FakeDevin) install(t *testing.T) func() []string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	var script strings.Builder
	script.WriteString("#!/bin/sh\nd=" + shellQuote(dir) + "\n")
	script.WriteString("phase=before\n[ -f \"$d/sent\" ] && phase=afterSend\n[ -f \"$d/returned\" ] && phase=afterReturn\n")
	script.WriteString("case \"$2\" in\n")
	getBefore, getAfter := f.getBefore, f.getAfter
	if getBefore == "" {
		getBefore = "devin"
	}
	if getAfter == "" {
		getAfter = getBefore
	}
	status := f.status
	if status == "" {
		status = "idle"
	}
	statusField := ""
	if status != "-" {
		statusField = ",\\\"agent_status\\\":\\\"" + status + "\\\""
	}
	script.WriteString("get) echo get >> \"$d/calls.log\"; if [ -f \"$d/sent\" ]; then a=" + shellQuote(getAfter) + "; else a=" + shellQuote(getBefore) + "; fi; echo \"{\\\"result\\\":{\\\"agent\\\":{\\\"agent\\\":\\\"$a\\\"" + statusField + "}}}\" ;;\n")
	if f.failPrompt {
		script.WriteString("prompt) echo prompt >> \"$d/calls.log\"; exit 1 ;;\n")
	} else {
		script.WriteString("prompt) echo prompt >> \"$d/calls.log\"; touch \"$d/sent\" ;;\n")
	}
	script.WriteString("send-keys) echo send-keys >> \"$d/calls.log\"; touch \"$d/returned\" ;;\n")
	script.WriteString("read) echo \"read:$5\" >> \"$d/calls.log\"\n")
	if f.failRead != "" {
		script.WriteString("  [ \"$phase\" = " + shellQuote(f.failRead) + " ] && exit 1\n")
	}
	for _, phase := range []struct {
		name    string
		screens map[string]string
	}{{"before", f.before}, {"afterSend", f.afterSend}, {"afterReturn", f.afterReturn}} {
		for source, screen := range phase.screens {
			file := filepath.Join(dir, phase.name+"."+source)
			if err := os.WriteFile(file, []byte(screen), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	script.WriteString("  if [ -f \"$d/$phase.$5\" ]; then cat \"$d/$phase.$5\"; fi ;;\n")
	script.WriteString("esac\n")
	installFakeHerdr(t, dir, script.String())
	return func() []string {
		b, err := os.ReadFile(log)
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		return strings.Fields(string(b))
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

func task547Count(calls []string, want string) int {
	n := 0
	for _, call := range calls {
		if call == want {
			n++
		}
	}
	return n
}

func task547Both(screen string) map[string]string {
	return map[string]string{"visible": screen, "recent-unwrapped": screen}
}

// Mutant (c) and AC2: when any source already shows the message, nothing is
// typed -- not on a local retry and not on a hub replay, which reaches the
// node as a fresh message.
func TestTask547DevinPresendMarkerBlocksAnyInject(t *testing.T) {
	cases := map[string]task547FakeDevin{
		"transcript echo in recent-unwrapped only": {before: map[string]string{"visible": task547IdleScreen, "recent-unwrapped": task547SubmittedScreen(task547RelayText)}},
		"queued row in visible only":               {before: map[string]string{"visible": task547QueuedScreen(task547RelayText), "recent-unwrapped": task547IdleScreen}},
		"residue in visible composer only":         {before: map[string]string{"visible": "────\n❭ " + task547RelayText + "\n────\n", "recent-unwrapped": task547IdleScreen}},
	}
	for name, fake := range cases {
		t.Run(name, func(t *testing.T) {
			calls := fake.install(t)
			result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", task547RelayText, nil)
			if result.Outcome != relayInjectMaybeInPane || !strings.HasPrefix(result.Evidence, "presend:") {
				t.Fatalf("result=%+v, want maybe_in_pane with presend evidence", result)
			}
			if got := calls(); task547Count(got, "prompt") != 0 || task547Count(got, "send-keys") != 0 {
				t.Fatalf("typed into a pane that already holds the message: %q", got)
			}
		})
	}
}

// AC4②, mutant (b): queued, one return keypress, still queued -> the message
// is in devin's queue. Never retryable; exactly one prompt and one return.
func TestTask547DevinStillQueuedAfterReturnIsMaybeInPane(t *testing.T) {
	calls := task547FakeDevin{
		before:      task547Both(task547IdleScreen),
		afterSend:   task547Both(task547QueuedScreen(task547RelayText)),
		afterReturn: task547Both(task547QueuedScreen(task547RelayText)),
	}.install(t)
	result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", task547RelayText, nil)
	if result.Outcome != relayInjectMaybeInPane {
		t.Fatalf("still-queued devin message result=%+v, want maybe_in_pane", result)
	}
	if !strings.Contains(result.Evidence, "devin_queue_banner") || !strings.Contains(result.Evidence, "after_return:") {
		t.Fatalf("evidence=%q does not name the queue banner before and after return", result.Evidence)
	}
	got := calls()
	if task547Count(got, "prompt") != 1 || task547Count(got, "send-keys") != 1 {
		t.Fatalf("calls=%q, want one prompt and one return keypress", got)
	}
	if task547Count(got, "read:visible") == 0 || task547Count(got, "read:recent-unwrapped") == 0 {
		t.Fatalf("calls=%q, verification must read both visible and recent-unwrapped", got)
	}
}

func TestTask547DevinVerdicts(t *testing.T) {
	cases := []struct {
		name         string
		fake         task547FakeDevin
		want         relayInjectOutcome
		evidence     string
		returnsCount int
	}{
		{"echo right after send is delivered", task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(task547SubmittedScreen(task547RelayText))}, relayInjectDelivered, "visible:marker_echo", 0},
		{"queued then echoed after one return is delivered", task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(task547QueuedScreen(task547RelayText)), afterReturn: task547Both(task547SubmittedScreen(task547RelayText))}, relayInjectDelivered, "after_return:visible:marker_echo", 1},
		{"queued then gone everywhere after return may be in pane", task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(task547QueuedScreen(task547RelayText)), afterReturn: task547Both(task547IdleScreen)}, relayInjectMaybeInPane, "after_return:visible+recent-unwrapped:none", 1},
		{"never seen after an accepted send may be in pane", task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(task547IdleScreen)}, relayInjectMaybeInPane, "postsend:visible+recent-unwrapped:none", 0},
		{"send rejected by herdr is retryable", task547FakeDevin{before: task547Both(task547IdleScreen), failPrompt: true}, relayInjectRetryable, "prompt_failed", 0},
		{"unreadable after send may be in pane", task547FakeDevin{before: task547Both(task547IdleScreen), failRead: "afterSend"}, relayInjectMaybeInPane, "postsend:read_failed", 0},
		{"unreadable before send is not typed", task547FakeDevin{failRead: "before"}, relayInjectMaybeInPane, "presend:read_failed", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := tc.fake.install(t)
			result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", task547RelayText, nil)
			if result.Outcome != tc.want || !strings.Contains(result.Evidence, tc.evidence) || result.Harness != "devin" {
				t.Fatalf("result=%+v, want outcome %d with evidence containing %q", result, tc.want, tc.evidence)
			}
			if got := task547Count(calls(), "send-keys"); got != tc.returnsCount {
				t.Fatalf("return keypresses=%d, want %d", got, tc.returnsCount)
			}
		})
	}
}

// task547RelayRun drives one held relay item through the busy manager with a
// verdict seam and returns the events and the number of inject calls.
func task547RelayRun(t *testing.T, verdict func(context.Context, string, string, []string) relayInjectResult) (*int, *HubClient, chan hubClientEvent) {
	t.Helper()
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 8)}
	store := NewMemoryStore(t)
	t.Cleanup(func() { store.Close() })
	injects := 0
	events := make(chan hubClientEvent, 64)
	client := &HubClient{outbox: store, relayCommand: fake.run, relayInjectVerdict: func(ctx context.Context, pane, text string, members []string) relayInjectResult {
		injects++
		return verdict(ctx, pane, text, members)
	}}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().restore(t.Context())
	client.relayBusyManager().offer(t.Context(), r27Directive(547, task547RelayText, "max_wait=1"))
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, fake)
	close(gate)
	return &injects, client, events
}

// Mutant (b): a devin inject that keeps failing is re-injected at most once.
func TestTask547DevinRetryableIsReinjectedAtMostOnce(t *testing.T) {
	injects, client, events := task547RelayRun(t, func(context.Context, string, string, []string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectRetryable, Harness: "devin", Evidence: "visible+recent-unwrapped:none"}
	})
	unconfirmed := task547Await(t, events, "relay.unconfirmed")
	if !strings.Contains(string(unconfirmed.Payload), `"reason":"visible+recent-unwrapped:none"`) {
		t.Fatalf("relay.unconfirmed carries no evidence: %s", unconfirmed.Payload)
	}
	task547Await(t, events, "relay.dropped")
	if *injects != relayDevinMaxInjectAttempts || relayDevinMaxInjectAttempts != 2 {
		t.Fatalf("devin injects=%d, want 2 (one re-inject)", *injects)
	}
	if items, err := client.relayStore().RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(items) != 0 {
		t.Fatalf("dropped item still held: %+v err=%v", items, err)
	}
}

// claude/codex keep relayMaxInjectAttempts; #547 does not change their cap.
func TestTask547ClaudeRetryCapUnchanged(t *testing.T) {
	injects, _, events := task547RelayRun(t, func(context.Context, string, string, []string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectRetryable, Harness: "claude"}
	})
	unconfirmed := task547Await(t, events, "relay.unconfirmed")
	if strings.Contains(string(unconfirmed.Payload), `"reason"`) {
		t.Fatalf("claude relay.unconfirmed payload changed: %s", unconfirmed.Payload)
	}
	task547Await(t, events, "relay.dropped")
	if *injects != relayMaxInjectAttempts {
		t.Fatalf("claude injects=%d, want relayMaxInjectAttempts=%d", *injects, relayMaxInjectAttempts)
	}
}

// Mutant (b): may-be-in-pane is never re-injected. The row is removed, the
// hub is told why, and no second inject follows.
func TestTask547DevinMaybeInPaneIsNeverReinjected(t *testing.T) {
	injects, client, events := task547RelayRun(t, func(context.Context, string, string, []string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: "devin", Evidence: "visible:devin_queue_banner after_return:visible:devin_queue_banner"}
	})
	unconfirmed := task547Await(t, events, "relay.unconfirmed")
	var ack relayAckPayload
	if err := json.Unmarshal(unconfirmed.Payload, &ack); err != nil || !strings.HasPrefix(ack.Reason, "maybe_in_pane visible:devin_queue_banner") {
		t.Fatalf("relay.unconfirmed=%s err=%v, want maybe_in_pane reason with evidence", unconfirmed.Payload, err)
	}
	dropped := task547Await(t, events, "relay.dropped")
	if !strings.Contains(string(dropped.Payload), `"reason":"maybe_in_pane"`) {
		t.Fatalf("relay.dropped=%s", dropped.Payload)
	}
	time.Sleep(300 * time.Millisecond)
	if *injects != 1 {
		t.Fatalf("maybe_in_pane message injected %d times, want 1", *injects)
	}
	if items, err := client.relayStore().RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(items) != 0 {
		t.Fatalf("maybe_in_pane item still held for retry: %+v err=%v", items, err)
	}
}

// C2 end to end: herdr rejects the first send after typing part of it, so
// the attempt is retryable; the retry's presend check finds the text and does
// not type it again. One prompt total, relay reported unconfirmed with cause.
func TestTask547DevinPartialSendIsNotTypedTwice(t *testing.T) {
	dir := t.TempDir()
	landed := filepath.Join(dir, "landed")
	script := "#!/bin/sh\ncase \"$2\" in\n" +
		"get) echo '{\"result\":{\"agent\":{\"agent\":\"devin\"}}}' ;;\n" +
		"prompt) echo prompt >> " + shellQuote(filepath.Join(dir, "calls.log")) + "; touch " + shellQuote(landed) + "; exit 1 ;;\n" +
		"read) if [ -f " + shellQuote(landed) + " ]; then printf '%s' " + shellQuote("────\n❭ "+task547RelayText+"\n────\n") + "; else printf '%s' " + shellQuote(task547IdleScreen) + "; fi ;;\n" +
		"esac\n"
	installFakeHerdr(t, dir, script)
	injects, _, events := task547RelayRun(t, func(ctx context.Context, pane, text string, members []string) relayInjectResult {
		return defaultHubRelayInjectVerdict(ctx, pane, text, members)
	})
	first := task547Await(t, events, "relay.unconfirmed")
	if !strings.Contains(string(first.Payload), `"reason":"prompt_failed"`) {
		t.Fatalf("first unconfirmed=%s", first.Payload)
	}
	second := task547Await(t, events, "relay.unconfirmed")
	if !strings.Contains(string(second.Payload), "maybe_in_pane presend:") {
		t.Fatalf("second unconfirmed=%s, want presend maybe_in_pane", second.Payload)
	}
	task547Await(t, events, "relay.dropped")
	b, _ := os.ReadFile(filepath.Join(dir, "calls.log"))
	if prompts := strings.Count(string(b), "prompt"); prompts != 1 || *injects != 2 {
		t.Fatalf("prompts=%d injects=%d, want 1 prompt over 2 attempts", prompts, *injects)
	}
}

// tester BLOCKER 1 (real devin pane, 6,046-byte event): a long message scrolls
// its head out of every read window. Its tail still identifies it, and an
// accepted send with no sign of the message is never retried.
func TestTask547DevinLongMessageIsNeverTypedTwice(t *testing.T) {
	long := "(같은 내용이 두 번 보이면 재실행 금지) [event] t547-scroll :: SCROLL-WINDOW head " + strings.Repeat("x", 3000) + " LONG-TAIL-END-547"
	tailOnly := "  " + strings.Repeat("x", 120) + " LONG-TAIL-END-547\n⠋ Thinking 1s\n────\n❭ Guide Devin while it works\n────\n"

	t.Run("tail visible after send is delivered", func(t *testing.T) {
		calls := task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(tailOnly)}.install(t)
		result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", long, nil)
		if result.Outcome != relayInjectDelivered || !strings.Contains(result.Evidence, "marker_echo") {
			t.Fatalf("result=%+v, want delivered on the tail echo", result)
		}
		if got := calls(); task547Count(got, "prompt") != 1 {
			t.Fatalf("calls=%q", got)
		}
	})
	t.Run("nothing visible after send is not retryable", func(t *testing.T) {
		task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(task547IdleScreen)}.install(t)
		if result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", long, nil); result.Outcome != relayInjectMaybeInPane {
			t.Fatalf("result=%+v, want maybe_in_pane", result)
		}
	})
	t.Run("replay with only the tail on screen is not typed", func(t *testing.T) {
		calls := task547FakeDevin{before: map[string]string{"visible": task547IdleScreen, "recent-unwrapped": tailOnly}}.install(t)
		result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", long, nil)
		if result.Outcome != relayInjectMaybeInPane || result.Evidence != "presend:recent-unwrapped:marker_present" {
			t.Fatalf("result=%+v", result)
		}
		if got := calls(); task547Count(got, "prompt") != 0 {
			t.Fatalf("typed a message whose tail is on screen: %q", got)
		}
	})
}

// tester BLOCKER 2: a batch whose later member is already in the pane is not
// typed, even though the batch text starts and ends with other members.
func TestTask547DevinBatchMemberAlreadyPresentBlocksBatch(t *testing.T) {
	first := "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: first-unique-message"
	second := "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: second-already-present-message"
	third := "(같은 내용이 두 번 보이면 재실행 금지) [event] lane-a :: third-unique-message"
	// The present member sits in the middle: neither the batch head nor the
	// batch tail covers it.
	batch := relayBatchText([]relayHeld{{Text: first}, {Text: second}, {Text: third}}, false, time.Now())
	calls := task547FakeDevin{before: map[string]string{"visible": task547IdleScreen, "recent-unwrapped": task547SubmittedScreen(second)}}.install(t)
	result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", batch, []string{first, second, third})
	if result.Outcome != relayInjectMaybeInPane || !strings.HasPrefix(result.Evidence, "presend:") {
		t.Fatalf("result=%+v, want presend maybe_in_pane", result)
	}
	if got := calls(); task547Count(got, "prompt") != 0 {
		t.Fatalf("batch typed although a member is in the pane: %q", got)
	}
}

// tester BLOCKER 3: a submitted transcript row quoting devin's hint or header
// is not a queue.
func TestTask547DevinQuotedQueueTextIsNotAQueue(t *testing.T) {
	quoted := "(같은 내용이 두 번 보이면 재실행 금지) [event] b534 :: quoted UI text: Press Enter to send queued messages now and ── 2 queued ── banner"
	screen := task547SubmittedScreen(quoted)
	if result := classifySubmission("devin", screen, devinRelayMarker(quoted)); result != "marker_observed" {
		t.Fatalf("quoted queue text classified %s, want marker_observed", result)
	}
	calls := task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(screen)}.install(t)
	result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", quoted, nil)
	if result.Outcome != relayInjectDelivered {
		t.Fatalf("result=%+v, want delivered", result)
	}
	if got := calls(); task547Count(got, "send-keys") != 0 {
		t.Fatalf("pressed return on a submitted pane: %q", got)
	}
	// The tester's live capture of the real header still counts.
	live := "── 1 queued ──────────── ↑ edit · ↵ send now ──\n○ [event] x :: y\n────\n❭ Press Enter to send queued messages now\n────\n"
	if !devinQueued(live) || !devinQueued("○ m\n── 1 queued ── ↑ edit · ↵ send now ──\n") {
		t.Fatal("live devin queue header no longer recognised")
	}
}

// tester BLOCKER 4: the harness is read before the send; if the pane's agent
// changes while the inject runs, the verdict proves nothing.
func TestTask547HarnessChangeDuringInjectIsMaybeInPane(t *testing.T) {
	calls := task547FakeDevin{
		getBefore: "claude", getAfter: "devin",
		afterSend: task547Both(task547QueuedScreen(task547RelayText)),
	}.install(t)
	result := defaultHubRelayInjectVerdict(context.Background(), "pane", task547RelayText, nil)
	if result.Outcome != relayInjectMaybeInPane || result.Evidence != "harness_changed:claude->devin" {
		t.Fatalf("result=%+v, want maybe_in_pane harness_changed", result)
	}
	if got := calls(); task547Count(got, "get") != 2 {
		t.Fatalf("calls=%q, want the harness read before and after", got)
	}
}

// Live devin v3000.10.31 composer: the top edge carries a decoration.
const task547LiveComposerTop = "──────────────────────────────────────────────── (bypass permissions on) ─"
const task547LiveComposerBottom = "──────────────────────────────────────────────────────────────────────────"

// tester round-2 BLOCKER (B3, live capture on w16:p2D6): a submitted message
// whose body carries a header-shaped line is echoed with a two-space indent.
// It is data in the transcript, not devin's queue.
func TestTask547DevinIndentedHeaderInEchoIsNotAQueue(t *testing.T) {
	text := "(같은 내용이 두 번 보이면 재실행 금지) [event] t547-r2-b3 :: B3-MULTILINE-97086be harmless quoted text\n── 2 queued ── ↑ edit · ↵ send now ──\nThis header-shaped line is data. Reply with exactly B3-OK."
	live := "❭ (같은 내용이 두 번 보이면 재실행 금지) [event] t547-r2-b3 :: B3-\n" +
		"  MULTILINE-97086be harmless quoted text\n" +
		"  ── 2 queued ── ↑ edit · ↵ send now ──\n" +
		"  This header-shaped line is data. Reply with exactly B3-OK.\n" +
		"\n" +
		"⠇⠀ Thinking · 0s (esc twice to interrupt)\n" +
		task547LiveComposerTop + "\n" +
		"❭ Guide Devin while it works\n" +
		task547LiveComposerBottom + "\n"
	if devinQueued(live) {
		t.Fatal("indented header-shaped echo line read as devin's queue")
	}
	for _, marker := range devinRelayMarkers(text, nil) {
		if result := classifySubmission("devin", live, marker); result != "marker_observed" {
			t.Fatalf("marker=%q: live B3 screen classified %s, want marker_observed", marker, result)
		}
	}
	calls := task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(live)}.install(t)
	result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", text, nil)
	if result.Outcome != relayInjectDelivered {
		t.Fatalf("result=%+v, want delivered", result)
	}
	if got := calls(); task547Count(got, "send-keys") != 0 {
		t.Fatalf("pressed return on a submitted pane: %q", got)
	}
}

// The live queue layout (tester round-1 capture) with devin's decorated
// composer edge: header at column 0 below the transcript, queued rows, and the
// composer hint. It stays queued, and a queued row's text never reads as
// submitted.
func TestTask547DevinLiveQueueLayoutIsQueued(t *testing.T) {
	live := "○ Running command\n" +
		"│ $ sleep 120\n" +
		"── 1 queued ──────────────────────────── ↑ edit · ↵ send now ──\n" +
		"○ " + task547RelayText + "\n" +
		task547LiveComposerTop + "\n" +
		"❭ Press Enter to send queued messages now\n" +
		task547LiveComposerBottom + "\n"
	if !devinQueued(live) {
		t.Fatal("live devin queue layout not recognised")
	}
	if result := classifySubmission("devin", live, devinRelayMarker(task547RelayText)); result != "queued" {
		t.Fatalf("live queue classified %s, want queued", result)
	}
	// Header alone (hint row scrolled or redrawn) is still the queue.
	headerOnly := strings.Replace(live, "❭ Press Enter to send queued messages now", "❭ Guide Devin while it works", 1)
	if !devinQueued(headerOnly) {
		t.Fatal("column-0 queue header below the transcript not recognised")
	}
	// Residue in the decorated composer is residue, not an echo.
	residue := "❭ earlier\n" + task547LiveComposerTop + "\n❭ " + task547RelayText + "\n" + task547LiveComposerBottom + "\n"
	if result := classifySubmission("devin", residue, devinRelayMarker(task547RelayText)); result != "composer_residue" {
		t.Fatalf("live composer residue classified %s, want composer_residue", result)
	}
}

// task547Await is r27Await with room for the fake-herdr shell calls of a
// devin inject on a loaded host. A timeout here would otherwise leave a
// retry running on context.Background() into the next test's fake herdr.
func task547Await(t *testing.T, events <-chan hubClientEvent, kind string) hubClientEvent {
	t.Helper()
	deadline := time.After(20 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Kind == kind {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}

// Live devin v3000.10.31 captures from the b547 probe pane (2026-09-22): a
// message queued while "sleep 75" ran. The spinner sits right above the queue
// header.
func task547LiveBusyQueue(message string) string {
	return "❭ Run the shell command: sleep 75 . Then reply with exactly DONE-1 and nothing else.\n\n" +
		" ○ Running command\n │ $ sleep 75\n │ Timeout: 1m 30s\n\n" +
		"⠉⠁ Running tools · 25s (esc twice to interrupt)\n" +
		"── 1 queued ────────────────────────────────────────\n" +
		"○ " + message + "\n" +
		task547LiveComposerBottom + "\n" +
		"❭ Press Enter to send queued messages now\n" +
		task547LiveComposerBottom + "\n"
}

// The b534 shape: a queue left behind on an idle pane -- no spinner, the
// turn is over.
func task547IdleQueue(message string) string {
	return "❭ earlier instruction\n\n DONE-1\n\n" +
		"── 2 queued ── ↑ edit · ↵ send now ──\n" +
		"○ " + message + "\n" +
		task547LiveComposerTop + "\n" +
		"❭ Press Enter to send queued messages now\n" +
		task547LiveComposerBottom + "\n"
}

// director BOUNCE F-1/F-2: Enter on a busy devin's queue interrupts the
// running command (seen live: "Canceled due to user interrupt"). Return is
// sent only when the pane is proven idle; busy or unknown leaves the message
// queued -- no keypress, no retry.
func TestTask547DevinQueueReturnOnlyWhenIdle(t *testing.T) {
	busy := task547LiveBusyQueue(task547RelayText)
	spinnerless := strings.Replace(busy, "⠉⠁ Running tools · 25s (esc twice to interrupt)\n", "", 1)
	cases := []struct {
		name    string
		fake    task547FakeDevin
		want    relayInjectOutcome
		returns int
		busy    string
	}{
		{"working status, live busy queue: no return", task547FakeDevin{status: "working", before: task547Both(task547IdleScreen), afterSend: task547Both(busy)}, relayInjectQueued, 0, "busy:agent_status_working"},
		{"status reads done mid-turn, spinner on screen: no return", task547FakeDevin{status: "done", before: task547Both(task547IdleScreen), afterSend: task547Both(busy)}, relayInjectQueued, 0, "busy:visible:spinner"},
		{"spinner scrolled out of visible, still in recent-unwrapped: no return", task547FakeDevin{status: "idle", before: task547Both(task547IdleScreen), afterSend: map[string]string{"visible": spinnerless, "recent-unwrapped": busy}}, relayInjectQueued, 0, "busy:recent-unwrapped:spinner"},
		{"status unreadable: no return", task547FakeDevin{status: "-", before: task547Both(task547IdleScreen), afterSend: task547Both(task547IdleQueue(task547RelayText))}, relayInjectQueued, 0, "busy:status_unknown"},
		{"idle queue left behind (b534 shape): one return", task547FakeDevin{status: "idle", before: task547Both(task547IdleScreen), afterSend: task547Both(task547IdleQueue(task547RelayText)), afterReturn: task547Both(task547SubmittedScreen(task547RelayText))}, relayInjectDelivered, 1, "after_return:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := tc.fake.install(t)
			result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", task547RelayText, nil)
			if result.Outcome != tc.want || !strings.Contains(result.Evidence, tc.busy) {
				t.Fatalf("result=%+v, want outcome %d with evidence containing %q", result, tc.want, tc.busy)
			}
			got := calls()
			if n := task547Count(got, "send-keys"); n != tc.returns {
				t.Fatalf("return keypresses=%d, want %d (calls=%q)", n, tc.returns, got)
			}
			if n := task547Count(got, "prompt"); n != 1 {
				t.Fatalf("prompts=%d, want 1", n)
			}
		})
	}
}

func TestTask547DevinActivityRule(t *testing.T) {
	cases := map[string]string{
		task547LiveBusyQueue("m"): "spinner",
		"❭ go\n\n⣄⠀ Thinking · 5s (esc twice to interrupt) · (195c · ctrl+o for details)\n" + task547LiveComposerTop + "\n❭ Guide Devin while it works\n" + task547LiveComposerBottom + "\n": "working_placeholder",
		task547IdleQueue("m"): "",
		// devin's idle banner logo is braille too; it is not a spinner.
		"⠀⣴⣾⣶⡄⠀⠀⠀⠀\n⠀⠛⠿⠟⠻⣶⣾⣶⡄  Devin CLI\n⠀⣤⣶⣦⣴⠿⢿⠿⠃  v3000.10.31\n\n── 1 queued ──\n○ m\n" + task547LiveComposerTop + "\n❭ Press Enter to send queued messages now\n" + task547LiveComposerBottom + "\n": "",
		// a narrow pane wraps the spinner line
		"❭ go\n⠉⠁ Running tools · 25s (esc twice to\n  interrupt)\n── 1 queued ──\n○ m\n" + task547LiveComposerBottom + "\n❭ Press Enter to send queued messages now\n" + task547LiveComposerBottom + "\n": "spinner",
	}
	for screen, want := range cases {
		if got := devinActivity(screen); got != want {
			t.Fatalf("devinActivity=%q, want %q for\n%s", got, want, screen)
		}
	}
}

// A queued result is accepted, not retried: relay.delivered carries the
// reason, the held row is gone, and nothing is injected again.
func TestTask547DevinQueuedIsAcceptedWithoutRetry(t *testing.T) {
	injects, client, events := task547RelayRun(t, func(context.Context, string, string, []string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectQueued, Harness: "devin", Evidence: "visible:devin_queue_banner busy:agent_status_working"}
	})
	delivered := task547Await(t, events, "relay.delivered")
	var ack relayAckPayload
	if err := json.Unmarshal(delivered.Payload, &ack); err != nil || ack.Reason != "queued visible:devin_queue_banner busy:agent_status_working" {
		t.Fatalf("relay.delivered=%s err=%v", delivered.Payload, err)
	}
	time.Sleep(300 * time.Millisecond)
	if *injects != 1 {
		t.Fatalf("queued message injected %d times, want 1", *injects)
	}
	if items, err := client.relayStore().RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(items) != 0 {
		t.Fatalf("queued item still held: %+v err=%v", items, err)
	}
}
