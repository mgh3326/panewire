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
	before, afterSend, afterReturn map[string]string // source -> screen
	failRead                       string            // phase whose reads fail
}

func (f task547FakeDevin) install(t *testing.T) func() []string {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	var script strings.Builder
	script.WriteString("#!/bin/sh\nd=" + shellQuote(dir) + "\n")
	script.WriteString("phase=before\n[ -f \"$d/sent\" ] && phase=afterSend\n[ -f \"$d/returned\" ] && phase=afterReturn\n")
	script.WriteString("case \"$2\" in\n")
	script.WriteString("get) echo get >> \"$d/calls.log\"; echo '{\"result\":{\"agent\":{\"agent\":\"devin\"}}}' ;;\n")
	script.WriteString("prompt) echo prompt >> \"$d/calls.log\"; touch \"$d/sent\" ;;\n")
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
			result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", task547RelayText)
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
	result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", task547RelayText)
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
		{"never seen after send is retryable", task547FakeDevin{before: task547Both(task547IdleScreen), afterSend: task547Both(task547IdleScreen)}, relayInjectRetryable, "visible+recent-unwrapped:none", 0},
		{"unreadable after send may be in pane", task547FakeDevin{before: task547Both(task547IdleScreen), failRead: "afterSend"}, relayInjectMaybeInPane, "postsend:read_failed", 0},
		{"unreadable before send is not typed", task547FakeDevin{failRead: "before"}, relayInjectMaybeInPane, "presend:read_failed", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := tc.fake.install(t)
			result := defaultHubRelayInjectVerdict(context.Background(), "devin-pane", task547RelayText)
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
func task547RelayRun(t *testing.T, verdict func(context.Context, string, string) relayInjectResult) (*int, *HubClient, chan hubClientEvent) {
	t.Helper()
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 8)}
	store := NewMemoryStore(t)
	t.Cleanup(func() { store.Close() })
	injects := 0
	events := make(chan hubClientEvent, 64)
	client := &HubClient{outbox: store, relayCommand: fake.run, relayInjectVerdict: func(ctx context.Context, pane, text string) relayInjectResult {
		injects++
		return verdict(ctx, pane, text)
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
	injects, client, events := task547RelayRun(t, func(context.Context, string, string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectRetryable, Harness: "devin", Evidence: "visible+recent-unwrapped:none"}
	})
	unconfirmed := r27Await(t, events, "relay.unconfirmed")
	if !strings.Contains(string(unconfirmed.Payload), `"reason":"visible+recent-unwrapped:none"`) {
		t.Fatalf("relay.unconfirmed carries no evidence: %s", unconfirmed.Payload)
	}
	r27Await(t, events, "relay.dropped")
	if *injects != relayDevinMaxInjectAttempts || relayDevinMaxInjectAttempts != 2 {
		t.Fatalf("devin injects=%d, want 2 (one re-inject)", *injects)
	}
	if items, err := client.relayStore().RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(items) != 0 {
		t.Fatalf("dropped item still held: %+v err=%v", items, err)
	}
}

// claude/codex keep relayMaxInjectAttempts; #547 does not change their cap.
func TestTask547ClaudeRetryCapUnchanged(t *testing.T) {
	injects, _, events := task547RelayRun(t, func(context.Context, string, string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectRetryable, Harness: "claude"}
	})
	unconfirmed := r27Await(t, events, "relay.unconfirmed")
	if strings.Contains(string(unconfirmed.Payload), `"reason"`) {
		t.Fatalf("claude relay.unconfirmed payload changed: %s", unconfirmed.Payload)
	}
	r27Await(t, events, "relay.dropped")
	if *injects != relayMaxInjectAttempts {
		t.Fatalf("claude injects=%d, want relayMaxInjectAttempts=%d", *injects, relayMaxInjectAttempts)
	}
}

// Mutant (b): may-be-in-pane is never re-injected. The row is removed, the
// hub is told why, and no second inject follows.
func TestTask547DevinMaybeInPaneIsNeverReinjected(t *testing.T) {
	injects, client, events := task547RelayRun(t, func(context.Context, string, string) relayInjectResult {
		return relayInjectResult{Outcome: relayInjectMaybeInPane, Harness: "devin", Evidence: "visible:devin_queue_banner after_return:visible:devin_queue_banner"}
	})
	unconfirmed := r27Await(t, events, "relay.unconfirmed")
	var ack relayAckPayload
	if err := json.Unmarshal(unconfirmed.Payload, &ack); err != nil || !strings.HasPrefix(ack.Reason, "maybe_in_pane visible:devin_queue_banner") {
		t.Fatalf("relay.unconfirmed=%s err=%v, want maybe_in_pane reason with evidence", unconfirmed.Payload, err)
	}
	dropped := r27Await(t, events, "relay.dropped")
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

// C2 end to end: the first send is not seen in time (render lag), so it is
// retryable; by the retry the message has landed, and the presend check stops
// the second prompt. One prompt total, relay reported unconfirmed with cause.
func TestTask547DevinLateLandingIsNotTypedTwice(t *testing.T) {
	dir := t.TempDir()
	landed := filepath.Join(dir, "landed")
	script := "#!/bin/sh\ncase \"$2\" in\n" +
		"get) echo '{\"result\":{\"agent\":{\"agent\":\"devin\"}}}' ;;\n" +
		"prompt) echo prompt >> " + shellQuote(filepath.Join(dir, "calls.log")) + " ;;\n" +
		"read) if [ -f " + shellQuote(landed) + " ]; then printf '%s' " + shellQuote(task547SubmittedScreen(task547RelayText)) + "; else printf '%s' " + shellQuote(task547IdleScreen) + "; fi ;;\n" +
		"esac\n"
	installFakeHerdr(t, dir, script)
	injects, _, events := task547RelayRun(t, func(ctx context.Context, pane, text string) relayInjectResult {
		result := defaultHubRelayInjectVerdict(ctx, pane, text)
		// The render catches up after the first verification read.
		_ = os.WriteFile(landed, nil, 0600)
		return result
	})
	first := r27Await(t, events, "relay.unconfirmed")
	if !strings.Contains(string(first.Payload), "visible+recent-unwrapped:none") {
		t.Fatalf("first unconfirmed=%s", first.Payload)
	}
	second := r27Await(t, events, "relay.unconfirmed")
	if !strings.Contains(string(second.Payload), "maybe_in_pane presend:") {
		t.Fatalf("second unconfirmed=%s, want presend maybe_in_pane", second.Payload)
	}
	r27Await(t, events, "relay.dropped")
	b, _ := os.ReadFile(filepath.Join(dir, "calls.log"))
	if prompts := strings.Count(string(b), "prompt"); prompts != 1 || *injects != 2 {
		t.Fatalf("prompts=%d injects=%d, want 1 prompt over 2 attempts", prompts, *injects)
	}
}
