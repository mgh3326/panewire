package panewire

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTask264InjectFailureRearmsThenDrops is the AC-2 assertion for #264 D1:
// deliver() used to leave a failed inject's row in relay_held forever (only
// relay.unconfirmed was emitted, the row was neither deleted nor re-armed).
// A pane whose inject keeps failing must be retried a bounded number of
// times and, once exhausted, explicitly dropped rather than left stuck.
func TestTask264InjectFailureRearmsThenDrops(t *testing.T) {
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 8)}
	store := NewMemoryStore(t)
	defer store.Close()
	var injectCalls []string
	events := make(chan hubClientEvent, 64)
	client := &HubClient{outbox: store, relayCommand: fake.run, relayInject: func(_ context.Context, pane, text string) bool {
		injectCalls = append(injectCalls, pane+"\x00"+text)
		return false // Every attempt fails: exercises the terminal-drop branch of D1.
	}}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().restore(t.Context())

	client.relayBusyManager().offer(t.Context(), r27Directive(264, "never lands", "max_wait=1"))
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, fake)
	close(gate)

	// Each failed attempt still reports relay.unconfirmed...
	r27Await(t, events, "relay.unconfirmed")
	// ...but the manager must not stop there forever: it either keeps
	// retrying (bounded) or gives up with an explicit terminal signal.
	dropped := r27Await(t, events, "relay.dropped")
	if !strings.Contains(string(dropped.Payload), `"original_event_id":264`) {
		t.Fatalf("relay.dropped payload=%s", dropped.Payload)
	}
	if len(injectCalls) < 2 {
		t.Fatalf("inject was not retried after failure: attempts=%d", len(injectCalls))
	}
	if len(injectCalls) != relayMaxInjectAttempts {
		t.Fatalf("inject attempts=%d, want bounded at relayMaxInjectAttempts=%d", len(injectCalls), relayMaxInjectAttempts)
	}
	if items, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(items) != 0 {
		t.Fatalf("dropped item still stuck in relay_held: items=%+v err=%v", items, err)
	}
}

// TestTask264InjectFailureThenSuccessDeliversWithoutSticking covers the
// "retried" half of AC-2: a transient inject failure must not need the full
// retry budget consumed before recovering once the pane accepts the prompt.
func TestTask264InjectFailureThenSuccessDeliversWithoutSticking(t *testing.T) {
	r27GuardInbox(t)
	gate := make(chan struct{})
	fake := &r27FakeHerdr{t: t, getStatus: "working", waitStatus: "idle", waitGate: gate, started: make(chan struct{}, 8)}
	store := NewMemoryStore(t)
	defer store.Close()
	var injectCalls []string
	events := make(chan hubClientEvent, 64)
	client := &HubClient{outbox: store, relayCommand: fake.run, relayInject: func(_ context.Context, pane, text string) bool {
		injectCalls = append(injectCalls, pane+"\x00"+text)
		return len(injectCalls) > 1 // fails once, then succeeds on the rearmed retry
	}}
	client.setRelayEmitter(func(event hubClientEvent) { events <- event })
	client.relayBusyManager().restore(t.Context())

	client.relayBusyManager().offer(t.Context(), r27Directive(265, "lands eventually", "max_wait=1"))
	r27Await(t, events, "relay.held")
	r27WaitStarted(t, fake)
	close(gate)

	r27Await(t, events, "relay.unconfirmed")
	r27Await(t, events, "relay.delivered")
	if len(injectCalls) != 2 {
		t.Fatalf("inject attempts=%d, want exactly 2 (fail once, then succeed)", len(injectCalls))
	}
	if items, err := store.RelayHeldForPane(t.Context(), "fixture-pane"); err != nil || len(items) != 0 {
		t.Fatalf("delivered item still stuck in relay_held: items=%+v err=%v", items, err)
	}
}

// TestTask264RelayInjectHarnessAwareSubmission is the AC-3 assertion for
// #264 D2: defaultHubRelayInject used to gate submission verification on
// claude's "[Pasted text" composer chip alone, so a non-claude harness
// reported success on err==nil regardless of whether the prompt actually
// left the composer. This exercises codex's own "still queued" screen text
// -- a harness string that is not "claude" -- and shows the classifier
// distinguishes an unresolved queue from an actually-submitted prompt.
func TestTask264RelayInjectHarnessAwareSubmission(t *testing.T) {
	// #683: a queued banner that newly appears after the paste is accepted
	// as landed -- the harness's own queue submits the message when the turn
	// ends. It is neither a failure nor a return-keypress candidate.
	t.Run("codex queued state is landed, not a failure", func(t *testing.T) {
		dir := t.TempDir()
		prompted := filepath.Join(dir, "prompted")
		text := task687Text(26401, "do the thing")
		script := "#!/bin/sh\ncase \"$2\" in\n" +
			"get) echo '{\"result\":{\"agent\":{\"agent\":\"codex\"}}}' ;;\n" +
			"prompt) touch \"" + prompted + "\" ;;\n" +
			"read) if [ -f \"" + prompted + "\" ]; then printf '%s\\n' 'Press up to edit queued messages' '" + text + "'; else echo 'codex working'; fi ;;\n" +
			"esac\n"
		installFakeHerdr(t, dir, script)
		if !defaultHubRelayInject(context.Background(), "codex-pane", text) {
			t.Fatal("codex message accepted into the queue, but injection reported failure")
		}
	})

	// #683: the queued verdict is relayInjectQueued -- the message was
	// accepted by the harness and is never re-injected; no return keypress
	// is sent either (the queue submits itself).
	t.Run("codex queued is landed without a return", func(t *testing.T) {
		dir := t.TempDir()
		log := filepath.Join(dir, "calls")
		prompted := filepath.Join(dir, "prompted")
		text := task687Text(26402, "do the thing")
		script := "#!/bin/sh\necho \"$2\" >> \"" + log + "\"\ncase \"$2\" in\n" +
			"get) echo '{\"result\":{\"agent\":{\"agent\":\"codex\"}}}' ;;\n" +
			"prompt) touch \"" + prompted + "\" ;;\n" +
			"read) if [ -f \"" + prompted + "\" ]; then printf '%s\\n' 'Press up to edit queued messages' '" + text + "'; else echo 'codex working'; fi ;;\n" +
			"esac\n"
		installFakeHerdr(t, dir, script)
		result := defaultHubRelayInjectVerdict(context.Background(), "codex-pane", text, nil)
		if result.Outcome != relayInjectQueued {
			t.Fatalf("codex queued: outcome=%v evidence=%q, want queued", result.Outcome, result.Evidence)
		}
		calls, _ := os.ReadFile(log)
		if strings.Contains(string(calls), "send-keys") {
			t.Fatalf("codex queued: return sent: %q", calls)
		}
	})

	// #683 tester S1: a banner that was already up before the paste belongs
	// to an older queue and proves nothing about this message -- with no
	// echo of the text either, the verdict is may-be-in-pane, never a
	// delivered claim, and still no keypress.
	t.Run("codex pre-existing queue banner is not proof of landing", func(t *testing.T) {
		dir := t.TempDir()
		log := filepath.Join(dir, "calls")
		script := "#!/bin/sh\necho \"$2\" >> \"" + log + "\"\ncase \"$2\" in\n" +
			"get) echo '{\"result\":{\"agent\":{\"agent\":\"codex\"}}}' ;;\n" +
			"read) echo 'Press up to edit queued messages' ;;\n" +
			"esac\n"
		installFakeHerdr(t, dir, script)
		result := defaultHubRelayInjectVerdict(context.Background(), "codex-pane", "do the thing", nil)
		if result.Outcome != relayInjectMaybeInPane {
			t.Fatalf("codex pre-existing banner: outcome=%v evidence=%q, want maybe_in_pane", result.Outcome, result.Evidence)
		}
		calls, _ := os.ReadFile(log)
		if strings.Contains(string(calls), "send-keys") {
			t.Fatalf("codex unresolved queue earned a return keypress: %q", calls)
		}
	})

	// #264 D2 AC0 was reversed by hk:doc
	// brief/2026-09-16/devin-submission-evidence: devin's screen syntax
	// (idle placeholder text, its own "send now" queue banner, and a fixed
	// two-divider composer layout classifySubmission now recognizes for
	// devin too) turned out to carry real submission evidence after all --
	// classifySubmission was just never taught to read it. devin now shares
	// claude/codex's evidence path (see harnessHasSubmissionEvidence), so the
	// AC0 carve-out only remains for a harness with genuinely no evidence
	// path (e.g. grok). A devin read with neither queue nor marker evidence
	// must now report unconfirmed and retry, exactly like claude/codex.
	t.Run("devin harness with no queue/marker evidence reports unconfirmed, not delivered", func(t *testing.T) {
		dir := t.TempDir()
		writeFakeHerdr(t, dir, map[string]string{
			"get":  `{"result":{"agent":{"agent":"devin"}}}`,
			"read": "task acknowledged",
		})
		if defaultHubRelayInject(context.Background(), "devin-pane", "do the thing") {
			t.Fatal("devin harness with no queue/marker evidence reported delivered; carve-out should be gone")
		}
	})

	// The other half of AC0's reversal: devin's own queue banner ("send
	// now") is real evidence -- since #547 a still-queued devin screen is
	// relayInjectQueued and reported delivered, never retried; see
	// task547_devin_relay_test.go. Since #683 the banner alone is not
	// enough: the queue must show this message's own text, because a banner
	// belonging to an older queue proves nothing (tester S1).
	t.Run("devin queued state is landed, not a failure", func(t *testing.T) {
		dir := t.TempDir()
		prompted := filepath.Join(dir, "prompted")
		text := task687Text(26403, "do the thing")
		queued := "⠋ Thinking 12m04s\n○ " + text + "\n── 1 queued ── ↑ edit · ↵ send now\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High"
		idle := "─────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n─────────────────────\nSWE-2 High"
		script := "#!/bin/sh\ncase \"$2\" in\n" +
			"get) echo '{\"result\":{\"agent\":{\"agent\":\"devin\",\"agent_status\":\"working\"}}}' ;;\n" +
			"prompt) touch \"" + prompted + "\" ;;\n" +
			"read) if [ -f \"" + prompted + "\" ]; then printf '%s\\n' \"" + queued + "\"; else printf '%s\\n' \"" + idle + "\"; fi ;;\n" +
			"esac\n"
		installFakeHerdr(t, dir, script)
		if !defaultHubRelayInject(context.Background(), "devin-pane", text) {
			t.Fatal("devin message accepted into the queue, but injection reported failure")
		}
	})

	// The queue lands the message before any keypress: the postsend read
	// already shows devin's queue banner, which #683 accepts as landed.
	t.Run("devin queue banner accepts the message, verification reports success", func(t *testing.T) {
		dir := t.TempDir()
		prompted := filepath.Join(dir, "prompted")
		marker := filepath.Join(dir, "returned")
		// An idle pane: since #547 the one return keypress on a devin queue
		// is sent only when the pane is proven idle. The queued screen is the
		// live capture in TestClassifySubmissionAllFourValues; since #626 the
		// keypress also needs the composer to hold only devin's queue hint.
		text := task687Text(26404, "do the thing")
		queued := "── 1 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ " + text + "\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High"
		idle := "─────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n─────────────────────\nSWE-2 High"
		script := "#!/bin/sh\ncase \"$2\" in\n" +
			"get) echo '{\"result\":{\"agent\":{\"agent\":\"devin\",\"agent_status\":\"idle\"}}}' ;;\n" +
			"prompt) touch \"" + prompted + "\" ;;\n" +
			"read) if [ -f \"" + marker + "\" ]; then echo '❭ " + text + "'; elif [ -f \"" + prompted + "\" ]; then printf '%s\\n' \"" + queued + "\"; else printf '%s\\n' \"" + idle + "\"; fi ;;\n" +
			"send-keys) touch \"" + marker + "\" ;;\n" +
			"esac\n"
		installFakeHerdr(t, dir, script)
		if !defaultHubRelayInject(context.Background(), "devin-pane", text) {
			t.Fatal("devin message echoed the nonce after return, but injection reported failure")
		}
	})
}

// writeFakeHerdr installs a fake herdr binary on PATH whose output for each
// subcommand (indexed by $2) is fixed for the whole test.
func writeFakeHerdr(t *testing.T, dir string, outputs map[string]string) {
	t.Helper()
	var script strings.Builder
	script.WriteString("#!/bin/sh\ncase \"$2\" in\n")
	for cmd, out := range outputs {
		script.WriteString(cmd + ") echo '" + out + "' ;;\n")
	}
	script.WriteString("esac\n")
	installFakeHerdr(t, dir, script.String())
}

func installFakeHerdr(t *testing.T, dir, script string) {
	t.Helper()
	binary := filepath.Join(dir, "herdr")
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestTask264HubConsumesRelayDropped is the BLOCKER-1 fix from the adversarial
// verify round: the node-emitted relay.dropped wire event (busy_relay.go's
// retryOrDrop) was never registered in knownHubEventKind, so the hub rejected
// it at the parse stage before dispatch or broadcast ever saw it -- the held
// projection (h.relayHeld) then stayed a permanent ghost row, reproducing the
// exact "stuck forever" symptom D1 fixed on the node one layer up. This drives
// the actual hub receive path (parseHubInbound + handleAgentMessage), not the
// node-local relayInject/setRelayEmitter seam the D1 tests use, so it would
// have caught the gap the D1 tests structurally could not.
func TestTask264HubConsumesRelayDropped(t *testing.T) {
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": "op", "host-a": "node"}})
	if err != nil {
		t.Fatal(err)
	}
	agent := &hubAgent{}
	hub.nodes["host-a"] = &hubNodeRecord{agent: agent}
	hub.relayHeld[601] = hubRelayHeldProjection{ID: 601, Lane: "lane-a", Pane: "fixture-pane", Machine: "host-a", Preview: "stuck", HeldSince: "2026-01-01T00:00:00Z", DeliverPolicy: "idle", JobID: "relay-job-601"}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	subscriber := &hubEventSubscriber{ctx: ctx, cancel: cancel, messages: make(chan hubSubscriptionMessage, 4)}
	hub.subscribers[subscriber] = struct{}{}

	dropped := relayDroppedPayload{JobID: "relay-job-601", Pane: "fixture-pane", Lane: "lane-a", OriginalEventID: 601, Reason: "inject_failed_max_attempts"}
	payload, err := json.Marshal(dropped)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(struct {
		Type    string          `json:"type"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Type: "event", Kind: "relay.dropped", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if _, valid := parseHubInbound(wire); !valid {
		t.Fatalf("parseHubInbound rejected relay.dropped: %s", wire)
	}
	hub.handleAgentMessage("host-a", "fixture", agent, wire)

	if _, exists := hub.relayHeld[601]; exists {
		t.Fatal("dropped relay remained held: relay.dropped did not reach the hub's projection cleanup")
	}
	select {
	case message := <-subscriber.messages:
		if message.event == nil || message.event.Kind != "relay.dropped" {
			t.Fatalf("broadcast=%+v, want relay.dropped", message)
		}
	default:
		t.Fatal("relay.dropped was not broadcast to subscribers")
	}
}
