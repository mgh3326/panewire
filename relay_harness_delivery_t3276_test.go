package panewire

// hk 3276: a review finding or an ESC answer addressed to a worker lane reaches
// the worker's pane on every harness, and relay.delivered is recorded only on
// that harness's own on-screen evidence. These tests run the whole path with
// fakes: a real hub over a fake handoffkeep, a real node relay, and a shell
// herdr that models one pane (its status and screens before and after the
// prompt). Nothing starts a live pane.

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

const (
	t3276Worker = "wk-t3276"
	t3276Design = "design-t3276"
)

// t3276Screens is what one harness's fake pane shows. pre is the pane before
// the prompt, echo after a submitted prompt, and pending after a paste that
// never submitted. Each is built from the exact text typed into the pane, so
// the nonce on screen is the one the relay composed.
type t3276Screens struct {
	harness      string
	preStatus    string
	postStatus   string
	pre          func(typed string) string
	echo         func(typed string) string
	pending      func(typed string) string
	delivered    string // substring of the relay.delivered reason
	deliveredOut string // outcome word: "delivered" or "queued"
}

func t3276Harnesses() map[string]t3276Screens {
	codexIdle := "• finished the last step\n\n› Ask Codex to do anything\n  gpt-5 high · ~/work/panewire"
	codexWorking := "• Working (12s • esc to interrupt)\n\n› Ask Codex to do anything\n  gpt-5 high · ~/work/panewire"
	devinIdle := "─────────────────────\n❭ Ask Devin to build features, fix bugs, or work on your code\n─────────────────────\nSWE-2 High"
	return map[string]t3276Screens{
		"codex-idle": {
			harness: "codex", preStatus: "idle", postStatus: "working",
			pre: func(string) string { return codexIdle },
			echo: func(typed string) string {
				return "• finished the last step\n\n› " + typed + "\n\n• Working (1s)\n\n› Ask Codex to do anything\n  gpt-5 high"
			},
			pending:   func(typed string) string { return "• finished the last step\n\n› " + typed + "\n  gpt-5 high" },
			delivered: "nonce_echo",
		},
		"codex-working": {
			harness: "codex", preStatus: "working", postStatus: "working",
			pre: func(string) string { return codexWorking },
			echo: func(typed string) string {
				return "• Working (12s • esc to interrupt)\n\nMessages to be submitted after next tool call\n  ↳ " + typed + "\n\n› Ask Codex to do anything\n  gpt-5 high"
			},
			pending:   func(string) string { return codexWorking },
			delivered: "queued_banner", deliveredOut: "queued",
		},
		"devin": {
			harness: "devin", preStatus: "idle", postStatus: "working",
			pre:  func(string) string { return devinIdle },
			echo: func(typed string) string { return "❭ " + typed + "\n" + devinIdle },
			// A paste devin has not taken shows nothing at all.
			pending:   func(string) string { return devinIdle },
			delivered: "nonce_echo",
		},
		"grok": {
			harness: "grok", preStatus: "idle", postStatus: "working",
			pre:       func(string) string { return "grok ready\n> " },
			echo:      func(typed string) string { return "you: " + typed + "\ngrok is thinking\n> " },
			pending:   func(typed string) string { return "grok ready\n> " + typed },
			delivered: "nonce_echo+status_working",
		},
		"omp": {
			harness: "omp", preStatus: "idle", postStatus: "working",
			pre:       func(string) string { return "omp ready\n$ " },
			echo:      func(typed string) string { return "user: " + typed + "\nomp is working\n$ " },
			pending:   func(typed string) string { return "omp ready\n$ " + typed },
			delivered: "nonce_echo+status_working",
		},
	}
}

// t3276Pane is the shell herdr for one pane. Screens and statuses switch when
// `agent prompt` runs, and every call is logged one word per line.
type t3276Pane struct {
	dir string
}

func t3276InstallPane(t *testing.T, sc t3276Screens, typed string, submitted bool) *t3276Pane {
	t.Helper()
	dir := t.TempDir()
	post := sc.pending(typed)
	if submitted {
		post = sc.echo(typed)
	}
	postStatus := sc.postStatus
	if !submitted {
		// A paste that never submitted leaves the pane where it was.
		postStatus = sc.preStatus
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []string{"visible", "recent-unwrapped"} {
		write("pre."+source+".last", sc.pre(typed))
		write("post."+source+".last", post)
	}
	write("status.pre", sc.preStatus)
	write("status.post", postStatus)
	script := `#!/bin/sh
d='` + dir + `'
echo "$2" >> "$d/calls"
case "$2" in
get)
  if [ -f "$d/prompted" ]; then st=$(cat "$d/status.post"); else st=$(cat "$d/status.pre"); fi
  printf '{"result":{"agent":{"agent":"` + sc.harness + `","agent_status":"%s","pane_id":"w1:pW"}}}\n' "$st" ;;
prompt) printf '%s' "$4" > "$d/typed"; touch "$d/prompted" ;;
read) src=visible; lines=60; prev=
  for a in "$@"; do [ "$prev" = "--source" ] && src=$a; [ "$prev" = "--lines" ] && lines=$a; prev=$a; done
  phase=pre; [ -f "$d/prompted" ] && phase=post
  tail -n "$lines" "$d/$phase.$src.last" ;;
esac
`
	installFakeHerdr(t, dir, script)
	return &t3276Pane{dir: dir}
}

func (pane *t3276Pane) calls(t *testing.T, word string) int {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(pane.dir, "calls"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return task683Count(strings.Fields(string(body)), word)
}

func (pane *t3276Pane) typed(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(pane.dir, "typed"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(body)
}

// t3276Rig is a hub with a worker lane whose parent is the design lane, and a
// node relay wired back into that hub the way a websocket would be.
type t3276Rig struct {
	t      *testing.T
	hub    *HubServer
	fake   *fakeHandoffkeep
	dest   *hubAgent
	src    *hubAgent
	node   *HubClient
	events chan hubClientEvent
}

func t3276Start(t *testing.T) *t3276Rig {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	lanes, _ := json.Marshal(map[string]any{"lanes": map[string]reportRelayRoute{
		t3276Worker: {Machine: "host-a", Pane: "w1:pW", Parent: t3276Design},
		t3276Design: {Machine: "host-a", Pane: "w1:pD"},
	}})
	hub := r20Hub(t, string(lanes), client, nil)
	hub.relayControl.relayAckTimeout = time.Hour
	rig := &t3276Rig{t: t, hub: hub, fake: fake, events: make(chan hubClientEvent, 64)}
	rig.dest = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	rig.src = &hubAgent{relays: make(chan hubRelayInjectEvent, 64), persisted: make(chan hubRelayPersistedEvent, 64)}
	hub.mu.Lock()
	hub.nodes["host-a"] = &hubNodeRecord{agent: rig.dest}
	hub.nodes["host-b"] = &hubNodeRecord{agent: rig.src}
	hub.mu.Unlock()

	store := NewMemoryStore(t)
	t.Cleanup(func() { store.Close() })
	rig.node = &HubClient{outbox: store, relayCommand: func(ctx context.Context, args ...string) ([]byte, error) {
		if len(args) >= 2 && args[1] == "wait" {
			// A held row waits for the pane to go idle; here it never does.
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return defaultRelayCommand(ctx, args...)
	}}
	rig.node.setRelayEmitter(func(event hubClientEvent) {
		rig.events <- event
		t650Send(t, hub, "host-a", rig.dest, event.Kind, json.RawMessage(event.Payload))
	})
	rig.node.relayBusyManager().restore(t.Context())
	return rig
}

// send hands the hub one lane.event for the worker lane and returns the
// directive it queued for the node.
func (rig *t3276Rig) send(eventID, notifyType, text string) hubRelayInjectEvent {
	rig.t.Helper()
	event := hubJobEventPayload{JobID: laneEventTransportID(t3276Worker, eventID), Epoch: 1, OwnerLane: t3276Worker, EventID: eventID, Text: text, NotifyType: notifyType}
	rig.hub.relayLaneEvent(event, rig.src)
	select {
	case directive := <-rig.dest.relays:
		if directive.Lane != t3276Worker || directive.EventID < 1 {
			rig.t.Fatalf("directive for the wrong lane or without a durable id: %+v", directive)
		}
		return directive
	case <-time.After(2 * time.Second):
		rig.t.Fatal("the hub queued no relay.inject for the worker lane")
	}
	return hubRelayInjectEvent{}
}

// typedFor is the exact text the node types for directive: nonce first.
func t3276TypedFor(directive hubRelayInjectEvent) string {
	return relayBatchText([]relayHeld{{Pane: directive.Pane, Lane: directive.Lane, EventID: directive.EventID, JobID: directive.JobID, Text: directive.Text}}, false, time.Now())
}

func t3276Message(directive hubRelayInjectEvent) hubOutboundMessage {
	return hubOutboundMessage{Type: "relay.inject", Kind: directive.Kind, JobID: directive.JobID, Pane: directive.Pane, Text: directive.Text, Lane: directive.Lane, EventID: directive.EventID, DeliverPolicy: directive.Deliver}
}

func (rig *t3276Rig) row(id int64) handoffkeepRelayEvent {
	rig.t.Helper()
	rig.fake.mu.Lock()
	defer rig.fake.mu.Unlock()
	for _, row := range rig.fake.rows {
		if row.ID == id {
			return *row
		}
	}
	rig.t.Fatalf("no durable row %d", id)
	return handoffkeepRelayEvent{}
}

// escalations returns the escalation directives the hub queued for the design
// lane since the last call.
func (rig *t3276Rig) escalations() []hubRelayInjectEvent {
	var out []hubRelayInjectEvent
	for {
		select {
		case directive := <-rig.dest.relays:
			if directive.Lane == t3276Design {
				out = append(out, directive)
			}
		default:
			return out
		}
	}
}

func (rig *t3276Rig) drainEvents() (kinds []string, payloads map[string]relayAckPayload) {
	payloads = map[string]relayAckPayload{}
	for {
		select {
		case event := <-rig.events:
			kinds = append(kinds, event.Kind)
			var ack relayAckPayload
			if json.Unmarshal(event.Payload, &ack) == nil {
				payloads[event.Kind] = ack
			}
		default:
			return kinds, payloads
		}
	}
}

func t3276Count(kinds []string, want string) int {
	n := 0
	for _, kind := range kinds {
		if kind == want {
			n++
		}
	}
	return n
}

// Acceptance 2, delivered half: a findings event and a decision (ESC answer)
// event each reach the pane of every harness, once, and relay.delivered is
// recorded -- with the harness's own proof signal in its reason -- and closes
// the durable row. Nothing escalates.
func TestHarnessDeliveryRecordsProofSignal_t3276(t *testing.T) {
	for name, sc := range t3276Harnesses() {
		for _, notify := range []string{"findings", "decision"} {
			t.Run(name+"/"+notify, func(t *testing.T) {
				rig := t3276Start(t)
				directive := rig.send("ev-"+name+"-"+notify, notify, "[mergewake] "+notify+" for the worker: fix the failing check")
				pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)

				rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

				if got := pane.calls(t, "prompt"); got != 1 {
					t.Fatalf("pane got %d prompts, want exactly 1", got)
				}
				if typed := pane.typed(t); !strings.Contains(typed, "fix the failing check") || !strings.Contains(typed, relayNonce(relayHeld{EventID: directive.EventID, Text: directive.Text})) {
					t.Fatalf("typed text lacks the event or its nonce: %q", typed)
				}
				kinds, payloads := rig.drainEvents()
				if t3276Count(kinds, "relay.held") != 0 {
					t.Fatalf("the interrupt was held instead of injected: %v", kinds)
				}
				if t3276Count(kinds, "relay.delivered") != 1 || t3276Count(kinds, "relay.unconfirmed") != 0 {
					t.Fatalf("events=%v, want one relay.delivered and no relay.unconfirmed", kinds)
				}
				if reason := payloads["relay.delivered"].Reason; !strings.Contains(reason, sc.delivered) {
					t.Fatalf("relay.delivered reason=%q, want the proof signal %q", reason, sc.delivered)
				}
				if sc.deliveredOut == "queued" && !strings.HasPrefix(payloads["relay.delivered"].Reason, "queued ") {
					t.Fatalf("a mid-turn inject must say it was queued: %q", payloads["relay.delivered"].Reason)
				}
				if row := rig.row(directive.EventID); row.DeliveredAt == "" {
					t.Fatalf("a proven delivery left the durable row undelivered: %+v", row)
				}
				if got := rig.escalations(); len(got) != 0 {
					t.Fatalf("a delivered event escalated: %+v", got)
				}
			})
		}
	}
}

// Acceptance 2, missing-proof half: the text reaches the pane (it was typed)
// but nothing shows it taken. No relay.delivered is recorded, the durable row
// stays open, and the design lane hears about it exactly once, with the event
// id -- also when the same row drops a second time.
func TestHarnessMissingProofEscalatesOnce_t3276(t *testing.T) {
	prevWindow := relayObserveWindow
	relayObserveWindow = 400 * time.Millisecond
	t.Cleanup(func() { relayObserveWindow = prevWindow })
	for name, sc := range t3276Harnesses() {
		for _, notify := range []string{"findings", "decision"} {
			t.Run(name+"/"+notify, func(t *testing.T) {
				rig := t3276Start(t)
				directive := rig.send("ev-miss-"+name+"-"+notify, notify, "[mergewake] "+notify+" for the worker: fix the failing check")
				pane := t3276InstallPane(t, sc, t3276TypedFor(directive), false)

				rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

				if got := pane.calls(t, "prompt"); got != 1 {
					t.Fatalf("pane got %d prompts, want exactly 1 (a missing proof never re-types)", got)
				}
				kinds, _ := rig.drainEvents()
				if t3276Count(kinds, "relay.delivered") != 0 {
					t.Fatalf("relay.delivered was recorded without proof: %v", kinds)
				}
				if t3276Count(kinds, "relay.unconfirmed") != 1 || t3276Count(kinds, "relay.dropped") != 1 {
					t.Fatalf("events=%v, want one relay.unconfirmed then one relay.dropped", kinds)
				}
				if row := rig.row(directive.EventID); row.DeliveredAt != "" {
					t.Fatalf("an unproven delivery closed the durable row: %+v", row)
				}
				got := rig.escalations()
				if len(got) != 1 {
					t.Fatalf("design lane got %d escalations, want exactly 1: %+v", len(got), got)
				}
				if want := fmt.Sprintf("event %d", directive.EventID); !strings.Contains(got[0].Text, want) || !strings.Contains(got[0].Text, "maybe_in_pane") || !strings.Contains(got[0].Text, t3276Worker) {
					t.Fatalf("escalation lacks the event id, the reason or the lane (%q): %q", want, got[0].Text)
				}

				// The same row dropping again (a replay that also fails) does
				// not tell the design lane twice.
				t650Send(t, rig.hub, "host-a", rig.dest, "relay.dropped", relayDroppedPayload{JobID: directive.JobID, Pane: directive.Pane, Lane: directive.Lane, OriginalEventID: directive.EventID, Reason: "maybe_in_pane"})
				if again := rig.escalations(); len(again) != 0 {
					t.Fatalf("the same event escalated twice: %+v", again)
				}
			})
		}
	}
}

// A late echo is proof, not a drop: codex renders its echo after the first
// post-send read, and the observation window finds it. Before hk 3276 only
// claude was observed, so codex's first unproven read dropped the row.
func TestCodexLateEchoIsObservedNotDropped_t3276(t *testing.T) {
	rig := t3276Start(t)
	sc := t3276Harnesses()["codex-idle"]
	directive := rig.send("ev-codex-late", "findings", "[mergewake] findings for the worker: fix the failing check")
	typed := t3276TypedFor(directive)
	pane := t3276InstallPane(t, sc, typed, false)
	// The first two post-prompt reads show a pending paste; the third shows the echo.
	for _, source := range []string{"visible", "recent-unwrapped"} {
		for i, body := range []string{sc.pending(typed), sc.pending(typed), sc.echo(typed)} {
			if err := os.WriteFile(filepath.Join(pane.dir, fmt.Sprintf("post.%s.%d", source, i+1)), []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	rewritten := strings.Replace(mustReadFile(t, filepath.Join(pane.dir, "herdr")), `tail -n "$lines" "$d/$phase.$src.last"`,
		`n=$(( $(cat "$d/$phase.$src.idx" 2>/dev/null || echo 0) + 1 )); echo $n > "$d/$phase.$src.idx"; f="$d/$phase.$src.$n"; [ -f "$f" ] || f="$d/$phase.$src.last"; tail -n "$lines" "$f"`, 1)
	if err := os.WriteFile(filepath.Join(pane.dir, "herdr"), []byte(rewritten), 0o700); err != nil {
		t.Fatal(err)
	}

	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	kinds, _ := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("a late echo was not observed: events=%v", kinds)
	}
	if got := pane.calls(t, "prompt"); got != 1 {
		t.Fatalf("observing re-typed: %d prompts", got)
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// Acceptance 2, grok and omp: a working pane is not injected mid-turn (their
// screens give no queue evidence), so the interrupt is held until the pane is
// idle -- the same as claude. Codex is the harness that takes input mid-work.
func TestMidTurnInjectionIsCodexOnly_t3276(t *testing.T) {
	for _, name := range []string{"grok", "omp"} {
		t.Run(name+" working pane is held", func(t *testing.T) {
			rig := t3276Start(t)
			sc := t3276Harnesses()[name]
			sc.preStatus = "working"
			directive := rig.send("ev-held-"+name, "findings", "[mergewake] findings for the worker")
			pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)

			rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

			if got := pane.calls(t, "prompt"); got != 0 {
				t.Fatalf("a working %s pane was prompted mid-turn", name)
			}
			kinds, _ := rig.drainEvents()
			if t3276Count(kinds, "relay.held") != 1 {
				t.Fatalf("events=%v, want the interrupt held until idle", kinds)
			}
		})
	}
	t.Run("a blocked codex pane is held too", func(t *testing.T) {
		rig := t3276Start(t)
		sc := t3276Harnesses()["codex-working"]
		sc.preStatus = "blocked"
		directive := rig.send("ev-held-codex-blocked", "decision", "[mergewake] decision for the worker")
		pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)

		rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

		if got := pane.calls(t, "prompt"); got != 0 {
			t.Fatal("a codex pane waiting on an approval prompt was typed into")
		}
	})
}

// The proof rules fail closed for a harness the relay has no adapter for, and
// for a harness whose composer cannot be located when the pane never started a
// turn -- the two cases the old carve-out reported delivered.
func TestUnknownHarnessAndPendingPasteAreUnconfirmed_t3276(t *testing.T) {
	text := task687Text(327600, "[mergewake] findings")
	for _, tc := range []struct {
		name, harness string
		reads         []string
	}{
		{"unknown harness shows the text", "some-future-harness", []string{text}},
		{"unknown harness shows nothing", "some-future-harness", []string{"nothing relevant"}},
		{"harness could not be read", "", []string{text}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unprovenSetupHerdr(t, tc.reads)
			result := relayInjectVerify(context.Background(), "test-pane", tc.harness, text, "")
			if result.Outcome == relayInjectDelivered || result.Outcome == relayInjectQueued {
				t.Fatalf("result=%+v, want unconfirmed", result)
			}
		})
	}
}

// The escalation is for a producer's lane.event. An idle-wake row, or an
// escalation that was itself lost, drops without telling the design lane.
func TestEscalationSkipsIdleWakeAndLostEscalations_t3276(t *testing.T) {
	for _, eventID := range []string{"idle-wake:0123456789abcdef0123456789abcdef:cGFuZQ:7", unconfirmedEscalationPrefix + "55"} {
		t.Run(eventID, func(t *testing.T) {
			rig := t3276Start(t)
			directive := rig.send(eventID, "report", "[panewire] routine notice")

			t650Send(t, rig.hub, "host-a", rig.dest, "relay.dropped", relayDroppedPayload{JobID: directive.JobID, Pane: directive.Pane, Lane: directive.Lane, OriginalEventID: directive.EventID, Reason: "maybe_in_pane"})

			if got := rig.escalations(); len(got) != 0 {
				t.Fatalf("escalated a row that must not escalate: %+v", got)
			}
		})
	}
}
