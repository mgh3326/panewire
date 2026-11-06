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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	t3276Worker = "wk-t3276"
	t3276Design = "design-t3276"
	// t3276Lone has no parent: the desk is such a lane.
	t3276Lone = "lone-t3276"
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
	claudeWorking := "⏺ Working…"
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
		"claude-idle": {
			harness: "claude", preStatus: "idle", postStatus: "working",
			pre:       func(string) string { return task626ClaudeEmpty },
			echo:      func(typed string) string { return task626Claude([]string{"❯ " + typed}, "❯") },
			pending:   func(string) string { return task626ClaudeEmpty },
			delivered: "nonce_echo",
		},
		"claude-working": {
			harness: "claude", preStatus: "working", postStatus: "working",
			pre: func(string) string { return task626Claude([]string{claudeWorking}, "❯") },
			echo: func(typed string) string {
				return task626Claude([]string{claudeWorking, "❯ " + typed, "Press up to edit queued messages"}, "❯")
			},
			pending:   func(string) string { return task626Claude([]string{claudeWorking}, "❯") },
			delivered: "queued_banner", deliveredOut: "queued",
		},
		"devin-idle": {
			harness: "devin", preStatus: "idle", postStatus: "working",
			pre:  func(string) string { return devinIdle },
			echo: func(typed string) string { return "❭ " + typed + "\n" + devinIdle },
			// A paste devin has not taken shows nothing at all.
			pending:   func(string) string { return devinIdle },
			delivered: "nonce_echo",
		},
		"devin-working": {
			harness: "devin", preStatus: "working", postStatus: "working",
			pre: func(string) string { return devinIdle },
			echo: func(typed string) string {
				return "── 1 queued ──────────────────────────────────────── ↑ edit · ↵ send now ──\n○ " + typed + "\n─────────────────────\n❭ Press Enter to send queued messages now\n─────────────────────\nSWE-2 High"
			},
			pending:   func(string) string { return devinIdle },
			delivered: "devin_queue_banner", deliveredOut: "queued",
		},
		"grok-idle": {
			harness: "grok", preStatus: "idle", postStatus: "working",
			pre:       func(string) string { return "grok ready\n> " },
			echo:      func(typed string) string { return "you: " + typed + "\ngrok is thinking\n> " },
			pending:   func(typed string) string { return "grok ready\n> " + typed },
			delivered: "nonce_echo+status_working",
		},
		"omp-idle": {
			harness: "omp", preStatus: "idle", postStatus: "working",
			pre:       func(string) string { return "omp ready\n$ " },
			echo:      func(typed string) string { return "user: " + typed + "\nomp is working\n$ " },
			pending:   func(typed string) string { return "omp ready\n$ " + typed },
			delivered: "nonce_echo+status_working",
		},
	}
}

// t3276WorkingUnprovable are the working grok and omp panes. The directive is
// typed at once, but with no captured screen the only rule they have needs the
// pane idle or done before the send, so nothing can prove it landed.
func t3276WorkingUnprovable() map[string]t3276Screens {
	out := map[string]t3276Screens{}
	for name, sc := range t3276Harnesses() {
		if sc.harness == "grok" || sc.harness == "omp" {
			sc.preStatus, sc.postStatus = "working", "working"
			out[sc.harness+"-working"] = sc
		}
		_ = name
	}
	return out
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
  printf '{"result":{"agent":{"agent":"` + sc.harness + `","agent_status":"%s","name":"` + t3276Worker + `","pane_id":"w1:pW"}}}\n' "$st" ;;
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
	// waits records the arguments of every `herdr agent wait` a hold started.
	waitMu    sync.Mutex
	waits     []string
	waitExits int
}

func t3276Start(t *testing.T) *t3276Rig {
	t.Helper()
	fake, client, closeServer := newFakeHandoffkeep(t)
	t.Cleanup(closeServer)
	lanes, _ := json.Marshal(map[string]any{"lanes": map[string]reportRelayRoute{
		t3276Worker: {Machine: "host-a", Pane: "w1:pW", Parent: t3276Design},
		t3276Design: {Machine: "host-a", Pane: "w1:pD"},
		t3276Lone:   {Machine: "host-a", Pane: "w1:pL"},
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
			rig.waitMu.Lock()
			rig.waits = append(rig.waits, strings.Join(args, " "))
			rig.waitMu.Unlock()
			<-ctx.Done()
			rig.waitMu.Lock()
			rig.waitExits++
			rig.waitMu.Unlock()
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
	return rig.sendTo(t3276Worker, eventID, notifyType, text)
}

func (rig *t3276Rig) sendTo(lane, eventID, notifyType, text string) hubRelayInjectEvent {
	rig.t.Helper()
	event := hubJobEventPayload{JobID: laneEventTransportID(lane, eventID), Epoch: 1, OwnerLane: lane, EventID: eventID, Text: text, NotifyType: notifyType}
	rig.hub.relayLaneEvent(event, rig.src)
	select {
	case directive := <-rig.dest.relays:
		if directive.Lane != lane || directive.EventID < 1 {
			rig.t.Fatalf("directive for the wrong lane or without a durable id: %+v", directive)
		}
		return directive
	case <-time.After(2 * time.Second):
		rig.t.Fatal("the hub queued no relay.inject for the lane")
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
				if directive.Deliver != relayDeliverDirective {
					t.Fatalf("a %s for a worker lane carries deliver=%q, want directive: it is never held for a quieter pane", notify, directive.Deliver)
				}
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
// but nothing shows it taken. It is typed once more at once, and then no
// relay.delivered is recorded, the durable row stays open, and the design lane
// hears about it exactly once, with the event id -- also when the same row drops
// a second time.
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

				if got := pane.calls(t, "prompt"); got != 2 {
					t.Fatalf("pane got %d prompts, want 2: the first and the one immediate retry", got)
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

// A working grok or omp pane gets the directive at once too. With no captured
// screen the only proof those harnesses have needs an idle or done pane before
// the send, so the typed text stays unconfirmed: one retry, then one escalation.
func TestWorkingGrokAndOmpAreInjectedThenEscalated_t3276(t *testing.T) {
	prevWindow := relayObserveWindow
	relayObserveWindow = 300 * time.Millisecond
	t.Cleanup(func() { relayObserveWindow = prevWindow })
	for name, sc := range t3276WorkingUnprovable() {
		t.Run(name, func(t *testing.T) {
			rig := t3276Start(t)
			directive := rig.send("ev-working-"+name, "findings", "[mergewake] findings for the worker")
			pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)

			rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

			if got := pane.calls(t, "prompt"); got != 2 {
				t.Fatalf("a working %s pane got %d prompts, want the directive and one retry", sc.harness, got)
			}
			kinds, _ := rig.drainEvents()
			if t3276Count(kinds, "relay.held") != 0 || t3276Count(kinds, "relay.delivered") != 0 {
				t.Fatalf("events=%v: want injected at once and not delivered without proof", kinds)
			}
			if got := rig.escalations(); len(got) != 1 {
				t.Fatalf("design lane got %d escalations, want 1", len(got))
			}
		})
	}
}

// The immediate path is for worker directives only. The same pane, working, is
// still waited for when the message is not a directive (a report) or the lane
// has no parent (the desk), and an explicit deliver on the event still wins.
func TestOnlyWorkerDirectivesSkipTheBusyHold_t3276(t *testing.T) {
	for _, tc := range []struct {
		name, lane, notify, deliver string
	}{
		{"report to a worker lane", t3276Worker, "report", "idle"},
		{"findings to a lane with no parent", t3276Lone, "findings", "idle"},
		{"order to a worker lane", t3276Worker, "order", relayDeliverDirective},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := t3276Start(t)
			directive := rig.sendTo(tc.lane, "ev-scope-"+strings.ReplaceAll(tc.name, " ", "-"), tc.notify, "[mergewake] text for the lane")
			if directive.Deliver != tc.deliver {
				t.Fatalf("deliver=%q, want %q", directive.Deliver, tc.deliver)
			}
			if tc.deliver != "idle" {
				return
			}
			pane := t3276InstallPane(t, t3276Harnesses()["codex-working"], t3276TypedFor(directive), true)
			rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))
			if got := pane.calls(t, "prompt"); got != 0 {
				t.Fatalf("a working pane was typed into for a message that is not a worker directive")
			}
			if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.held") != 1 {
				t.Fatalf("events=%v, want the row held until the pane is idle", kinds)
			}
		})
	}
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

// A worker directive is typed into a working pane at once, but a blocked pane
// is waiting on a prompt (an approval) the text could answer, so R27's hold
// still applies to it: nothing is typed until the pane is released.
func TestWorkerDirectiveIsHeldWhileBlocked_t3276(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-blocked", "findings", "[mergewake] findings for the worker")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "blocked"
	pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)

	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	if got := pane.calls(t, "prompt"); got != 0 {
		t.Fatalf("a blocked pane was typed into %d times", got)
	}
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.held") != 1 {
		t.Fatalf("events=%v, want the directive held while the pane is blocked", kinds)
	}
}

// One retry covers every unconfirmed outcome that does not say the text sits
// in a foreign or changed composer: a harness with no adapter, an unreadable
// pane and the reads showing nothing all retry once and then escalate once.
func TestDirectiveRetriesOnceWhenProofIsUndecidable_t3276(t *testing.T) {
	for _, evidence := range []string{"unproven:no_adapter", "unproven:read_failed", "presend:unproven:read_failed", "unproven:composer_unlocated", "postsend:visible+recent-unwrapped:none", "presend:read_failed", "postsend:read_failed"} {
		if !relayRetriesOnce(relayInjectResult{Outcome: relayInjectMaybeInPane, Evidence: evidence}) {
			t.Errorf("evidence %q: want one retry", evidence)
		}
	}
	for _, evidence := range []string{"composer_foreign", "presend:composer_residue", "harness_changed"} {
		if relayRetriesOnce(relayInjectResult{Outcome: relayInjectMaybeInPane, Evidence: evidence}) {
			t.Errorf("evidence %q: want no retry", evidence)
		}
	}

	rig := t3276Start(t)
	directive := rig.send("ev-no-adapter", "findings", "[mergewake] findings for the worker")
	sc := t3276Harnesses()["codex-working"]
	sc.harness = "some-future-harness"
	pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)
	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))
	if got := pane.calls(t, "prompt"); got != 2 {
		t.Fatalf("a harness with no adapter got %d prompts, want the directive and one retry", got)
	}
	if got := rig.escalations(); len(got) != 1 {
		t.Fatalf("design lane got %d escalations, want 1", len(got))
	}
}

// The retry-once path belongs to worker directives. An explicit deliver=now on
// any other event keeps the one send it always had.
func TestExplicitNowIsNotAnImmediateDirective_t3276(t *testing.T) {
	row := func(policy string) []relayHeld {
		return []relayHeld{{Pane: "w1:pW", Lane: t3276Worker, EventID: 7, DeliverPolicy: policy}}
	}
	if !relayGroupImmediate(row(relayDeliverDirective)) {
		t.Fatal("a directive row must be immediate")
	}
	for _, policy := range []string{"now", "idle", "max_wait=30"} {
		if relayGroupImmediate(row(policy)) {
			t.Fatalf("deliver=%s must not take the directive retry path", policy)
		}
	}
	if relayGroupImmediate(nil) {
		t.Fatal("an empty group is not immediate")
	}
}

// The escalation is kept until it is durable. The drop is consumed before the
// escalation is written, so a handoffkeep outage at that moment would otherwise
// lose it for good; it is retried and, the durable id being fixed, sent once.
func TestEscalationRetriesUntilDurable_t3276(t *testing.T) {
	prev := unconfirmedEscalationBackoff
	unconfirmedEscalationBackoff = []time.Duration{40 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond}
	t.Cleanup(func() { unconfirmedEscalationBackoff = prev })

	rig := t3276Start(t)
	directive := rig.send("ev-outage", "findings", "[mergewake] findings for the worker")
	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = t3276Design
	rig.fake.mu.Unlock()

	t650Send(t, rig.hub, "host-a", rig.dest, "relay.dropped", relayDroppedPayload{JobID: directive.JobID, Pane: directive.Pane, Lane: directive.Lane, OriginalEventID: directive.EventID, Reason: "maybe_in_pane"})
	time.Sleep(120 * time.Millisecond) // simulated outage: the writes must keep failing for a few retries
	if got := rig.escalations(); len(got) != 0 {
		t.Fatalf("escalation reached the design lane during the outage: %+v", got)
	}

	rig.fake.mu.Lock()
	rig.fake.failPostOwnerLane = ""
	rig.fake.mu.Unlock()
	var got []hubRelayInjectEvent
	eventually(t, 3*time.Second, "the escalation after the outage", func() bool {
		got = append(got, rig.escalations()...)
		return len(got) > 0
	})
	time.Sleep(200 * time.Millisecond) // a window that proves no second escalation follows
	got = append(got, rig.escalations()...)
	if len(got) != 1 || !strings.Contains(got[0].Text, fmt.Sprintf("event %d", directive.EventID)) {
		t.Fatalf("want exactly one escalation naming the event, got %+v", got)
	}
}

// A held directive wakes when its blocked pane turns working, but a row on the
// idle policy held beside it keeps waiting for the pane to finish.
func TestBlockedHeldDirectiveReleasedWhenPaneTurnsWorking_t3276(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-unblock", "findings", "[mergewake] findings for the worker")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "working"
	pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)

	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(directive))
	if got := pane.calls(t, "prompt"); got != 0 {
		t.Fatalf("blocked pane typed into %d times", got)
	}
	// The hold waits for the pane to turn working as well as idle or done.
	eventually(t, 2*time.Second, "the held directive's wait", func() bool {
		rig.waitMu.Lock()
		defer rig.waitMu.Unlock()
		return len(rig.waits) > 0 && strings.Contains(rig.waits[len(rig.waits)-1], "--until idle --until done --until working")
	})
	// The user answers the approval: the pane turns working.
	manager.releaseWorking(t.Context(), "w1:pW", false)
	if got := pane.calls(t, "prompt"); got < 1 {
		t.Fatalf("the directive was not released when the blocked pane turned working")
	}
}

// A directive keeps its own retry and escalation disposition, so a batch built
// from rows released together never mixes it with an ordinary held row.
func TestDirectiveNeverSharesBatchWithOrdinaryRow_t3276(t *testing.T) {
	now := time.Now()
	directive := func(id int64) relayHeld {
		return relayHeld{Pane: "w1:pW", Lane: t3276Worker, EventID: id, RecvSeq: id, Text: "directive", DeliverPolicy: relayDeliverDirective}
	}
	ordinary := func(id int64) relayHeld {
		return relayHeld{Pane: "w1:pW", Lane: t3276Worker, EventID: id, RecvSeq: id, Text: "report", DeliverPolicy: "idle"}
	}
	for _, tc := range []struct {
		name   string
		items  []relayHeld
		groups int
	}{
		{"two directives batch together", []relayHeld{directive(1), directive(2)}, 1},
		{"two ordinary rows batch together", []relayHeld{ordinary(1), ordinary(2)}, 1},
		{"directive then ordinary", []relayHeld{directive(1), ordinary(2)}, 2},
		{"ordinary then directive", []relayHeld{ordinary(1), directive(2)}, 2},
	} {
		if got := relayBatchGroups(tc.items, false, now); len(got) != tc.groups {
			t.Errorf("%s: %d groups, want %d", tc.name, len(got), tc.groups)
		}
	}
}

// The harness name is one normalized identity before any dispatch or proof
// check, so a padded or upper-case name from herdr cannot slip past a rule.
func TestRelayHarnessNameIsNormalized_t3276(t *testing.T) {
	for _, raw := range []string{` {"result":{"agent":{"harness":" Codex "}}}`, `{"result":{"agent":{"kind":"CODEX"}}}`, `{"result":{"agent":{"agent":"\tcodex\n"}}}`} {
		if got := parseRelayAgentHarness([]byte(raw)); got != "codex" {
			t.Errorf("%s -> %q, want codex", raw, got)
		}
	}
	if got := parseRelayAgentHarness([]byte(`{"result":{"agent":{"harness":"   ","kind":"Devin"}}}`)); got != "devin" {
		t.Errorf("a blank harness must fall through to kind, got %q", got)
	}
}

// Waking on working releases the directive only; a report held beside it on
// the idle policy keeps waiting for the pane to finish its turn.
func TestWorkingWakeLeavesIdleRowHeld_t3276(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-mixed-directive", "findings", "[mergewake] findings for the worker")
	report := rig.send("ev-mixed-report", "report", "[worker] progress")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "working"
	pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(report))
	manager.offer(t.Context(), t3276Message(directive))
	if got := pane.calls(t, "prompt"); got != 0 {
		t.Fatalf("a blocked pane was typed into %d times", got)
	}

	manager.releaseWorking(t.Context(), "w1:pW", false)

	if got := pane.calls(t, "prompt"); got != 1 {
		t.Fatalf("prompts=%d, want only the directive typed", got)
	}
	if typed := pane.typed(t); !strings.Contains(typed, "findings for the worker") || strings.Contains(typed, "progress") {
		t.Fatalf("typed %q: want the directive alone", typed)
	}
	rows, err := rig.node.relayStore().RelayHeldForPane(t.Context(), "w1:pW")
	if err != nil || len(rows) != 1 || rows[0].EventID != report.EventID {
		t.Fatalf("held rows=%+v err=%v, want the report still held", rows, err)
	}
}

// A row an offer holds while a working wake is releasing the directives must
// stay in the in-memory held set, or it is stranded until its deadline: the
// wake removes only the rows it delivers, not everything it saw in the store.
func TestWorkingWakeKeepsRowHeldMeanwhile_t3276(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-race-directive", "findings", "[mergewake] findings for the worker")
	report := rig.send("ev-race-report", "report", "[worker] progress")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "working"
	t3276InstallPane(t, sc, t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(report))
	manager.offer(t.Context(), t3276Message(directive))

	// An offer persisted and registered this row after the wake's snapshot.
	late := relayHeld{Pane: "w1:pW", Lane: t3276Worker, EventID: 9999, JobID: "late", Text: "late", DeliverPolicy: "idle", HeldSince: time.Now(), RecvSeq: 9999}
	if _, err := rig.node.relayStore().InsertRelayHeld(t.Context(), late); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	manager.held["w1:pW"] = append(manager.held["w1:pW"], late)
	manager.mu.Unlock()

	manager.releaseWorking(t.Context(), "w1:pW", false)

	manager.mu.Lock()
	defer manager.mu.Unlock()
	var ids []int64
	for _, item := range manager.held["w1:pW"] {
		ids = append(ids, item.EventID)
	}
	if !slices.Contains(ids, int64(9999)) || !slices.Contains(ids, report.EventID) || slices.Contains(ids, directive.EventID) {
		t.Fatalf("held set after the wake = %v, want the late row and the report kept and the delivered directive gone", ids)
	}
}

// hold registers a row under the same lock a working wake takes around its own
// snapshot. Holding that lock, a hold must not have inserted its row yet:
// otherwise a release could deliver the row between the offer's snapshot and
// its registration, and the offer would then put the delivered row back.
func TestHoldIsSerializedWithRelease_t3276(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-serial", "findings", "[mergewake] findings for the worker")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "blocked"
	t3276InstallPane(t, sc, t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()

	manager.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.hold(t.Context(), relayHeld{Pane: directive.Pane, Lane: directive.Lane, EventID: directive.EventID, JobID: directive.JobID, Text: directive.Text, HeldSince: time.Now(), DeliverPolicy: directive.Deliver, MaxWait: time.Minute, RecvSeq: 1}, "blocked")
	}()
	time.Sleep(150 * time.Millisecond) // a window that proves the offer does not progress while the lock is held
	rows, err := rig.node.relayStore().RelayHeldForPane(t.Context(), "w1:pW")
	manager.mu.Unlock()
	if err != nil || len(rows) != 0 {
		t.Fatalf("hold inserted its row (%+v, err=%v) while the manager lock was held", rows, err)
	}
	<-done
	if rows, err := rig.node.relayStore().RelayHeldForPane(t.Context(), "w1:pW"); err != nil || len(rows) != 1 {
		t.Fatalf("after the lock was released the held row is missing: %+v err=%v", rows, err)
	}
}

// After a wake the in-memory held set is made to match the store, so a row that
// was delivered and deleted cannot linger and send the next wait looking for a
// state that is already true (a loop that starves the idle rows).
func TestReconcileDropsDeliveredRowFromMemory_t3276(t *testing.T) {
	rig := t3276Start(t)
	report := rig.send("ev-recon-report", "report", "[worker] progress")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "blocked"
	t3276InstallPane(t, sc, t3276TypedFor(report), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(report))
	stale := relayHeld{Pane: "w1:pW", Lane: t3276Worker, EventID: 9998, JobID: "gone", Text: "gone", DeliverPolicy: relayDeliverDirective, HeldSince: time.Now()}
	manager.mu.Lock()
	manager.held["w1:pW"] = append(manager.held["w1:pW"], stale)
	manager.mu.Unlock()

	manager.reconcileHeld(t.Context(), "w1:pW")

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.held["w1:pW"]) != 1 || manager.held["w1:pW"][0].EventID != report.EventID {
		t.Fatalf("held set = %+v, want only the report that is still in the store", manager.held["w1:pW"])
	}
}

// An unreadable store during a working wake must not leave the held rows with
// no waiter: the wait is armed again shortly.
func TestWorkingWakeRearmsWhenStoreUnreadable_t3276(t *testing.T) {
	prev := relayWorkingRetryDelay
	relayWorkingRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { relayWorkingRetryDelay = prev })
	rig := t3276Start(t)
	directive := rig.send("ev-store-down", "findings", "[mergewake] findings for the worker")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "blocked"
	t3276InstallPane(t, sc, t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(directive))
	waits := func() int { rig.waitMu.Lock(); defer rig.waitMu.Unlock(); return len(rig.waits) }
	eventually(t, 2*time.Second, "the first wait", func() bool { return waits() == 1 })

	// The wake runs on the context the manager registered for the pane.
	waitCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	manager.mu.Lock()
	manager.waits["w1:pW"] = cancel
	manager.mu.Unlock()

	rig.node.relayStore().Close() // the store becomes unreadable
	manager.releaseWorking(waitCtx, "w1:pW", false)

	eventually(t, 2*time.Second, "the wait armed again after the store error", func() bool { return waits() >= 2 })
	time.Sleep(150 * time.Millisecond) // a window that proves the new wait is not exiting
	rig.waitMu.Lock()
	defer rig.waitMu.Unlock()
	if rig.waitExits != 0 {
		t.Fatalf("the re-armed wait exited at once: it was derived from the canceled wait context")
	}
}

// releaseWorking runs on the context of the wait that just ended, and arm
// cancels that wait as "the previous one". The next wait must not be derived
// from it, or it exits at once and the idle rows held beside the directive
// wait for nothing.
func TestWorkingWakeRearmsOnAStableContext_t3276(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-ctx-directive", "findings", "[mergewake] findings for the worker")
	report := rig.send("ev-ctx-report", "report", "[worker] progress")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "working"
	t3276InstallPane(t, sc, t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(report))
	manager.offer(t.Context(), t3276Message(directive))

	// Holding the directive re-armed the pane and ended the first wait; let that
	// settle so only exits caused by the wake are counted.
	eventually(t, 2*time.Second, "the first wait to end", func() bool {
		rig.waitMu.Lock()
		defer rig.waitMu.Unlock()
		return len(rig.waits) == 2 && rig.waitExits == 1
	})

	// The wake runs on the context the manager has registered for the pane.
	waitCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	manager.mu.Lock()
	manager.waits["w1:pW"] = cancel
	manager.mu.Unlock()
	rig.waitMu.Lock()
	before := len(rig.waits)
	rig.waitMu.Unlock()

	manager.releaseWorking(waitCtx, "w1:pW", false)

	eventually(t, 2*time.Second, "the next wait for the idle row", func() bool {
		rig.waitMu.Lock()
		defer rig.waitMu.Unlock()
		return len(rig.waits) > before
	})
	time.Sleep(150 * time.Millisecond) // a window that proves the new wait is not exiting
	rig.waitMu.Lock()
	defer rig.waitMu.Unlock()
	if rig.waitExits != 1 {
		t.Fatalf("%d wait(s) in all exited, want only the first: the wake's re-armed wait was derived from the canceled one", rig.waitExits)
	}
}

// A working wake that finds only directives releases exactly the snapshot it
// decided on. An idle row an offer holds after that decision is not part of it,
// and a working pane must not be typed into for it.
func TestWorkingWakeDeliversOnlyItsOwnSnapshot_t3276(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-gap-directive", "findings", "[mergewake] findings for the worker")
	report := rig.send("ev-gap-report", "report", "[worker] progress")
	sc := t3276Harnesses()["codex-working"]
	sc.preStatus, sc.postStatus = "blocked", "blocked"
	pane := t3276InstallPane(t, sc, t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(directive))

	relayWorkingReleaseGap = func() {
		manager.offer(t.Context(), t3276Message(report))
	}
	t.Cleanup(func() { relayWorkingReleaseGap = nil })

	manager.releaseWorking(t.Context(), "w1:pW", false)

	if got := pane.calls(t, "prompt"); got != 1 {
		t.Fatalf("prompts=%d, want only the directive typed", got)
	}
	if typed := pane.typed(t); strings.Contains(typed, "progress") {
		t.Fatalf("the idle row was typed into a working pane: %q", typed)
	}
	rows, err := rig.node.relayStore().RelayHeldForPane(t.Context(), "w1:pW")
	if err != nil || len(rows) != 1 || rows[0].EventID != report.EventID {
		t.Fatalf("held rows=%+v err=%v, want the report still held", rows, err)
	}
}
