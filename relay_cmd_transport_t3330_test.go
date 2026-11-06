package panewire

// hk 3330: a cmd pane takes lane events through the pane-level transport. herdr
// answers `agent get` and `agent read` for a cmd pane but refuses `agent prompt`
// and `agent send-keys` with agent_not_ready, so the relay types with
// `pane send-text` and submits with `pane send-keys return` — the pair the spawn
// path already uses. The proof rule stays relayProofEchoAndWorking, so a working
// cmd pane is typed into but reported unconfirmed, exactly as for grok and omp.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// t3330CmdPane is a shell herdr that models one cmd pane: reads answer, agent
// writes are refused the way herdr refuses them, and only the pane-level pair
// types and submits.
type t3330CmdPane struct {
	dir string
}

// t3330InstallPaneFor models one cmd pane. preStatus is its status before the
// send and postStatus after a submitted prompt; typed is the text it echoes.
func t3330InstallPaneFor(t *testing.T, preStatus, postStatus, typed string, submitted bool) *t3330CmdPane {
	t.Helper()
	dir := t.TempDir()
	idle := strings.Join([]string{t3330CmdDivider, "❯", t3330CmdDivider}, "\n")
	echo := strings.Join([]string{"❯ " + typed, "done", t3330CmdDivider, "❯", t3330CmdDivider}, "\n")
	post := idle
	if submitted {
		post = echo
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, source := range []string{"visible", "recent-unwrapped"} {
		write("pre."+source+".last", idle)
		write("post."+source+".last", post)
	}
	write("status.pre", preStatus)
	write("status.post", postStatus)
	// $1 $2 is the subcommand pair, so a test can tell an agent write from a
	// pane write without parsing the whole argv. The text of `pane send-text`
	// lands in typed, and the return is what flips the pane to its post state.
	script := `#!/bin/sh
d='` + dir + `'
echo "$1 $2" >> "$d/calls"
case "$1 $2" in
"agent get")
  if [ -f "$d/prompted" ]; then st=$(cat "$d/status.post"); else st=$(cat "$d/status.pre"); fi
  printf '{"result":{"agent":{"agent":"cmd","agent_status":"%s","name":"` + t3276Worker + `","pane_id":"w1:pW"}}}\n' "$st" ;;
"agent read"|"pane read") src=visible; lines=60; prev=
  for a in "$@"; do [ "$prev" = "--source" ] && src=$a; [ "$prev" = "--lines" ] && lines=$a; prev=$a; done
  phase=pre; [ -f "$d/prompted" ] && phase=post
  tail -n "$lines" "$d/$phase.$src.last" ;;
"agent prompt"|"agent send-keys")
  echo '{"error":{"code":"agent_not_ready","message":"not an active named agent"}}' >&2
  exit 1 ;;
"pane send-text") printf '%s' "$4" > "$d/typed" ;;
"pane send-keys") [ "$4" = "return" ] && touch "$d/prompted" ;;
esac
`
	installFakeHerdr(t, dir, script)
	return &t3330CmdPane{dir: dir}
}

const t3330CmdDivider = "───────────────────────────────────────────────────────────────"

// calls counts logged subcommand pairs. word is matched against the pair, so
// "pane send-text" and "agent prompt" are told apart.
func (pane *t3330CmdPane) calls(t *testing.T, pair string) int {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(pane.dir, "calls"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if line == pair {
			n++
		}
	}
	return n
}

func (pane *t3330CmdPane) typedText(t *testing.T) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(pane.dir, "typed"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	return string(body)
}

// Acceptance 1 (delivered half): the fake herdr refuses the agent-level writes
// for a cmd pane, the idle pane still gets the event through pane send-text plus
// return, and relay.delivered is recorded with the echo-and-working proof.
func TestCmdPaneDeliveredThroughPaneTransport_t3330(t *testing.T) {
	for _, notify := range []string{"findings", "decision"} {
		t.Run(notify, func(t *testing.T) {
			rig := t3276Start(t)
			directive := rig.send("ev-cmd-"+notify, notify, "[mergewake] "+notify+" for the worker: fix the failing check")
			pane := t3330InstallPaneFor(t, "idle", "working", t3276TypedFor(directive), true)

			rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

			if got := pane.calls(t, "agent prompt"); got != 0 {
				t.Fatalf("agent prompt called %d times on a cmd pane: herdr refuses it", got)
			}
			if got := pane.calls(t, "agent send-keys"); got != 0 {
				t.Fatalf("agent send-keys called %d times on a cmd pane", got)
			}
			if got := pane.calls(t, "pane send-text"); got != 1 {
				t.Fatalf("pane send-text called %d times, want exactly 1", got)
			}
			if got := pane.calls(t, "pane send-keys"); got != 1 {
				t.Fatalf("pane send-keys return called %d times, want exactly 1", got)
			}
			if typed := pane.typedText(t); !strings.Contains(typed, "fix the failing check") || !strings.Contains(typed, relayNonce(relayHeld{EventID: directive.EventID, Text: directive.Text})) {
				t.Fatalf("typed text lacks the event or its nonce: %q", typed)
			}
			kinds, payloads := rig.drainEvents()
			if t3276Count(kinds, "relay.delivered") != 1 || t3276Count(kinds, "relay.unconfirmed") != 0 {
				t.Fatalf("events=%v, want one relay.delivered and no relay.unconfirmed", kinds)
			}
			if reason := payloads["relay.delivered"].Reason; !strings.Contains(reason, "nonce_echo+status_working") {
				t.Fatalf("relay.delivered reason=%q, want the echo-and-working proof", reason)
			}
			if row := rig.row(directive.EventID); row.DeliveredAt == "" {
				t.Fatalf("a proven delivery left the durable row undelivered: %+v", row)
			}
			if got := rig.escalations(); len(got) != 0 {
				t.Fatalf("a delivered cmd event escalated: %+v", got)
			}
		})
	}
}

// Acceptance 1 (unconfirmed half): a working cmd pane is typed into through the
// pane transport, but with no idle-to-working transition nothing proves the
// submit. It is typed once more at once and the design lane hears exactly once.
func TestWorkingCmdPaneInjectedThenEscalated_t3330(t *testing.T) {
	prevWindow := relayObserveWindow
	relayObserveWindow = 300 * time.Millisecond
	t.Cleanup(func() { relayObserveWindow = prevWindow })

	rig := t3276Start(t)
	directive := rig.send("ev-cmd-working", "findings", "[mergewake] findings for the worker")
	pane := t3330InstallPaneFor(t, "working", "working", t3276TypedFor(directive), true)

	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	if got := pane.calls(t, "agent prompt"); got != 0 {
		t.Fatalf("a working cmd pane got %d agent prompts, want the pane transport only", got)
	}
	if got := pane.calls(t, "pane send-text"); got != 2 {
		t.Fatalf("pane send-text called %d times, want the directive and one retry", got)
	}
	kinds, _ := rig.drainEvents()
	if t3276Count(kinds, "relay.held") != 0 || t3276Count(kinds, "relay.delivered") != 0 {
		t.Fatalf("events=%v: want injected at once and not delivered without proof", kinds)
	}
	if t3276Count(kinds, "relay.unconfirmed") != 1 || t3276Count(kinds, "relay.dropped") != 1 {
		t.Fatalf("events=%v, want one relay.unconfirmed then one relay.dropped", kinds)
	}
	if got := rig.escalations(); len(got) != 1 {
		t.Fatalf("design lane got %d escalations, want exactly 1", len(got))
	}
}

// Acceptance 1 (last bullet): the other harnesses keep the agent-level
// transport. Only cmd is pane-level, and both the text and the return it sends
// go through agent prompt / agent send-keys.
func TestOnlyCmdUsesPaneTransport_t3330(t *testing.T) {
	for _, harness := range []string{"claude", "codex", "devin", "grok", "omp"} {
		if harnessPaneTransport(harness) {
			t.Errorf("%s is pane-level, want the agent transport it had before", harness)
		}
	}
	if !harnessPaneTransport("cmd") {
		t.Fatal("cmd is not pane-level: herdr refuses its agent writes")
	}
	if adapter, ok := relayAdapterFor("cmd"); !ok || adapter.proof != relayProofEchoAndWorking {
		t.Fatalf("cmd adapter=%+v ok=%v, want the echo-and-working proof", adapter, ok)
	}
	if _, ok := relayAdapterFor("cmd"); !ok {
		t.Fatal("cmd has no relay adapter: no proof rule would ever close its row")
	}

	dir := t.TempDir()
	log := filepath.Join(dir, "calls")
	script := "#!/bin/sh\necho \"$1 $2\" >> \"" + log + "\"\nexit 0\n"
	installFakeHerdr(t, dir, script)
	for _, harness := range []string{"claude", "codex", "devin", "grok", "omp"} {
		if err := relaySendText(t.Context(), "w1:p1", harness, "text"); err != nil {
			t.Fatal(err)
		}
		if err := relaySendReturn(t.Context(), "w1:p1", harness); err != nil {
			t.Fatal(err)
		}
	}
	if err := relaySendText(t.Context(), "w1:p1", "cmd", "text"); err != nil {
		t.Fatal(err)
	}
	if err := relaySendReturn(t.Context(), "w1:p1", "cmd"); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Fields(string(body))
	if got := task683Count(lines, "prompt"); got != 5 {
		t.Fatalf("agent prompt calls=%d, want 5 (one per non-cmd harness): %q", got, body)
	}
	if got := task683Count(lines, "send-text"); got != 1 {
		t.Fatalf("pane send-text calls=%d, want 1 (cmd only): %q", got, body)
	}
	if got := task683Count(lines, "send-keys"); got != 6 {
		t.Fatalf("send-keys calls=%d, want 6 (five agent, one pane): %q", got, body)
	}
}
