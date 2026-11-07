package panewire

// hk 3370: a cmd pane cannot prove a directive typed into it while it works, and
// the retry-once rule would type that directive a second time. The directive
// policy therefore holds a working cmd pane and types when it turns idle or
// done, where the composer proof applies. Other harnesses keep typing at once.
// Everything runs on fakes: a real hub, a real node relay and a shell herdr.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func t3370SetStatus(t *testing.T, pane *t3330CmdPane, status string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(pane.dir, "status.pre"), []byte(status), 0o600); err != nil {
		t.Fatal(err)
	}
}

// A working cmd pane gets no keystroke. The hold waits for idle or done only,
// since a wait that also ended on working would end at once. When the pane
// turns idle the directive is typed once, return is pressed once, and the
// composer proof closes the row.
func TestWorkingCmdDirectiveHeldUntilIdle_t3370(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-cmd-hold", "findings", "[mergewake] findings for the worker: fix the failing check")
	pane := t3330InstallPaneFor(t, "working", "working", t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()

	manager.offer(t.Context(), t3276Message(directive))

	for _, pair := range []string{"pane send-text", "pane send-keys", "agent prompt", "agent send-keys"} {
		if got := pane.calls(t, pair); got != 0 {
			t.Fatalf("a working cmd pane got %d %q calls while the directive was held", got, pair)
		}
	}
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.held") != 1 || t3276Count(kinds, "relay.delivered") != 0 {
		t.Fatalf("events=%v, want the directive held and not delivered", kinds)
	}
	eventually(t, 2*time.Second, "the held directive's wait", func() bool {
		rig.waitMu.Lock()
		defer rig.waitMu.Unlock()
		return len(rig.waits) > 0
	})
	rig.waitMu.Lock()
	wait := rig.waits[len(rig.waits)-1]
	rig.waitMu.Unlock()
	if !strings.Contains(wait, "--until idle --until done") || strings.Contains(wait, "--until working") {
		t.Fatalf("wait=%q, want idle or done only: a working wake would end the hold at once", wait)
	}

	t3370SetStatus(t, pane, "idle")
	manager.release(t.Context(), "w1:pW", false)

	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times after the pane turned idle, want exactly 1", got)
	}
	if got := pane.calls(t, "pane send-keys"); got != 1 {
		t.Fatalf("pane send-keys return called %d times, want exactly 1", got)
	}
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.delivered") != 1 || t3276Count(kinds, "relay.unconfirmed") != 0 {
		t.Fatalf("events=%v, want one proven relay.delivered and no relay.unconfirmed", kinds)
	}
	if reason := payloads["relay.delivered"].Reason; !strings.Contains(reason, "nonce_echo+status_working") {
		t.Fatalf("relay.delivered reason=%q, want the echo-and-working proof", reason)
	}
	if row := rig.row(directive.EventID); row.DeliveredAt == "" {
		t.Fatalf("a proven delivery left the durable row undelivered: %+v", row)
	}
	if got := rig.escalations(); len(got) != 0 {
		t.Fatalf("a delivered directive escalated: %+v", got)
	}
}

// A cmd pane that is still working when max_wait runs out is not typed into:
// the delivery is reported unconfirmed and dropped, the design lane hears once,
// and the durable row stays undelivered.
func TestWorkingCmdDirectiveNotTypedPastMaxWait_t3370(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-cmd-expiry", "findings", "[mergewake] findings for the worker")
	pane := t3330InstallPaneFor(t, "working", "working", t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(directive))

	manager.release(t.Context(), "w1:pW", true)

	for _, pair := range []string{"pane send-text", "pane send-keys"} {
		if got := pane.calls(t, pair); got != 0 {
			t.Fatalf("an expired hold on a working cmd pane typed: %d %q calls", got, pair)
		}
	}
	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.unconfirmed") != 1 || t3276Count(kinds, "relay.dropped") != 1 || t3276Count(kinds, "relay.delivered") != 0 {
		t.Fatalf("events=%v, want one relay.unconfirmed then one relay.dropped", kinds)
	}
	var escalations []hubRelayInjectEvent
	eventually(t, 3*time.Second, "the escalation to the design lane", func() bool {
		escalations = append(escalations, rig.escalations()...)
		return len(escalations) > 0
	})
	if len(escalations) != 1 || !strings.Contains(escalations[0].Text, relayDroppedWorkingExpired) {
		t.Fatalf("design lane escalations=%+v, want exactly one naming %s", escalations, relayDroppedWorkingExpired)
	}
	if row := rig.row(directive.EventID); row.DeliveredAt != "" {
		t.Fatalf("an undelivered directive was marked delivered: %+v", row)
	}
	_ = payloads
}

// An expired hold whose cmd pane has since gone idle is typed as today: the
// pane is no longer working, so the composer proof applies.
func TestWorkingCmdDirectiveExpiredOnIdlePaneIsTyped_t3370(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-cmd-expiry-idle", "findings", "[mergewake] findings for the worker")
	pane := t3330InstallPaneFor(t, "working", "working", t3276TypedFor(directive), true)
	manager := rig.node.relayBusyManager()
	manager.offer(t.Context(), t3276Message(directive))
	t3370SetStatus(t, pane, "idle")

	manager.release(t.Context(), "w1:pW", true)

	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times for an expired hold on an idle pane, want 1", got)
	}
}

// A working claude pane is unaffected: the directive is typed at once, with no
// hold and no wait.
func TestWorkingClaudeDirectiveStillTypedAtOnce_t3370(t *testing.T) {
	rig := t3276Start(t)
	directive := rig.send("ev-claude-working", "findings", "[mergewake] findings for the worker")
	pane := t3276InstallPane(t, t3276Harnesses()["claude-working"], t3276TypedFor(directive), true)

	rig.node.relayBusyManager().offer(t.Context(), t3276Message(directive))

	if got := pane.calls(t, "prompt"); got != 1 {
		t.Fatalf("a working claude pane got %d prompts, want exactly 1 at once", got)
	}
	if kinds, _ := rig.drainEvents(); t3276Count(kinds, "relay.held") != 0 || t3276Count(kinds, "relay.delivered") != 1 {
		t.Fatalf("events=%v, want delivered at once and never held", kinds)
	}
}

// Only cmd cannot prove a directive typed into a working pane.
func TestOnlyCmdIsWorkingUnprovable_t3370(t *testing.T) {
	for _, harness := range []string{"claude", "codex", "devin", "grok", "omp", "", "some-future-harness"} {
		if relayHarnessWorkingUnprovable(harness) {
			t.Errorf("%q holds a directive while working, want it typed at once", harness)
		}
	}
	for _, harness := range []string{"cmd", " CMD "} {
		if !relayHarnessWorkingUnprovable(harness) {
			t.Errorf("%q does not hold a directive while working", harness)
		}
	}
}
