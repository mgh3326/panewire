package panewire_test

import (
	"testing"

	"github.com/mgh3326/panewire"
)

// hk 3276: a directive to a job's worker is never refused because the worker is
// working. Without --directive the same target keeps today's rule (exit 6, no
// prompt sent; TestPromptStatusTransitionNeedsIdleToWorking). With it, the
// prompt is sent and judged on its submission evidence, since no idle-to-working
// transition can be seen on a pane that is already working.
func TestPromptDirectiveReachesWorkingTarget_t3276(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(true))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", promptEchoScreen)
	// The pane is working before and after the send.
	fixture.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": "claude", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "working"}}}
	})
	d, _ := startPromptDaemonWithSchema(t, fixture, promptFixtureSchema(true))
	defer d.Stop()

	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--uptake", "status-transition", "--directive"}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("directive to a working target exit=%d, want %d", got, panewire.ExitOK)
	}
	if got := fixture.Requests("agent.prompt"); got != 1 {
		t.Fatalf("directive prompt requests=%d, want 1", got)
	}
}
