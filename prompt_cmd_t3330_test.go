package panewire_test

// hk 3330: `panewire prompt --to <cmd pane>` uses the same pane-level transport
// as the relay. herdr refuses agent.prompt for a cmd pane, so the text goes
// through pane.send_text and the one submit through pane.send_keys. cmd draws
// claude's composer, so the prompt's own submission proof works unchanged.

import (
	"testing"

	"github.com/mgh3326/panewire"
)

const t3330CmdComposer = "───────────────────────────────────────────────────────────────"

// t3330ConfigureCmdFixture is configurePromptFixture with a cmd pane: reads
// answer, the pane transport carries the text and its return, and the echoed
// screen appears once the return was pressed.
func t3330ConfigureCmdFixture(f *herdrFixture, screen string) {
	f.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "cmd", "name": "orch", "label": "orch", "harness": "cmd", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "idle"}}}
	})
	f.On("agent.read", func() any {
		if f.Requests("pane.send_text") == 0 {
			return map[string]any{"text": "idle pane\n", "revision": 10}
		}
		return map[string]any{"text": screen, "revision": 11}
	})
	// The agent-level send is refused the way herdr refuses it for a cmd pane;
	// a test that reaches it must fail rather than pass on a permissive stub.
	f.On("agent.prompt", func() any { return map[string]any{"error": "agent_not_ready"} })
	f.On("pane.send_text", func() any { return map[string]any{"accepted": true} })
	f.On("pane.send_keys", func() any { return map[string]any{"type": "ok"} })
}

func t3330CmdEcho(marker string) string {
	return "❯ " + marker + "\ndone\n" + t3330CmdComposer + "\n❯\n" + t3330CmdComposer + "\n  » permission bypass on [shift+tab]\n"
}

func TestPromptToCmdPaneUsesPaneTransport_t3330(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	t3330ConfigureCmdFixture(fixture, t3330CmdEcho("R2-MARKER"))
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()

	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("prompt to a cmd pane exit=%d, want %d", got, panewire.ExitOK)
	}
	if got := fixture.Requests("agent.prompt"); got != 0 {
		t.Fatalf("agent.prompt requests=%d, want 0: herdr refuses it for a cmd pane", got)
	}
	if got := fixture.Requests("pane.send_text"); got != 1 {
		t.Fatalf("pane.send_text requests=%d, want 1", got)
	}
	if got := fixture.Requests("agent.send_keys"); got != 0 {
		t.Fatalf("agent.send_keys requests=%d, want 0: the submit is pane-level too", got)
	}
}

// A cmd prompt that leaves its text in the composer gets the one return through
// pane.send_keys, and the return is what submits it.
func TestPromptToCmdPaneSubmitsThroughPaneReturn_t3330(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	residue := t3330CmdComposer + "\n❯ R2-MARKER\n" + t3330CmdComposer + "\n  » permission bypass on [shift+tab]\n"
	t3330ConfigureCmdFixture(fixture, residue)
	// The read after the return shows the submitted echo.
	fixture.On("agent.read", func() any {
		if fixture.Requests("pane.send_keys") == 0 {
			if fixture.Requests("pane.send_text") == 0 {
				return map[string]any{"text": "idle pane\n", "revision": 10}
			}
			return map[string]any{"text": residue, "revision": 11}
		}
		return map[string]any{"text": t3330CmdEcho("R2-MARKER"), "revision": 12}
	})
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()

	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--timeout", "2s"}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("cmd prompt with composer residue exit=%d, want %d", got, panewire.ExitOK)
	}
	if got := fixture.Requests("pane.send_keys"); got != 1 {
		t.Fatalf("pane.send_keys requests=%d, want exactly one return", got)
	}
	if got := fixture.Requests("agent.send_keys"); got != 0 {
		t.Fatalf("agent.send_keys requests=%d, want 0", got)
	}
}

// A real cmd screen (captured from a cmd pane, 2026-11-07): the transcript
// above, the ❯ composer between two dividers, then the footer. The promoted
// work must not be mistaken for a submitted echo and the echo must be found
// above the composer.
func t3330RealCmdEcho(marker string) string {
	return "⠶ I'll read the brief file first.\n" +
		" READ  [~/work/example/brief-delivered.md] 18 lines\n" +
		"✻ Thought for 1 second [ctrl+o to expand]\n" +
		"❯ " + marker + "\n" +
		"⠶ Working on it.\n" +
		"〰 Abracadabraing…  esc to interrupt • 65m 20s • ⇓ 440.2k\n" +
		t3330CmdComposer + "\n" +
		"❯ Ask your question...\n" +
		t3330CmdComposer + "\n" +
		"  » permission bypass on [shift+tab]\n" +
		"  ? for shortcuts · taste on\n"
}

// The recorded pane shape, not a hand-drawn one: the prompt proves submission
// from the echo above the located composer, without any pane-side keypress.
func TestPromptToCmdPaneReadsRealComposerShape_t3330(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	t3330ConfigureCmdFixture(fixture, t3330RealCmdEcho("R2-MARKER"))
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()

	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("cmd prompt on the captured screen exit=%d, want %d", got, panewire.ExitOK)
	}
	if got := fixture.Requests("pane.send_keys"); got != 0 {
		t.Fatalf("pane.send_keys requests=%d: a proven echo must not earn a keypress", got)
	}
}

// The other harnesses keep agent.prompt: the same fixture with a claude harness
// never touches the pane transport.
func TestPromptToClaudePaneKeepsAgentTransport_t3330(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", promptEchoScreen)
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()

	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("claude prompt exit=%d, want %d", got, panewire.ExitOK)
	}
	if got := fixture.Requests("agent.prompt"); got != 1 {
		t.Fatalf("agent.prompt requests=%d, want 1", got)
	}
	if got := fixture.Requests("pane.send_text"); got != 0 {
		t.Fatalf("pane.send_text requests=%d, want 0 for claude", got)
	}
}
