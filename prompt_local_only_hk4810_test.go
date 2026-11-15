package panewire_test

import (
	"strings"
	"testing"

	"github.com/mgh3326/panewire"
)

// hk 4810: a target this host's herdr session does not serve — a worker in
// another herdr session on the same host (m1's 'fleet' session) or a job id
// whose pane is on another host — is refused as a usage error that names the
// command which does reach it. Nothing is typed, and the refusal is not a bare
// not-found.
func TestPromptRemoteTargetRefusedWithLaneEventHint_hk4810(t *testing.T) {
	for _, target := range []string{"fleet-worker", "hk-4810"} {
		t.Run(target, func(t *testing.T) {
			fixture := newHerdrFixture(t, promptFixtureSchema(false))
			defer fixture.Close()
			configurePromptFixture(fixture, "claude", promptEchoScreen)
			d, _ := startPromptDaemon(t, fixture)
			defer d.Stop()

			code, stderr := runCLIChild(t, dSocket(d), "prompt", "--from", "sender", "--to", target, "--file", promptFileForTarget(t, target))
			if code != panewire.ExitUsage {
				t.Fatalf("remote target %q exit=%d want %d; stderr=%s", target, code, panewire.ExitUsage, stderr)
			}
			for _, want := range []string{"panewire emit --kind lane.event --lane", "panewire inbox status --event-id"} {
				if !strings.Contains(stderr, want) {
					t.Fatalf("remote target %q refusal lacks %q:\n%s", target, want, stderr)
				}
			}
			if got := fixture.Requests("agent.prompt"); got != 0 {
				t.Fatalf("remote target %q typed %d prompts, want 0", target, got)
			}
		})
	}
}

// hk 4810: a queue banner already on screen before the send proves nothing
// about this call's text — the pane may be queueing someone else's message —
// so it is not arrival proof and the call must not exit 0. A banner this send
// created stays proof (TestPromptClaudeQueuedWithoutChipIsSubmissionEvidence).
func TestPromptQueuedBannerFromBeforeTheSendIsNotArrivalProof_hk4810(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	const queued = "assistant is working\nPress up to edit queued messages\n"
	fixture.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": "claude", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "working"}}}
	})
	fixture.On("agent.read", func() any {
		return map[string]any{"text": queued, "revision": 10}
	})
	fixture.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()

	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--timeout", "300ms"}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitDeliveryFailure {
		t.Fatalf("pre-existing queue banner exit=%d want %d", got, panewire.ExitDeliveryFailure)
	}
}

// hk 4810: a target that resolved before the preflight read and is gone at the
// re-resolve closed or was renamed during preflight — a race, not a target on
// another host or herdr session — so it keeps the plain condition_invalid
// answer and takes no remote hint.
func TestPromptTargetGoneAtReResolveKeepsPlainRefusal_hk4810(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	lists := 0
	fixture.On("agent.list", func() any {
		lists++
		if lists > 1 {
			return map[string]any{"agents": []any{}}
		}
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": "claude", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "idle"}}}
	})
	fixture.On("agent.read", func() any { return map[string]any{"text": "idle pane\n", "revision": 10} })
	fixture.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()

	code, stderr := runCLIChild(t, dSocket(d), "prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t))
	if code != panewire.ExitConditionInvalid {
		t.Fatalf("target gone at re-resolve exit=%d want %d; stderr=%s", code, panewire.ExitConditionInvalid, stderr)
	}
	if strings.Contains(stderr, "reaches only panes") {
		t.Fatalf("a preflight race carried the remote hint:\n%s", stderr)
	}
	if got := fixture.Requests("agent.prompt"); got != 0 {
		t.Fatalf("prompt requests=%d want 0", got)
	}
}

// hk 4810: a paste chip on the preflight screen must not hide an already
// present queue banner. classifySubmissionEvidence checks the chip first, so
// banner detection for the pre-send snapshot is read directly; the post-send
// screen (chip gone, same banner) is then still not this call's proof.
func TestPromptPresendBannerBehindPasteChipIsNotArrivalProof_hk4810(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	const banner = "Press up to edit queued messages\n"
	reads := 0
	fixture.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": "claude", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "working"}}}
	})
	fixture.On("agent.read", func() any {
		reads++
		if reads == 1 {
			return map[string]any{"text": "assistant is working\n[Pasted text #1 +2 lines]\n" + banner, "revision": 10}
		}
		return map[string]any{"text": "assistant is working\n" + banner, "revision": 10}
	})
	fixture.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()

	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--timeout", "300ms"}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitDeliveryFailure {
		t.Fatalf("preflight chip over an existing queue banner exit=%d want %d", got, panewire.ExitDeliveryFailure)
	}
}
