package panewire_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	panewire "github.com/mgh3326/panewire"
)

func TestPromptClaudePositiveAndNegativeMatchers(t *testing.T) {
	tests := []struct {
		name, screen string
		want         int
	}{
		{"positive marker", "assistant saw R2-MARKER\n", panewire.ExitOK},
		{"negative composer chip", "❯ R2-MARKER\n[Pasted text #1 +2 lines]\n", panewire.ExitDeliveryFailure},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newHerdrFixture(t, promptFixtureSchema(false))
			defer fixture.Close()
			configurePromptFixture(fixture, "claude", tc.screen)
			d, _ := startPromptDaemon(t, fixture)
			defer d.Stop()
			path := promptFile(t)
			if got := panewire.RunCLI([]string{"prompt", "--from", "sender (role, p0)", "--to", "orch", "--file", path}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != tc.want {
				t.Fatalf("exit=%d want %d", got, tc.want)
			}
			if fixture.Requests("agent.prompt") != 1 {
				t.Fatalf("prompt requests=%d want 1", fixture.Requests("agent.prompt"))
			}
		})
	}
}

func TestPromptCodexPositiveAndNegativeMatchers(t *testing.T) {
	tests := []struct {
		name, screen string
		want         int
	}{
		{"positive receipt", "• Ran R2-MARKER\n", panewire.ExitOK},
		{"negative unrelated receipt", "• Ran unrelated command\n", panewire.ExitDeliveryFailure},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newHerdrFixture(t, promptFixtureSchema(false))
			defer fixture.Close()
			configurePromptFixture(fixture, "codex", tc.screen)
			d, _ := startPromptDaemon(t, fixture)
			defer d.Stop()
			if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != tc.want {
				t.Fatalf("exit=%d want %d", got, tc.want)
			}
		})
	}
}

func TestPromptClaudeQueuedWithoutChipIsSubmissionEvidence(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", "Press up to edit queued messages\n")
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("exit=%d want %d", got, panewire.ExitOK)
	}
}

func TestPromptKimiAndAgyHaveNoPositiveMatcher(t *testing.T) {
	for _, harness := range []string{"kimi", "agy"} {
		t.Run(harness, func(t *testing.T) {
			fixture := newHerdrFixture(t, promptFixtureSchema(false))
			defer fixture.Close()
			configurePromptFixture(fixture, harness, "assistant saw R2-MARKER\nPress up to edit queued messages\n")
			d, _ := startPromptDaemon(t, fixture)
			defer d.Stop()
			if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitDeliveryFailure {
				t.Fatalf("exit=%d want %d", got, panewire.ExitDeliveryFailure)
			}
		})
	}
}

func TestPromptToolUptakeRequiresHarnessReceiptAndRevision(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "codex", "• Ran R2-MARKER\n")
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--uptake", "tool"}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("positive exit=%d", got)
	}

	fixture2 := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture2.Close()
	configurePromptFixture(fixture2, "codex", "• Ran unrelated command\n")
	d2, _ := startPromptDaemon(t, fixture2)
	defer d2.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--uptake", "tool"}, panewire.CLIConfig{SocketPath: dSocket(d2)}); got != panewire.ExitDeliveryFailure {
		t.Fatalf("negative exit=%d", got)
	}
}

func TestPromptStatusTransitionNeedsIdleToWorking(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(true))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", "assistant saw R2-MARKER\n")
	fixture.On("agent.prompt", func() any {
		go func() {
			// The event is emitted only after the prompt request is observed.
			fixture.Event(map[string]any{"event": map[string]any{"type": "pane_agent_status_changed", "pane_id": "p1", "agent_status": "working", "revision": 12}})
		}()
		return map[string]any{"accepted": true}
	})
	d, _ := startPromptDaemonWithSchema(t, fixture, promptFixtureSchema(true))
	defer d.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--uptake", "status-transition"}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("transition exit=%d", got)
	}

	fixture2 := newHerdrFixture(t, promptFixtureSchema(true))
	defer fixture2.Close()
	fixture2.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": "claude", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "working"}}}
	})
	fixture2.On("agent.read", func() any { return map[string]any{"text": "assistant saw R2-MARKER\n", "revision": 10} })
	fixture2.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
	d2, _ := startPromptDaemonWithSchema(t, fixture2, promptFixtureSchema(true))
	defer d2.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--uptake", "status-transition"}, panewire.CLIConfig{SocketPath: dSocket(d2)}); got != panewire.ExitDeliveryFailure {
		t.Fatalf("already working exit=%d", got)
	}
	if got := fixture2.Requests("agent.prompt"); got != 0 {
		t.Fatalf("already working prompt requests=%d", got)
	}
}

func TestPromptCompleteSwallowIsUnproven(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", "")
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitDeliveryFailure {
		t.Fatalf("complete swallow exit=%d want %d", got, panewire.ExitDeliveryFailure)
	}
}

func TestPromptExpectMismatchDoesNotCallHerdrPrompt(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", "assistant saw R2-MARKER\n")
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()
	path := filepath.Join(t.TempDir(), "prompt.md")
	if err := os.WriteFile(path, []byte("expect: name=wrong cwd=/work\n\nR2-MARKER\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", path}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitConditionInvalid {
		t.Fatalf("exit=%d want %d", got, panewire.ExitConditionInvalid)
	}
	if got := fixture.Requests("agent.prompt"); got != 0 {
		t.Fatalf("prompt requests=%d want 0", got)
	}
}

func TestPromptMissingTargetDoesNotCallHerdrPrompt(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", "assistant saw R2-MARKER\n")
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "missing", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitConditionInvalid {
		t.Fatalf("exit=%d want %d", got, panewire.ExitConditionInvalid)
	}
	if got := fixture.Requests("agent.prompt"); got != 0 {
		t.Fatalf("prompt requests=%d", got)
	}
}

func TestPromptRetryDoesNotInjectTwice(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", "assistant saw R2-MARKER\n")
	d, _ := startPromptDaemon(t, fixture)
	defer d.Stop()
	args := []string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}
	for i := 0; i < 2; i++ {
		if got := panewire.RunCLI(args, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
			t.Fatalf("attempt %d exit=%d", i+1, got)
		}
	}
	if got := fixture.Requests("agent.prompt"); got != 1 {
		t.Fatalf("prompt requests=%d want 1", got)
	}
}

func TestPromptLabelFallbackAndRevisionDriftAreRecorded(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	lists := 0
	fixture.On("agent.list", func() any {
		lists++
		return map[string]any{"agents": []any{map[string]any{"label": "tab-one", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "harness": "claude", "revision": 10 + lists - 1, "agent_status": "idle"}}}
	})
	reads := 0
	fixture.On("agent.read", func() any {
		reads++
		if reads == 1 {
			return map[string]any{"text": "idle pane\n", "revision": 10}
		}
		return map[string]any{"text": "assistant saw R2-MARKER\n", "revision": 11}
	})
	d, db := startPromptDaemon(t, fixture)
	defer d.Stop()
	path := filepath.Join(t.TempDir(), "label-prompt.md")
	if err := os.WriteFile(path, []byte("expect: label=tab-one cwd=/work\n\nR2-MARKER\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "tab-one", "--file", path}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("exit=%d", got)
	}
	delivery, ok, err := db.LatestDelivery(t.Context())
	if err != nil || !ok {
		t.Fatalf("delivery=%+v ok=%v err=%v", delivery, ok, err)
	}
	if delivery.ResolvedPaneID != "p1" || delivery.PreflightRevision != 10 || delivery.SendRevision != 11 || delivery.PreflightResult != "passed" {
		t.Fatalf("delivery=%+v", delivery)
	}
}

func TestPromptIdentityChangeAndAmbiguousLabelAreFailClosed(t *testing.T) {
	tests := []struct {
		name string
		list func(int) any
	}{
		{"pane id changed", func(n int) any {
			pane := map[string]any{"agent": "orch", "name": "orch", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "harness": "claude", "revision": 10, "agent_status": "idle"}
			if n > 1 {
				pane["pane_id"] = "p2"
			}
			return map[string]any{"agents": []any{pane}}
		}},
		{"ambiguous label", func(n int) any {
			return map[string]any{"agents": []any{
				map[string]any{"label": "same", "pane_id": "p1", "cwd": "/work", "harness": "claude", "agent_status": "idle"},
				map[string]any{"label": "same", "pane_id": "p2", "cwd": "/work", "harness": "claude", "agent_status": "idle"},
			}}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newHerdrFixture(t, promptFixtureSchema(false))
			defer fixture.Close()
			calls := 0
			fixture.On("agent.list", func() any { calls++; return tc.list(calls) })
			fixture.On("agent.read", func() any { return map[string]any{"text": "idle pane\n", "revision": 10} })
			d, _ := startPromptDaemon(t, fixture)
			defer d.Stop()
			target := "orch"
			if tc.name == "ambiguous label" {
				target = "same"
			}
			if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", target, "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitConditionInvalid {
				t.Fatalf("exit=%d", got)
			}
			if got := fixture.Requests("agent.prompt"); got != 0 {
				t.Fatalf("prompt requests=%d", got)
			}
		})
	}
}

func TestPromptPrivacyBodyOptInOnly(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	configurePromptFixture(fixture, "claude", "assistant saw R2-MARKER\n")
	d, db := startPromptDaemon(t, fixture)
	defer d.Stop()
	path := promptFile(t)
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", path}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("default exit=%d", got)
	}
	delivery, ok, err := db.LatestDelivery(t.Context())
	if err != nil || !ok {
		t.Fatal(err)
	}
	if delivery.BodyStored {
		t.Fatal("body stored by default")
	}
	if body, found, err := db.PromptBody(t.Context(), delivery.DeliveryID); err != nil || found || body != "" {
		t.Fatalf("default body=%q found=%v err=%v", body, found, err)
	}

	fixture2 := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture2.Close()
	configurePromptFixture(fixture2, "claude", "assistant saw R2-MARKER\n")
	d2, db2 := startPromptDaemon(t, fixture2)
	defer d2.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t), "--store-prompt-body"}, panewire.CLIConfig{SocketPath: dSocket(d2)}); got != panewire.ExitOK {
		t.Fatalf("opt-in exit=%d", got)
	}
	delivery2, ok, err := db2.LatestDelivery(t.Context())
	if err != nil || !ok {
		t.Fatal(err)
	}
	if body, found, err := db2.PromptBody(t.Context(), delivery2.DeliveryID); err != nil || !found || body == "" {
		t.Fatalf("opt-in body=%q found=%v err=%v", body, found, err)
	}
}

func TestPromptDaemonUnavailableStillAuditsRequest(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	d, db := startPromptDaemon(t, fixture)
	fixture.Close()
	defer d.Stop()
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", promptFile(t)}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitDaemonUnavailable {
		t.Fatalf("exit=%d want %d", got, panewire.ExitDaemonUnavailable)
	}
	if got := db.CountDeliveries(); got != 1 {
		t.Fatalf("deliveries=%d want 1", got)
	}
}

func promptFixtureSchema(events bool) string {
	methods := `[` +
		`"agent.read","agent.prompt","agent.list"`
	if events {
		methods += `,"events.subscribe"`
	}
	suffix := ``
	if events {
		suffix = `,"subscriptions":["pane.agent_status_changed"]`
	}
	return `{"protocol":20,"schema_version":1,"schemas":{"request":{"methods":` + methods + `],"read_source":["visible","recent","recent_unwrapped"]` + suffix + `}}}`
}

func configurePromptFixture(f *herdrFixture, harness, screen string) {
	f.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": harness, "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "idle"}}}
	})
	reads := 0
	f.On("agent.read", func() any {
		reads++
		if reads <= 1 {
			return map[string]any{"text": "idle pane\n", "revision": 10}
		}
		return map[string]any{"text": screen, "revision": 11}
	})
	f.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
}

func startPromptDaemon(t *testing.T, fixture *herdrFixture) (*panewire.Daemon, *panewire.Store) {
	return startPromptDaemonWithSchema(t, fixture, promptFixtureSchema(false))
}

func startPromptDaemonWithSchema(t *testing.T, fixture *herdrFixture, schemaText string) (*panewire.Daemon, *panewire.Store) {
	t.Helper()
	db := panewire.NewMemoryStore(t)
	schema := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(schema, []byte(schemaText), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := filepath.Join(t.TempDir(), "schema.sh")
	if err := os.WriteFile(cmd, []byte("#!/bin/sh\ncat \"$1\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join("/tmp", "pw-d-"+filepath.Base(t.TempDir())+".sock")
	t.Cleanup(func() { _ = os.Remove(socket) })
	d := panewire.NewDaemon(panewire.Config{Store: db, SocketPath: socket, HerdrSocket: fixture.Path(), SchemaCommand: []string{"sh", cmd, schema}})
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	return d, db
}

func dSocket(d *panewire.Daemon) string { return d.SocketPath() }

func promptFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prompt.md")
	if err := os.WriteFile(path, []byte("expect: name=orch cwd=/work\n\nR2-MARKER\n"), 0600); err != nil {
		t.Fatal(fmt.Errorf("write prompt: %w", err))
	}
	return path
}
