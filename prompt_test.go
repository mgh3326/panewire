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

func promptFixtureSchema(events bool) string {
	methods := `[` +
		`"agent.read","agent.prompt","agent.list"`
	if events {
		methods += `,"events.subscribe"`
	}
	return `{"protocol":20,"schema_version":1,"schemas":{"request":{"methods":` + methods + `],"read_source":["visible","recent","recent_unwrapped"]}}}`
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
	t.Helper()
	db := panewire.NewMemoryStore(t)
	schema := filepath.Join(t.TempDir(), "schema.json")
	if err := os.WriteFile(schema, []byte(promptFixtureSchema(false)), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := filepath.Join(t.TempDir(), "schema.sh")
	if err := os.WriteFile(cmd, []byte("#!/bin/sh\ncat \"$1\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	d := panewire.NewDaemon(panewire.Config{Store: db, SocketPath: filepath.Join(t.TempDir(), "panewire.sock"), HerdrSocket: fixture.Path(), SchemaCommand: []string{"sh", cmd, schema}})
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
