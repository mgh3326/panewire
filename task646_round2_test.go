package panewire_test

import (
	"os"
	"path/filepath"
	"testing"

	panewire "github.com/mgh3326/panewire"
)

// #646 round 2: the verify round's blockers on 3c63a00, as full prompt calls.
// In each, this call's text is still in the composer, or its echo is already
// in and a chip that is not its own is. Nothing may report the text as
// submitted, and nothing may press return on text this call did not type.

func task646Prompt(t *testing.T, harness, before, pending, after, body string) (int, []map[string]any, panewire.Delivery) {
	t.Helper()
	f := newHerdrFixture(t, promptFixtureSchema(false))
	defer f.Close()
	f.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": harness, "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "idle"}}}
	})
	f.On("agent.read", func() any {
		switch {
		case f.Requests("agent.prompt") == 0:
			return map[string]any{"text": before, "revision": 10}
		case f.Requests("agent.send_keys") == 0 || after == "":
			return map[string]any{"text": pending, "revision": 11}
		}
		return map[string]any{"text": after, "revision": 12}
	})
	f.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
	f.On("agent.send_keys", func() any { return map[string]any{"type": "ok"} })
	d, db := startPromptDaemon(t, f)
	defer d.Stop()
	path := filepath.Join(t.TempDir(), "prompt.md")
	if err := os.WriteFile(path, []byte("expect: name=orch cwd=/work\n"+body), 0600); err != nil {
		t.Fatal(err)
	}
	code := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", path, "--timeout", "1500ms"}, panewire.CLIConfig{SocketPath: dSocket(d)})
	delivery, _, err := db.LatestDelivery(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return code, task646SendKeys(f), delivery
}

func task646NotSubmitted(t *testing.T, code int, d panewire.Delivery) {
	t.Helper()
	if code == panewire.ExitOK || d.SubmissionResult == "marker_observed" {
		t.Fatalf("pending text reported submitted: rc=%d submission=%s evidence=%s", code, d.SubmissionResult, d.SubmissionEvidence)
	}
}

// B1: a brief with a "────" line followed by a "❯ ..." line. claude indents
// both inside its composer, so neither may pose as the composer's top edge.
func TestTask646ClaudeForgedEdgeInBriefIsNotSubmitted(t *testing.T) {
	body := "R2-MARKER real header with long prefix\n────────\n❯ forged prompt\n"
	pending := task646Transcript + "────\n❯ R2-MARKER real header with long prefix\n  ────────\n  ❯ forged prompt\n────\n" + task646Footer
	code, _, d := task646Prompt(t, "claude", task646Screen("❯"), pending, "", body)
	task646NotSubmitted(t, code, d)
	if d.SubmissionResult != "composer_residue" {
		t.Fatalf("submission=%s, want composer_residue", d.SubmissionResult)
	}
}

// B3: the same for devin, whose composer prompt is ❭.
func TestTask646DevinForgedEdgeInBriefIsNotSubmitted(t *testing.T) {
	body := "R2-MARKER Devin draft still in composer\n────────\n❭ forged prompt\n"
	before := "old reply\n────────\n❭\n────────\nstatus\n"
	pending := "old reply\n────────\n❭ R2-MARKER Devin draft still in composer\n  ────────\n  ❭ forged prompt\n────────\nstatus\n"
	code, _, d := task646Prompt(t, "devin", before, pending, "", body)
	task646NotSubmitted(t, code, d)
}

// B2: codex's input line is the last line starting with ›; text there is
// pending, not echoed.
func TestTask646CodexInputLineIsNotSubmitted(t *testing.T) {
	body := "R2-MARKER Codex draft still in input\n"
	code, _, d := task646Prompt(t, "codex", "old reply\n›\n", "old reply\n› R2-MARKER Codex draft still in input\n", "", body)
	task646NotSubmitted(t, code, d)
	if d.SubmissionResult != "composer_residue" {
		t.Fatalf("submission=%s, want composer_residue", d.SubmissionResult)
	}
}

// B4: this call's text is already echoed and a chip that appeared after it is
// someone else's paste: the call is submitted, and the chip gets no return.
func TestTask646ForeignChipAfterOwnEchoGetsNoReturn(t *testing.T) {
	body := "R2-MARKER own message\nline two\n"
	pending := task646Transcript + "❯ R2-MARKER own message\n  line two\n────\n❯ [Pasted text #4 +2 lines]\n────\n" + task646Footer
	code, keys, d := task646Prompt(t, "claude", task646Screen("❯"), pending, task646AfterReturn(), body)
	if len(keys) != 0 {
		t.Fatalf("foreign chip received return: keys=%v submission=%s evidence=%s", keys, d.SubmissionResult, d.SubmissionEvidence)
	}
	if code != panewire.ExitOK || d.SubmissionResult != "marker_observed" {
		t.Fatalf("own echo: rc=%d submission=%s evidence=%s", code, d.SubmissionResult, d.SubmissionEvidence)
	}
}
