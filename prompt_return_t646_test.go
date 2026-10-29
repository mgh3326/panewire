package panewire_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	panewire "github.com/mgh3326/panewire"
)

// #646 AC3(a): a direct prompt whose own text stays in claude's composer gets
// one return keypress, and only when the composer holds nothing but that text
// (the #626 part B rule). The pane below shows three phases: before the
// prompt, after it (text left in the composer: the Enter was dropped), and
// after a return keypress (the text echoed, composer empty).

const task646Transcript = "⏺ earlier answer\n✻ Worked for 3s · done 오후 3:12\n"
const task646Footer = "  [Opus 5.5 (1M context)] │ cwd\n  ⏵⏵ auto mode on\n"

func task646Screen(composer string) string {
	return task646Transcript + "────\n" + composer + "\n────\n" + task646Footer
}

type task646Pane struct {
	before, pending, afterReturn string
}

func (p task646Pane) install(f *herdrFixture) {
	f.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{"agent": "orch", "name": "orch", "label": "orch", "harness": "claude", "pane_id": "p1", "workspace_id": "w1", "cwd": "/work", "revision": 10, "agent_status": "idle"}}}
	})
	f.On("agent.read", func() any {
		switch {
		case f.Requests("agent.prompt") == 0:
			return map[string]any{"text": p.before, "revision": 10}
		case f.Requests("agent.send_keys") == 0:
			return map[string]any{"text": p.pending, "revision": 11}
		}
		return map[string]any{"text": p.afterReturn, "revision": 12}
	})
	f.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
	f.On("agent.send_keys", func() any { return map[string]any{"type": "ok"} })
}

func task646SendKeys(f *herdrFixture) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, req := range f.requests {
		if req["method"] == "agent.send_keys" {
			params, _ := req["params"].(map[string]any)
			out = append(out, params)
		}
	}
	return out
}

const task646Body = "R2-MARKER line one of the brief\nline two of the brief\n"

func task646PromptFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "prompt.md")
	if err := os.WriteFile(path, []byte("expect: name=orch cwd=/work\n\n"+task646Body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func task646Run(t *testing.T, pane task646Pane) (int, []map[string]any, panewire.Delivery) {
	t.Helper()
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	pane.install(fixture)
	d, db := startPromptDaemon(t, fixture)
	defer d.Stop()
	code := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "orch", "--file", task646PromptFile(t), "--timeout", "3s"}, panewire.CLIConfig{SocketPath: dSocket(d)})
	delivery, _, err := db.LatestDelivery(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return code, task646SendKeys(fixture), delivery
}

func task646AfterReturn() string {
	return "⏺ earlier answer\n✻ Worked for 3s · done 오후 3:12\n❯ R2-MARKER line one of the brief\n  line two of the brief\n\n✶ Thinking…\n────\n❯\n────\n" + task646Footer
}

func TestTask646OwnComposerTextGetsOneReturn(t *testing.T) {
	code, keys, delivery := task646Run(t, task646Pane{
		before:      task646Screen("❯"),
		pending:     task646Screen("❯ R2-MARKER line one of the brief\n  line two of the brief"),
		afterReturn: task646AfterReturn(),
	})
	if code != panewire.ExitOK {
		t.Fatalf("exit=%d want %d (delivery %+v)", code, panewire.ExitOK, delivery)
	}
	if len(keys) != 1 || keys[0]["target"] != "p1" || strings.Join(task646Keys(keys[0]), ",") != "return" {
		t.Fatalf("send_keys=%v, want exactly one return to p1", keys)
	}
	if delivery.SubmissionResult != "marker_observed" || !strings.HasSuffix(delivery.SubmissionEvidence, "+return_once:composer_self") {
		t.Fatalf("delivery %s / %q, want marker_observed with +return_once:composer_self", delivery.SubmissionResult, delivery.SubmissionEvidence)
	}
}

func TestTask646OwnChipGetsOneReturn(t *testing.T) {
	code, keys, delivery := task646Run(t, task646Pane{
		before:      task646Screen("❯"),
		pending:     task646Screen("❯ [Pasted text #3 +1 lines]"),
		afterReturn: task646AfterReturn(),
	})
	if code != panewire.ExitOK || len(keys) != 1 {
		t.Fatalf("exit=%d send_keys=%v, want 0 and one return", code, keys)
	}
	if !strings.HasSuffix(delivery.SubmissionEvidence, "+return_once:composer_self_chip") {
		t.Fatalf("evidence %q", delivery.SubmissionEvidence)
	}
}

// A prompt suggestion or draft ahead of the paste is not this call's text:
// a return would submit it too.
func TestTask646ForeignComposerTextGetsNoReturn(t *testing.T) {
	code, keys, delivery := task646Run(t, task646Pane{
		before:      task646Screen("❯ reply with the ack in each paste"),
		pending:     task646Screen("❯ reply with the ack in each pasteR2-MARKER line one of the brief\n  line two of the brief"),
		afterReturn: task646AfterReturn(),
	})
	if len(keys) != 0 {
		t.Fatalf("send_keys=%v, want none", keys)
	}
	if code != panewire.ExitDeliveryFailure || delivery.SubmissionResult != "composer_residue" {
		t.Fatalf("exit=%d submission=%s, want %d composer_residue", code, delivery.SubmissionResult, panewire.ExitDeliveryFailure)
	}
}

// A chip hides its text, so one already in the composer before the paste may
// be someone else's.
func TestTask646ChipPresentBeforeGetsNoReturn(t *testing.T) {
	code, keys, _ := task646Run(t, task646Pane{
		before:      task646Screen("❯ [Pasted text #1 +4 lines]"),
		pending:     task646Screen("❯ [Pasted text #2 +1 lines]"),
		afterReturn: task646AfterReturn(),
	})
	if len(keys) != 0 || code != panewire.ExitDeliveryFailure {
		t.Fatalf("exit=%d send_keys=%v, want %d and none", code, keys, panewire.ExitDeliveryFailure)
	}
}

func task646Keys(params map[string]any) []string {
	raw, _ := params["keys"].([]any)
	keys := make([]string, 0, len(raw))
	for _, k := range raw {
		s, _ := k.(string)
		keys = append(keys, s)
	}
	return keys
}
