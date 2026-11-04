package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newCodexHooksFixture is a Spawn of a Codex seat that proves its launch by
// footer. The pane shows a bare banner on the first read after `agent start`
// (the trust check before the footer loop) and screen on every read after it, so
// the footer loop is what meets the screen. It returns the count of those reads.
func newCodexHooksFixture(t *testing.T, screen string) (*App, Spawn, *fakeRunner, *int) {
	t.Helper()
	t.Setenv("PANEWIRE_E6_ARM", "codex-terra61@high")
	a, s, f, _ := cmdFixture(t)
	s.Profile = "builder-terra61-high"
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Profile: "codex-terra61", ModelID: "gpt-6.1-example", Effort: "high", Grade: "A+", Gate: "default"}
	q.GateDo = func(QuotaRequest) (string, error) { return "pool=example profile=codex-terra61\n", nil }
	started, reads := false, 0
	original := f.Do
	f.Do = func(c Command) Result {
		args := strings.Join(c.Args, " ")
		switch {
		case strings.HasPrefix(args, "agent start"):
			started = true
			return Result{}
		case started && strings.HasPrefix(args, "pane process-info"):
			return Result{Out: `{"result":{"process_info":{"shell_pid":1,"foreground_processes":[{"pid":2,"argv0":"codex"}]}}}`}
		case started && strings.HasPrefix(args, "agent list"):
			seq := int64(1)
			raw, _ := json.Marshal(map[string]any{"agents": []Agent{{Pane: "w:p1", Name: s.Label, Kind: "codex", Status: "idle", CWD: s.CWD, StateChangeSeq: &seq}}})
			return Result{Out: string(raw)}
		case started && strings.HasPrefix(args, "pane read"):
			reads++
			if reads == 1 {
				return Result{Out: "  >_ OpenAI Codex (v0.166.0)\n"}
			}
			return Result{Out: screen}
		}
		return original(c)
	}
	return a, s, f, &reads
}

func readFixture(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func jobInput(f *fakeRunner) []string {
	var got []string
	for _, c := range f.Calls {
		args := strings.Join(c.Args, " ")
		if strings.HasPrefix(args, "agent prompt") || strings.Contains(args, "send-keys") || strings.Contains(args, "send-text") || strings.Contains(args, "tasks claim") {
			got = append(got, args)
		}
	}
	return got
}

func TestCodexHooksReviewScreenRefusesBeforeTheFooterDeadline(t *testing.T) {
	for _, name := range []string{"codex-hooks-review-40col.txt", "codex-hooks-review-62col-trust-error.txt"} {
		t.Run(name, func(t *testing.T) {
			a, s, f, reads := newCodexHooksFixture(t, readFixture(t, name))
			err := a.Spawn(context.Background(), s)
			if code(err) != exitTrustPrompt || err == nil || !strings.HasPrefix(err.Error(), "HOOKS_REVIEW_REQUIRED:") || !strings.Contains(err.Error(), "executing CODEX_HOME") {
				t.Fatalf("hooks review not refused: code=%d err=%v", code(err), err)
			}
			if *reads < 2 {
				t.Fatalf("pane reads after start = %d: the footer loop never polled", *reads)
			}
			if input := jobInput(f); len(input) > 0 {
				t.Fatalf("hooks review screen received job input: %v", input)
			}
			if got := SpawnRefusalCode(err.Error(), code(err)); got != "hooks_review" {
				t.Fatalf("refusal code %q", got)
			}
		})
	}
}

func TestCodexTrustPromptStillRefusedInFooterLoop(t *testing.T) {
	a, s, f, _ := newCodexHooksFixture(t, readFixture(t, "codex-trust-current.txt"))
	err := a.Spawn(context.Background(), s)
	if code(err) != exitTrustPrompt || err == nil || !strings.HasPrefix(err.Error(), "WORKSPACE_TRUST_REQUIRED:") {
		t.Fatalf("trust prompt not refused: code=%d err=%v", code(err), err)
	}
	if input := jobInput(f); len(input) > 0 {
		t.Fatalf("trust prompt received job input: %v", input)
	}
}

func TestCodexFooterStillProvesLaunchBesideHooksCheck(t *testing.T) {
	footer := "  >_ OpenAI Codex (v0.166.0)\n  directory: ~/tree\n  model: gpt-6.1-example high\n› Ask Codex to do anything\n\n  GPT-6.1 Example high · ~/tree\n  ? for shortcuts\n"
	// The second screen is the footer under agent output that talks about hooks.
	mention := "• Hooks need review? I will list the hooks that changed.\n  Trust all and continue only after the tests pass.\n" + footer
	for name, screen := range map[string]string{"footer": footer, "footer under hooks talk": mention} {
		t.Run(name, func(t *testing.T) {
			a, s, f, reads := newCodexHooksFixture(t, screen)
			err := a.Spawn(context.Background(), s)
			if err != nil && (strings.Contains(err.Error(), "HOOKS_REVIEW_REQUIRED") || strings.Contains(err.Error(), "footer not proven")) {
				t.Fatalf("launch not proven: %v", err)
			}
			if *reads < 2 {
				t.Fatalf("footer loop never read the pane (%d reads): err=%v", *reads, err)
			}
			// The scripted pane never shows the submitted turn, so delivery ends
			// the spawn; reaching it means the footer proved the launch.
			if err != nil && code(err) != exitSubmitUnconfirmed {
				t.Fatalf("spawn failed before delivery: %v", err)
			}
			if len(jobInput(f)) == 0 {
				t.Fatal("brief never sent after the footer proof")
			}
		})
	}
}

func TestCodexHooksMentionedInAgentOutputIsNotTheReviewScreen(t *testing.T) {
	for _, screen := range []string{
		"› fix the hooks\n• Hooks need review? I will list the hooks that changed.\n  Trust all and continue only after the tests pass.\n› Ask Codex to do anything\n",
		"  Hooks can run outside the sandbox after you trust them.\n› Ask Codex to do anything\n",
		"› Ask Codex to do anything\nContext 0%",
	} {
		if codexHooksReviewPrompt(screen) {
			t.Fatalf("agent output mistaken for the hooks review screen:\n%s", screen)
		}
	}
	if !codexHooksReviewPrompt(readFixture(t, "codex-hooks-review-40col.txt")) {
		t.Fatal("upstream hooks review snapshot missed")
	}
}
