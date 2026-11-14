package quotastop

import (
	"os"
	"strings"
	"testing"
	"time"
)

// hk 4680 (operator decision B): the real Devin screen, with its injected text
// above the error, is a free-model stop for a devin pane. The banner is two
// indented rows after a blank row, under the user turn of a mergewake directive.
// Its last banner is followed by Devin chrome only.

func devinRealScreenT4680(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/devin_dot_cwd_screen_t4680.txt")
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// insertBeforeCwdRowT4680 splices text above the last working-directory row, so
// it lands between the last banner and the chrome that follows it.
func insertBeforeCwdRowT4680(t *testing.T, screen, text string) string {
	t.Helper()
	i := strings.LastIndex(screen, " ● ~/work/")
	if i < 0 {
		t.Fatal("fixture has no working-directory row")
	}
	return screen[:i] + text + screen[i:]
}

func TestClassifyAgentDevinIndentedBannerRealScreenT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	want := time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC)
	got, ok := ClassifyAgent(devinRealScreenT4680(t), "devin", now, time.UTC)
	if !ok {
		t.Fatalf("ClassifyAgent(devin) = (%+v, false), want a devin-free stop", got)
	}
	if got.Pool != "devin-free" || !got.Reset.Equal(want) {
		t.Fatalf("got pool %q reset %v, want devin-free reset %v", got.Pool, got.Reset, want)
	}
}

// Guard 1: only a devin pane may have an indented banner read as a new limit.
// Classify, with no agent kind, keeps refusing it as before.
func TestClassifyAgentDevinIndentedBannerNeedsDevinAgentT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	screen := devinRealScreenT4680(t)
	for _, agent := range []string{"", "codex", "claude"} {
		if got, ok := ClassifyAgent(screen, agent, now, time.UTC); ok {
			t.Fatalf("agent %q: ClassifyAgent = %+v, want no stop", agent, got)
		}
	}
	if got, ok := Classify(screen, now, time.UTC); ok {
		t.Fatalf("Classify (no agent kind) = %+v, want no stop", got)
	}
}

// Negative fixtures, each the real screen with one change; all are not_quota.
func TestClassifyAgentDevinIndentedBannerRefusesT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	screen := devinRealScreenT4680(t)
	for name, s := range map[string]string{
		"banner quoted mid-output": insertBeforeCwdRowT4680(t, screen, "I will retry once the limit resets.\n"),
		"user turn after banner":   insertBeforeCwdRowT4680(t, screen, "❭ keep going on the fix\n"),
		"footer-like prose output": insertBeforeCwdRowT4680(t, screen, "SWE-2 is retrying the task now.\n"),
		"no blank before banner": strings.Replace(screen,
			"\n\n ▲ Something went wrong\n   Reached free model rate limit. Upgrade to Max for higher limits, or switch to a different model. Your limit will\n   reset in 13 minutes",
			"\n ▲ Something went wrong\n   Reached free model rate limit. Upgrade to Max for higher limits, or switch to a different model. Your limit will\n   reset in 13 minutes", 1),
	} {
		if got, ok := ClassifyAgent(s, "devin", now, time.UTC); ok {
			t.Fatalf("%s: ClassifyAgent = %+v, want no stop", name, got)
		}
	}
}

// Guard 2: the banner must carry a reset. A limit with no reset is not read.
func TestClassifyAgentDevinIndentedBannerNeedsResetT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	const screen = "❭ [relay] go on\n\n ▲ Something went wrong\n   Reached free model rate limit. Upgrade to Max for higher limits. Send a message to retry\n\n ● ~/work/x\n────────────────────────────────────────\n❭ Ask Devin to build features\n────────────────────────────────────────\n  SWE-2 Medium\n"
	if got, ok := ClassifyAgent(screen, "devin", now, time.UTC); ok {
		t.Fatalf("banner without a reset: ClassifyAgent = %+v, want no stop", got)
	}
}

// A one-row indented banner after a blank row is read the same way for a devin
// pane, with its reset given as "at HH:MM UTC".
func TestClassifyAgentDevinOneRowIndentedBannerT4680(t *testing.T) {
	now := time.Date(2026, 11, 15, 16, 48, 0, 0, time.UTC)
	want := time.Date(2026, 11, 15, 17, 1, 0, 0, time.UTC)
	const screen = "❭ [relay] go on\n  more text\n\n ▲ Something went wrong — Reached free model rate limit. Send a message to retry at 17:01 UTC.\n\n ⏺ ~/work/x\n────────────────────────────────────────\n❭ Ask Devin to build features\n────────────────────────────────────────\n  SWE-2 Medium\n"
	got, ok := ClassifyAgent(screen, "devin", now, time.UTC)
	if !ok || got.Pool != "devin-free" || !got.Reset.Equal(want) {
		t.Fatalf("ClassifyAgent = (%+v, %v), want devin-free reset %v", got, ok, want)
	}
}
