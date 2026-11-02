package fleet

import (
	"strings"
	"testing"
)

func TestActionableSpawnRefusals(t *testing.T) {
	for _, test := range []struct {
		message, fix string
		status       int
	}{
		{"agent not in foreground; brief withheld", "harness binary", exitConfig},
		{"workspace workers not found", "herdr workspace list", exitConfig},
		{"workspace trust: malformed workspace trust store", "Repair", exitConfig},
		{"workspace trust: refusing broad workspace trust", "specific worktree", exitConfig},
		{"workspace trust: permission denied", "trust store", exitConfig},
		{"brief is empty", "-p", exitUsage},
		{"active duplicate job job-a", "panewire find", exitDuplicate},
		{"task scale does not match --t", "--t", exitSeatRefused},
		{"initial submission unconfirmed", "saved", exitSubmitUnconfirmed},
		{"account unavailable", "Sign in", exitAuthUnavailable},
	} {
		err := fail(test.status, "%s", test.message)
		if code(err) != test.status || !strings.Contains(spawnRefusalFix(err), test.fix) {
			t.Fatalf("%s: code=%d fix=%s", test.message, code(err), spawnRefusalFix(err))
		}
	}
	err := trustPromptError("/work/project")
	if code(err) != exitTrustPrompt || !strings.Contains(spawnRefusalFix(err), "executing CODEX_HOME") {
		t.Fatalf("real Codex trust refusal: code=%d fix=%s", code(err), spawnRefusalFix(err))
	}
}
