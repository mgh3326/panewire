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
}
