package panewire

import (
	"slices"
	"testing"
)

// --review-pass crosses the hub allowlist for 1..9 and nothing else. A
// reviewer holds no reservation (hk 4737), so the flag no longer widens a slot;
// it stays accepted on the wire because mergewake's reviewer argv carries it,
// and it still names the pass in the operation record.
func TestHubSpawnAllowlistCarriesReviewPass(t *testing.T) {
	base := []string{"-l", "rev", "--job", "rev", "--t", "T1", "--task", "42", "--owner", "parent", "--role", "reviewer"}
	for _, tc := range []struct {
		pass string
		want bool
	}{{"1", true}, {"9", true}, {"0", false}, {"10", false}, {"p1", false}, {"", false}} {
		if got := validHubSpawnArgs(append(slices.Clone(base), "--review-pass", tc.pass)); got != tc.want {
			t.Errorf("--review-pass %q accepted=%v, want %v", tc.pass, got, tc.want)
		}
	}
	if validHubSpawnArgs(append(slices.Clone(base), "--review-pass", "1", "--review-pass", "2")) {
		t.Error("a repeated --review-pass was accepted")
	}
}
