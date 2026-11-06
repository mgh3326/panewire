package fleet

import (
	"slices"
	"testing"
)

// A codex seat never stops on the startup update prompt: the launch turns the
// check off itself, so a host whose config.toml lacks the key still starts.
func TestCodexLaunchDisablesTheStartupUpdateCheck(t *testing.T) {
	a := &App{}
	argv, _, err := a.launchArgs(Seat{Kind: "codex"}, Launch{ModelID: "gpt-test", Effort: "high"})
	if err != nil {
		t.Fatal(err)
	}
	i := slices.Index(argv, "check_for_update_on_startup=false")
	if i < 1 || argv[i-1] != "-c" {
		t.Fatalf("codex argv lacks -c check_for_update_on_startup=false: %v", argv)
	}
	if j := slices.Index(argv, "model_reasoning_effort=high"); j < 1 || argv[j-1] != "-c" {
		t.Fatalf("codex argv lost the effort override: %v", argv)
	}
}
