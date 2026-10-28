package panewire

import (
	"os/exec"
	"testing"
)

// The structural + dry-run harness for ci-any-ref lives in
// tests/test-ci-any-ref.sh; run it here so `go test ./...` covers it.
func TestCiAnyRefHarness(t *testing.T) {
	cmd := exec.Command("bash", "tests/test-ci-any-ref.sh")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("test-ci-any-ref.sh: %v\n%s", err, out)
	}
}
