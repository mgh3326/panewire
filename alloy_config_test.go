package panewire

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestAlloyConfigFormat(t *testing.T) {
	alloy, err := exec.LookPath("alloy")
	if err != nil {
		t.Skip("alloy binary is not installed; skipping Alloy configuration format check")
	}

	for _, config := range []string{
		"deploy/alloy/config.linux.alloy",
		"deploy/alloy/config.mac.alloy",
	} {
		t.Run(filepath.Base(config), func(t *testing.T) {
			contents, err := os.ReadFile(config)
			if err != nil {
				t.Fatal(err)
			}
			copy := filepath.Join(t.TempDir(), filepath.Base(config))
			if err := os.WriteFile(copy, contents, 0o600); err != nil {
				t.Fatal(err)
			}
			if output, err := exec.Command(alloy, "fmt", copy).CombinedOutput(); err != nil {
				t.Fatalf("alloy fmt %s: %v\n%s", config, err, output)
			}
		})
	}
}
