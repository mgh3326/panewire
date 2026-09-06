package panewire

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

func TestAlloyMetricLabelContract(t *testing.T) {
	for _, config := range []string{"deploy/alloy/config.linux.alloy", "deploy/alloy/config.mac.alloy"} {
		contents, err := os.ReadFile(config)
		if err != nil {
			t.Fatal(err)
		}
		text := string(contents)
		for _, required := range []string{"prometheus.exporter.unix", "prometheus.remote_write", "sys.env(\"MACHINE_ID\")", "sys.env(\"PROM_REMOTE_WRITE_URL\")"} {
			if !strings.Contains(text, required) {
				t.Fatalf("%s missing %s", config, required)
			}
		}
		if strings.Contains(text, "machine_id = \"mac-personal\"") {
			t.Fatalf("%s has a static machine label", config)
		}
	}
	install, err := os.ReadFile("deploy/alloy/install-linux.sh")
	if err != nil || !strings.Contains(string(install), "PROM_REMOTE_WRITE_URL is required") {
		t.Fatal("linux installer must reject a missing remote write URL")
	}
	checker := "deploy/alloy/run-alloy.sh"
	if output, err := exec.Command("sh", checker, "true", "config").CombinedOutput(); err == nil || !strings.Contains(string(output), "MACHINE_ID is required") {
		t.Fatalf("mac launcher must reject missing machine ID: %v %s", err, output)
	}
	command := exec.Command("sh", checker, "true", "config")
	command.Env = append(os.Environ(), "MACHINE_ID=test", "PROM_REMOTE_WRITE_URL=http://prom.example")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("mac launcher rejected complete environment: %v %s", err, output)
	}
}
