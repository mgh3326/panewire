package panewire

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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
		for _, required := range []string{"prometheus.exporter.unix", "prometheus.remote_write", "sys.env(\"MACHINE_ID\")", "sys.env(\"PROM_REMOTE_WRITE_URL\")", "sys.env(\"PROM_REMOTE_WRITE_USER\")", "sys.env(\"PROM_REMOTE_WRITE_TOKEN\")"} {
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
	command.Env = append(os.Environ(), "MACHINE_ID=test", "PROM_REMOTE_WRITE_URL=http://prom.example", "PROM_REMOTE_WRITE_USER=user", "PROM_REMOTE_WRITE_TOKEN=token")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("mac launcher rejected complete environment: %v %s", err, output)
	}
}

func TestAlloyConfigValidates(t *testing.T) {
	alloy, err := exec.LookPath("alloy")
	if err != nil {
		t.Skip("alloy binary is not installed; skipping Alloy configuration validation")
	}
	for _, config := range []string{"deploy/alloy/config.linux.alloy", "deploy/alloy/config.mac.alloy"} {
		t.Run(filepath.Base(config), func(t *testing.T) {
			if output, err := exec.Command(alloy, "validate", config).CombinedOutput(); err != nil {
				t.Fatalf("alloy validate %s: %v\n%s", config, err, output)
			}
		})
	}
}

// The free Grafana Cloud tier allows 10k active series for the whole account.
// Alloy's process exporter cannot cap its groups (measured fleet hosts have
// 148-239 distinct process names), so per-process metrics come only from the
// capped proc-metrics.sh textfile, and no config may run the exporter.
func TestAlloyConfigsDoNotRunTheUncappedProcessExporter(t *testing.T) {
	for _, config := range []string{"deploy/alloy/config.linux.alloy", "deploy/alloy/config.mac.alloy"} {
		contents, err := os.ReadFile(config)
		if err != nil {
			t.Fatal(err)
		}
		text := string(contents)
		if strings.Contains(text, "prometheus.exporter.process") {
			t.Errorf("%s must not run prometheus.exporter.process; use proc-metrics.sh", config)
		}
		if names := regexp.MustCompile(`namedprocess_namegroup_[a-z_]+`).FindAllString(text, -1); len(names) > 0 {
			t.Errorf("%s names process metrics %v; they belong to proc-metrics.sh", config, names)
		}
		if !strings.Contains(text, "textfile {") || !strings.Contains(text, `sys.env("FLEET_TEXTFILE_DIR")`) {
			t.Errorf("%s must read the textfile directory that proc-metrics.sh writes", config)
		}
	}
}
