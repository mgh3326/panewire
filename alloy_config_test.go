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

// The free Grafana Cloud tier allows 10k active series for the whole account,
// so the process exporter may only forward three metric families, and only
// through the allowlist relabel.
func TestAlloyLinuxProcessMetricsAreAllowlisted(t *testing.T) {
	contents, err := os.ReadFile("deploy/alloy/config.linux.alloy")
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	for _, required := range []string{
		"prometheus.exporter.process \"procs\"",
		"\"{{.Comm}}\"",
		"track_children      = false",
		"prometheus.relabel \"procs\"",
		"action        = \"keep\"",
		`regex         = "up;|namedprocess_namegroup_(cpu_seconds_total|num_procs);|namedprocess_namegroup_memory_bytes;resident"`,
	} {
		if !strings.Contains(text, required) {
			t.Errorf("config.linux.alloy missing %s", required)
		}
	}
	start := strings.Index(text, "prometheus.scrape \"procs\"")
	if start < 0 {
		t.Fatal("config.linux.alloy missing the process scrape")
	}
	scrape := text[start:]
	end := strings.Index(scrape, "}")
	if end < 0 {
		t.Fatal("process scrape block is not closed")
	}
	scrape = scrape[:end]
	if !strings.Contains(scrape, "forward_to = [prometheus.relabel.procs.receiver]") || strings.Contains(scrape, "remote_write") {
		t.Errorf("process scrape must forward through the allowlist relabel, got:\n%s", scrape)
	}
	// Every namedprocess_* name in the file belongs to the kept set; a
	// denylist-style rule would name other families.
	for _, name := range regexp.MustCompile(`namedprocess_namegroup_[a-z_]+`).FindAllString(text, -1) {
		switch name {
		case "namedprocess_namegroup_cpu_seconds_total", "namedprocess_namegroup_num_procs", "namedprocess_namegroup_memory_bytes":
		default:
			t.Errorf("unexpected process metric %s in config.linux.alloy", name)
		}
	}
	mac, err := os.ReadFile("deploy/alloy/config.mac.alloy")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mac), "prometheus.exporter.process") {
		t.Error("process exporter needs /proc and must not be configured on macOS; use proc-metrics.sh")
	}
}
