package panewire

import (
	"bytes"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const fleetEventsBegin, fleetEventsEnd = "// BEGIN fleet events", "// END fleet events"

// fleetEventsBlock returns a config's fleet-events pipeline (between its
// markers, inclusive).
func fleetEventsBlock(t *testing.T, config string) string {
	t.Helper()
	contents, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	text := string(contents)
	begin, end := strings.Index(text, fleetEventsBegin), strings.Index(text, fleetEventsEnd)
	if begin < 0 || end < begin || strings.Count(text, fleetEventsBegin) != 1 {
		t.Fatalf("%s must hold exactly one fleet events block", config)
	}
	return text[begin : end+len(fleetEventsEnd)]
}

// The pipeline is identical on both platforms and keeps Loki's label set to
// host, tool, kind and outcome (plus the source and external labels).
func TestAlloyFleetEventsLabelContract(t *testing.T) {
	linux := fleetEventsBlock(t, "deploy/alloy/config.linux.alloy")
	if mac := fleetEventsBlock(t, "deploy/alloy/config.mac.alloy"); mac != linux {
		t.Fatal("the fleet events block must be the same in both configs")
	}
	for _, required := range []string{
		`sys.env("FLEET_EVENTS_ROOT")`,
		`/*/events/fleet-events.jsonl"`,
		"forward_to = [loki.write.cloud.receiver]",
		`values = ["filename"]`,
		"stage.limit {",
		`format = "RFC3339Nano"`,
	} {
		if !strings.Contains(linux, required) {
			t.Fatalf("fleet events block lacks %s", required)
		}
	}
	labels := regexp.MustCompile(`(?s)stage\.labels \{\s*values = \{(.*?)\}`).FindStringSubmatch(linux)
	if labels == nil {
		t.Fatal("no stage.labels")
	}
	var names []string
	for _, line := range strings.Split(labels[1], "\n") {
		if name, _, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
			names = append(names, strings.TrimSpace(name))
		}
	}
	if strings.Join(names, ",") != "host,tool,kind,outcome" {
		t.Fatalf("labels %v: job, reason and detail must stay in the line", names)
	}
}

// lockedBuffer collects a child process's output across goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestAlloyFleetEventsPipelineShipsLabelledLines runs the real pipeline from
// the config, with loki.echo in place of the Grafana Cloud writer, against a
// temporary state home, and checks what would be pushed.
func TestAlloyFleetEventsPipelineShipsLabelledLines(t *testing.T) {
	alloy, err := exec.LookPath("alloy")
	if err != nil {
		t.Skip("alloy binary is not installed; skipping the fleet events pipeline run")
	}
	block := fleetEventsBlock(t, "deploy/alloy/config.linux.alloy")
	config := strings.Replace(block, "loki.write.cloud.receiver", "loki.echo.test.receiver", 1) + "\n\nloki.echo \"test\" { }\n"
	work := t.TempDir()
	configPath := filepath.Join(work, "config.alloy")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(work, "state")
	events := filepath.Join(root, "runtug", "events", "fleet-events.jsonl")
	if err := os.MkdirAll(filepath.Dir(events), 0o750); err != nil {
		t.Fatal(err)
	}
	// The rotated generation must never be shipped.
	if err := os.WriteFile(events+".1", []byte(`{"ts":"2026-11-01T00:00:00Z","host":"node-a","tool":"runtug","kind":"rotated","job":"","outcome":"refused","reason":"x","detail":""}`+"\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(events, nil, 0o640); err != nil {
		t.Fatal(err)
	}

	// Alloy's cluster node refuses port 0, so reserve a free port first.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listen := listener.Addr().String()
	_ = listener.Close()
	output := &lockedBuffer{}
	command := exec.Command(alloy, "run", "--server.http.listen-addr="+listen, "--storage.path="+filepath.Join(work, "data"), "--disable-reporting", configPath)
	command.Env = append(os.Environ(), "FLEET_EVENTS_ROOT="+root)
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	})

	line := `{"ts":"2026-11-01T12:34:56.789Z","host":"node-a","tool":"runtug","kind":"spawn","job":"job-a","outcome":"refused","reason":"config","detail":"exit=70 quota gate refused"}` + "\n"
	var last time.Time
	eventually(t, 60*time.Second, "loki.echo to print a fleet event", func() bool {
		if strings.Contains(output.String(), "loki.echo.test") && strings.Contains(output.String(), "job-a") {
			return true
		}
		// tail_from_end starts at the end once the tailer is up, so keep
		// appending until one line arrives.
		if time.Since(last) > 500*time.Millisecond {
			last = time.Now()
			file, err := os.OpenFile(events, os.O_WRONLY|os.O_APPEND, 0)
			if err == nil {
				_, _ = file.WriteString(line)
				_ = file.Close()
			}
		}
		return false
	})
	_ = command.Process.Kill()
	_ = command.Wait()
	text := output.String()
	var echoed string
	for _, l := range strings.Split(text, "\n") {
		if strings.Contains(l, "loki.echo.test") && strings.Contains(l, "job-a") {
			echoed = l
			break
		}
	}
	for _, want := range []string{`host=\"node-a\"`, `tool=\"runtug\"`, `kind=\"spawn\"`, `outcome=\"refused\"`, `source=\"fleet_events\"`, "entry_timestamp=2026-11-01T12:34:56.789Z"} {
		if !strings.Contains(echoed, want) {
			t.Fatalf("echoed entry lacks %s:\n%s", want, echoed)
		}
	}
	for _, unwanted := range []string{`filename=`, `reason=\"`, `job=\"`} {
		if strings.Contains(echoed, unwanted) {
			t.Fatalf("echoed entry carries label %s:\n%s", unwanted, echoed)
		}
	}
	if strings.Contains(text, `\"kind\":\"rotated\"`) || strings.Contains(text, `kind=\"rotated\"`) {
		t.Fatal("the rotated generation was shipped")
	}
}
