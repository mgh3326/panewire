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

// Both platforms keep Loki's label set to host, os_host, tool, kind and
// outcome (plus the source and external labels). Linux uses explicit paths;
// macOS can glob because the user agent owns the tool state directories.
func TestAlloyFleetEventsLabelContract(t *testing.T) {
	linux := fleetEventsBlock(t, "deploy/alloy/config.linux.alloy")
	mac := fleetEventsBlock(t, "deploy/alloy/config.mac.alloy")
	for _, required := range []string{
		`sys.env("FLEET_EVENTS_ROOT")`,
		"forward_to = [loki.write.cloud.receiver]",
		`values = ["filename"]`,
		"stage.limit {",
		`format = "RFC3339Nano"`,
		`os_host = "host"`,
		`coalesce(sys.env("MACHINE_ID"), `,
	} {
		if !strings.Contains(linux, required) {
			t.Fatalf("fleet events block lacks %s", required)
		}
	}
	if !strings.Contains(mac, `/*/events/fleet-events.jsonl"`) {
		t.Fatal("macOS fleet events must keep the user-state directory glob")
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
	if strings.Join(names, ",") != "host,os_host,tool,kind,outcome" {
		t.Fatalf("labels %v: job, reason and detail must stay in the line", names)
	}
}

func TestLinuxAlloyFleetEventsUseExplicitToolPaths(t *testing.T) {
	linux := fleetEventsBlock(t, "deploy/alloy/config.linux.alloy")
	for _, tool := range []string{"panewire", "mergewake"} {
		path := `+ "/` + tool + `/events/fleet-events.jsonl"`
		if !strings.Contains(linux, path) {
			t.Errorf("Linux fleet events must match the explicit %s path", tool)
		}
	}
	if strings.Contains(linux, `/*/events/fleet-events.jsonl`) {
		t.Fatal("Linux fleet events must not glob across tool state directories")
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

// alloyFleetRun is one alloy process running the fleet-events block with
// loki.echo in place of the cloud writer.
type alloyFleetRun struct {
	command *exec.Cmd
	output  *lockedBuffer
}

// machineID is the run's MACHINE_ID; empty runs it as a host without one.
func startAlloyFleetRun(t *testing.T, alloy, configPath, storage, root, machineID string) *alloyFleetRun {
	t.Helper()
	// Alloy's cluster node refuses port 0, so reserve a free port first.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listen := listener.Addr().String()
	_ = listener.Close()
	run := &alloyFleetRun{output: &lockedBuffer{}}
	run.command = exec.Command(alloy, "run", "--server.http.listen-addr="+listen, "--storage.path="+storage, "--disable-reporting", configPath)
	run.command.Env = append(os.Environ(), "FLEET_EVENTS_ROOT="+root, "MACHINE_ID="+machineID)
	run.command.Stdout, run.command.Stderr = run.output, run.output
	if err := run.command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(run.stop)
	return run
}

// stop kills the process by PID and reaps it; safe to call twice.
func (run *alloyFleetRun) stop() {
	if run.command.ProcessState == nil {
		_ = run.command.Process.Kill()
		_ = run.command.Wait()
	}
}

// echoed returns the loki.echo lines that carry job.
func (run *alloyFleetRun) echoed(job string) []string {
	var lines []string
	for _, line := range strings.Split(run.output.String(), "\n") {
		if strings.Contains(line, "loki.echo.test") && strings.Contains(line, `\"job\":\"`+job+`\"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

func fleetEventLine(tool, job, ts string) string {
	return `{"ts":"` + ts + `","host":"node-a","tool":"` + tool + `","kind":"spawn","job":"` + job + `","outcome":"refused","reason":"config","detail":"exit=70 quota gate refused"}` + "\n"
}

func appendLine(t *testing.T, path, line string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(line); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestAlloyFleetEventsPipelineShipsLabelledLines runs the real pipeline from
// the config, with loki.echo in place of the Grafana Cloud writer, against a
// temporary state home. It checks the labels and timestamp, that a line
// written before the first start ships, that a line written while alloy was
// down ships after a restart (positions, not tail-from-end), that nothing
// ships twice, and that the rotated generation never ships. The first run has
// no MACHINE_ID, so host stays the line's OS hostname; the restart sets one,
// which replaces host while os_host keeps the OS hostname.
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
	storage := filepath.Join(work, "data")
	var eventFiles []string
	for _, tool := range []string{"panewire", "mergewake"} {
		events := filepath.Join(root, tool, "events", "fleet-events.jsonl")
		if err := os.MkdirAll(filepath.Dir(events), 0o750); err != nil {
			t.Fatal(err)
		}
		eventFiles = append(eventFiles, events)
		appendLine(t, events+".1", fleetEventLine(tool, "job-rotated-"+tool, "2026-11-01T00:00:00Z"))
		appendLine(t, events, fleetEventLine(tool, "job-before-"+tool, "2026-11-01T12:34:56.789Z"))
	}

	first := startAlloyFleetRun(t, alloy, configPath, storage, root, "")
	for _, tool := range []string{"panewire", "mergewake"} {
		job := "job-before-" + tool
		eventually(t, 60*time.Second, "the "+tool+" line written before start", func() bool { return len(first.echoed(job)) == 1 })
		if echoed := first.echoed(job)[0]; !strings.Contains(echoed, `tool="`+tool+`"`) {
			t.Fatalf("%s path emitted the wrong tool label: %s", tool, echoed)
		}
	}
	echoed := first.echoed("job-before-panewire")[0]
	for _, want := range []string{`{host=\"node-a\"`, `os_host=\"node-a\"`, `tool=\"panewire\"`, `kind=\"spawn\"`, `outcome=\"refused\"`, `source=\"fleet_events\"`, "entry_timestamp=2026-11-01T12:34:56.789Z"} {
		if !strings.Contains(echoed, want) {
			t.Fatalf("echoed entry lacks %s:\n%s", want, echoed)
		}
	}
	for _, unwanted := range []string{`filename=`, `reason=\"`, `job=\"`} {
		if strings.Contains(echoed, unwanted) {
			t.Fatalf("echoed entry carries label %s:\n%s", unwanted, echoed)
		}
	}
	// Positions are synced periodically and on shutdown; give the first
	// run a moment, then stop it the way a restart would.
	time.Sleep(time.Second)
	_ = first.command.Process.Signal(os.Interrupt)
	_ = first.command.Wait()

	appendLine(t, eventFiles[0], fleetEventLine("panewire", "job-while-down", "2026-11-01T12:35:00Z"))
	second := startAlloyFleetRun(t, alloy, configPath, storage, root, "fleet-a")
	eventually(t, 60*time.Second, "the line written while alloy was down", func() bool { return len(second.echoed("job-while-down")) == 1 })
	for _, want := range []string{`{host=\"fleet-a\"`, `os_host=\"node-a\"`} {
		if echoed := second.echoed("job-while-down")[0]; !strings.Contains(echoed, want) {
			t.Fatalf("with MACHINE_ID set, the echoed entry lacks %s:\n%s", want, echoed)
		}
	}
	time.Sleep(time.Second)
	second.stop()
	if n := len(second.echoed("job-before-panewire")) + len(second.echoed("job-before-mergewake")); n != 0 {
		t.Fatalf("the restart re-shipped %d already shipped line(s)", n)
	}
	for _, tool := range []string{"panewire", "mergewake"} {
		job := "job-rotated-" + tool
		if len(first.echoed(job))+len(second.echoed(job)) != 0 {
			t.Fatalf("the rotated %s generation was shipped", tool)
		}
	}
}
