package panewire

import (
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const procMetricsScript = "deploy/alloy/proc-metrics.sh"

// runProcMetrics runs the macOS textfile script against a fake ps that prints
// psOutput, at the given epoch second, and returns the written textfile.
func runProcMetrics(t *testing.T, dir, psOutput string, now int) string {
	t.Helper()
	fakePS := filepath.Join(t.TempDir(), "fake-ps")
	script := "#!/bin/sh\ncat <<'PS'\n" + psOutput + "PS\n"
	if err := os.WriteFile(fakePS, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("sh", procMetricsScript, dir)
	command.Env = append(os.Environ(), "PROC_METRICS_PS="+fakePS, fmt.Sprintf("PROC_METRICS_NOW=%d", now))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("proc-metrics.sh: %v\n%s", err, output)
	}
	contents, err := os.ReadFile(filepath.Join(dir, "proc-metrics.prom"))
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

// procMetricsSeries parses `name{labels} value` lines into series -> value.
func procMetricsSeries(t *testing.T, text string) map[string]string {
	t.Helper()
	series := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cut := strings.LastIndex(line, " ")
		if cut < 0 {
			t.Fatalf("malformed sample line %q", line)
		}
		series[line[:cut]] = line[cut+1:]
	}
	return series
}

func TestProcMetricsScriptFormat(t *testing.T) {
	dir := t.TempDir()
	text := runProcMetrics(t, dir, strings.Join([]string{
		"  12.5 204800 /usr/libexec/syspolicyd",
		"   7.5 102400 /usr/libexec/syspolicyd",
		"   1.0  51200 /Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
		"   0.0   1024 -zsh",
		"   0.0   1024 /weird/na\"me\\x",
	}, "\n")+"\n", 1000)

	for _, header := range []string{
		"# TYPE namedprocess_namegroup_cpu_seconds_total counter",
		"# TYPE namedprocess_namegroup_memory_bytes gauge",
		"# TYPE namedprocess_namegroup_num_procs gauge",
	} {
		if !strings.Contains(text, header) {
			t.Errorf("missing %q in\n%s", header, text)
		}
	}
	series := procMetricsSeries(t, text)
	want := map[string]string{
		`namedprocess_namegroup_cpu_seconds_total{groupname="syspolicyd"}`:                  "0.000",
		`namedprocess_namegroup_memory_bytes{groupname="syspolicyd",memtype="resident"}`:    "314572800",
		`namedprocess_namegroup_num_procs{groupname="syspolicyd"}`:                          "2",
		`namedprocess_namegroup_memory_bytes{groupname="Google Chrome",memtype="resident"}`: "52428800",
		`namedprocess_namegroup_num_procs{groupname="-zsh"}`:                                "1",
		`namedprocess_namegroup_num_procs{groupname="na_me_x"}`:                             "1",
	}
	for name, value := range want {
		if series[name] != value {
			t.Errorf("%s = %q, want %q", name, series[name], value)
		}
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if strings.Join(names, " ") != ".proc-metrics.state proc-metrics.prom" {
		t.Errorf("output dir holds %v; temp files must not outlive the run", names)
	}
	info, err := os.Stat(filepath.Join(dir, "proc-metrics.prom"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Errorf("textfile mode = %v, want 0644", info.Mode().Perm())
	}
}

func TestProcMetricsScriptLabelValuesAreSafe(t *testing.T) {
	text := runProcMetrics(t, t.TempDir(), "1.0 100 /weird/na\"me\\x\n1.0 100 /tmp/日本語\n", 1000)
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "namedprocess_") {
			continue
		}
		if !regexp.MustCompile(`^[a-z_]+\{groupname="[A-Za-z0-9._ ()+@:_-]+"(,memtype="resident")?\} [0-9.]+$`).MatchString(line) {
			t.Errorf("unsafe or malformed sample: %q", line)
		}
	}
}

func TestProcMetricsScriptTopNCap(t *testing.T) {
	var lines []string
	// 40 groups with distinct CPU and distinct RSS; the CPU ranking is the
	// reverse of the RSS ranking so the two top-15 sets are disjoint.
	for i := 1; i <= 40; i++ {
		lines = append(lines, fmt.Sprintf("%d.0 %d /bin/proc%02d", i, (41-i)*1000, i))
	}
	text := runProcMetrics(t, t.TempDir(), strings.Join(lines, "\n")+"\n", 1000)
	series := procMetricsSeries(t, text)

	groups := map[string]bool{}
	for name := range series {
		groups[regexp.MustCompile(`groupname="([^"]+)"`).FindStringSubmatch(name)[1]] = true
	}
	if len(groups) != 31 {
		t.Fatalf("got %d groups, want 30 named + other: %v", len(groups), groups)
	}
	if len(series) != 31*3 {
		t.Errorf("got %d series, want %d", len(series), 31*3)
	}
	for _, name := range []string{"proc40", "proc26", "proc01", "proc15"} { // top CPU x2, top RSS x2
		if !groups[name] {
			t.Errorf("%s should have its own group", name)
		}
	}
	for _, name := range []string{"proc16", "proc25"} { // middle of both rankings
		if groups[name] {
			t.Errorf("%s should have been folded into other", name)
		}
	}
	// The ten folded groups (proc16..proc25) are summed, not dropped.
	if got := series[`namedprocess_namegroup_num_procs{groupname="other"}`]; got != "10" {
		t.Errorf("other num_procs = %s, want 10", got)
	}
	var wantRSS int
	for i := 16; i <= 25; i++ {
		wantRSS += (41 - i) * 1000 * 1024
	}
	if got := series[`namedprocess_namegroup_memory_bytes{groupname="other",memtype="resident"}`]; got != fmt.Sprint(wantRSS) {
		t.Errorf("other resident = %s, want %d", got, wantRSS)
	}
}

func TestProcMetricsScriptCPUCounterIsMonotonic(t *testing.T) {
	dir := t.TempDir()
	sample := "50.0 1000 /bin/busy\n"
	runProcMetrics(t, dir, sample, 1000)
	second := procMetricsSeries(t, runProcMetrics(t, dir, sample, 1030))
	third := procMetricsSeries(t, runProcMetrics(t, dir, sample, 1060))
	key := `namedprocess_namegroup_cpu_seconds_total{groupname="busy"}`
	if second[key] != "15.000" || third[key] != "30.000" {
		t.Fatalf("cpu seconds after 30s and 60s = %s, %s; want 15.000, 30.000", second[key], third[key])
	}
	// A gap longer than two minutes (a sleeping laptop) is not integrated.
	after := procMetricsSeries(t, runProcMetrics(t, dir, sample, 1060+3600))
	if after[key] != "30.000" {
		t.Fatalf("cpu seconds after a long gap = %s, want 30.000", after[key])
	}
}

func TestProcMetricsScriptRemovesStaleTextfileWhenPSFails(t *testing.T) {
	dir := t.TempDir()
	runProcMetrics(t, dir, "1.0 100 /bin/a\n", 1000)
	failing := filepath.Join(t.TempDir(), "failing-ps")
	if err := os.WriteFile(failing, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("sh", procMetricsScript, dir)
	command.Env = append(os.Environ(), "PROC_METRICS_PS="+failing)
	if err := command.Run(); err == nil {
		t.Fatal("script succeeded although ps failed")
	}
	if _, err := os.Stat(filepath.Join(dir, "proc-metrics.prom")); !os.IsNotExist(err) {
		t.Fatalf("a failed run must remove the stale textfile, stat err = %v", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 || entries[0].Name() != ".proc-metrics.state" {
		t.Fatalf("a failed run must leave only the previous state file: %v %v", entries, err)
	}
}

func TestProcMetricsPlistExample(t *testing.T) {
	contents, err := os.ReadFile("deploy/alloy/proc-metrics.plist.example")
	if err != nil {
		t.Fatal(err)
	}
	if err := xml.Unmarshal(contents, new(struct{})); err != nil {
		t.Fatalf("plist example is not well-formed XML: %v", err)
	}
	text := string(contents)
	for _, required := range []string{"<key>StartInterval</key>", "<integer>30</integer>", "proc-metrics.sh", "FLEET_TEXTFILE_DIR"} {
		if !strings.Contains(text, required) {
			t.Errorf("plist example missing %s", required)
		}
	}
}

// When the CPU and memory leaders are the same programs, the memory ranking
// must still stop at 15 rather than reaching further down the list.
func TestProcMetricsScriptTopNOverlap(t *testing.T) {
	var lines []string
	for i := 1; i <= 40; i++ {
		lines = append(lines, fmt.Sprintf("%d.0 %d /bin/proc%02d", i, i*1000, i))
	}
	series := procMetricsSeries(t, runProcMetrics(t, t.TempDir(), strings.Join(lines, "\n")+"\n", 1000))
	if len(series) != 16*3 {
		t.Fatalf("got %d series, want 15 shared leaders + other = %d", len(series), 16*3)
	}
	if got := series[`namedprocess_namegroup_num_procs{groupname="other"}`]; got != "25" {
		t.Errorf("other num_procs = %s, want 25", got)
	}
}
