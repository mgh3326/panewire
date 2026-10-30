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

// procLine is one fake `ps -o pid=,ppid=,time=,rss=,comm=` row.
func procLine(pid, ppid int, cpuTime string, rssKiB int, comm string) string {
	return fmt.Sprintf("%d %d %s %d %s\n", pid, ppid, cpuTime, rssKiB, comm)
}

// runProcMetricsOS runs the textfile script against a fake ps that prints
// psOutput, as the given uname -s, at the given epoch second, and returns the
// written textfile.
func runProcMetricsOS(t *testing.T, dir, osName, psOutput string, now int) string {
	t.Helper()
	fakePS := filepath.Join(t.TempDir(), "fake-ps")
	script := "#!/bin/sh\ncat <<'PS'\n" + psOutput + "PS\n"
	if err := os.WriteFile(fakePS, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("sh", procMetricsScript, dir)
	command.Env = append(os.Environ(), "PROC_METRICS_PS="+fakePS, "PROC_METRICS_OS="+osName, fmt.Sprintf("PROC_METRICS_NOW=%d", now))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("proc-metrics.sh: %v\n%s", err, output)
	}
	contents, err := os.ReadFile(filepath.Join(dir, "proc-metrics.prom"))
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

func runProcMetrics(t *testing.T, dir, psOutput string, now int) string {
	t.Helper()
	return runProcMetricsOS(t, dir, "Linux", psOutput, now)
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

func cpuKey(group string) string {
	return fmt.Sprintf(`namedprocess_namegroup_cpu_seconds_total{groupname=%q}`, group)
}

func TestProcMetricsScriptFormat(t *testing.T) {
	dir := t.TempDir()
	text := runProcMetricsOS(t, dir, "Darwin", strings.Join([]string{
		procLine(10, 1, "0:12.50", 204800, "/usr/libexec/syspolicyd"),
		procLine(11, 1, "0:07.50", 102400, "/usr/libexec/syspolicyd"),
		procLine(12, 1, "1:00.00", 51200, "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
		procLine(13, 1, "0:00.00", 1024, "-zsh"),
		procLine(14, 1, "0:00.00", 1024, `/weird/na"me\x`),
	}, ""), 1000)

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
		cpuKey("syspolicyd"): "0.000", // first run only baselines
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
	text := runProcMetrics(t, t.TempDir(), procLine(10, 1, "0:01", 100, `/weird/na"me\x`)+procLine(11, 1, "0:01", 100, "/tmp/日本語"), 1000)
	for _, line := range strings.Split(text, "\n") {
		if !strings.HasPrefix(line, "namedprocess_") {
			continue
		}
		if !regexp.MustCompile(`^[a-z_]+\{groupname="[A-Za-z0-9._ ()+@:_-]+"(,memtype="resident")?\} [0-9.]+$`).MatchString(line) {
			t.Errorf("unsafe or malformed sample: %q", line)
		}
	}
}

// topNSample builds 40 groups. In the baseline run every process has used
// 100s; in the second run proc i has used another i seconds (so CPU rank is
// i) and holds (offset+i)*1000 KiB.
func topNSample(rssOf func(i int) int, extraCPU bool) string {
	var out strings.Builder
	for i := 1; i <= 40; i++ {
		used := 100
		if extraCPU {
			used += i
		}
		out.WriteString(procLine(100+i, 1, fmt.Sprintf("0:%02d:%02d", used/60, used%60), rssOf(i), fmt.Sprintf("/bin/proc%02d", i)))
	}
	return out.String()
}

func TestProcMetricsScriptTopNCap(t *testing.T) {
	dir := t.TempDir()
	// The CPU ranking is the reverse of the RSS ranking so the two top-15 sets are disjoint.
	rss := func(i int) int { return (41 - i) * 1000 }
	runProcMetrics(t, dir, topNSample(rss, false), 1000)
	series := procMetricsSeries(t, runProcMetrics(t, dir, topNSample(rss, true), 1030))

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
	var wantRSS, wantCPU int
	for i := 16; i <= 25; i++ {
		wantRSS += (41 - i) * 1000 * 1024
		wantCPU += i
	}
	if got := series[`namedprocess_namegroup_memory_bytes{groupname="other",memtype="resident"}`]; got != fmt.Sprint(wantRSS) {
		t.Errorf("other resident = %s, want %d", got, wantRSS)
	}
	if got := series[cpuKey("other")]; got != fmt.Sprintf("%d.000", wantCPU) {
		t.Errorf("other cpu = %s, want %d.000", got, wantCPU)
	}
	if got := series[cpuKey("proc40")]; got != "40.000" {
		t.Errorf("proc40 cpu = %s, want 40.000", got)
	}
}

// When the CPU and memory leaders are the same programs, the memory ranking
// must still stop at 15 rather than reaching further down the list.
func TestProcMetricsScriptTopNOverlap(t *testing.T) {
	dir := t.TempDir()
	rss := func(i int) int { return i * 1000 }
	runProcMetrics(t, dir, topNSample(rss, false), 1000)
	series := procMetricsSeries(t, runProcMetrics(t, dir, topNSample(rss, true), 1030))
	if len(series) != 16*3 {
		t.Fatalf("got %d series, want 15 shared leaders + other = %d", len(series), 16*3)
	}
	if got := series[`namedprocess_namegroup_num_procs{groupname="other"}`]; got != "25" {
		t.Errorf("other num_procs = %s, want 25", got)
	}
}

func TestProcMetricsScriptCPUUsesCumulativeTimeDeltas(t *testing.T) {
	dir := t.TempDir()
	// A long-running process that was busy for days but is idle now must add nothing.
	idle := procLine(10, 1, "5-04:03:02", 1000, "/bin/oldbusy") + procLine(20, 1, "0:10", 1000, "/bin/worker")
	runProcMetrics(t, dir, idle, 1000)
	second := procMetricsSeries(t, runProcMetrics(t, dir,
		procLine(10, 1, "5-04:03:02", 1000, "/bin/oldbusy")+procLine(20, 1, "0:25", 1000, "/bin/worker")+
			procLine(30, 1, "0:07", 1000, "/bin/newcomer"), 1030))
	if second[cpuKey("oldbusy")] != "0.000" {
		t.Errorf("idle process added CPU: %s", second[cpuKey("oldbusy")])
	}
	if second[cpuKey("worker")] != "15.000" {
		t.Errorf("worker cpu = %s, want 15.000", second[cpuKey("worker")])
	}
	// A process first seen since the previous run contributes all its CPU time.
	if second[cpuKey("newcomer")] != "7.000" {
		t.Errorf("newcomer cpu = %s, want 7.000", second[cpuKey("newcomer")])
	}
	// The counter accumulates across runs and never goes backwards.
	third := procMetricsSeries(t, runProcMetrics(t, dir,
		procLine(10, 1, "5-04:03:02", 1000, "/bin/oldbusy")+procLine(20, 1, "1:00:00", 1000, "/bin/worker")+
			procLine(30, 1, "0:09", 1000, "/bin/newcomer"), 1060))
	if third[cpuKey("worker")] != "3590.000" || third[cpuKey("newcomer")] != "9.000" {
		t.Errorf("after third run worker=%s newcomer=%s, want 3590.000 and 9.000", third[cpuKey("worker")], third[cpuKey("newcomer")])
	}
	// A reused pid (cumulative time went down, or a different program) starts over.
	fourth := procMetricsSeries(t, runProcMetrics(t, dir,
		procLine(20, 1, "0:02", 1000, "/bin/worker")+procLine(30, 1, "0:03", 1000, "/bin/other-prog"), 1090))
	if fourth[cpuKey("worker")] != "3592.000" || fourth[cpuKey("other-prog")] != "3.000" {
		t.Errorf("after pid reuse worker=%s other-prog=%s, want 3592.000 and 3.000", fourth[cpuKey("worker")], fourth[cpuKey("other-prog")])
	}
	// A gap longer than two minutes (a sleeping laptop) only re-baselines.
	after := procMetricsSeries(t, runProcMetrics(t, dir, procLine(20, 1, "0:30", 1000, "/bin/worker"), 1090+3600))
	if after[cpuKey("worker")] != "3592.000" {
		t.Errorf("cpu after a long gap = %s, want 3592.000", after[cpuKey("worker")])
	}
}

func TestProcMetricsScriptSkipsLinuxKernelThreads(t *testing.T) {
	sample := procLine(1, 0, "0:05", 1000, "systemd") +
		procLine(2, 0, "0:00", 0, "kthreadd") +
		procLine(15, 2, "0:09", 0, "kworker/0:1") +
		procLine(16, 2, "0:09", 0, "ksoftirqd/0")
	linux := procMetricsSeries(t, runProcMetricsOS(t, t.TempDir(), "Linux", sample, 1000))
	if len(linux) != 3 || linux[`namedprocess_namegroup_num_procs{groupname="systemd"}`] != "1" {
		t.Errorf("Linux sample should keep only systemd, got %v", linux)
	}
	// pid 2 is not special on macOS, where kernel_task is worth seeing.
	darwin := procMetricsSeries(t, runProcMetricsOS(t, t.TempDir(), "Darwin", sample, 1000))
	if darwin[`namedprocess_namegroup_num_procs{groupname="kthreadd"}`] != "1" {
		t.Errorf("Darwin sample should keep every process, got %v", darwin)
	}
}

func TestProcMetricsScriptRemovesStaleTextfileWhenPSFails(t *testing.T) {
	dir := t.TempDir()
	runProcMetrics(t, dir, procLine(10, 1, "0:01", 100, "/bin/a"), 1000)
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

func TestProcMetricsUnitExamples(t *testing.T) {
	plist, err := os.ReadFile("deploy/alloy/proc-metrics.plist.example")
	if err != nil {
		t.Fatal(err)
	}
	if err := xml.Unmarshal(plist, new(struct{})); err != nil {
		t.Fatalf("plist example is not well-formed XML: %v", err)
	}
	for _, required := range []string{"<key>StartInterval</key>", "<integer>30</integer>", "proc-metrics.sh", "FLEET_TEXTFILE_DIR"} {
		if !strings.Contains(string(plist), required) {
			t.Errorf("plist example missing %s", required)
		}
	}
	service, err := os.ReadFile("deploy/alloy/proc-metrics.service.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Type=oneshot", "FLEET_TEXTFILE_DIR=", "ExecStart=/ABSOLUTE/PATH/TO/proc-metrics.sh"} {
		if !strings.Contains(string(service), required) {
			t.Errorf("service example missing %s", required)
		}
	}
	timer, err := os.ReadFile("deploy/alloy/proc-metrics.timer.example")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(timer), "OnUnitActiveSec=30s") {
		t.Error("timer example must fire every 30 seconds")
	}
}
