package fleet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A node daemon whose environment sets FLEET_TEXTFILE_DIR publishes the pass's
// metric there for Alloy's textfile collector — which on Linux runs as its own
// user, so it cannot read the 0600 state-dir copy — and keeps that copy.
func TestPaneSweepPublishesTheMetricToTheTextfileDir(t *testing.T) {
	f := newPaneSweepFixture(t)
	dir := t.TempDir()
	t.Setenv(paneSweepTextfileDirEnv, dir)
	pane := f.job("r", "reviewer", "")
	f.sessions[pane] = `{"value":"sess-other"}`
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.endedSweep(61, time.Hour)
	path := filepath.Join(dir, paneSweepMetricFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("the metric was not published to %s: %v", paneSweepTextfileDirEnv, err)
	}
	if perm := info.Mode().Perm(); perm != 0644 {
		t.Fatalf("published metric mode = %04o, want 0644", perm)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `panewire_ended_job_panes_open{reason="session_mismatch"} 1`) {
		t.Fatalf("published metric = %q", body)
	}
	if state := readPaneSweepMetric(t, f.a); state != string(body) {
		t.Fatalf("state-dir copy = %q, published copy = %q", state, body)
	}
	if warnings := f.a.Err.(*bytes.Buffer).String(); warnings != "" {
		t.Fatalf("a successful publication warned: %q", warnings)
	}
}

// Without FLEET_TEXTFILE_DIR the pass leaves only the state-dir copy.
func TestPaneSweepWritesNoMetricWithoutTheTextfileDir(t *testing.T) {
	f := newPaneSweepFixture(t)
	dir := t.TempDir()
	t.Setenv(paneSweepTextfileDirEnv, "")
	pane := f.job("r", "reviewer", "")
	f.sessions[pane] = `{"value":"sess-other"}`
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.endedSweep(61, time.Hour)
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("an unset %s still wrote %v (err %v)", paneSweepTextfileDirEnv, entries, err)
	}
	if metric := readPaneSweepMetric(t, f.a); !strings.Contains(metric, `reason="session_mismatch"`) {
		t.Fatalf("state-dir metric = %q", metric)
	}
}

// A textfile directory that does not exist warns once per reason instead of on
// every pass, and never fails the pass or drops the state-dir copy.
func TestPaneSweepWarnsOnceWhenTheTextfileDirIsMissing(t *testing.T) {
	f := newPaneSweepFixture(t)
	t.Setenv(paneSweepTextfileDirEnv, filepath.Join(t.TempDir(), "absent"))
	pane := f.job("r", "reviewer", "")
	f.sessions[pane] = `{"value":"sess-other"}`
	putEvent(t, f.a, "r", "job.completed", map[string]any{})
	f.endedSweep(61, time.Hour)
	f.endedSweep(62, time.Hour)
	warnings := f.a.Err.(*bytes.Buffer).String()
	if got := strings.Count(warnings, "pane metric not written to FLEET_TEXTFILE_DIR (dir_missing)"); got != 1 {
		t.Fatalf("warned %d times for one reason: %q", got, warnings)
	}
	if metric := readPaneSweepMetric(t, f.a); !strings.Contains(metric, `reason="session_mismatch"`) {
		t.Fatalf("state-dir metric = %q", metric)
	}
}
