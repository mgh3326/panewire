//go:build unix

package devin

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/quota"
)

// fakeDevin is a shell script that behaves like the devin TUI on a real
// pseudo-terminal: it draws its boot screen ending in the U+276D prompt, waits
// for a typed line, and on /usage draws the panel and then idles for a minute
// (as devin does), so only the probe's own teardown can end it.
const fakeDevin = `#!/bin/sh
echo $$ > "$FAKE_DEVIN_PID"
printf 'Devin CLI\r\nv3000.11.234\r\n\r\n\342\235\255 Ask Devin to build features\r\n'
while read line; do
  case "$line" in
    /usage) printf 'Usage\r\nWeekly: 62%% used \302\267 resets in 6d 2h\r\n'; sleep 60 ;;
  esac
done
`

// The real pseudo-terminal path: the binary is started on a PTY in a private
// directory, the U+276D prompt is recognised on wall-clock time, the panel is
// parsed, and the process is gone when the measurement returns.
func TestProbeOnARealPTY(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "devin")
	if err := os.WriteFile(script, []byte(fakeDevin), 0o700); err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(dir, "pid")
	t.Setenv("FAKE_DEVIN_PID", pidFile)
	cfg := config(loginFiles(), map[string]string{"PACELEVER_DEVIN_BIN": script})
	cfg.LookPath = func(name string) (string, error) { return name, nil }
	cfg.ProbeTimeout = 20 * time.Second // an upper bound; the probe takes about a second
	cfg.Binary = script
	o, err := Measure(t.Context(), cfg)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if o.Source != quota.SourceCLI || len(o.Buckets) != 1 || o.Buckets[0].Window != "7d" || *o.Buckets[0].UsedPct != 62 {
		t.Fatalf("observation %+v", o)
	}
	data, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatalf("the devin process %d outlived the measurement", pid)
	}
}

// A binary that is not there is probe_error, with nothing started.
func TestProbeWithoutADevinBinary(t *testing.T) {
	cfg := config(loginFiles(), map[string]string{"PACELEVER_DEVIN_BIN": filepath.Join(t.TempDir(), "missing")})
	if _, err := Measure(t.Context(), cfg); kindOf(err) != quota.ErrKindProbe {
		t.Fatalf("err = %v", err)
	}
}
