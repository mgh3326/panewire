package configkeys

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// Warn logs each (file, key) once per process, so a file re-read on every
// request does not repeat its warning; a different file still reports its own.
func TestWarnLogsOncePerFileAndKey(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	first := "/etc/test/" + t.Name() + "/placement.json"
	second := "/etc/test/" + t.Name() + "/other.json"

	Warn(logger, first, []string{"wake_on_spill"})
	Warn(logger, first, []string{"wake_on_spill"})
	Warn(logger, second, []string{"wake_on_spill"})

	out := buf.String()
	if n := strings.Count(out, "file="+first); n != 1 {
		t.Fatalf("warnings for %s=%d, want 1; log:\n%s", first, n, out)
	}
	if !strings.Contains(out, "key=wake_on_spill") || !strings.Contains(out, "file="+second) {
		t.Fatalf("second file not reported with key and file:\n%s", out)
	}
}
