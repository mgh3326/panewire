package panewire

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The completion watcher finds a worker's READY by the pane on the record, so
// emit must record the pane whether or not the worker passes --pane.
func TestEmitLaneEventRecordsPane(t *testing.T) {
	for _, tt := range []struct {
		name, env string
		args      []string
		want      string
	}{
		{name: "from the pane environment", env: "w1:p1", want: "w1:p1"},
		{name: "explicit flag wins", env: "w1:p1", args: []string{"--pane", "w1:p2"}, want: "w1:p2"},
		{name: "outside a pane", want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HERDR_PANE_ID", tt.env)
			root := t.TempDir()
			args := append([]string{"--kind", "lane.event", "--lane", "lane-a", "--event-id", "pane-stamp", "--text", "READY", "--inbox-root", root}, tt.args...)
			var stderr bytes.Buffer
			if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(root, "absent.sock")}); code != ExitOK {
				t.Fatalf("code=%d stderr=%q", code, stderr.String())
			}
			matches, err := filepath.Glob(filepath.Join(root, "events-lane", "*.json"))
			if err != nil || len(matches) != 1 {
				t.Fatalf("records=%v err=%v", matches, err)
			}
			body, err := os.ReadFile(matches[0])
			if err != nil {
				t.Fatal(err)
			}
			var record emitRecord
			if err := json.Unmarshal(body, &record); err != nil {
				t.Fatal(err)
			}
			if record.PaneID != tt.want {
				t.Fatalf("pane_id=%q, want %q", record.PaneID, tt.want)
			}
		})
	}
}
