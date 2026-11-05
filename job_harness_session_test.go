package panewire

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJobCompletionHarnessSession(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "job-a", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	spawn := `{"kind":"job.spawned","payload":{"pane_id":"w:p1","herdr_session":"fleet","harness_session":{"kind":"codex","id":"unknown","source":"unavailable"}}}`
	if err := os.WriteFile(filepath.Join(events, "00001-job.spawned.json"), []byte(spawn), 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "herdr")
	script := "#!/bin/sh\nif [ \"$HERDR_SESSION\" != fleet ]; then exit 1; fi\nprintf '%s' '{\"agents\":[{\"pane_id\":\"w:p1\",\"agent\":\"codex\",\"agent_session\":{\"agent\":\"codex\",\"kind\":\"id\",\"value\":\"late-id\",\"source\":\"herdr:codex\"}}]}'\n"
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root)
	report := filepath.Join(root, "report.md")
	if err := os.WriteFile(report, []byte("done\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c := &jobCLI{stdout: io.Discard, stderr: io.Discard, now: time.Now}
	if got := c.completionEvent(root, "job-a", "owner", "agent", "w:p1", "host", report, ""); got != "created" {
		t.Fatalf("completion: %s", got)
	}
	data, err := os.ReadFile(filepath.Join(events, "00002-job.completed.json"))
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		HarnessSession struct{ Kind, ID, Source string } `json:"harness_session"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if event.HarnessSession.Kind != "codex" || event.HarnessSession.ID != "late-id" || event.HarnessSession.Source != "herdr:codex" {
		t.Fatalf("event: %s", data)
	}
	if err := os.Remove(bin); err != nil {
		t.Fatal(err)
	}
	session := jobHarnessSession(events, "w:p1")
	if session == nil || session.ID != "unknown" || session.Source != "unavailable" {
		t.Fatalf("fallback: %+v", session)
	}
}
