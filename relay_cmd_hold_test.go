package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRelayLaneJobEndedAcceptsDottedJobID(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "jobs", "legacy.parent", "events")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"00001-job.claim.json":     `{"kind":"job.claim"}`,
		"00002-job.completed.json": `{"kind":"job.completed"}`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if !relayLaneJobEnded(root, "legacy.parent") {
		t.Fatal("dotted job id was refused before reading its terminal event")
	}
}
