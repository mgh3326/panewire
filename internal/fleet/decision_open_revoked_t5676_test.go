package fleet

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A hub revocation marker (type job.revoked, no kind) stays in a job's ledger
// after the job is granted back; the question scan must read past it, and still
// refuse a record that is neither.
func TestOpenLaneQuestionReadsAPastHubRevocation_t5676(t *testing.T) {
	root := t.TempDir()
	events := filepath.Join(root, "jobs", "job-1", "events")
	if err := os.MkdirAll(events, 0o700); err != nil {
		t.Fatal(err)
	}
	write := func(name, body string) {
		if err := os.WriteFile(filepath.Join(events, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("00001-job.revoked.json", `{"type":"job.revoked","job_id":"job-1","epoch":2}`)
	write("00002-job.spawned.json", `{"kind":"job.spawned","created_at":"2026-11-14T00:00:01Z","payload":{"pane_id":"w1:p1"}}`)
	if _, open, err := OpenLaneQuestion(root, "job-1", "w1:p1", time.Now(), 0); err != nil || open {
		t.Fatalf("open=%v err=%v", open, err)
	}
	write("00003-job.mystery.json", `{"type":"job.something","epoch":1}`)
	if _, _, err := OpenLaneQuestion(root, "job-1", "w1:p1", time.Now(), 0); err == nil {
		t.Fatal("a record with neither a kind nor a hub revocation type must stay unreadable")
	}
}
