package fleet

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/jobowner"
)

// The spawn receipt names the grant epoch the attempt launched under, so a
// ladder close can tell it from a later grant of the same job (hk 5864).
func TestSpawnReceiptNamesTheGrantEpoch(t *testing.T) {
	a := &App{Jobs: t.TempDir(), Now: time.Now, Runner: &fakeRunner{}, Err: os.Stderr}
	s := Spawn{Job: "job-a", Label: "agent-a"}
	if got := a.spawnReceipt(t.Context(), s, Seat{}, Launch{}, "w:p1", "w:t1", "pool", "owner", "machine")["epoch"]; got != "1" {
		t.Fatalf("a job with no regrant: epoch=%v, want \"1\"", got)
	}
	if err := jobowner.Assign(a.Jobs, "job-a", 3); err != nil {
		t.Fatal(err)
	}
	if got := a.spawnReceipt(t.Context(), s, Seat{}, Launch{}, "w:p1", "w:t1", "pool", "owner", "machine")["epoch"]; got != "3" {
		t.Fatalf("regranted job: epoch=%v, want \"3\"", got)
	}
}

// A hub revocation marker has no kind field; the question scan reads past it,
// as it does every other event, instead of failing the whole close.
func TestOpenLaneQuestionReadsPastARevocationMarker(t *testing.T) {
	inbox := t.TempDir()
	writeDecisionLedger(t, inbox, "job-a", map[string]any{"kind": "job.spawned", "created_at": time.Now().UTC().Format(time.RFC3339Nano), "payload": map[string]any{"pane_id": "w1:p1"}})
	marker := filepath.Join(inbox, "jobs", "job-a", "events", "00002-job.revoked.json")
	if err := os.WriteFile(marker, []byte(`{"type":"job.revoked","job_id":"job-a","epoch":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, open, err := OpenLaneQuestion(inbox, "job-a", "w1:p1", time.Now(), DefaultDecisionTimeout); err != nil || open {
		t.Fatalf("open=%v err=%v, want no question and no error", open, err)
	}
}
