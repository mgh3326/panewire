package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

// The fleet end-path tests (internal/fleet hub_revocation_end_t5062_test.go)
// write the hub revocation record by hand, because an internal package cannot
// import this one. This pins that hand-written body and file name to what
// writeHubRevocation really writes, so the two cannot drift apart (hk 5062).
func TestWriteHubRevocationRecordShapeMatchesFleetFixture_t5062(t *testing.T) {
	inbox := t.TempDir()
	if err := writeHubRevocation(inbox, hubJobRevokedEvent{Type: "job.revoked", JobID: "j", Epoch: 2}); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(inbox, "jobs", "j", "events", "*-job.revoked.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("revocation files = %v, err = %v", files, err)
	}
	if name := filepath.Base(files[0]); name != "00001-job.revoked.json" {
		t.Fatalf("file name = %q", name)
	}
	body, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"job.revoked","job_id":"j","epoch":2}`; string(body) != want {
		t.Fatalf("record = %s, want %s", body, want)
	}
}
