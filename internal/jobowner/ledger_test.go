package jobowner

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAssignmentSurvivesRestartAndDoesNotRegress(t *testing.T) {
	jobs := filepath.Join(t.TempDir(), "jobs")
	if err := Assign(jobs, "job-a", 3); err != nil {
		t.Fatal(err)
	}
	if err := Assign(jobs, "job-a", 2); err != nil {
		t.Fatal(err)
	}
	epoch, err := Epoch(jobs, "job-a")
	if err != nil || epoch != 3 {
		t.Fatalf("epoch=%d err=%v, want persisted epoch 3", epoch, err)
	}
	info, err := os.Stat(filepath.Join(jobs, "job-a", assignmentFile))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("assignment mode=%v err=%v", info, err)
	}
}

func TestDelayedRevocationDoesNotRevokeNewGrant(t *testing.T) {
	jobs := filepath.Join(t.TempDir(), "jobs")
	if err := Assign(jobs, "job-a", 3); err != nil {
		t.Fatal(err)
	}
	if err := Revoke(jobs, "job-a", 2); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(jobs, "job-a", "events", "*-job.revoked.json"))
	if err != nil || len(files) != 0 {
		t.Fatalf("delayed revocation replaced newer grant: files=%v err=%v", files, err)
	}
	if err := Revoke(jobs, "job-a", 4); err != nil {
		t.Fatal(err)
	}
	files, err = filepath.Glob(filepath.Join(jobs, "job-a", "events", "*-job.revoked.json"))
	if err != nil || len(files) != 1 {
		t.Fatalf("newer revocation missing: files=%v err=%v", files, err)
	}
}
