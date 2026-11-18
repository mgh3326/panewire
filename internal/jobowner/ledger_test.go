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
