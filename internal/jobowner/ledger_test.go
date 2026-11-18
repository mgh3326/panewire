package jobowner

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestProducerEpoch(t *testing.T) {
	jobs := filepath.Join(t.TempDir(), "jobs")
	for _, tc := range []struct {
		name            string
		grant           uint64
		bound, explicit uint64
		want            uint64
		err             error
	}{
		{name: "no identity at the initial grant", grant: 1, want: 1},
		{name: "no identity above the initial grant", grant: 3, err: ErrIdentityRequired},
		{name: "bound current", grant: 3, bound: 3, want: 3},
		{name: "bound superseded", grant: 3, bound: 1, err: ErrStaleAttempt},
		{name: "bound ahead", grant: 3, bound: 4, err: ErrAttemptAhead},
		{name: "explicit current", grant: 3, explicit: 3, want: 3},
		{name: "explicit superseded", grant: 3, explicit: 1, err: ErrStaleAttempt},
		{name: "explicit equals bound", grant: 3, bound: 3, explicit: 3, want: 3},
		{name: "explicit overrides bound", grant: 3, bound: 1, explicit: 3, err: ErrEpochConflict},
		{name: "bound at the initial grant", grant: 1, bound: 1, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := "job-" + strings.ReplaceAll(tc.name, " ", "-")
			if tc.grant > 1 {
				if err := Assign(jobs, id, tc.grant); err != nil {
					t.Fatal(err)
				}
			}
			got, err := ProducerEpoch(jobs, id, tc.bound, tc.explicit)
			if !errors.Is(err, tc.err) || got != tc.want {
				t.Fatalf("ProducerEpoch = %d, %v; want %d, %v", got, err, tc.want, tc.err)
			}
		})
	}
}
