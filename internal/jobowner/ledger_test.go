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

func TestProducerEpochRefusesARevokedGrantUntilSuperseded(t *testing.T) {
	jobs := filepath.Join(t.TempDir(), "jobs")
	if err := Revoke(jobs, "job-a", 2); err != nil {
		t.Fatal(err)
	}
	for _, bound := range []uint64{0, 1} {
		if _, err := ProducerEpoch(jobs, "job-a", bound, 0); !errors.Is(err, ErrRevoked) {
			t.Fatalf("bound %d: err=%v, want ErrRevoked", bound, err)
		}
	}
	if err := Assign(jobs, "job-a", 3); err != nil {
		t.Fatal(err)
	}
	if got, err := ProducerEpoch(jobs, "job-a", 3, 0); err != nil || got != 3 {
		t.Fatalf("after regrant: %d, %v", got, err)
	}
	// A local end writes job.revoked with a kind and no epoch; it is not a hub revocation.
	local := filepath.Join(jobs, "job-b", "events")
	if err := os.MkdirAll(local, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(local, "00001-job.revoked.json"), []byte(`{"kind":"job.revoked","job_id":"job-b"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := ProducerEpoch(jobs, "job-b", 0, 0); err != nil || got != 1 {
		t.Fatalf("local revocation: %d, %v", got, err)
	}
}
