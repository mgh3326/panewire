package panewire

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/jobowner"
	"github.com/mgh3326/panewire/internal/laneevent"
)

// hk 7014: `panewire emit --type ready` run inside a job records the READY's
// producer event id in the job dir, so `panewire done` can pass it as the
// completion's result_id without the worker repeating it. Mutants: record on
// every lane.event type, record without a resolvable job dir, keep the first
// READY instead of the latest, or fail the emit when the marker write fails.
func TestEmitReadyRecordsEventIDInJobDir(t *testing.T) {
	root := t.TempDir()
	jobDir := filepath.Join(root, "jobs", "job-1")
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(jobowner.JobEnv, "job-1")
	var stdout, stderr bytes.Buffer
	args := []string{"--kind", "lane.event", "--lane", "lane-a", "--type", "ready", "--event-id", "ready-1", "--text", "READY", "--inbox-root", root}
	if code := runEmitCLI(args, &stdout, &stderr, CLIConfig{SocketPath: filepath.Join(root, "absent.sock")}); code != ExitOK {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	body, err := os.ReadFile(filepath.Join(jobDir, laneevent.ReadyEventFile))
	if err != nil {
		t.Fatalf("ready event record: %v", err)
	}
	if !strings.Contains(string(body), `"ready-1"`) {
		t.Fatalf("recorded body %q, want event id ready-1", body)
	}
	info, err := os.Stat(filepath.Join(jobDir, laneevent.ReadyEventFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode %o, want 0600", info.Mode().Perm())
	}
}

// The generated event id is the one the hub sees, so the record holds exactly
// what emit printed. Mutant: record a different or empty id.
func TestEmitReadyRecordsGeneratedEventID(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "jobs", "job-1"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(jobowner.JobEnv, "job-1")
	var stdout, stderr bytes.Buffer
	args := []string{"--kind", "lane.event", "--lane", "lane-a", "--type", "ready", "--text", "READY", "--inbox-root", root}
	if code := runEmitCLI(args, &stdout, &stderr, CLIConfig{SocketPath: filepath.Join(root, "absent.sock")}); code != ExitOK {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	var printed string
	for _, line := range strings.Split(stdout.String(), "\n") {
		if id, ok := strings.CutPrefix(line, "event_id="); ok {
			printed = id
		}
	}
	if printed == "" {
		t.Fatalf("no event_id in %q", stdout.String())
	}
	record, ok := laneevent.ReadReadyEvent(filepath.Join(root, "jobs", "job-1"))
	if !ok || record.ID != printed {
		t.Fatalf("recorded %+v ok=%t, want printed %q", record, ok, printed)
	}
}

// Only a READY records: another notify type, an emit outside a job (no
// ARBITER_JOB, no --job, or a job dir that does not exist) writes nothing.
// Mutants: record for every type, or create the job dir.
func TestEmitReadyRecordOnlyInsideAJob(t *testing.T) {
	for _, tc := range []struct {
		name    string
		typ     string
		job     string
		env     string
		jobDir  bool
		wantRec bool
	}{
		{name: "info type", typ: "info", env: "job-1", jobDir: true},
		{name: "outside a job", typ: "ready"},
		{name: "job dir absent", typ: "ready", env: "job-1"},
		{name: "job flag names it", typ: "ready", job: "job-2", jobDir: true, wantRec: true},
		{name: "job flag wins over env", typ: "ready", job: "job-2", env: "job-1", jobDir: true, wantRec: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			job := tc.job
			if job == "" {
				job = tc.env
			}
			if job == "" {
				job = "job-1"
			}
			if tc.jobDir {
				if err := os.MkdirAll(filepath.Join(root, "jobs", job), 0700); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv(jobowner.JobEnv, tc.env)
			id := "ev-" + strings.ReplaceAll(tc.name, " ", "-")
			args := []string{"--kind", "lane.event", "--lane", "lane-a", "--type", tc.typ, "--event-id", id, "--text", "text", "--inbox-root", root}
			if tc.job != "" {
				args = append(args, "--job", tc.job)
			}
			var stderr bytes.Buffer
			if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(root, "absent.sock")}); code != ExitOK {
				t.Fatalf("code=%d stderr=%q", code, stderr.String())
			}
			_, ok := laneevent.ReadReadyEvent(filepath.Join(root, "jobs", job))
			if tc.wantRec && !ok {
				t.Fatalf("no record for %s", id)
			}
			if !tc.wantRec && ok {
				t.Fatal("record written outside the contract")
			}
		})
	}
}

// A later READY replaces the recorded id: the completion pairs with the
// newest READY. Mutant: keep the first record (no-clobber write).
func TestEmitReadyLatestReplacesRecord(t *testing.T) {
	root := t.TempDir()
	jobDir := filepath.Join(root, "jobs", "job-1")
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(jobowner.JobEnv, "job-1")
	for _, id := range []string{"ready-1", "ready-2"} {
		var stderr bytes.Buffer
		args := []string{"--kind", "lane.event", "--lane", "lane-a", "--type", "ready", "--event-id", id, "--text", "READY", "--inbox-root", root}
		if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(root, "absent.sock")}); code != ExitOK {
			t.Fatalf("%s: code=%d stderr=%q", id, code, stderr.String())
		}
	}
	record, ok := laneevent.ReadReadyEvent(jobDir)
	if !ok || record.ID != "ready-2" {
		t.Fatalf("recorded %+v ok=%t, want ready-2", record, ok)
	}
}

// The record binds the READY to the attempt's grant epoch: a worker whose
// bound epoch the grant has moved past (a regrant or revocation) still emits
// its lane event but writes no record, so it cannot replace the live
// attempt's pairing. Mutants: skip the epoch check, or fail the emit.
func TestEmitReadyRecordRequiresCurrentGrant(t *testing.T) {
	for _, tc := range []struct {
		name      string
		grant     uint64
		bound     string
		wantRec   bool
		wantEpoch uint64
	}{
		{name: "bound to current grant", grant: 2, bound: "2", wantRec: true, wantEpoch: 2},
		{name: "stale bound epoch", grant: 2, bound: "1"},
		{name: "unbound at epoch 1", grant: 1, wantRec: true, wantEpoch: 1},
		{name: "unbound past epoch 1", grant: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			jobs := filepath.Join(root, "jobs")
			if err := os.MkdirAll(filepath.Join(jobs, "job-1"), 0700); err != nil {
				t.Fatal(err)
			}
			if tc.grant > 1 {
				if err := jobowner.Assign(jobs, "job-1", tc.grant); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv(jobowner.JobEnv, "job-1")
			t.Setenv(jobowner.EpochEnv, tc.bound)
			var stderr bytes.Buffer
			args := []string{"--kind", "lane.event", "--lane", "lane-a", "--type", "ready", "--event-id", "ready-epoch", "--text", "READY", "--inbox-root", root}
			if code := runEmitCLI(args, &bytes.Buffer{}, &stderr, CLIConfig{SocketPath: filepath.Join(root, "absent.sock")}); code != ExitOK {
				t.Fatalf("code=%d stderr=%q", code, stderr.String())
			}
			record, ok := laneevent.ReadReadyEvent(filepath.Join(jobs, "job-1"))
			if tc.wantRec {
				if !ok || record.ID != "ready-epoch" || record.Epoch != tc.wantEpoch {
					t.Fatalf("record %+v ok=%t, want id ready-epoch epoch %d", record, ok, tc.wantEpoch)
				}
			} else if ok {
				t.Fatalf("stale or unbound emit recorded %+v", record)
			}
		})
	}
}
