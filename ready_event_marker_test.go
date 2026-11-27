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
	if got := strings.TrimSpace(string(body)); got != "ready-1" {
		t.Fatalf("recorded id %q, want ready-1", got)
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
	if got := laneevent.ReadReadyEvent(filepath.Join(root, "jobs", "job-1")); got != printed {
		t.Fatalf("recorded id %q, want printed %q", got, printed)
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
			got := laneevent.ReadReadyEvent(filepath.Join(root, "jobs", job))
			if tc.wantRec && got != id {
				t.Fatalf("recorded id %q, want %q", got, id)
			}
			if !tc.wantRec && got != "" {
				t.Fatalf("recorded id %q outside the contract", got)
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
	if got := laneevent.ReadReadyEvent(jobDir); got != "ready-2" {
		t.Fatalf("recorded id %q, want ready-2", got)
	}
}
