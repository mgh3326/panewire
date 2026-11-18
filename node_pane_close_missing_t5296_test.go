package panewire

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// The missing-job answer of pane.close_status (hk 5296): the node proves a job
// directory gone, fences the request with a durable tombstone outside every
// job directory, and a delayed pane.close_op under that id never acts on a
// directory recreated later. docs/node-commands.md, "A missing job".

// recordJobIdentity writes the job's identity record the way a spawn does: a
// private file under the jobs root's .job-ids, which outlives the directory.
func recordJobIdentity(t *testing.T, inbox, id string) {
	t.Helper()
	jobs, err := filepath.EvalSymlinks(filepath.Join(inbox, "jobs"))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(jobs, ".job-ids")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, strings.ToLower(id)), []byte(id+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func missingJobNode(t *testing.T, inbox string, fake *nodeFakeHerdr) *HubClient {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(inbox, "jobs"), 0o700); err != nil {
		t.Fatal(err)
	}
	return &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox,
		nodeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}}
}

func missingStatusArgs(id string, epoch int) map[string]any {
	return map[string]any{"request_id": id, "job": closeOpJob, "epoch": epoch}
}

func tombstoneFile(inbox, id string) string {
	return filepath.Join(inbox, "jobs", ".close-missing", id+".json")
}

func TestCloseStatusProvesAMissingJob_t5296(t *testing.T) {
	inbox := t.TempDir()
	client := missingJobNode(t, inbox, idlePaneHerdr())
	recordJobIdentity(t, inbox, closeOpJob)
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, missingStatusArgs("op-1", 3))
	if code != "" || result["state"] != nodeCloseOpNone || result["missing_epoch"] != float64(3) || result["attempt"] != "" || result["released"] != false {
		t.Fatalf("status=%v code=%q, want the missing-job proof at epoch 3", result, code)
	}
	info, err := os.Stat(tombstoneFile(inbox, "op-1"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("tombstone info=%v err=%v, want a mode 0600 file", info, err)
	}
	// The same ask again answers the same proof, and the epoch never lowers.
	for _, epoch := range []int{3, 2} {
		again, code := nodeRun(t, client, nodeCommandPaneCloseStatus, missingStatusArgs("op-1", epoch))
		if code != "" || again["missing_epoch"] != float64(3) {
			t.Fatalf("repeat at epoch %d: %v code=%q", epoch, again, code)
		}
	}
}

// Bare absence is not proof: each of these answers plain none, and leaves no
// tombstone, so the hub keeps its hold.
func TestCloseStatusBareAbsenceIsNotProof_t5296(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, inbox string)
		args  map[string]any
		code  string
	}{
		{"no identity record", func(t *testing.T, inbox string) {}, missingStatusArgs("op-1", 1), ""},
		{"no epoch asked", func(t *testing.T, inbox string) { recordJobIdentity(t, inbox, closeOpJob) }, closeOpRef("op-1"), ""},
		{"identity of another spelling", func(t *testing.T, inbox string) { recordJobIdentity(t, inbox, strings.ToUpper(closeOpJob)) }, missingStatusArgs("op-1", 1), ""},
		{"unreadable identity record", func(t *testing.T, inbox string) {
			recordJobIdentity(t, inbox, closeOpJob)
			path := filepath.Join(inbox, "jobs", ".job-ids", closeOpJob)
			if err := os.WriteFile(path, []byte("not an id"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, missingStatusArgs("op-1", 1), ""},
		{"job entry is not a directory", func(t *testing.T, inbox string) {
			recordJobIdentity(t, inbox, closeOpJob)
			if err := os.WriteFile(filepath.Join(inbox, "jobs", closeOpJob), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, missingStatusArgs("op-1", 1), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inbox := t.TempDir()
			client := missingJobNode(t, inbox, idlePaneHerdr())
			tc.setup(t, inbox)
			result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, tc.args)
			if code != tc.code {
				t.Fatalf("code=%q, want %q (result %v)", code, tc.code, result)
			}
			if code == "" && (result["state"] != nodeCloseOpNone || result["missing_epoch"] != nil) {
				t.Fatalf("status=%v, want plain none", result)
			}
			if _, err := os.Stat(tombstoneFile(inbox, "op-1")); !os.IsNotExist(err) {
				t.Fatalf("a tombstone was written without proof: %v", err)
			}
		})
	}
}

// A jobs root that is not there (an unmounted volume, a wrong inbox) proves
// nothing, and the node does not create it.
func TestCloseStatusMissingJobsRootIsNotProof_t5296(t *testing.T) {
	inbox := t.TempDir()
	client := &HubClient{nodeHerdr: idlePaneHerdr().run, jobsInboxRoot: inbox, nodeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}}
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, missingStatusArgs("op-1", 1))
	if code != "" || result["state"] != nodeCloseOpNone || result["missing_epoch"] != nil {
		t.Fatalf("status=%v code=%q", result, code)
	}
	if _, err := os.Stat(filepath.Join(inbox, "jobs")); !os.IsNotExist(err) {
		t.Fatalf("the jobs root was created: %v", err)
	}
}

// An unreadable ledger is a job that is there: the directory exists, so the
// node never answers it as missing.
func TestCloseStatusUnreadableLedgerIsNotMissing_t5296(t *testing.T) {
	inbox := t.TempDir()
	client := missingJobNode(t, inbox, idlePaneHerdr())
	recordJobIdentity(t, inbox, closeOpJob)
	if err := os.MkdirAll(filepath.Join(inbox, "jobs", closeOpJob), 0o700); err != nil {
		t.Fatal(err)
	}
	result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, missingStatusArgs("op-1", 1))
	if code != "" || result["state"] != nodeCloseOpNone || result["attempt"] != "" || result["missing_epoch"] != nil {
		t.Fatalf("status=%v code=%q, want none with no proof", result, code)
	}
	if _, err := os.Stat(tombstoneFile(inbox, "op-1")); !os.IsNotExist(err) {
		t.Fatalf("a tombstone was written for a present directory: %v", err)
	}
}

// The delayed close: the directory is recreated for a new attempt (the same
// attempt token, pane and epoch, as a respawn under the same id looks) after
// the node fenced the request. The old request must not close the new pane;
// a new request id still can.
func TestDelayedCloseAfterRecreationIsRefused_t5296(t *testing.T) {
	inbox := t.TempDir()
	fake := idlePaneHerdr()
	client := missingJobNode(t, inbox, fake)
	recordJobIdentity(t, inbox, closeOpJob)
	if result, code := nodeRun(t, client, nodeCommandPaneCloseStatus, missingStatusArgs("op-old", 1)); code != "" || result["missing_epoch"] != float64(1) {
		t.Fatalf("status=%v code=%q", result, code)
	}
	seedCloseOpJob(t, inbox)
	wantCloseOp(t, client, "op-old", fleet.CloseOpCancelled, true)
	if fake.called("pane", "close") != 0 {
		t.Fatalf("a delayed close acted on the recreated job: %v", fake.calls)
	}
	// A restart loses nothing: the tombstone is a file.
	restarted := &HubClient{nodeHerdr: fake.run, nodeCloseSettle: time.Millisecond, jobsInboxRoot: inbox, nodeHubClose: &fakeHubClose{watcher: fleet.HubCloseWatcherAware}}
	wantCloseOp(t, restarted, "op-old", fleet.CloseOpCancelled, true)
	// The control: the recreated job is closable, so the refusal above is the
	// tombstone's and not the fixture's.
	wantCloseOp(t, restarted, "op-new", fleet.CloseOpClosed, false)
	if fake.called("pane", "close") != 1 {
		t.Fatalf("the new request did not close the pane: %v", fake.calls)
	}
}

func TestMissingJobTombstoneBindsItsJob_t5296(t *testing.T) {
	inbox := t.TempDir()
	client := missingJobNode(t, inbox, idlePaneHerdr())
	recordJobIdentity(t, inbox, closeOpJob)
	if _, code := nodeRun(t, client, nodeCommandPaneCloseStatus, missingStatusArgs("op-old", 2)); code != "" {
		t.Fatalf("code=%q", code)
	}
	seedCloseOpJob(t, inbox)
	other := closeOpArgs("op-old")
	other["job"] = "job-2"
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, other); code != "invalid_args" {
		t.Fatalf("another job under the fenced id: code=%q", code)
	}
	later := closeOpArgs("op-old")
	later["epoch"] = 3
	if _, code := nodeRun(t, client, nodeCommandPaneCloseOp, later); code != "invalid_args" {
		t.Fatalf("a later epoch under the fenced id: code=%q", code)
	}
}

// pane.close_release and every older argument shape are unchanged: a release
// of a missing job still answers none, which the hub does not take for proof.
func TestCloseReleaseOfAMissingJobIsUnchanged_t5296(t *testing.T) {
	inbox := t.TempDir()
	client := missingJobNode(t, inbox, idlePaneHerdr())
	recordJobIdentity(t, inbox, closeOpJob)
	wantCloseStatus(t, client, nodeCommandPaneCloseRelease, "op-1", nodeCloseOpNone, false)
	wantCloseStatus(t, client, nodeCommandPaneCloseStatus, "op-1", nodeCloseOpNone, false)
	if _, code := nodeRun(t, client, nodeCommandPaneCloseRelease, missingStatusArgs("op-1", 1)); code != "invalid_args" {
		t.Fatalf("release with an epoch: code=%q", code)
	}
	if _, err := os.Stat(tombstoneFile(inbox, "op-1")); !os.IsNotExist(err) {
		t.Fatalf("a tombstone was written without an epoch: %v", err)
	}
}
