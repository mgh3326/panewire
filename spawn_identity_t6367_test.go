package panewire

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

// purposeIsJobFlag is an operator argv the wire admits whose --purpose value is
// the text "--job". The flags are in the order the hub planner sends them
// (purpose before the names), so a reader that takes that value for the flag
// answers with "-m" instead of the job.
func purposeIsJobFlag(t *testing.T, names ...string) []string {
	t.Helper()
	args := append([]string{"--purpose", "--job", "-m", "sonnet"}, names...)
	if refusal := spawnargs.Refusal(args, true); refusal != "" {
		t.Fatalf("the wire refuses %q: %s", args, refusal)
	}
	return args
}

func TestUngrantedTombstoneNamesEffectiveJob_t6367(t *testing.T) {
	for i, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no --job", []string{"-l", "review-a"}, "review-a"},
		{"--job wins", []string{"-l", "review-a", "--job", "job-real"}, "job-real"},
		{"purpose value", purposeIsJobFlag(t, "-l", "review-a"), "review-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, runs := grantNode(t)
			peer, results, _ := grantPeer(t, client, "grant")
			message := admissionMessage(636701+i, true)
			message.Args = tc.args
			message.StartByMS = time.Now().Add(-time.Second).UnixMilli()
			client.handleHubSpawn(t.Context(), peer, message)
			result := nextSpawnResult(t, results)
			if runs.Load() != 0 || result.Error != "deadline_not_started" || result.JobID != tc.want {
				t.Fatalf("runs=%d tombstone=%+v, want deadline_not_started for job %q", runs.Load(), result, tc.want)
			}
			// A replay is answered from the journaled tombstone.
			client.handleHubSpawn(t.Context(), peer, message)
			if replay := nextSpawnResult(t, results); replay.JobID != tc.want {
				t.Fatalf("replayed tombstone job %q, want %q", replay.JobID, tc.want)
			}
		})
	}
}

func TestTimeoutJobIDIgnoresPurposeValue_t6367(t *testing.T) {
	for i, tc := range []struct {
		name  string
		names []string
		want  string
	}{
		{"label only", []string{"-l", "review-a"}, "review-a"},
		{"--job after the purpose", []string{"-l", "review-a", "--job", "job-real"}, "job-real"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, runs := grantNode(t)
			peer, results, _ := grantPeer(t, client, "grant")
			// An exclusive holder keeps the request waiting to its deadline.
			release, _ := client.nodeAdmission().tryAdmit("", 1)
			if release == nil {
				t.Fatal("the holder was not admitted")
			}
			defer release()
			message := admissionMessage(636711+i, true)
			message.Args = purposeIsJobFlag(t, tc.names...)
			message.DeadlineMS = time.Now().Add(200 * time.Millisecond).UnixMilli()
			client.handleHubSpawn(t.Context(), peer, message)
			result := nextSpawnResult(t, results)
			if runs.Load() != 0 || result.RC != 124 || result.Stage != "claim" || result.JobID != tc.want {
				t.Fatalf("runs=%d timeout=%+v, want job %q", runs.Load(), result, tc.want)
			}
		})
	}
}

// The other answers a node or hub gives without the launcher's receipt name the
// same job: the expired claim, the node_spawn events and the hub's fenced
// attempt.
func TestDeadlineAndEventJobIDsNameEffectiveJob_t6367(t *testing.T) {
	events := &fleetEventRecorder{}
	client := &HubClient{fleetEvents: events.write}
	message := admissionMessage(636721, true)
	message.Args = purposeIsJobFlag(t, "-l", "review-a")
	message.DeadlineMS = time.Now().Add(-time.Second).UnixMilli()

	if result := client.runClaimedHubSpawn(t.Context(), message); result.Error != "deadline_not_started" || result.JobID != "review-a" {
		t.Errorf("expired claim=%+v, want job review-a", result)
	}
	client.recordHubSpawnOutcome(message, hubSpawnResult{RequestID: message.RequestID}, "", "", time.Now())
	client.recordHubSpawnOutcome(message, hubSpawnResult{RequestID: message.RequestID, RC: 124, Error: "timeout"}, "", "", time.Now())
	recorded := events.all()
	if len(recorded) != 2 || recorded[0].Outcome != "ok" || recorded[1].Outcome != "failed" {
		t.Fatalf("events=%+v, want one ok and one failed", recorded)
	}
	for _, event := range recorded {
		if event.Job != "review-a" {
			t.Errorf("%s event job %q, want review-a", event.Outcome, event.Job)
		}
	}
	fenced := spawnFencedResult(hubSpawnRequest{RequestID: message.RequestID, Machine: "machine-a", Args: message.Args}, time.Now())
	if fenced.JobID != "review-a" {
		t.Errorf("fenced attempt job %q, want review-a", fenced.JobID)
	}
}

func TestJournalCompletedRowNamesEffectiveJob_t6367(t *testing.T) {
	journal, err := openNodeSpawnJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.db.Close() })
	completed := admissionMessage(636731, true)
	completed.Args = []string{"--purpose", "--job=forged", "-l", "review-a"}
	if refused, err := journal.refuseUnstarted(t.Context(), completed, hubSpawnResult{RequestID: completed.RequestID, RC: 124}); err != nil || !refused {
		t.Fatalf("refused=%v err=%v", refused, err)
	}
	var status, job, label string
	if err := journal.db.QueryRowContext(t.Context(), `SELECT status,job_id,label FROM spawn_inbox WHERE request_id=?`, completed.RequestID).Scan(&status, &job, &label); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || job != "review-a" || label != "review-a" {
		t.Fatalf("completed row status=%q job=%q label=%q, want job and label review-a", status, job, label)
	}

	// The executing row names it too, so an attempt without --job holds that
	// job's release while it runs.
	executing := admissionMessage(636732, true)
	executing.Args = []string{"-l", "review-b"}
	if claimed, _, err := journal.claim(t.Context(), executing); err != nil || !claimed {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	rows, err := journal.executing(t.Context())
	if err != nil || len(rows) != 1 || rows[0].Job != "review-b" || rows[0].Label != "review-b" || !rows[0].Strict {
		t.Fatalf("executing rows=%+v err=%v, want a strict row for review-b", rows, err)
	}
	if !jobAttemptLive("review-b", rows, nil) || jobAttemptLive("review-c", rows, nil) {
		t.Fatal("the executing attempt must hold its own job's release and no other")
	}
}

// Two keys conflict when either part is shared.
func TestOverlapKeyConflictsOnEitherPart_t6367(t *testing.T) {
	held := spawnOverlapKey("dir-1", []string{"--job", "job-a", "-l", "label-a"})
	for _, tc := range []struct {
		name string
		dir  string
		args []string
		want bool
	}{
		{"same dir, different job", "dir-1", []string{"--job", "job-b", "-l", "label-b"}, true},
		{"different dir, same job", "dir-2", []string{"--job", "job-a", "-l", "label-b"}, true},
		{"different dir, same job from the label", "dir-2", []string{"-l", "job-a"}, true},
		{"different dir, different job", "dir-2", []string{"--job", "job-b", "-l", "label-a"}, false},
	} {
		key := spawnOverlapKey(tc.dir, tc.args)
		if got := spawnOverlapConflicts(held, key); got != tc.want {
			t.Errorf("%s: conflict=%v, want %v", tc.name, got, tc.want)
		}
		admission := &nodeSpawnAdmission{dirs: map[string]bool{}, changed: make(chan struct{})}
		if release, _ := admission.tryAdmit(held, 2); release == nil {
			t.Fatalf("%s: the holder was not admitted", tc.name)
		}
		if release, _ := admission.tryAdmit(key, 2); (release == nil) != tc.want {
			t.Errorf("%s: admitted beside the holder=%v, want %v", tc.name, release != nil, !tc.want)
		}
	}
}

// The node's key, the hub's key and the lane both node checks read follow the
// launcher's parse. Defence in depth for older hubs: spawnargs.Refusal stops
// these repeated and single-dash forms on the wire today.
func TestOverlapKeyReadsLauncherIdentity_t6367(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(t.TempDir(), "spawn.json")
	if err := os.WriteFile(config, []byte(`{"work_root":"`+root+`","workspace":"w","herdr_session":"w","enabled":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	client := &HubClient{spawnConfigPath: config}
	for _, tc := range []struct {
		args []string
		job  string
	}{
		{[]string{"-l", "a", "-l", "b"}, "b"},
		{[]string{"--job", "a", "--job", "b"}, "b"},
		{[]string{"-job", "a", "-l", "b"}, "a"},
		{[]string{"--job=a", "-l", "b"}, "a"},
		{[]string{"--purpose", "--job", "--l", "b"}, "b"},
		{[]string{"--keep", "--wait", "--job", "a"}, "a"},
	} {
		identity, err := fleet.ParseSpawnIdentity(tc.args)
		if err != nil || identity.Job != tc.job {
			t.Fatalf("%q: launcher names %+v err=%v, want %q", tc.args, identity, err, tc.job)
		}
		message := concurrencyMessage(1, "job-dir")
		message.Args = tc.args
		want := "dir:job-dir\x00job:" + tc.job
		if node, hub := client.spawnAdmissionKey(message), hubSpawnOverlapKey(&hubSpawnRequest{Checkout: message.Checkout, Args: tc.args}); node != want || hub != want {
			t.Errorf("%q: node key %q, hub key %q, want %q", tc.args, node, hub, want)
		}
	}

	// The last -L is the lane: the launcher enters it, so admission and the
	// check after prepare must look at it.
	dir := filepath.Join(root, "job-lane")
	if err := os.MkdirAll(filepath.Join(dir, "mock"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, "live")); err != nil {
		t.Fatal(err)
	}
	node, runs := grantNode(t)
	node.spawnPrepare = func(context.Context, nodeWorktreePrepareArgs) (any, string) {
		return nodeWorktreePrepareResult{Outcome: nodePreparePrepared, Worktree: &nodeWorktree{Path: dir}}, ""
	}
	message := concurrencyMessage(2, "job-lane")
	message.Args = []string{"--job", "job-lane", "-L", "mock", "-L", "live"}
	if key := client.spawnAdmissionKey(message); key != "" {
		t.Errorf("a linked last lane kept the key %q, want an exclusive admission", key)
	}
	if result := node.runHubSpawn(t.Context(), message); result.Error != "checkout_refused:lane_outside_worktree" || runs.Load() != 0 {
		t.Errorf("runs=%d result=%+v, want lane_outside_worktree", runs.Load(), result)
	}
}
