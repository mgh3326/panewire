package panewire

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mgh3326/panewire/internal/jobowner"
)

func TestJobOwnerFenceJobDoneKeepsProducingGrant_t5067(t *testing.T) {
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	eventsDir := filepath.Join(jobs, "fenced-job", "events")
	if err := os.MkdirAll(eventsDir, 0700); err != nil {
		t.Fatal(err)
	}
	claim := `{"job_id":"fenced-job","kind":"job.claim","payload":{"agent_label":"worker-a","owner_lane":"lane-a","pane_id":"w:p1"}}`
	if err := os.WriteFile(filepath.Join(eventsDir, "00001-job.claim.json"), []byte(claim), 0600); err != nil {
		t.Fatal(err)
	}
	if err := jobowner.Assign(jobs, "fenced-job", 2); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(root, "report.md")
	if err := os.WriteFile(report, []byte("finished\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARBITER_INBOX_ROOT", jobs)
	t.Setenv("HANDOFFKEEP_BIN", filepath.Join(root, "no-handoffkeep"))
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(root, "no-service-credential"))
	// The worker was launched under grant 2 (hk 5310), so its call records 2.
	t.Setenv("ARBITER_JOB", "fenced-job")
	t.Setenv("ARBITER_JOB_EPOCH", "2")
	pushes := &r20t4Pushes{}
	socket := r20t4Socket(t, pushes.add)
	var out, stderr bytes.Buffer
	if rc := runJobCLI([]string{"done", "fenced-job", "--report", report}, &out, &stderr, CLIConfig{SocketPath: socket}); rc != ExitOK {
		t.Fatalf("done rc=%d stderr=%s", rc, stderr.String())
	}
	pushed := pushes.await(t, 1)
	if pushed[0].Epoch != 2 {
		t.Fatalf("socket completion epoch=%d, want 2", pushed[0].Epoch)
	}
	client := &HubClient{jobsInboxRoot: root, completedJobs: make(map[string]uint64)}
	completions := client.jobCompletionEvents()
	if len(completions) != 1 {
		t.Fatalf("scanned completions=%+v", completions)
	}
	completion, valid := decodeHubJobCompletionPayload(completions[0].Payload)
	if !valid || completion.Epoch != 2 {
		t.Fatalf("scanned completion=%s", completions[0].Payload)
	}
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-b", "owner_epoch": float64(2)}}
	hub, _ := ownerFenceHub(t, hk)
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-b", Epoch: 2}, false); err != nil {
		t.Fatal(err)
	}
	hub.jobs["fenced-job"].Node, hub.jobs["fenced-job"].Epoch = "machine-b", 2
	if !hub.observeJobCompletion("machine-b", completion, hub.now()) || !hub.jobs["fenced-job"].Completed {
		t.Fatalf("hub did not accept producing grant: completion=%+v job=%+v", completion, hub.jobs["fenced-job"])
	}
}

// emit records the attempt it presents: the epoch bound at launch or --epoch,
// which must be the current grant (hk 5310).
func TestJobOwnerFenceEmitRecordsPresentedGrant_t5067(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bound string
		epoch []string
		want  uint64
	}{
		{name: "bound", bound: "2", want: 2},
		{name: "explicit", epoch: []string{"--epoch", "2"}, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := jobowner.Assign(filepath.Join(root, "jobs"), "job-a", 2); err != nil {
				t.Fatal(err)
			}
			t.Setenv("ARBITER_JOB", "job-a")
			t.Setenv("ARBITER_JOB_EPOCH", tc.bound)
			pushes := &r20t4Pushes{}
			socket := r20t4Socket(t, pushes.add)
			args := []string{"--kind", "job.completed", "--job", "job-a", "--owner-lane", "lane-a", "--report", "/tmp/report.md", "--inbox-root", root}
			args = append(args, tc.epoch...)
			if rc := runEmitCLI(args, io.Discard, io.Discard, CLIConfig{SocketPath: socket}); rc != ExitOK {
				t.Fatalf("emit rc=%d", rc)
			}
			pushed := pushes.await(t, 1)
			events := scanHubRelayEvents(root)
			if len(events) != 1 || events[0].Epoch != tc.want || pushed[0].Epoch != tc.want {
				t.Fatalf("scanned=%+v pushed=%+v, want epoch %d", events, pushed, tc.want)
			}
		})
	}
}

func TestJobOwnerFenceJobRecordRejectsInvalidGrant_t5067(t *testing.T) {
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	dir := filepath.Join(jobs, "job-a")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".panewire-owner-epoch.json"), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	rc := runEmitCLI([]string{"--kind", "job.completed", "--job", "job-a", "--report", "/tmp/report.md", "--inbox-root", root}, io.Discard, &stderr, CLIConfig{SocketPath: filepath.Join(root, "missing.sock")})
	if rc == ExitOK || len(scanHubRelayEvents(root)) != 0 {
		body, _ := json.Marshal(scanHubRelayEvents(root))
		t.Fatalf("invalid grant emitted a relay record: rc=%d stderr=%q records=%s", rc, stderr.String(), body)
	}
}

func TestJobOwnerFenceJobCloseKeepsProducingGrant_t5067(t *testing.T) {
	f := newJobCloseFixture(t)
	t.Cleanup(func() { _ = f.socket.stop() })
	f.claim("j1", "00001-job.claim.json", "job.claim", "director-1")
	if err := jobowner.Assign(f.jobs, "j1", 2); err != nil {
		t.Fatal(err)
	}
	if rc := f.run("j1", "--lane", "director-1", "--outcome", "completed", "--reason", "finished"); rc != ExitOK {
		t.Fatalf("close rc=%d stderr=%q", rc, f.stderr.String())
	}
	records := f.revokedRecords("j1")
	if len(records) != 1 || records[0]["epoch"] != float64(2) {
		t.Fatalf("close records=%v, want epoch 2", records)
	}
	if lines := f.socket.stop(); len(lines) != 0 {
		t.Fatalf("close unexpectedly contacted daemon: %q", lines)
	}
}
