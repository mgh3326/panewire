package panewire

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/jobowner"
)

func TestJobOwnerFenceOldAttemptCannotBorrowRegrantedEpoch_t5067(t *testing.T) {
	root := t.TempDir()
	jobs := filepath.Join(root, "jobs")
	events := filepath.Join(jobs, "fenced-job", "events")
	if err := os.MkdirAll(events, 0700); err != nil {
		t.Fatal(err)
	}
	claim := []byte(`{"kind":"job.claim","job_id":"fenced-job","payload":{"agent_label":"worker-a","owner_lane":"lane-a","pane_id":"w:p1"}}`)
	if err := os.WriteFile(filepath.Join(events, "00001-job.claim.json"), claim, 0600); err != nil {
		t.Fatal(err)
	}
	if err := jobowner.Revoke(jobs, "fenced-job", 2); err != nil {
		t.Fatal(err)
	}
	if err := jobowner.Assign(jobs, "fenced-job", 3); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(events, "00003-job.claim.json"), claim, 0600); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(root, "old-report.md")
	if err := os.WriteFile(report, []byte("old attempt result\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARBITER_INBOX_ROOT", jobs)
	t.Setenv("HANDOFFKEEP_URL", "")
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "")
	t.Setenv("PANEWIRE_HK_ENV", filepath.Join(root, "no-service-credential"))
	socket := r20t4Socket(t, func(localRequest) {})
	// The A/1 worker was launched at grant epoch 1 (hk 5310) and is still alive.
	t.Setenv("ARBITER_JOB", "fenced-job")
	t.Setenv("ARBITER_JOB_EPOCH", "1")
	var refusal bytes.Buffer
	if rc := runJobCLI([]string{"done", "fenced-job", "--report", report}, io.Discard, &refusal, CLIConfig{SocketPath: socket}); rc == ExitOK || !strings.Contains(refusal.String(), "stale attempt") {
		t.Fatalf("old attempt done rc=%d stderr=%q, want a refusal naming the stale attempt", rc, refusal.String())
	}
	client := &HubClient{jobsInboxRoot: root, completedJobs: make(map[string]uint64)}
	if completions := client.jobCompletionEvents(); len(completions) != 0 {
		t.Fatalf("old attempt wrote a completion: %+v", completions)
	}
	// The regranted A/3 worker carries the new epoch and is accepted.
	t.Setenv("ARBITER_JOB_EPOCH", "3")
	if rc := runJobCLI([]string{"done", "fenced-job", "--report", report}, io.Discard, io.Discard, CLIConfig{SocketPath: socket}); rc != ExitOK {
		t.Fatalf("done rc=%d", rc)
	}
	completions := client.jobCompletionEvents()
	if len(completions) != 1 {
		t.Fatalf("completions=%+v", completions)
	}
	completion, valid := decodeHubJobCompletionPayload(completions[0].Payload)
	if !valid {
		t.Fatalf("invalid completion %s", completions[0].Payload)
	}
	hk := &ownerFenceHK{record: map[string]any{"state": "spawned", "owner_node": "machine-a", "owner_epoch": float64(3)}}
	hub, _ := ownerFenceHub(t, hk)
	if err := hub.saveOwnerAssignment(hubOwnerAssignment{Job: "fenced-job", Node: "machine-a", Epoch: 3}, false); err != nil {
		t.Fatal(err)
	}
	hub.jobs["fenced-job"].Epoch = 3
	if completion.Epoch != 3 {
		t.Fatalf("completion epoch=%d, want the bound 3", completion.Epoch)
	}
	if !hub.observeJobCompletion("machine-a", completion, hub.now()) {
		t.Fatalf("current attempt completion refused at epoch %d", completion.Epoch)
	}
	// What the old worker would have sent had it been admitted: the hub fences it.
	old := completion
	old.Epoch = 1
	if hub.observeJobCompletion("machine-a", old, hub.now()) {
		t.Fatalf("hub accepted a completion from epoch %d under grant 3", old.Epoch)
	}
}
