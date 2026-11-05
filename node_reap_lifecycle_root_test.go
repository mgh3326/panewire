package panewire

import (
	"context"
	"io"
	"path/filepath"
	"testing"
)

// The node's job.done path handed the inbox root to the lifecycle, whose trust
// check requires <home>/work/herdr-inbox/jobs, so every node job.done failed
// with "HOME and ARBITER_INBOX_ROOT must select canonical paths" (#157).
func TestNodeLifecycleRunnerPassesJobsRoot(t *testing.T) {
	inbox := t.TempDir()
	var got string
	old := nodeExecuteLifecycle
	nodeExecuteLifecycle = func(_ context.Context, jobsRoot string, _ []string, _, _ io.Writer) error {
		got = jobsRoot
		return nil
	}
	t.Cleanup(func() { nodeExecuteLifecycle = old })
	client := &HubClient{jobsInboxRoot: inbox}
	if _, err := client.nodeLifecycleRunner()(context.Background(), "done", "--job", "j1"); err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(inbox, "jobs"); got != want {
		t.Fatalf("lifecycle jobs root = %q, want %q (the inbox root's jobs directory)", got, want)
	}
}
