package panewire

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

func TestCloseOpsDrainWaitsForInFlightClose_t4632(t *testing.T) {
	inbox := t.TempDir()
	client := closeOpNode(t, inbox, idlePaneHerdr())
	jobs := filepath.Join(inbox, "jobs")
	for _, marker := range []string{".close-ops-enabled", ".close-ops-advertise"} {
		if err := os.WriteFile(filepath.Join(jobs, marker), []byte("ready\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	client.nodeHubClose.(*fakeHubClose).record = func(string, fleet.HubCloseIntent) error {
		close(entered)
		<-finish
		return nil
	}
	args := nodePaneCloseOpArgs{RequestID: "op-1", Job: closeOpJob, Epoch: 1, Attempt: closeOpAttempt, PaneID: "w1:p1", Name: "job-a", AgentStatus: "idle", Cwd: "/work/a", EndReason: "rollback test"}
	closeDone := make(chan string, 1)
	go func() { _, code := client.nodePaneCloseOp(context.Background(), args); closeDone <- code }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not reach admission")
	}
	var out, stderr bytes.Buffer
	started, drainDone := make(chan struct{}), make(chan int, 1)
	go func() {
		close(started)
		drainDone <- runNodeCloseOpsCLI([]string{"drain", "--inbox-root", inbox}, &out, &stderr, nil)
	}()
	<-started
	// This window proves withdrawal cannot finish before the admitted close.
	select {
	case rc := <-drainDone:
		close(finish)
		t.Fatalf("drain returned %d while the admitted close was in flight", rc)
	case <-time.After(150 * time.Millisecond):
	}
	close(finish)
	select {
	case code := <-closeDone:
		if code != "" {
			t.Fatalf("close code=%q", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close did not finish")
	}
	select {
	case rc := <-drainDone:
		if rc != ExitOK {
			t.Fatalf("drain rc=%d output=%s error=%s", rc, out.String(), stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish")
	}
	for _, marker := range []string{".close-ops-enabled", ".close-ops-advertise"} {
		if _, err := os.Stat(filepath.Join(jobs, marker)); !os.IsNotExist(err) {
			t.Fatalf("readiness marker %s after drain: %v", marker, err)
		}
	}
	if _, code := client.nodePaneCloseOp(context.Background(), args); code != "close_ops_not_ready" {
		t.Fatalf("close admitted after withdrawal: code=%q", code)
	}
}

// This uses the CLI, a durable orphaned intent and a fresh settlement object,
// as on a node that restarted before the operator began the rollback.
func TestCloseOpsDrainCLIResolvesInterruptedIntent_t4632(t *testing.T) {
	inbox := t.TempDir()
	t.Setenv("XDG_DATA_HOME", t.TempDir())
	jobDir := seedCloseOpJob(t, inbox)
	jobs := filepath.Join(inbox, "jobs")
	in := fleet.HubCloseIntent{RequestID: "orphan", Attempt: closeOpAttempt, Pane: "w1:p1", Label: "job-a", EndReason: "rollback test"}
	if err := fleet.RecordHubCloseIntent(context.Background(), jobs, closeOpJob, in); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if rc := runNodeCloseOpsCLI([]string{"drain", "--inbox-root", inbox}, &out, &stderr, nil); rc != ExitConditionInvalid || !strings.Contains(out.String(), closeOpJob) {
		t.Fatalf("initial drain rc=%d output=%s error=%s", rc, out.String(), stderr.String())
	}
	app := &fleet.App{Jobs: jobs, Home: t.TempDir(), Now: time.Now}
	if state, err := (fleet.HubCloseSettlement{App: app}).Settle(context.Background(), closeOpJob); err != nil || state != fleet.HubCloseCancelled {
		t.Fatalf("settlement=%v err=%v", state, err)
	}
	out.Reset()
	if rc := runNodeCloseOpsCLI([]string{"drain", "--inbox-root", inbox}, &out, &stderr, nil); rc != ExitOK || !strings.Contains(out.String(), "no pending jobs") {
		t.Fatalf("completed drain rc=%d output=%s error=%s", rc, out.String(), stderr.String())
	}
	root, err := os.OpenRoot(jobDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	op, found, err := fleet.ReadCloseOp(root, "orphan")
	if err != nil || !found || op.State != fleet.CloseOpCancelled || op.Released == nil {
		t.Fatalf("tombstone=%+v found=%v err=%v", op, found, err)
	}
	clean := func(context.Context) ([]byte, error) { return nil, nil }
	if err := fleet.EnableCloseOps(context.Background(), jobs, clean, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := fleet.AdvertiseCloseOps(jobs, time.Now()); err != nil {
		t.Fatal(err)
	}
	client := closeOpNode(t, inbox, idlePaneHerdr())
	result, code := client.nodePaneCloseOp(context.Background(), nodePaneCloseOpArgs{
		RequestID: "orphan", Job: closeOpJob, Epoch: 1, Attempt: closeOpAttempt, PaneID: "w1:p1", Name: "job-a", AgentStatus: "idle", Cwd: "/work/a", EndReason: "rollback test",
	})
	replayed, ok := result.(nodePaneCloseOpResult)
	if code != "" || !ok || replayed.Outcome != fleet.CloseOpCancelled || !replayed.Replayed {
		t.Fatalf("delayed retry result=%v code=%q, want cancelled replay", result, code)
	}
}
