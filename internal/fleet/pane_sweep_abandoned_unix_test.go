//go:build unix

package fleet

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// A findings name swapped for a FIFO is never opened blocking: the sweep keeps
// the job and the pass returns. The read opens with O_NONBLOCK and checks the
// opened handle, so a substituted FIFO cannot stall every job on the node.
func TestPaneSweepNeverBlocksOnAFindingsFIFO(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	if err := syscall.Mkfifo(filepath.Join(f.a.Jobs, "r", "findings.json"), 0600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	done := make(chan struct{})
	go func() {
		f.at(76)
		close(done)
	}()
	// The guard is for a hang, which would otherwise end only at the package
	// timeout: a blocking open never returns.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the pane sweep blocked on a FIFO findings file")
	}
	// The FIFO is no regular file, so the result cannot be read: the job is
	// kept rather than ended.
	if got := endReasons(t, f.a, "r"); len(got) != 0 {
		t.Fatalf("a FIFO findings file was read as absent: %v", got)
	}
}

// Unjudgeable evidence blocks only the quiet timeout: a pane both herdr lists
// lost is still cleaned up as pane_gone.
func TestPaneSweepEndsAGonePaneWithAFIFOFinding(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.status[pane] = "working"
	if err := syscall.Mkfifo(filepath.Join(f.a.Jobs, "r", "findings.json"), 0600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	delete(f.status, pane)
	f.at(1)
	if got := endReasons(t, f.a, "r"); len(got) != 1 || got[0] != SweepPaneGone {
		t.Fatalf("job.lost reasons = %v", got)
	}
	if f.reaped("r") == nil {
		t.Fatal("the job whose pane vanished was not reaped")
	}
}

// A posted findings object wins over an unjudgeable report: an unreadable
// artifact never hides positive evidence.
func TestPaneSweepCountsFindingsWhenTheReportIsAFIFO(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	f.writeJobFile("r", "findings.json", `{"findings":[]}`)
	if err := syscall.Mkfifo(filepath.Join(f.a.Jobs, "r", "report.md"), 0600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	f.status[pane] = "working"
	f.at(76)
	if got := endReasons(t, f.a, "r"); len(got) != 0 {
		t.Fatalf("a valid findings object was hidden by an unjudgeable report: %v", got)
	}
}
