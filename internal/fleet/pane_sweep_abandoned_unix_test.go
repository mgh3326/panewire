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
