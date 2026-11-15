package fleet

import (
	"os"
	"path/filepath"
	"testing"
)

// A reviewer, a consult or a builder pane is judged by its own job
// end, not by a task: its spawn role is what decides, since the claim records
// only implementer or builder.

func TestPaneSweepKeepsAWorkingReviewerPane(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.status[pane] = "working"
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 || f.reaped("r") != nil {
		t.Fatalf("working reviewer pane closed")
	}
}

// A reviewer's job.lost of a quota stop ends the job (decision 266): the hub
// hands the review to a replacement in a new pane, so this one closes.
func TestPaneSweepClosesAReviewerPaneStoppedByQuota(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.lost", map[string]any{"reason": "quota_stop", "pool": "pool-a"})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 1 || f.reaped("r") == nil {
		t.Fatalf("quota-stopped reviewer pane kept: %v", f.info)
	}
}

func TestPaneSweepKeepsALostWorkerPane(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("w", "implementer", "")
	putEvent(t, f.a, "w", "job.lost", map[string]any{"reason": "host_gone"})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("lost worker pane closed: %v", f.info)
	}
}

// A reviewer whose herdr row lost its name is matched by pane id and the job's
// worktree, never by pane id alone.
func TestPaneSweepKeepsANamelessPaneInAnotherWorktree(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "", map[string]any{"cwd": t.TempDir()})
	f.names[pane] = ""
	f.cwds[pane] = t.TempDir()
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("nameless pane in another worktree closed: %v", f.info)
	}
}

// An abandoned job's close removes its node-prepared worktree after saving
// its changes to the job dir (docs/pane-sweep.md).
func TestPaneSweepKeepsAReviewerWorktree(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "r")
	pane := f.job("r", "implementer", "", map[string]any{"role": "reviewer", "cwd": cwd})
	putEvent(t, f.a, "r", "job.revoked", map[string]any{"outcome": "abandoned"})
	f.at(0)
	f.at(11)
	if got := f.closed(); len(got) != 1 || got[0] != pane {
		t.Fatalf("abandoned reviewer pane kept: %v", f.info)
	}
	if _, err := os.Stat(cwd); !os.IsNotExist(err) {
		t.Fatalf("abandoned worktree kept, err %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.a.Jobs, "r", AbandonedWorktreeFile)); err != nil {
		t.Fatalf("abandoned worktree patch not saved: %v", err)
	}
}
