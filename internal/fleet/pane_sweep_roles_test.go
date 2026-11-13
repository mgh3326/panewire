package fleet

import (
	"os"
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

// A job.lost of a quota stop is handed to a replacement by the reviewer
// watcher, so its pane stays open until that hand-off ends (watch_quota_stop.go).
func TestPaneSweepKeepsAReviewerPaneStoppedByQuota(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.lost", map[string]any{"reason": "quota_stop", "pool": "pool-a"})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("quota-stopped reviewer pane closed: %v", f.info)
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

// A reviewer's worktree stays with mergewake (docs/pane-sweep.md): closing the
// pane never removes it, even when the job was abandoned.
func TestPaneSweepKeepsAReviewerWorktree(t *testing.T) {
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "r")
	f.job("r", "implementer", "", map[string]any{"role": "reviewer", "cwd": cwd})
	putEvent(t, f.a, "r", "job.revoked", map[string]any{"outcome": "abandoned"})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("abandoned reviewer pane closed without ownership proof: %v", f.closed())
	}
	if _, err := os.Stat(cwd); err != nil {
		t.Fatalf("reviewer worktree removed: %v", err)
	}
	if !f.infoHas("job=r", "reason=ownership-unproven") {
		t.Fatalf("pane was not retained for unproven ownership: %v", f.info)
	}
}
