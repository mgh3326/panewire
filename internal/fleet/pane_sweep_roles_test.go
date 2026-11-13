package fleet

import (
	"errors"
	"os"
	"testing"
)

// A reviewer, a counsel (consult) or a builder pane is judged by its own job
// end, not by a task: its spawn role is what decides, since the claim records
// only worker or builder.

func TestPaneSweepClosesARevokedReviewerPaneAfterTheGrace(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.at(0)
	f.at(11)
	if got := f.closed(); len(got) != 1 || got[0] != pane {
		t.Fatalf("closed = %v, want [%s]", got, pane)
	}
	if payload := f.reaped("r"); payload["reason"] != "job_revoked" {
		t.Fatalf("reason = %v, want job_revoked", payload["reason"])
	}
	if !f.infoHas("task pane closed", "job=r", "role=reviewer") {
		t.Fatalf("no close line with the role: %v", f.info)
	}
}

func TestPaneSweepClosesACompletedCounselPaneAfterTheGrace(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("c", "counsel", "")
	putEvent(t, f.a, "c", "job.completed", map[string]any{})
	f.at(0)
	f.at(11)
	if got := f.closed(); len(got) != 1 || got[0] != pane {
		t.Fatalf("closed = %v, want [%s]", got, pane)
	}
	if payload := f.reaped("c"); payload["reason"] != "job_completed" {
		t.Fatalf("reason = %v, want job_completed", payload["reason"])
	}
}

// The task's read does not reach a reviewer: an hk outage keeps a worker's
// pane, but a revoked reviewer's pane closes on its own record.
func TestPaneSweepJudgesAReviewerByItsOwnEndNotItsTask(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.taskErr = errors.New("hk unreachable")
	f.job("r", "reviewer", "42")
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 1 {
		t.Fatalf("revoked reviewer kept while hk is unreachable: %v", f.info)
	}
}

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

func TestPaneSweepClosesALostReviewerPaneAsJobLost(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("r", "reviewer", "")
	putEvent(t, f.a, "r", "job.lost", map[string]any{"reason": "host_gone"})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 1 {
		t.Fatalf("lost reviewer pane kept: %v", f.info)
	}
	if payload := f.reaped("r"); payload["reason"] != "job_lost" {
		t.Fatalf("reason = %v, want job_lost", payload["reason"])
	}
}

// A lost job.lost of a worker still waits for its owner: a worker's end is
// read from its task and PR, as before.
func TestPaneSweepKeepsALostWorkerPane(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("w", "worker", "")
	putEvent(t, f.a, "w", "job.lost", map[string]any{"reason": "host_gone"})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 0 {
		t.Fatalf("lost worker pane closed: %v", f.info)
	}
}

// A reviewer whose herdr row lost its name is matched by pane id and the job's
// worktree, never by pane id alone.
func TestPaneSweepClosesAReviewerPaneWhoseNameWasLostAtItsWorktree(t *testing.T) {
	worktree := t.TempDir()
	f := newPaneSweepFixture(t)
	pane := f.job("r", "reviewer", "", map[string]any{"cwd": worktree})
	f.names[pane] = ""
	f.cwds[pane] = worktree
	putEvent(t, f.a, "r", "job.revoked", map[string]any{})
	f.at(0)
	f.at(11)
	if got := f.closed(); len(got) != 1 || got[0] != pane {
		t.Fatalf("closed = %v, want [%s]: %v", got, pane, f.info)
	}
}

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
	f.job("r", "worker", "", map[string]any{"role": "reviewer", "cwd": cwd})
	putEvent(t, f.a, "r", "job.revoked", map[string]any{"outcome": "abandoned"})
	f.at(0)
	f.at(11)
	if len(f.closed()) != 1 {
		t.Fatalf("abandoned reviewer pane kept: %v", f.info)
	}
	if _, err := os.Stat(cwd); err != nil {
		t.Fatalf("reviewer worktree removed: %v", err)
	}
}
