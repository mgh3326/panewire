package fleet

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// hk 4633: when the hub hand-over could not push the old worktree's snapshot,
// the node marks the job with HandoverKeepFile and the successor is told to
// copy from host:path. Neither the pane sweep's tail nor checkout.sweep may
// then remove that worktree.
func TestSweepKeepsAHandedOverWorktree(t *testing.T) {
	t.Run("pane sweep tail", func(t *testing.T) {
		f, g, id, _ := checkoutFixture(t)
		if err := os.WriteFile(filepath.Join(f.a.Jobs, id, HandoverKeepFile), []byte("/work/x\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		f.endedSweep(61, time.Hour)
		if len(g.removed) != 0 {
			t.Fatalf("a kept hand-over worktree was removed: %v", g.removed)
		}
		if !f.infoHas("task worktree kept", "reason="+sweepWorktreeHandoverKept) {
			t.Fatalf("info lines = %q", f.info)
		}
	})
	t.Run("checkout sweep", func(t *testing.T) {
		f, g, sweep := checkoutSweepFixture(t)
		id := "impl-9-kept01"
		dir := g.worktree(t, id+"-checkout")
		pane := f.job(id, "implementer", "", map[string]any{"cwd": dir})
		putEvent(t, f.a, id, "job.completed", map[string]any{})
		delete(f.status, pane)
		if err := os.WriteFile(filepath.Join(f.a.Jobs, id, HandoverKeepFile), []byte(dir+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		rows, err := sweep.Pass(t.Context(), f.a)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0].Outcome != "kept" || rows[0].Reason != sweepWorktreeHandoverKept || len(g.removed) != 0 {
			t.Fatalf("rows=%+v removed=%v", rows, g.removed)
		}
	})
}

// Jobs may share a worktree: another job's mark naming it (or a directory
// inside it) keeps it while this job is the sweep candidate.
func TestSweepKeepsAWorktreeAnotherJobHandedOver(t *testing.T) {
	f, g, _, cwd := checkoutFixture(t)
	other := filepath.Join(f.a.Jobs, "handed-over-job")
	if err := os.MkdirAll(other, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, HandoverKeepFile), []byte(cwd+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	f.endedSweep(61, time.Hour)
	if len(g.removed) != 0 {
		t.Fatalf("a worktree another job's hand-over keeps was removed: %v", g.removed)
	}
	if !f.infoHas("task worktree kept", "reason="+sweepWorktreeHandoverKept) {
		t.Fatalf("info lines = %q", f.info)
	}
}

// The jobs root holds plain files (close-op flags and locks): they are not
// job directories and never read as a keep mark.
func TestSweepSkipsNonJobEntriesWhenLookingForKeepMarks(t *testing.T) {
	f, g, _, _ := checkoutFixture(t)
	for _, name := range []string{".close-ops-enabled", ".close-ops-admission.lock"} {
		if err := os.WriteFile(filepath.Join(f.a.Jobs, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	f.endedSweep(61, time.Hour)
	if len(g.removed) != 1 || f.infoHas("task worktree kept", "reason="+sweepWorktreeHandoverKept) {
		t.Fatalf("a plain file in the jobs root kept the worktree: removed=%v info=%q", g.removed, f.info)
	}
}

// The job-directory TTL never expires a directory holding a keep mark: it is
// the only record that keeps the handed-over worktree.
func TestJobDirTTLKeepsAHandoverMark(t *testing.T) {
	r := newTTLRig(t)
	r.seedJob("j1")
	r.write("j1", HandoverKeepFile, []byte("/work/j1-checkout\n"))
	r.age("j1")
	out := r.pass(true)
	wantKeep(t, r, out, "j1", "handover-kept")
	if !r.exists("j1") {
		t.Fatal("the TTL deleted the hand-over's keep mark")
	}
}
