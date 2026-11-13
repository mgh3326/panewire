package fleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The sweep's gaps (hk 3383): a task state that belongs to a category, a fix
// author whose task is its parent's, a job that was closed, and the worktree
// of a non-merged end.

// closeJob appends the record `panewire job close` writes: flat, with the
// outcome beside the kind.
func (f *paneSweepFixture) closeJob(id, outcome string) {
	f.t.Helper()
	dir := filepath.Join(f.a.Jobs, id, "events")
	files, _ := os.ReadDir(dir)
	record := map[string]any{"kind": "job.revoked", "job_id": id, "outcome": outcome, "reason": "test", "created_at": f.now.Add(time.Duration(len(files)) * time.Nanosecond).Format(time.RFC3339Nano)}
	b, err := json.Marshal(record)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%04d-job.revoked.json", len(files))), b, 0600); err != nil {
		f.t.Fatal(err)
	}
}

// fixAuthor seeds a worker spawned with --parent-job: its claim names no task
// of its own, only the job it inherited from. extra adds receipt fields.
func (f *paneSweepFixture) fixAuthor(id, parent string, extra ...map[string]any) string {
	f.t.Helper()
	pane := "p-" + id
	putEvent(f.t, f.a, id, "job.claim", map[string]any{"owner_lane": "owner", "parent_lane": "parent", "t_level": "T1", "role": "implementer", "task_inherited_from": parent})
	spawned := map[string]any{"pane_id": pane, "tab_id": "t-" + pane, "label": "agent-" + id, "profile": "cmd-dsflash41", "family": "deepseek"}
	for _, more := range extra {
		for key, value := range more {
			spawned[key] = value
		}
	}
	putEvent(f.t, f.a, id, "job.spawned", spawned)
	f.status[pane], f.names[pane] = "idle", "agent-"+id
	return pane
}

// (a) A task in a state the sweep has no name for, whose category is completed,
// ends the job as completed. Without the category read nothing closes it.
func TestPaneSweepKeepsAPaneWhoseCategoryIsLive(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "merged", "started"
	f.at(0)
	f.at(60)
	if len(f.closed()) != 0 {
		t.Fatalf("pane closed for a task in a live category")
	}
}

// (b) A fix author is judged on its parent's task, found in the parent's job
// record on this node or, failing that, in hk.
func TestPaneSweepKeepsAWorkingPaneOfARevokedJob(t *testing.T) {
	f := newPaneSweepFixture(t)
	pane := f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "developing", "started"
	f.closeJob("j", "abandoned")
	f.status[pane] = "working"
	f.at(0)
	f.at(60)
	if len(f.closed()) != 0 || f.reaped("j") != nil {
		t.Fatalf("a working pane was closed")
	}
	if !f.infoHas("task pane kept", "job=j", "reason=pane-working") {
		t.Fatalf("info lines = %q", f.info)
	}
}

// A job revived after its close (a reprompt) is live again and keeps its pane.
func TestPaneSweepKeepsARevivedJob(t *testing.T) {
	f := newPaneSweepFixture(t)
	f.job("j", "implementer", "42")
	f.tasks["42"], f.category["42"] = "developing", "started"
	f.closeJob("j", "abandoned")
	putEvent(t, f.a, "j", "job.reprompted", map[string]any{})
	f.at(0)
	f.at(60)
	if len(f.closed()) != 0 {
		t.Fatalf("a revived job's pane was closed")
	}
}

// Only a worker or tester is swept, closed or not.
func TestPaneSweepNeverClosesAProtectedRoleForItsClosing(t *testing.T) {
	for _, role := range []string{"builder", "lead", "captain"} {
		t.Run(role, func(t *testing.T) {
			f := newPaneSweepFixture(t)
			f.job("j", role, "42")
			f.closeJob("j", "abandoned")
			f.at(0)
			f.at(60)
			if len(f.closed()) != 0 {
				t.Fatalf("a %s pane was closed", role)
			}
		})
	}
}

// abandonedFixture is a closed, abandoned job whose receipt names a node
// worktree, with a sweeper that can reach git.
func abandonedFixture(t *testing.T) (*paneSweepFixture, *sweepGitFake, string) {
	t.Helper()
	g := newSweepGitFake(t)
	f := newPaneSweepFixture(t)
	f.withGit(g)
	cwd := g.worktree(t, "job-a")
	f.sweptJob("a", "42", cwd)
	f.tasks["42"], f.category["42"] = "developing", "started"
	f.closeJob("a", "abandoned")
	return f, g, cwd
}

// (e) An abandoned dirty worktree is saved, then removed; its branch stays
// while its tip is not on origin, and the patch exists before the removal.
