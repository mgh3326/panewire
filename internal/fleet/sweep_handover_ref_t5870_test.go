package fleet

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The hand-over ref release (hk 5870) runs against real git: a bare origin, a
// clone at <work>/repo as the node prepares it, and snapshot refs pushed the
// way worktree.handover_snapshot leaves them.

type handoverRefFixture struct {
	origin  string
	work    string
	clone   string
	feat    string // tip of origin/feat before any successor
	scratch string // the clone the last snapshot was committed in
}

func handoverGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v in %s: %v\n%s", args, dir, err, out)
	}
	return strings.TrimSpace(string(out))
}

func newHandoverRefFixture(t *testing.T) *handoverRefFixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	t.Setenv("GIT_CONFIG_GLOBAL", "/dev/null")
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_AUTHOR_NAME", "panewire")
	t.Setenv("GIT_AUTHOR_EMAIL", "panewire@localhost")
	t.Setenv("GIT_COMMITTER_NAME", "panewire")
	t.Setenv("GIT_COMMITTER_EMAIL", "panewire@localhost")
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f := &handoverRefFixture{origin: filepath.Join(root, "origin.git"), work: filepath.Join(root, "work")}
	if err := os.MkdirAll(f.work, 0700); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, root, "init", "-q", "--bare", f.origin)
	seed := filepath.Join(root, "seed")
	if err := os.Mkdir(seed, 0700); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, seed, "init", "-q", "-b", "main")
	handoverGit(t, seed, "commit", "-q", "--allow-empty", "-m", "base")
	handoverGit(t, seed, "checkout", "-q", "-b", "feat")
	handoverGit(t, seed, "commit", "-q", "--allow-empty", "-m", "feat")
	handoverGit(t, seed, "remote", "add", "origin", f.origin)
	handoverGit(t, seed, "push", "-q", "origin", "main", "feat")
	f.feat = handoverGit(t, seed, "rev-parse", "feat")
	f.clone = filepath.Join(f.work, "repo")
	handoverGit(t, root, "clone", "-q", f.origin, f.clone)
	return f
}

// pushSnapshot commits an empty snapshot on top of base in a scratch clone and
// pushes it to the hand-over ref of job, returning the commit and the ref.
func (f *handoverRefFixture) pushSnapshot(t *testing.T, job, base string) (sha, ref string) {
	t.Helper()
	tmp := filepath.Join(filepath.Dir(f.origin), "snap."+job)
	handoverGit(t, filepath.Dir(f.origin), "clone", "-q", f.origin, tmp)
	handoverGit(t, tmp, "checkout", "-q", "--detach", base)
	handoverGit(t, tmp, "commit", "-q", "--allow-empty", "-m", "snapshot of "+job)
	sha = handoverGit(t, tmp, "rev-parse", "HEAD")
	ref = HandoverRefPrefix + job + "/" + sha
	handoverGit(t, tmp, "push", "-q", "origin", "HEAD:"+ref)
	f.scratch = tmp
	return sha, ref
}

// advance fast-forwards origin's feat to sha: the successor's branch now
// contains the snapshot commit.
func (f *handoverRefFixture) advance(t *testing.T, sha string) {
	t.Helper()
	handoverGit(t, f.scratch, "push", "-q", "origin", sha+":refs/heads/feat")
}

// originRefs lists the hand-over refs origin holds.
func (f *handoverRefFixture) originRefs(t *testing.T) []string {
	t.Helper()
	out := handoverGit(t, f.origin, "for-each-ref", "--format=%(refname)", HandoverRefPrefix)
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}

type handoverRefLog struct{ lines []string }

func (l *handoverRefLog) info(message string, attrs ...any) {
	parts := []string{message}
	for _, attr := range attrs {
		parts = append(parts, fmt.Sprint(attr))
	}
	l.lines = append(l.lines, strings.Join(parts, " "))
}

// has reports whether one logged line names every part.
func (l *handoverRefLog) has(parts ...string) bool {
	for _, line := range l.lines {
		all := true
		for _, part := range parts {
			if !strings.Contains(line, part) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

// A successor whose branch on origin contains the snapshot commit releases the
// ref: the commit is on origin under the branch, so the ref is no longer the
// only copy.
func TestHandoverRefReleaseDeletesContainedRef5870(t *testing.T) {
	f := newHandoverRefFixture(t)
	sha, ref := f.pushSnapshot(t, "job-a", f.feat)
	f.advance(t, sha)
	log := &handoverRefLog{}

	releaseHandoverRefsOf(context.Background(), &App{Runner: ExecRunner{}}, f.clone, "feat", log.info)

	if refs := f.originRefs(t); len(refs) != 0 {
		t.Fatalf("origin still holds %v, want the ref %s deleted", refs, ref)
	}
	if !log.has("hand-over ref released", ref, handoverRefReleased) {
		t.Fatalf("log=%q, want a released line for %s", log.lines, ref)
	}
	if got := handoverGit(t, f.origin, "rev-parse", "feat"); got != sha {
		t.Fatalf("origin/feat=%s, want the branch kept at %s", got, sha)
	}
	if got := handoverGit(t, f.origin, "cat-file", "-t", sha); got != "commit" {
		t.Fatalf("the snapshot commit %s is gone from origin: %q", sha, got)
	}
}

// A snapshot the branch does not contain is kept with its reason: the
// successor's work is not on origin under that branch, so the ref is the only
// copy of the snapshot.
func TestHandoverRefReleaseKeepsRefNotInBranch5870(t *testing.T) {
	f := newHandoverRefFixture(t)
	_, ref := f.pushSnapshot(t, "job-a", f.feat)
	log := &handoverRefLog{}

	releaseHandoverRefsOf(context.Background(), &App{Runner: ExecRunner{}}, f.clone, "feat", log.info)

	if refs := f.originRefs(t); len(refs) != 1 || refs[0] != ref {
		t.Fatalf("origin refs=%v, want %s kept", refs, ref)
	}
	if !log.has("hand-over ref kept", ref, handoverRefNotContained) {
		t.Fatalf("log=%q, want a not_contained_kept line for %s", log.lines, ref)
	}
}

// A branch origin does not have proves nothing: every ref is kept, and the
// log says why.
func TestHandoverRefReleaseKeepsRefsWithoutOriginBranch5870(t *testing.T) {
	f := newHandoverRefFixture(t)
	_, ref := f.pushSnapshot(t, "job-a", f.feat)
	log := &handoverRefLog{}

	releaseHandoverRefsOf(context.Background(), &App{Runner: ExecRunner{}}, f.clone, "never-pushed", log.info)

	if refs := f.originRefs(t); len(refs) != 1 || refs[0] != ref {
		t.Fatalf("origin refs=%v, want %s kept", refs, ref)
	}
	if !log.has("hand-over refs kept", "never-pushed", handoverRefBranchKept) {
		t.Fatalf("log=%q, want a branch_unproven_kept line", log.lines)
	}
}

// The end gate: only a successor that completed (a merged PR is a completed
// task) releases its refs. A job lost any other way keeps them, even when its
// branch contains the snapshot.
func TestHandoverRefReleaseEndGate5870(t *testing.T) {
	cases := []struct {
		name    string
		end     paneSweepEnd
		deleted bool
	}{
		{name: "a completed successor releases", end: paneSweepEnd{state: "completed"}, deleted: true},
		{name: "a merged successor releases", end: paneSweepEnd{state: "completed", merged: true}, deleted: true},
		{name: "a lost successor keeps its refs", end: paneSweepEnd{state: "lost", discard: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newHandoverRefFixture(t)
			sha, ref := f.pushSnapshot(t, "job-a", f.feat)
			f.advance(t, sha)
			handoverGit(t, f.clone, "worktree", "add", "-q", "-b", "feat", filepath.Join(f.work, "repo.job"), "origin/feat")
			log := &handoverRefLog{}
			s := &TaskPaneSweeper{WorkRoot: f.work, Info: log.info}

			s.releaseHandoverRefs(context.Background(), &App{Runner: ExecRunner{}}, filepath.Join(f.work, "repo.job"), tc.end)

			deleted := len(f.originRefs(t)) == 0
			if deleted != tc.deleted {
				t.Fatalf("ref %s deleted=%v, want %v (log=%q)", ref, deleted, tc.deleted, log.lines)
			}
		})
	}
}
