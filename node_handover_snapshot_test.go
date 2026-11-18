package panewire

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestHandoverSnapshotTreeCapturesTrackedOnly(t *testing.T) {
	work, _, _ := handoverRepo(t)
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("modified"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "new*.txt"), []byte("staged"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "new*.txt")
	if err := os.WriteFile(filepath.Join(work, "new-secret.txt"), []byte("not tracked"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "removed.txt"), []byte("before"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "removed.txt")
	handoverGit(t, work, "commit", "-m", "track removed")
	handoverGit(t, work, "rm", "--cached", "--", "removed.txt")
	if err := os.WriteFile(filepath.Join(work, "removed.txt"), []byte("not tracked"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "another.txt"), []byte("staged new"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "another.txt")
	if err := os.WriteFile(filepath.Join(work, "staged*.txt"), []byte("literal"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "--literal-pathspecs", "add", "--", "staged*.txt")
	if err := os.WriteFile(filepath.Join(work, "staged-secret.txt"), []byte("not tracked"), 0600); err != nil {
		t.Fatal(err)
	}
	replaced := filepath.Join(work, "replaced")
	if err := os.WriteFile(replaced, []byte("staged"), 0600); err != nil {
		t.Fatal(err)
	}
	handoverGit(t, work, "add", "--", "replaced")
	if err := os.Remove(replaced); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(replaced, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(replaced, "private"), []byte("local"), 0600); err != nil {
		t.Fatal(err)
	}
	before := handoverGit(t, work, "status", "--porcelain=v1")
	tree, err := handoverSnapshotTree(t.Context(), work, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if got := handoverGit(t, work, "show", tree+":a.txt"); got != "modified" {
		t.Fatalf("tracked content = %q", got)
	}
	if got := handoverGit(t, work, "show", tree+":another.txt"); got != "staged new" {
		t.Fatalf("staged content = %q", got)
	}
	if got := handoverGit(t, work, "show", tree+":staged*.txt"); got != "literal" {
		t.Fatalf("literal staged content = %q", got)
	}
	for _, name := range []string{"removed.txt", "new-secret.txt", "staged-secret.txt", "replaced/private"} {
		cmd := exec.CommandContext(t.Context(), "git", "-C", work, "cat-file", "-e", tree+":"+name)
		if err := cmd.Run(); err == nil {
			t.Fatalf("untracked path %q captured", name)
		}
	}
	if after := handoverGit(t, work, "status", "--porcelain=v1"); before != after {
		t.Fatalf("user index changed: before %q after %q", before, after)
	}
}

func TestHandoverSnapshotPushPublishesVerifiedTrackedWork(t *testing.T) {
	work, origin, _ := handoverRepo(t)
	if err := os.WriteFile(filepath.Join(work, "a.txt"), []byte("snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(work, "private.txt"), []byte("local"), 0600); err != nil {
		t.Fatal(err)
	}
	commit, ref, _, err := handoverSnapshotPush(t.Context(), work, t.TempDir(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	if got := handoverGit(t, origin, "rev-parse", ref); got != commit {
		t.Fatalf("origin ref = %q, want %q", got, commit)
	}
	if got := handoverGit(t, origin, "show", ref+":a.txt"); got != "snapshot" {
		t.Fatalf("snapshot content = %q", got)
	}
	cmd := exec.CommandContext(t.Context(), "git", "-C", origin, "cat-file", "-e", ref+":private.txt")
	if err := cmd.Run(); err == nil {
		t.Fatal("untracked file was published")
	}
}

func TestHandoverSnapshotPushRefusesFailedOrigin(t *testing.T) {
	work, _, _ := handoverRepo(t)
	handoverGit(t, work, "remote", "set-url", "origin", filepath.Join(t.TempDir(), "missing"))
	commit, ref, _, err := handoverSnapshotPush(t.Context(), work, t.TempDir(), "job-a")
	if err == nil || commit != "" || ref != "" {
		t.Fatalf("commit=%q ref=%q err=%v", commit, ref, err)
	}
}
