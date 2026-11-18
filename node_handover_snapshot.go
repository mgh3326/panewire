package panewire

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// handoverSnapshotTree captures tracked worktree contents without touching the
// user's index. Paths removed from that index are excluded before add -u: a
// removed path can contain a secret which is now untracked.
func handoverSnapshotTree(ctx context.Context, top, scratch string) (string, error) {
	run := func(env []string, args ...string) (string, error) {
		cmd := nodeGitCommand(ctx, append([]string{"-C", top, "--literal-pathspecs"}, args...)...)
		cmd.Env = append(cmd.Env, env...)
		out, err := cmd.Output()
		return string(out), err
	}
	deleted, err := run(nil, "diff", "--cached", "--name-only", "-z", "--no-renames", "--diff-filter=D")
	if err != nil {
		return "", err
	}
	added, err := run(nil, "diff", "--cached", "--name-only", "-z", "--no-renames", "--diff-filter=A")
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(scratch, ".handover-index-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	defer os.Remove(name)
	if err := os.Remove(name); err != nil {
		return "", err
	}
	env := []string{"GIT_INDEX_FILE=" + name}
	if _, err := run(env, "read-tree", "HEAD"); err != nil {
		return "", err
	}
	for _, path := range strings.Split(deleted, "\x00") {
		if path == "" {
			continue
		}
		if _, err := run(env, "update-index", "--force-remove", "--", path); err != nil {
			return "", err
		}
	}
	// Seed newly staged paths using their index entries, never with pathspec
	// expansion or recursive directory adds. add -u then captures their
	// current worktree content along with the paths already in HEAD.
	for _, path := range strings.Split(added, "\x00") {
		if path == "" {
			continue
		}
		entry, err := run(nil, "ls-files", "--stage", "-z", "--", path)
		if err != nil {
			return "", err
		}
		parts := strings.SplitN(entry, "\t", 2)
		if len(parts) != 2 || parts[1] != path+"\x00" || !strings.HasSuffix(parts[0], " 0") {
			return "", fmt.Errorf("invalid staged entry")
		}
		cmd := nodeGitCommand(ctx, "-C", top, "update-index", "-z", "--index-info")
		cmd.Env = append(cmd.Env, env...)
		cmd.Stdin = strings.NewReader(entry)
		if err := cmd.Run(); err != nil {
			return "", err
		}
	}
	if _, err := run(env, "add", "-u", "--", "."); err != nil {
		return "", err
	}

	tree, err := run(env, "write-tree")
	return strings.TrimSpace(tree), err
}

// handoverSnapshotPush publishes a content-addressed ref and accepts it only
// after a read from origin confirms the exact commit.
func handoverSnapshotPush(ctx context.Context, top, scratch, job string) (commit, ref, tree string, err error) {
	if !validNodeJobName(job) {
		return "", "", "", fmt.Errorf("invalid job")
	}
	tree, err = handoverSnapshotTree(ctx, top, scratch)
	if err != nil {
		return "", "", "", err
	}
	run := func(env []string, args ...string) (string, error) {
		cmd := nodeGitCommand(ctx, append([]string{"-C", top}, args...)...)
		cmd.Env = append(cmd.Env, env...)
		out, err := cmd.Output()
		return strings.TrimSpace(string(out)), err
	}
	identity := []string{"GIT_AUTHOR_NAME=panewire", "GIT_AUTHOR_EMAIL=panewire@localhost", "GIT_COMMITTER_NAME=panewire", "GIT_COMMITTER_EMAIL=panewire@localhost"}
	commit, err = run(identity, "commit-tree", "--no-gpg-sign", tree, "-p", "HEAD", "-m", "panewire hand-over snapshot of "+job)
	if err != nil || !nodeShaPattern.MatchString(commit) {
		return "", "", "", fmt.Errorf("snapshot commit failed")
	}
	ref = "refs/panewire/handover/" + job + "/" + commit
	if _, err := run(nil, "-c", "push.recurseSubmodules=no", "push", "--no-verify", "--no-follow-tags", "--recurse-submodules=no", "origin", commit+":"+ref); err != nil {
		return "", "", "", err
	}
	remote, err := run(nil, "ls-remote", "origin", ref)
	if err != nil || remote != commit+"\t"+ref {
		return "", "", "", fmt.Errorf("snapshot origin verification failed")
	}
	return commit, ref, tree, nil
}
