package panewire

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/mgh3326/panewire/internal/fleet"
)

// worktree.handover_snapshot is the old node's half of a hub hand-over (hk
// 4633, docs/quota-stop.md "An exhausted pool"). From the worktree's top
// level it reports the branch, its last pushed commit (git ls-remote on the
// node, which holds the origin's credentials) or the base it was cut from.
// With Snapshot set — at the close, after the hub saw the pane idle — it also
// commits the whole worktree, untracked files included, through a temporary
// index (the user's index, stash and branch stay untouched) on top of HEAD and
// pushes that commit to refs/panewire/handover/<job> on origin (no tags, no
// submodules, read back after the push). A snapshot always writes
// fleet.HandoverKeepFile into the job's directory as well: every sweep then
// keeps the worktree, and the successor's note names host:path for what the
// commit cannot carry. A dirty worktree never blocks the hand-over.
const nodeCommandHandoverSnapshot = "worktree.handover_snapshot"

// hubCapabilityHandoverSnapshot: the node answers worktree.handover_snapshot.
// A hub hands an implementer over itself only on a node that advertises it.
const hubCapabilityHandoverSnapshot = "handover_snapshot_v1"

// handoverSnapshotRefPrefix is the ref namespace on origin the snapshots go to.
const handoverSnapshotRefPrefix = "refs/panewire/handover/"

const (
	nodeHandoverSnapshotSaved    = "saved"
	nodeHandoverSnapshotNotAJob  = "not-a-job"
	nodeHandoverSnapshotBranch   = "branch_mismatch"
	nodeHandoverSnapshotGitError = "git_failed"
	// nodeHandoverSnapshotDiverged: the pushed tip is not in the worktree's
	// history, so no successor started from it can take the local work.
	nodeHandoverSnapshotDiverged = "diverged"
)

type nodeHandoverSnapshotArgs struct {
	Job string `json:"job"`
	Cwd string `json:"cwd"`
	// Branch is the branch the worktree must be on; empty accepts the
	// current one (a successor that started detached and made the branch).
	Branch string `json:"branch"`
	// Snapshot: commit and push the worktree. Without it the command only
	// reads, so the sweep can build the successor before anything changes.
	Snapshot bool `json:"snapshot"`
}

type nodeHandoverSnapshotResult struct {
	Outcome string `json:"outcome"`
	// Branch is the worktree's branch and Top its top level.
	Branch string `json:"branch,omitempty"`
	Top    string `json:"top,omitempty"`
	// Pushed is origin's refs/heads/<branch> as ls-remote read it; empty when
	// the branch was never pushed.
	Pushed string `json:"pushed,omitempty"`
	// Base is the merge-base of HEAD with origin's default branch, BaseRef
	// that branch as refs/heads/<name>: where a successor starts when nothing
	// was pushed.
	Base    string `json:"base,omitempty"`
	BaseRef string `json:"base_ref,omitempty"`
	// Dirty: the worktree holds work its start point lacks (uncommitted,
	// untracked or unpushed commits). Set only with Snapshot.
	Dirty bool `json:"dirty"`
	// Commit and Ref: the snapshot commit and the origin ref it was pushed to;
	// both empty when the worktree is clean or the push failed.
	Commit string `json:"commit,omitempty"`
	Ref    string `json:"ref,omitempty"`
	// Submodule: a submodule holds changes the commit does not carry.
	Submodule bool `json:"submodule"`
	// Kept: the job's worktree is kept by HandoverKeepFile; the successor
	// copies what the ref lacks from Top on this node.
	Kept bool `json:"kept"`
}

func validNodeHandoverSnapshotArgs(args nodeHandoverSnapshotArgs) bool {
	return validNodeJobName(args.Job) && validNodeCommandPath(args.Cwd) && (args.Branch == "" || validNodeRefName(args.Branch))
}

func validNodeHandoverSnapshotResult(result nodeHandoverSnapshotResult) bool {
	switch result.Outcome {
	case nodeHandoverSnapshotSaved:
		if result.Pushed == "" && (!nodeShaPattern.MatchString(result.Base) || !validNodeFetchRef(result.BaseRef)) {
			return false
		}
		if (result.Commit == "") != (result.Ref == "") || result.Commit != "" && (!nodeShaPattern.MatchString(result.Commit) || !strings.HasPrefix(result.Ref, handoverSnapshotRefPrefix) || !validNodeJobName(strings.TrimPrefix(result.Ref, handoverSnapshotRefPrefix))) {
			return false
		}
		return validNodeRefName(result.Branch) && validNodeCommandPath(result.Top) && (result.Pushed == "" || nodeShaPattern.MatchString(result.Pushed))
	case nodeHandoverSnapshotNotAJob, nodeHandoverSnapshotBranch, nodeHandoverSnapshotGitError, nodeHandoverSnapshotDiverged:
		return result == nodeHandoverSnapshotResult{Outcome: result.Outcome}
	}
	return false
}

// handoverSnapshotIdentity is the fixed author of a snapshot commit, so a
// node without a git identity can still make one.
var handoverSnapshotIdentity = []string{
	"GIT_AUTHOR_NAME=panewire", "GIT_AUTHOR_EMAIL=panewire@localhost",
	"GIT_COMMITTER_NAME=panewire", "GIT_COMMITTER_EMAIL=panewire@localhost",
}

// gitWithEnv runs git like every node git call, with env added: the
// temporary index and the snapshot identity.
func gitWithEnv(ctx context.Context, env []string, args ...string) (string, error) {
	cmd := nodeGitCommand(ctx, args...)
	cmd.Env = append(cmd.Env, env...)
	out, err := cmd.Output()
	return string(out), err
}

func (client *HubClient) nodeHandoverSnapshot(ctx context.Context, args nodeHandoverSnapshotArgs) (any, string) {
	job, dir, found, err := client.openNodeJobDir(args.Job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "capture_failed"
	case !found:
		return nodeHandoverSnapshotResult{Outcome: nodeHandoverSnapshotNotAJob}, ""
	}
	defer job.Close()
	refuse := func(outcome string) (any, string) { return nodeHandoverSnapshotResult{Outcome: outcome}, "" }
	git := client.nodeGitRunner()
	top := args.Cwd
	read := func(gitArgs ...string) (string, bool) {
		out, err := git(ctx, append([]string{"-C", top}, gitArgs...)...)
		return string(out), err == nil
	}
	// Everything below runs from the top level: a pane may sit in a
	// subdirectory (an execution lane), and the commit is of the whole tree.
	root, ok := read("rev-parse", "--show-toplevel")
	top = strings.TrimSpace(root)
	if !ok || !validNodeCommandPath(top) {
		return refuse(nodeHandoverSnapshotGitError)
	}
	branch, ok := read("rev-parse", "--abbrev-ref", "HEAD")
	branch = strings.TrimSpace(branch)
	if !ok {
		return refuse(nodeHandoverSnapshotGitError)
	}
	if branch == "HEAD" || !validNodeRefName(branch) || args.Branch != "" && branch != args.Branch {
		return refuse(nodeHandoverSnapshotBranch)
	}
	result := nodeHandoverSnapshotResult{Outcome: nodeHandoverSnapshotSaved, Branch: branch, Top: top}
	remote, ok := read("ls-remote", "origin", "refs/heads/"+branch)
	if !ok {
		return refuse(nodeHandoverSnapshotGitError)
	}
	if fields := strings.Fields(remote); len(fields) >= 2 && fields[1] == "refs/heads/"+branch && nodeShaPattern.MatchString(fields[0]) {
		result.Pushed = strings.ToLower(fields[0])
	}
	from := result.Pushed
	if from != "" {
		if _, known := read("cat-file", "-e", from+"^{commit}"); !known {
			if _, fetched := read("fetch", "origin", "refs/heads/"+branch); !fetched {
				return refuse(nodeHandoverSnapshotGitError)
			}
		}
		// The successor starts at the pushed tip and fast-forwards to the
		// snapshot: the tip must be in this worktree's history.
		if _, ancestor := read("merge-base", "--is-ancestor", from, "HEAD"); !ancestor {
			return refuse(nodeHandoverSnapshotDiverged)
		}
	} else {
		head, ok := read("symbolic-ref", "--short", "refs/remotes/origin/HEAD")
		name := strings.TrimPrefix(strings.TrimSpace(head), "origin/")
		if !ok || !validNodeRefName(name) {
			return refuse(nodeHandoverSnapshotGitError)
		}
		base, ok := read("merge-base", "HEAD", "origin/"+name)
		base = strings.ToLower(strings.TrimSpace(base))
		if !ok || !nodeShaPattern.MatchString(base) {
			return refuse(nodeHandoverSnapshotGitError)
		}
		result.Base, result.BaseRef = base, "refs/heads/"+name
	}
	if !args.Snapshot {
		return result, ""
	}
	if from == "" {
		from = result.Base
	}
	// A submodule's own edits never reach the superproject's tree.
	status, ok1 := read("status", "--porcelain", "--ignore-submodules=none")
	staged, ok2 := read("ls-files", "-s")
	head, ok3 := read("rev-parse", "HEAD")
	headTree, ok4 := read("rev-parse", "HEAD^{tree}")
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return refuse(nodeHandoverSnapshotGitError)
	}
	head, headTree = strings.TrimSpace(head), strings.TrimSpace(headTree)
	for _, line := range strings.Split(staged, "\n") {
		if mode, rest, _ := strings.Cut(line, " "); mode == "160000" {
			if _, path, found := strings.Cut(rest, "\t"); found && handoverStatusNames(status, path) {
				result.Submodule = true
			}
		}
	}
	tree, commit, err := handoverSnapshotCommit(ctx, top, dir, args.Job)
	if err != nil {
		return refuse(nodeHandoverSnapshotGitError)
	}
	result.Dirty = tree != headTree || head != from
	if result.Dirty {
		ref := handoverSnapshotRefPrefix + args.Job
		// Only the snapshot ref: no tags followed (a v* tag would start a
		// release), no submodule pushes. Then the ref must read back on
		// origin as the commit, or the push did not happen.
		_, err := gitWithEnv(ctx, nil, "-C", top, "-c", "push.recurseSubmodules=no", "push", "--no-verify", "--no-follow-tags", "--recurse-submodules=no", "origin", "+"+commit+":"+ref)
		if out, lsErr := gitWithEnv(ctx, nil, "-C", top, "ls-remote", "origin", ref); err == nil && lsErr == nil && strings.HasPrefix(strings.TrimSpace(out), commit+"\t") {
			result.Commit, result.Ref = commit, ref
		}
	}
	// Losing nothing matters more than starting clean: the commit is
	// what git add stores (LFS pointers, clean-filtered files, nested
	// repositories as gitlinks), and the pane's processes may write
	// after it. The worktree stays until an operator clears the mark
	// (hk 5246).
	if err := replaceFileAtomicallyInRoot(job, fleet.HandoverKeepFile, []byte(top+"\n"), 0600); err != nil {
		return nil, "capture_failed"
	}
	result.Kept = true
	return result, ""
}

// handoverSnapshotCommit writes the whole worktree at top, untracked files
// included and ignored files left out, as a commit on top of HEAD through a
// temporary index in the job's directory.
func handoverSnapshotCommit(ctx context.Context, top, jobDir, job string) (tree, commit string, err error) {
	index, err := os.CreateTemp(jobDir, ".handover-index-*")
	if err != nil {
		return "", "", err
	}
	path := index.Name()
	index.Close()
	defer os.Remove(path)
	// git refuses an empty file as an index; it creates one where none is.
	if err := os.Remove(path); err != nil {
		return "", "", err
	}
	env := append([]string{"GIT_INDEX_FILE=" + path}, handoverSnapshotIdentity...)
	if _, err := gitWithEnv(ctx, env, "-C", top, "read-tree", "HEAD"); err != nil {
		return "", "", err
	}
	if _, err := gitWithEnv(ctx, env, "-C", top, "add", "-A", "--", "."); err != nil {
		return "", "", err
	}
	if tree, err = gitWithEnv(ctx, env, "-C", top, "write-tree"); err != nil {
		return "", "", err
	}
	tree = strings.TrimSpace(tree)
	if commit, err = gitWithEnv(ctx, env, "-C", top, "commit-tree", "--no-gpg-sign", tree, "-p", "HEAD", "-m", "panewire hand-over snapshot of "+job); err != nil {
		return "", "", err
	}
	return tree, strings.TrimSpace(commit), nil
}

// handoverStatusNames reports whether a porcelain status names path.
func handoverStatusNames(status, path string) bool {
	for _, line := range strings.Split(status, "\n") {
		if len(line) > 3 && strings.TrimSpace(line[3:]) == path {
			return true
		}
	}
	return false
}
