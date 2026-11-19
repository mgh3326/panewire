package panewire

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
)

// The read-only eligibility check for a pool hand-over (docs/quota-stop.md).
// The same check runs again within pane.close_op, bound to the saved commit.
const (
	nodeCommandHandoverCheck   = "worktree.handover_check"
	hubCapabilityHandoverCheck = "handover_check_v1"
	handoverCheckOK            = "ok"
	handoverDirty              = "dirty_worktree"
	handoverUnpushed           = "unpushed_commits"
	handoverBranchMismatch     = "branch_mismatch"
	handoverGitFailed          = "git_failed"
)

type nodeHandoverCheckArgs struct {
	Job    string `json:"job"`
	Cwd    string `json:"cwd"`
	Origin string `json:"origin"`
	Branch string `json:"branch"`
}

type nodeHandoverCheckResult struct {
	Outcome string `json:"outcome"`
	Top     string `json:"top,omitempty"`
	Branch  string `json:"branch,omitempty"`
	Pushed  string `json:"pushed,omitempty"`
}

func validNodeHandoverCheckArgs(args nodeHandoverCheckArgs) bool {
	return validNodeJobName(args.Job) && validNodeCommandPath(args.Cwd) && args.Origin != "" && validNodeRefName(args.Branch)
}

func validNodeHandoverCheckResult(result nodeHandoverCheckResult) bool {
	switch result.Outcome {
	case handoverCheckOK:
		return validNodeCommandPath(result.Top) && validNodeRefName(result.Branch) && nodeShaPattern.MatchString(result.Pushed)
	case handoverDirty, handoverUnpushed, handoverBranchMismatch, handoverGitFailed, nodeCaptureNotAJob:
		return reflect.DeepEqual(result, nodeHandoverCheckResult{Outcome: result.Outcome})
	}
	return false
}

func (client *HubClient) nodeHandoverCheck(ctx context.Context, args nodeHandoverCheckArgs) (any, string) {
	job, _, found, err := client.openNodeJobDir(args.Job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "worktree_unavailable"
	case !found:
		return nodeHandoverCheckResult{Outcome: nodeCaptureNotAJob}, ""
	}
	defer job.Close()
	target, err := readNodeCloseTarget(job)
	if err != nil || !target.receipt || target.ended {
		return nodeHandoverCheckResult{Outcome: handoverGitFailed}, ""
	}
	return client.handoverCheckTarget(ctx, args, target), ""
}

// nodeHandoverPrepared holds the remote tip and worktree identity obtained
// before the final pane look. The local check compares HEAD with this tip.
type nodeHandoverPrepared struct {
	args                 nodeHandoverCheckArgs
	top, tip, receiptCwd string
}

// handoverIdentity validates the worktree root, branch, origin and the
// receipt's worktree, with local reads only. Nothing here writes the checkout.
func (client *HubClient) handoverIdentity(ctx context.Context, args nodeHandoverCheckArgs, target *nodeCloseTarget) (nodeHandoverPrepared, string) {
	git := client.nodeGitRunner()
	read := func(dir string, gitArgs ...string) (string, bool) {
		out, err := git(ctx, append([]string{"-C", dir}, gitArgs...)...)
		return string(out), err == nil && ctx.Err() == nil
	}
	top, ok := read(args.Cwd, "rev-parse", "--show-toplevel")
	top = strings.TrimSpace(top)
	if !ok || !validNodeCommandPath(top) {
		return nodeHandoverPrepared{}, handoverGitFailed
	}
	branch, ok := read(top, "symbolic-ref", "--quiet", "--short", "HEAD")
	if !ok || strings.TrimSpace(branch) != args.Branch {
		return nodeHandoverPrepared{}, handoverBranchMismatch
	}
	origin, ok := read(top, "remote", "get-url", "origin")
	if !ok || strings.TrimSpace(origin) != args.Origin {
		return nodeHandoverPrepared{}, handoverGitFailed
	}
	prepared := nodeHandoverPrepared{args: args, top: top}
	if target != nil {
		if !validNodeCommandPath(target.cwd) {
			return nodeHandoverPrepared{}, handoverGitFailed
		}
		out, ok := read(target.cwd, "rev-parse", "--show-toplevel")
		want, err1 := filepath.EvalSymlinks(strings.TrimSpace(out))
		got, err2 := filepath.EvalSymlinks(top)
		if !ok || err1 != nil || err2 != nil || want != got {
			return nodeHandoverPrepared{}, handoverGitFailed
		}
		prepared.receiptCwd = target.cwd
	}
	return prepared, ""
}

// handoverPrepare does the identity checks and the network read before the
// final pane look.
func (client *HubClient) handoverPrepare(ctx context.Context, args nodeHandoverCheckArgs, target *nodeCloseTarget) (nodeHandoverPrepared, string) {
	prepared, reason := client.handoverIdentity(ctx, args, target)
	if reason != "" {
		return nodeHandoverPrepared{}, reason
	}
	git := client.nodeGitRunner()
	out, err := git(ctx, "-C", prepared.top, "ls-remote", "origin", "refs/heads/"+args.Branch)
	if err != nil || ctx.Err() != nil {
		return nodeHandoverPrepared{}, handoverGitFailed
	}
	fields := strings.Fields(string(out))
	if len(fields) != 2 || fields[1] != "refs/heads/"+args.Branch || !nodeShaPattern.MatchString(fields[0]) {
		return nodeHandoverPrepared{}, handoverUnpushed
	}
	prepared.tip = strings.ToLower(fields[0])
	return prepared, ""
}

// handoverCheckLocal uses only local Git reads. It inspects every populated
// submodule because its index can hide changes from the superproject's status.
func (client *HubClient) handoverCheckLocal(ctx context.Context, prepared nodeHandoverPrepared) nodeHandoverCheckResult {
	refuse := func(reason string) nodeHandoverCheckResult { return nodeHandoverCheckResult{Outcome: reason} }
	git := client.nodeGitRunner()
	read := func(dir string, gitArgs ...string) (string, bool) {
		// A configured fsmonitor hook can invoke arbitrary slow work: the last
		// check uses Git's local view, within the caller's deadline.
		out, err := git(ctx, append([]string{"-C", dir, "-c", "core.fsmonitor=false"}, gitArgs...)...)
		return string(out), err == nil && ctx.Err() == nil
	}
	args, top := prepared.args, prepared.top
	current, ok := read(args.Cwd, "rev-parse", "--show-toplevel")
	if !ok || strings.TrimSpace(current) != top {
		return refuse(handoverGitFailed)
	}
	if prepared.receiptCwd != "" {
		current, ok = read(prepared.receiptCwd, "rev-parse", "--show-toplevel")
		if !ok || strings.TrimSpace(current) != top {
			return refuse(handoverGitFailed)
		}
	}
	branch, ok := read(top, "symbolic-ref", "--quiet", "--short", "HEAD")
	if !ok || strings.TrimSpace(branch) != args.Branch {
		return refuse(handoverBranchMismatch)
	}
	origin, ok := read(top, "remote", "get-url", "origin")
	if !ok || strings.TrimSpace(origin) != args.Origin {
		return refuse(handoverGitFailed)
	}
	submodules, ok := read(top, "submodule", "foreach", "--quiet", "--recursive", `printf '%s\0' "$PWD"`)
	if !ok {
		return refuse(handoverGitFailed)
	}
	dirs := append([]string{top}, strings.Split(submodules, "\x00")...)
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		status, ok := read(dir, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignore-submodules=none")
		if !ok {
			return refuse(handoverGitFailed)
		}
		if status != "" {
			return refuse(handoverDirty)
		}
		files, ok := read(dir, "ls-files", "-v", "-z")
		if !ok {
			return refuse(handoverGitFailed)
		}
		for _, entry := range strings.Split(files, "\x00") {
			if len(entry) >= 2 && (entry[0] >= 'a' && entry[0] <= 'z' || entry[0] == 'S') {
				return refuse(handoverDirty)
			}
		}
	}
	head, ok := read(top, "rev-parse", "--verify", "HEAD^{commit}")
	head = strings.ToLower(strings.TrimSpace(head))
	if !ok || !nodeShaPattern.MatchString(head) {
		return refuse(handoverGitFailed)
	}
	if head != prepared.tip {
		return refuse(handoverUnpushed)
	}
	return nodeHandoverCheckResult{Outcome: handoverCheckOK, Top: top, Branch: args.Branch, Pushed: prepared.tip}
}

func (client *HubClient) handoverCheck(ctx context.Context, args nodeHandoverCheckArgs) nodeHandoverCheckResult {
	prepared, reason := client.handoverPrepare(ctx, args, nil)
	if reason != "" {
		return nodeHandoverCheckResult{Outcome: reason}
	}
	return client.handoverCheckLocal(ctx, prepared)
}

func (client *HubClient) handoverCheckTarget(ctx context.Context, args nodeHandoverCheckArgs, target nodeCloseTarget) nodeHandoverCheckResult {
	prepared, reason := client.handoverPrepare(ctx, args, &target)
	if reason != "" {
		return nodeHandoverCheckResult{Outcome: reason}
	}
	return client.handoverCheckLocal(ctx, prepared)
}
