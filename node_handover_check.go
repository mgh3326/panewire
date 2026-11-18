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

// The receipt binds the worktree to the job, not merely to a matching basename
// and origin. A pane may have moved into a different checkout on the same branch.
func (client *HubClient) handoverCheckTarget(ctx context.Context, args nodeHandoverCheckArgs, target nodeCloseTarget) nodeHandoverCheckResult {
	result := client.handoverCheck(ctx, args)
	if result.Outcome != handoverCheckOK {
		return result
	}
	if !validNodeCommandPath(target.cwd) {
		return nodeHandoverCheckResult{Outcome: handoverGitFailed}
	}
	out, err := client.nodeGitRunner()(ctx, "-C", target.cwd, "rev-parse", "--show-toplevel")
	if err != nil {
		return nodeHandoverCheckResult{Outcome: handoverGitFailed}
	}
	want, err1 := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	got, err2 := filepath.EvalSymlinks(result.Top)
	if err1 != nil || err2 != nil || want != got {
		return nodeHandoverCheckResult{Outcome: handoverGitFailed}
	}
	return result
}

// handoverCheck refuses on any unreadable or hidden state. It checks every
// populated submodule separately because the superproject's status cannot see
// assume-unchanged or skip-worktree flags in a submodule's index.
func (client *HubClient) handoverCheck(ctx context.Context, args nodeHandoverCheckArgs) nodeHandoverCheckResult {
	refuse := func(reason string) nodeHandoverCheckResult { return nodeHandoverCheckResult{Outcome: reason} }
	git := client.nodeGitRunner()
	read := func(dir string, gitArgs ...string) (string, bool) {
		out, err := git(ctx, append([]string{"-C", dir}, gitArgs...)...)
		return string(out), err == nil
	}
	top, ok := read(args.Cwd, "rev-parse", "--show-toplevel")
	top = strings.TrimSpace(top)
	if !ok || !validNodeCommandPath(top) {
		return refuse(handoverGitFailed)
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
	remote, ok := read(top, "ls-remote", "origin", "refs/heads/"+args.Branch)
	if !ok {
		return refuse(handoverGitFailed)
	}
	fields := strings.Fields(remote)
	if len(fields) != 2 || fields[1] != "refs/heads/"+args.Branch || !nodeShaPattern.MatchString(fields[0]) || !strings.EqualFold(fields[0], head) {
		return refuse(handoverUnpushed)
	}
	return nodeHandoverCheckResult{Outcome: handoverCheckOK, Top: top, Branch: args.Branch, Pushed: head}
}
