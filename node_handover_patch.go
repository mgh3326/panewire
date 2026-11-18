package panewire

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/mgh3326/panewire/internal/fleet"
)

// worktree.handover_patch is the old node's half of a hub hand-over (hk 4633,
// docs/quota-stop.md "An exhausted pool"): before the hub stores the
// successor, the node reads the job's worktree and reports the branch's last
// pushed commit (git ls-remote on the node, which holds the origin's
// credentials) and the base it was cut from, and saves what a successor on
// another host cannot see — `git status --porcelain`, the untracked file list
// and `git diff --binary` against the pushed commit (or the base when nothing
// was pushed) — into the job's directory and to hk as evidence. It changes
// nothing in the worktree, so a dirty worktree never blocks the hand-over and
// a repeat after a hub restart only rewrites the same file.
const nodeCommandHandoverPatch = "worktree.handover_patch"

// hubCapabilityHandoverPatch: the node answers worktree.handover_patch. A hub
// hands an implementer over itself only on a node that advertises it.
const hubCapabilityHandoverPatch = "handover_patch_v1"

const (
	nodeHandoverPatchSaved    = "saved"
	nodeHandoverPatchNotAJob  = "not-a-job"
	nodeHandoverPatchBranch   = "branch_mismatch"
	nodeHandoverPatchGitError = "git_failed"
)

type nodeHandoverPatchArgs struct {
	Job    string `json:"job"`
	Cwd    string `json:"cwd"`
	Branch string `json:"branch"`
}

type nodeHandoverPatchResult struct {
	Outcome string `json:"outcome"`
	// Pushed is origin's refs/heads/<branch> as ls-remote read it; empty when
	// the branch was never pushed.
	Pushed string `json:"pushed,omitempty"`
	// Base is the merge-base of HEAD with origin's default branch, BaseRef
	// that branch as refs/heads/<name>: where a successor starts when nothing
	// was pushed.
	Base    string `json:"base,omitempty"`
	BaseRef string `json:"base_ref,omitempty"`
	// Dirty: the worktree holds work the pushed commit (or the base) lacks.
	Dirty bool   `json:"dirty"`
	Saved string `json:"saved,omitempty"`
	Doc   string `json:"doc,omitempty"`
}

func validNodeHandoverPatchArgs(args nodeHandoverPatchArgs) bool {
	return validNodeJobName(args.Job) && validNodeCommandPath(args.Cwd) && validNodeRefName(args.Branch)
}

func validNodeHandoverPatchResult(result nodeHandoverPatchResult) bool {
	switch result.Outcome {
	case nodeHandoverPatchSaved:
		if result.Pushed == "" && (!nodeShaPattern.MatchString(result.Base) || !validNodeFetchRef(result.BaseRef)) {
			return false
		}
		return (result.Pushed == "" || nodeShaPattern.MatchString(result.Pushed)) && validNodeCommandPath(result.Saved) &&
			(result.Doc == "" || result.Doc == fleet.HandoverPatchDocKey(handoverPatchJob(result.Saved)))
	case nodeHandoverPatchNotAJob, nodeHandoverPatchBranch, nodeHandoverPatchGitError:
		return result.Pushed == "" && result.Base == "" && result.Saved == "" && result.Doc == ""
	}
	return false
}

// handoverPatchJob is the job directory a saved patch path names.
func handoverPatchJob(saved string) string {
	if filepath.Base(saved) != fleet.HandoverPatchFile {
		return ""
	}
	return filepath.Base(filepath.Dir(saved))
}

func (client *HubClient) nodeHandoverPatch(ctx context.Context, args nodeHandoverPatchArgs) (any, string) {
	job, dir, found, err := client.openNodeJobDir(args.Job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "capture_failed"
	case !found:
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchNotAJob}, ""
	}
	defer job.Close()
	git := client.nodeGitRunner()
	read := func(gitArgs ...string) (string, bool) {
		out, err := git(ctx, append([]string{"-C", args.Cwd}, gitArgs...)...)
		return string(out), err == nil
	}
	branch, ok := read("rev-parse", "--abbrev-ref", "HEAD")
	if !ok {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
	}
	if strings.TrimSpace(branch) != args.Branch {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchBranch}, ""
	}
	result := nodeHandoverPatchResult{Outcome: nodeHandoverPatchSaved}
	remote, ok := read("ls-remote", "origin", "refs/heads/"+args.Branch)
	if !ok {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
	}
	if fields := strings.Fields(remote); len(fields) >= 2 && fields[1] == "refs/heads/"+args.Branch && nodeShaPattern.MatchString(fields[0]) {
		result.Pushed = strings.ToLower(fields[0])
	}
	from := result.Pushed
	if from != "" {
		if _, known := read("cat-file", "-e", from+"^{commit}"); !known {
			// Pushed from elsewhere and never fetched here: diff the commit
			// the worktree has, so the patch still applies to something real.
			if _, fetched := read("fetch", "origin", "refs/heads/"+args.Branch); !fetched {
				return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
			}
		}
	} else {
		head, ok := read("symbolic-ref", "--short", "refs/remotes/origin/HEAD")
		name := strings.TrimPrefix(strings.TrimSpace(head), "origin/")
		if !ok || !validNodeRefName(name) {
			return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
		}
		base, ok := read("merge-base", "HEAD", "origin/"+name)
		base = strings.ToLower(strings.TrimSpace(base))
		if !ok || !nodeShaPattern.MatchString(base) {
			return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
		}
		result.Base, result.BaseRef, from = base, "refs/heads/"+name, base
	}
	status, ok1 := read("status", "--porcelain")
	untracked, ok2 := read("ls-files", "--others", "--exclude-standard")
	diff, ok3 := read("diff", "--binary", from)
	head, ok4 := read("rev-parse", "HEAD")
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
	}
	result.Dirty = strings.TrimSpace(status) != "" || strings.TrimSpace(head) != from
	var body strings.Builder
	fmt.Fprintf(&body, "# hand-over patch for job %s\nbranch: %s\nhead: %s\ndiff from: %s\n", args.Job, args.Branch, strings.TrimSpace(head), from)
	fmt.Fprintf(&body, "\n## git status --porcelain\n%s\n## untracked files (contents not included)\n%s\n## git diff --binary %s\n%s", status, untracked, from, diff)
	if err := replaceFileAtomicallyInRoot(job, fleet.HandoverPatchFile, []byte(body.String()), 0600); err != nil {
		return nil, "capture_failed"
	}
	result.Saved = filepath.Join(dir, fleet.HandoverPatchFile)
	if result.Dirty {
		// A failed upload is one warning: the local file holds the patch and
		// the successor's note names that path instead.
		doc, err := fleet.UploadHandoverPatch(ctx, args.Job, nodeJobTask(job), []byte(body.String()))
		if err != nil {
			slog.Warn("hand-over patch upload failed; local patch retained", "job", args.Job, "err", err)
		}
		result.Doc = doc
	}
	return result, ""
}
