package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"unicode/utf8"

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
	// The refusals a hand-over cannot carry safely: the pushed tip is not in
	// the worktree's history, the patch is over hk's cap or more untracked
	// files than nodeHandoverPatchMaxUntracked, it is not UTF-8 (a JSON
	// string would rewrite it), or hk did not store it.
	nodeHandoverPatchDiverged     = "diverged"
	nodeHandoverPatchTooLarge     = "too_large"
	nodeHandoverPatchUnportable   = "unportable"
	nodeHandoverPatchUploadFailed = "upload_failed"
)

const nodeHandoverPatchMaxUntracked = 500

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
	Dirty bool `json:"dirty"`
	// Digest is the patch's sha256: the hub captures again just before the
	// close and releases the hand-over when the worktree changed meanwhile.
	Digest string `json:"digest,omitempty"`
	Saved  string `json:"saved,omitempty"`
	Doc    string `json:"doc,omitempty"`
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
		return (result.Pushed == "" || nodeShaPattern.MatchString(result.Pushed)) && validNodeCommandPath(result.Saved) && len(result.Digest) == 64 &&
			(result.Dirty == (result.Doc != "")) &&
			(result.Doc == "" || result.Doc == fleet.HandoverPatchDocKey(handoverPatchJob(result.Saved)))
	case nodeHandoverPatchNotAJob, nodeHandoverPatchBranch, nodeHandoverPatchGitError,
		nodeHandoverPatchDiverged, nodeHandoverPatchTooLarge, nodeHandoverPatchUnportable, nodeHandoverPatchUploadFailed:
		return result.Pushed == "" && result.Base == "" && result.Saved == "" && result.Doc == "" && result.Digest == ""
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
			if _, fetched := read("fetch", "origin", "refs/heads/"+args.Branch); !fetched {
				return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
			}
		}
		// The pushed tip must be in this worktree's history: a tip another
		// host moved ahead (or a diverged branch) would turn the diff into a
		// reversal of work already pushed.
		if _, ancestor := read("merge-base", "--is-ancestor", from, "HEAD"); !ancestor {
			return nodeHandoverPatchResult{Outcome: nodeHandoverPatchDiverged}, ""
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
	untracked, ok2 := read("ls-files", "-z", "--others", "--exclude-standard")
	diff, ok3 := read("diff", "--binary", from)
	head, ok4 := read("rev-parse", "HEAD")
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
	}
	var files []string
	for _, name := range strings.Split(untracked, "\x00") {
		if name != "" {
			files = append(files, name)
		}
	}
	if len(files) > nodeHandoverPatchMaxUntracked {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchTooLarge}, ""
	}
	// Untracked files travel with their contents: a new-file diff each, so
	// the one `git apply` restores them too.
	var added strings.Builder
	for _, name := range files {
		// --no-index exits 1 when the files differ, which they always do
		// here, so the output decides, not the exit status.
		out, _ := git(ctx, "-C", args.Cwd, "diff", "--no-index", "--binary", "--", "/dev/null", name)
		if !strings.HasPrefix(string(out), "diff --git") {
			return nodeHandoverPatchResult{Outcome: nodeHandoverPatchGitError}, ""
		}
		added.Write(out)
	}
	result.Dirty = strings.TrimSpace(status) != "" || strings.TrimSpace(head) != from
	var body strings.Builder
	fmt.Fprintf(&body, "# hand-over patch for job %s\nbranch: %s\nhead: %s\ndiff from: %s\n", args.Job, args.Branch, strings.TrimSpace(head), from)
	fmt.Fprintf(&body, "\n## git status --porcelain\n%s\n## patch: apply everything below with git apply\n%s%s", status, diff, added.String())
	patch := []byte(body.String())
	sum := sha256.Sum256(patch)
	result.Digest = hex.EncodeToString(sum[:])
	// The successor applies the hk copy, so it must be the whole patch, byte
	// for byte: never a cut tail, never bytes JSON would rewrite.
	if len(patch) > fleet.PaneScreenDocMaxBytes {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchTooLarge}, ""
	}
	if !utf8.Valid(patch) {
		return nodeHandoverPatchResult{Outcome: nodeHandoverPatchUnportable}, ""
	}
	if err := replaceFileAtomicallyInRoot(job, fleet.HandoverPatchFile, patch, 0600); err != nil {
		return nil, "capture_failed"
	}
	result.Saved = filepath.Join(dir, fleet.HandoverPatchFile)
	if result.Dirty {
		// No hand-over rests on a copy only the old host holds: a failed
		// upload refuses it, and the hub falls back to the owner notice.
		doc, err := fleet.UploadHandoverPatch(ctx, args.Job, nodeJobTask(job), patch)
		if err != nil {
			slog.Warn("hand-over patch upload failed; hand-over refused", "job", args.Job, "err", err)
			return nodeHandoverPatchResult{Outcome: nodeHandoverPatchUploadFailed}, ""
		}
		result.Doc = doc
	}
	return result, ""
}
