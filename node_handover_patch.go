package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/fleet"
)

// worktree.handover_patch is the old node's half of a hub hand-over (hk 4633,
// docs/quota-stop.md "An exhausted pool"): the node reads the job's worktree
// from its top level and reports the branch, its last pushed commit (git
// ls-remote on the node, which holds the origin's credentials) or the base it
// was cut from, and saves one patch a successor on another host can apply
// with git apply — every tracked change against the pushed commit (or the
// base) and every untracked file as a new-file diff — into the job's
// directory and, whole, to hk as evidence. It changes nothing in the
// worktree, so a dirty worktree never blocks the hand-over, and a repeat
// rewrites the same file and document. Whatever it cannot carry exactly is a
// refusal, never a partial patch.
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
	// the worktree's history, the patch is over hk's cap (or more untracked
	// files than nodeHandoverPatchMaxUntracked), it is not UTF-8 (a JSON
	// string would rewrite it), a submodule holds changes the patch cannot
	// restore, or hk did not store it.
	nodeHandoverPatchDiverged     = "diverged"
	nodeHandoverPatchTooLarge     = "too_large"
	nodeHandoverPatchUnportable   = "unportable"
	nodeHandoverPatchSubmodule    = "submodule_changed"
	nodeHandoverPatchUploadFailed = "upload_failed"
)

const nodeHandoverPatchMaxUntracked = 500

// nodeHandoverDiffFlags make every diff machine-applicable whatever the
// user's config says: no color, no textconv or external driver, the standard
// a/ b/ prefixes, paths from the top level.
var nodeHandoverDiffFlags = []string{"--binary", "--no-color", "--no-textconv", "--no-ext-diff", "--src-prefix=a/", "--dst-prefix=b/", "--no-relative"}

type nodeHandoverPatchArgs struct {
	Job string `json:"job"`
	Cwd string `json:"cwd"`
	// Branch is the branch the worktree must be on; empty accepts the
	// current one (a successor that started detached and made the branch).
	Branch string `json:"branch"`
}

type nodeHandoverPatchResult struct {
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
	// Dirty: the worktree holds work the pushed commit (or the base) lacks.
	Dirty bool `json:"dirty"`
	// Digest is the patch's sha256: the hub captures again just before the
	// close and releases the hand-over when the worktree changed meanwhile.
	Digest string `json:"digest,omitempty"`
	Saved  string `json:"saved,omitempty"`
	// Doc is the hk document holding the whole patch; always set on saved.
	Doc string `json:"doc,omitempty"`
}

func validNodeHandoverPatchArgs(args nodeHandoverPatchArgs) bool {
	return validNodeJobName(args.Job) && validNodeCommandPath(args.Cwd) && (args.Branch == "" || validNodeRefName(args.Branch))
}

func validNodeHandoverPatchResult(result nodeHandoverPatchResult) bool {
	switch result.Outcome {
	case nodeHandoverPatchSaved:
		if result.Pushed == "" && (!nodeShaPattern.MatchString(result.Base) || !validNodeFetchRef(result.BaseRef)) {
			return false
		}
		return validNodeRefName(result.Branch) && validNodeCommandPath(result.Top) &&
			(result.Pushed == "" || nodeShaPattern.MatchString(result.Pushed)) && validNodeCommandPath(result.Saved) && len(result.Digest) == 64 &&
			result.Doc == fleet.HandoverPatchDocKey(handoverPatchJob(result.Saved))
	case nodeHandoverPatchNotAJob, nodeHandoverPatchBranch, nodeHandoverPatchGitError,
		nodeHandoverPatchDiverged, nodeHandoverPatchTooLarge, nodeHandoverPatchUnportable, nodeHandoverPatchSubmodule, nodeHandoverPatchUploadFailed:
		return result == nodeHandoverPatchResult{Outcome: result.Outcome}
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

// errHandoverPatchTooLarge: a git output passed the patch's byte budget and
// the command was stopped before reading more.
var errHandoverPatchTooLarge = errors.New("hand-over patch over its budget")

// boundedGit runs one git command and keeps at most limit bytes of its
// output: past that it stops the command and answers errHandoverPatchTooLarge,
// so a huge diff never sits in the node's memory. ok reports an exit status
// in exits (git diff --no-index exits 1 when the files differ).
func (client *HubClient) boundedGit(ctx context.Context, limit int, exits []int, args ...string) ([]byte, error) {
	if client.nodeGit != nil {
		out, err := client.nodeGit(ctx, args...)
		if len(out) > limit {
			return nil, errHandoverPatchTooLarge
		}
		if err != nil && !slices.Contains(exits, 1) {
			return nil, err
		}
		return out, nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := nodeGitCommand(ctx, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	out, readErr := io.ReadAll(io.LimitReader(stdout, int64(limit)+1))
	if len(out) > limit {
		cancel()
		_ = cmd.Wait()
		return nil, errHandoverPatchTooLarge
	}
	waitErr := cmd.Wait()
	if readErr != nil {
		return nil, readErr
	}
	code := 0
	var exit *exec.ExitError
	switch {
	case errors.As(waitErr, &exit):
		code = exit.ExitCode()
	case waitErr != nil && !errors.Is(waitErr, exec.ErrWaitDelay):
		return nil, waitErr
	}
	if !slices.Contains(exits, code) {
		return nil, fmt.Errorf("git exited %d", code)
	}
	return out, nil
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
	refuse := func(outcome string) (any, string) { return nodeHandoverPatchResult{Outcome: outcome}, "" }
	git := client.nodeGitRunner()
	top := args.Cwd
	read := func(gitArgs ...string) (string, bool) {
		out, err := git(ctx, append([]string{"-C", top}, gitArgs...)...)
		return string(out), err == nil
	}
	// Every path below is from the top level: a pane may sit in a
	// subdirectory (an execution lane), and the successor applies the patch
	// at the checkout's root.
	root, ok := read("rev-parse", "--show-toplevel")
	top = strings.TrimSpace(root)
	if !ok || !validNodeCommandPath(top) {
		return refuse(nodeHandoverPatchGitError)
	}
	branch, ok := read("rev-parse", "--abbrev-ref", "HEAD")
	branch = strings.TrimSpace(branch)
	if !ok {
		return refuse(nodeHandoverPatchGitError)
	}
	if branch == "HEAD" || !validNodeRefName(branch) || args.Branch != "" && branch != args.Branch {
		return refuse(nodeHandoverPatchBranch)
	}
	result := nodeHandoverPatchResult{Outcome: nodeHandoverPatchSaved, Branch: branch, Top: top}
	remote, ok := read("ls-remote", "origin", "refs/heads/"+branch)
	if !ok {
		return refuse(nodeHandoverPatchGitError)
	}
	if fields := strings.Fields(remote); len(fields) >= 2 && fields[1] == "refs/heads/"+branch && nodeShaPattern.MatchString(fields[0]) {
		result.Pushed = strings.ToLower(fields[0])
	}
	from := result.Pushed
	if from != "" {
		if _, known := read("cat-file", "-e", from+"^{commit}"); !known {
			if _, fetched := read("fetch", "origin", "refs/heads/"+branch); !fetched {
				return refuse(nodeHandoverPatchGitError)
			}
		}
		// The pushed tip must be in this worktree's history: a tip another
		// host moved ahead (or a diverged branch) would turn the diff into a
		// reversal of work already pushed.
		if _, ancestor := read("merge-base", "--is-ancestor", from, "HEAD"); !ancestor {
			return refuse(nodeHandoverPatchDiverged)
		}
	} else {
		head, ok := read("symbolic-ref", "--short", "refs/remotes/origin/HEAD")
		name := strings.TrimPrefix(strings.TrimSpace(head), "origin/")
		if !ok || !validNodeRefName(name) {
			return refuse(nodeHandoverPatchGitError)
		}
		base, ok := read("merge-base", "HEAD", "origin/"+name)
		base = strings.ToLower(strings.TrimSpace(base))
		if !ok || !nodeShaPattern.MatchString(base) {
			return refuse(nodeHandoverPatchGitError)
		}
		result.Base, result.BaseRef, from = base, "refs/heads/"+name, base
	}
	status, ok1 := read("status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none")
	untracked, ok2 := read("ls-files", "-z", "--others", "--exclude-standard")
	staged, ok3 := read("ls-files", "-s")
	head, ok4 := read("rev-parse", "HEAD")
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return refuse(nodeHandoverPatchGitError)
	}
	// A submodule's own edits never reach the superproject's diff: any change
	// under a gitlink is refused rather than silently left behind.
	for _, line := range strings.Split(staged, "\n") {
		if mode, rest, _ := strings.Cut(line, " "); mode == "160000" {
			if _, path, found := strings.Cut(rest, "\t"); found && handoverStatusNames(status, path) {
				return refuse(nodeHandoverPatchSubmodule)
			}
		}
	}
	var files []string
	budget := int64(fleet.PaneScreenDocMaxBytes)
	for _, name := range strings.Split(untracked, "\x00") {
		if name == "" {
			continue
		}
		files = append(files, name)
		// The file's size bounds its new-file diff from below: refuse before
		// running git on anything that cannot fit.
		if info, err := os.Lstat(filepath.Join(top, name)); err == nil {
			budget -= info.Size()
		}
	}
	if len(files) > nodeHandoverPatchMaxUntracked || budget < 0 {
		return refuse(nodeHandoverPatchTooLarge)
	}
	limit := fleet.PaneScreenDocMaxBytes
	var patch strings.Builder
	diff, err := client.boundedGit(ctx, limit, []int{0}, append(append([]string{"-C", top, "diff"}, nodeHandoverDiffFlags...), from)...)
	if errors.Is(err, errHandoverPatchTooLarge) {
		return refuse(nodeHandoverPatchTooLarge)
	} else if err != nil {
		return refuse(nodeHandoverPatchGitError)
	}
	patch.Write(diff)
	for _, name := range files {
		// --no-index exits 1 when the files differ, which they always do;
		// any other status is a failed or cut output.
		out, err := client.boundedGit(ctx, limit-patch.Len(), []int{1}, append(append([]string{"-C", top, "diff", "--no-index"}, nodeHandoverDiffFlags[:6]...), "--", "/dev/null", name)...)
		if errors.Is(err, errHandoverPatchTooLarge) {
			return refuse(nodeHandoverPatchTooLarge)
		}
		if err != nil || !strings.HasPrefix(string(out), "diff --git") {
			return refuse(nodeHandoverPatchGitError)
		}
		patch.Write(out)
	}
	result.Dirty = strings.TrimSpace(status) != "" || len(files) > 0 || strings.TrimSpace(head) != from
	var body strings.Builder
	fmt.Fprintf(&body, "# hand-over patch for job %s\nbranch: %s\nhead: %s\ndiff from: %s\n", args.Job, branch, strings.TrimSpace(head), from)
	fmt.Fprintf(&body, "\n## git status --porcelain\n%s\n## patch: apply everything below with git apply at the checkout's top level\n%s", status, patch.String())
	saved := []byte(body.String())
	// The successor applies the hk copy, so it must be the whole patch, byte
	// for byte: never a cut tail, never bytes JSON would rewrite.
	if len(saved) > fleet.PaneScreenDocMaxBytes {
		return refuse(nodeHandoverPatchTooLarge)
	}
	if !utf8.Valid(saved) {
		return refuse(nodeHandoverPatchUnportable)
	}
	sum := sha256.Sum256(saved)
	if err := replaceFileAtomicallyInRoot(job, fleet.HandoverPatchFile, saved, 0600); err != nil {
		return nil, "capture_failed"
	}
	// Always uploaded, clean or not: the note names one document, and a
	// capture repeated after the close replaces it with the final state. No
	// hand-over rests on a copy only the old host holds.
	doc, err := fleet.UploadHandoverPatch(ctx, args.Job, nodeJobTask(job), saved)
	if err != nil {
		slog.Warn("hand-over patch upload failed; hand-over refused", "job", args.Job, "err", err)
		return refuse(nodeHandoverPatchUploadFailed)
	}
	result.Digest, result.Saved, result.Doc = hex.EncodeToString(sum[:]), filepath.Join(dir, fleet.HandoverPatchFile), doc
	return result, ""
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
