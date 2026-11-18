package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/fleet"
)

// worktree.handover_keep is the old node's half of a hub hand-over (hk 4633,
// docs/quota-stop.md "An exhausted pool"). From the worktree's top level it
// reports the branch, its last pushed commit (git ls-remote on the node,
// which holds the origin's credentials) or the base it was cut from, HEAD,
// and — with Keep set, at the close, after the hub saw the pane idle — the
// uncommitted paths (git status, untracked included), and writes
// fleet.HandoverKeepFile into the job's directory. That mark keeps the old
// worktree from every sweep, worktree.remove and the job-directory TTL, and
// the successor's note names it as host:path. Nothing is committed or pushed
// (a ref snapshot is hk 5259), so a dirty worktree never blocks the hand-over
// and no file leaves the host.
const nodeCommandHandoverKeep = "worktree.handover_keep"

// hubCapabilityHandoverKeep: the node answers worktree.handover_keep. A hub
// hands an implementer over itself only on a node that advertises it.
const hubCapabilityHandoverKeep = "handover_keep_v1"

const (
	nodeHandoverKeepOK       = "ok"
	nodeHandoverKeepNotAJob  = "not-a-job"
	nodeHandoverKeepBranch   = "branch_mismatch"
	nodeHandoverKeepGitError = "git_failed"
	// nodeHandoverKeepDiverged: the pushed tip is not in the worktree's
	// history, so a successor started from it does not continue this work.
	nodeHandoverKeepDiverged = "diverged"
)

// nodeHandoverKeepMaxListed and nodeHandoverKeepListBytes bound the listed
// paths, so the result stays well inside nodeCommandMaxResultBytes.
const (
	nodeHandoverKeepMaxListed = 50
	nodeHandoverKeepListBytes = 8 << 10
)

type nodeHandoverKeepArgs struct {
	Job string `json:"job"`
	Cwd string `json:"cwd"`
	// Branch is the branch the worktree must be on; empty accepts the
	// current one (a successor that started detached and made the branch).
	Branch string `json:"branch"`
	// Keep: list the uncommitted paths and mark the worktree kept. Without
	// it the command only reads, so the sweep can build the successor
	// before anything changes.
	Keep bool `json:"keep"`
}

type nodeHandoverKeepResult struct {
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
	// Head is the worktree's HEAD; set with Keep. It differs from the start
	// point when the old job made commits it never pushed.
	Head string `json:"head,omitempty"`
	// Dirty: the worktree holds work its start point lacks (uncommitted
	// paths or unpushed commits). Set only with Keep.
	Dirty bool `json:"dirty"`
	// Uncommitted lists the paths git status names (at most
	// nodeHandoverKeepMaxListed and nodeHandoverKeepListBytes;
	// UncommittedMore counts the rest).
	Uncommitted     []string `json:"uncommitted,omitempty"`
	UncommittedMore int      `json:"uncommitted_more,omitempty"`
	// Kept: the job's worktree is marked by HandoverKeepFile.
	Kept bool `json:"kept"`
}

// validHandoverListedPath is one printable relative path for the note.
func validHandoverListedPath(path string) bool {
	if path == "" || len(path) > 1024 || !utf8.ValidString(path) || strings.HasPrefix(path, "/") {
		return false
	}
	for _, r := range path {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validNodeHandoverKeepArgs(args nodeHandoverKeepArgs) bool {
	return validNodeJobName(args.Job) && validNodeCommandPath(args.Cwd) && (args.Branch == "" || validNodeRefName(args.Branch))
}

func validNodeHandoverKeepResult(result nodeHandoverKeepResult) bool {
	switch result.Outcome {
	case nodeHandoverKeepOK:
		if result.Pushed == "" && (!nodeShaPattern.MatchString(result.Base) || !validNodeFetchRef(result.BaseRef)) {
			return false
		}
		if result.Head != "" && !nodeShaPattern.MatchString(result.Head) || len(result.Uncommitted) > nodeHandoverKeepMaxListed || result.UncommittedMore < 0 {
			return false
		}
		listed := 0
		for _, path := range result.Uncommitted {
			listed += len(path)
			if !validHandoverListedPath(path) || listed > nodeHandoverKeepListBytes {
				return false
			}
		}
		return validNodeRefName(result.Branch) && validNodeCommandPath(result.Top) && (result.Pushed == "" || nodeShaPattern.MatchString(result.Pushed))
	case nodeHandoverKeepNotAJob, nodeHandoverKeepBranch, nodeHandoverKeepGitError, nodeHandoverKeepDiverged:
		return reflect.DeepEqual(result, nodeHandoverKeepResult{Outcome: result.Outcome})
	}
	return false
}

func (client *HubClient) nodeHandoverKeep(ctx context.Context, args nodeHandoverKeepArgs) (any, string) {
	job, _, found, err := client.openNodeJobDir(args.Job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "capture_failed"
	case !found:
		return nodeHandoverKeepResult{Outcome: nodeHandoverKeepNotAJob}, ""
	}
	defer job.Close()
	refuse := func(outcome string) (any, string) { return nodeHandoverKeepResult{Outcome: outcome}, "" }
	git := client.nodeGitRunner()
	top := args.Cwd
	read := func(gitArgs ...string) (string, bool) {
		out, err := git(ctx, append([]string{"-C", top}, gitArgs...)...)
		return string(out), err == nil
	}
	// Everything below runs from the top level: a pane may sit in a
	// subdirectory (an execution lane).
	root, ok := read("rev-parse", "--show-toplevel")
	top = strings.TrimSpace(root)
	if !ok || !validNodeCommandPath(top) {
		return refuse(nodeHandoverKeepGitError)
	}
	branch, ok := read("rev-parse", "--abbrev-ref", "HEAD")
	branch = strings.TrimSpace(branch)
	if !ok {
		return refuse(nodeHandoverKeepGitError)
	}
	if branch == "HEAD" || !validNodeRefName(branch) || args.Branch != "" && branch != args.Branch {
		return refuse(nodeHandoverKeepBranch)
	}
	result := nodeHandoverKeepResult{Outcome: nodeHandoverKeepOK, Branch: branch, Top: top}
	remote, ok := read("ls-remote", "origin", "refs/heads/"+branch)
	if !ok {
		return refuse(nodeHandoverKeepGitError)
	}
	if fields := strings.Fields(remote); len(fields) >= 2 && fields[1] == "refs/heads/"+branch && nodeShaPattern.MatchString(fields[0]) {
		result.Pushed = strings.ToLower(fields[0])
	}
	from := result.Pushed
	if from != "" {
		if _, known := read("cat-file", "-e", from+"^{commit}"); !known {
			if _, fetched := read("fetch", "origin", "refs/heads/"+branch); !fetched {
				return refuse(nodeHandoverKeepGitError)
			}
		}
		// The successor starts at the pushed tip: it must be in this
		// worktree's history, or the kept work is not a continuation of it.
		if _, ancestor := read("merge-base", "--is-ancestor", from, "HEAD"); !ancestor {
			return refuse(nodeHandoverKeepDiverged)
		}
	} else {
		head, ok := read("symbolic-ref", "--short", "refs/remotes/origin/HEAD")
		name := strings.TrimPrefix(strings.TrimSpace(head), "origin/")
		if !ok || !validNodeRefName(name) {
			return refuse(nodeHandoverKeepGitError)
		}
		base, ok := read("merge-base", "HEAD", "origin/"+name)
		base = strings.ToLower(strings.TrimSpace(base))
		if !ok || !nodeShaPattern.MatchString(base) {
			return refuse(nodeHandoverKeepGitError)
		}
		result.Base, result.BaseRef, from = base, "refs/heads/"+name, base
	}
	if !args.Keep {
		return result, ""
	}
	// The mark and the git lock are published under the clone's git-op lock,
	// the one worktree.remove holds while it rechecks both before deleting.
	common, ok := read("rev-parse", "--path-format=absolute", "--git-common-dir")
	if !ok {
		return refuse(nodeHandoverKeepGitError)
	}
	lockPath, err := nodeGitOpLockPath(filepath.Dir(strings.TrimSpace(common)))
	if err != nil {
		return refuse(nodeHandoverKeepGitError)
	}
	release, err := acquireNodeGitOpLock(ctx, lockPath, client.nodeGitOpWait())
	if err != nil {
		// Busy or cancelled: the hub retries the keep.
		return refuse(nodeHandoverKeepGitError)
	}
	defer release()
	status, ok1 := read("status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignore-submodules=none")
	head, ok2 := read("rev-parse", "HEAD")
	files, ok3 := read("ls-files", "-v", "-z")
	if !ok1 || !ok2 || !ok3 {
		return refuse(nodeHandoverKeepGitError)
	}
	result.Head = strings.ToLower(strings.TrimSpace(head))
	listed := 0
	// git status hides an edit to a path marked assume-unchanged: list
	// every such path, since the kept worktree may hold its only copy.
	for _, path := range append(handoverStatusPaths(status), handoverAssumedPaths(top, files)...) {
		if len(result.Uncommitted) < nodeHandoverKeepMaxListed && validHandoverListedPath(path) && listed+len(path) <= nodeHandoverKeepListBytes {
			result.Uncommitted = append(result.Uncommitted, path)
			listed += len(path)
		} else {
			result.UncommittedMore++
		}
	}
	result.Dirty = len(result.Uncommitted) > 0 || result.UncommittedMore > 0 || result.Head != from
	// Every hand-over keeps the old worktree: whatever is not on origin
	// (uncommitted, untracked and ignored files, unpushed commits) is
	// recovered from it. The mark stays until it is cleared (hk 5246).
	if err := replaceFileAtomicallyInRoot(job, fleet.HandoverKeepFile, []byte(top+"\n"), 0600); err != nil {
		return nil, "capture_failed"
	}
	// A git worktree lock also holds against node binaries that predate the
	// mark (a rollback): their removals pass a single --force, which git
	// refuses for a locked worktree. A main checkout cannot be locked and
	// no sweep removes it.
	gitDir, _ := read("rev-parse", "--path-format=absolute", "--git-dir")
	if strings.TrimSpace(gitDir) != strings.TrimSpace(common) {
		if _, locked := read("worktree", "lock", "--reason", nodeHandoverKeepLockReason, top); !locked {
			if list, ok := read("worktree", "list", "--porcelain"); !ok || !handoverWorktreeLocked(list, top) {
				return refuse(nodeHandoverKeepGitError)
			}
		}
	}
	result.Kept = true
	return result, ""
}

// handoverStatusPaths reads the paths of git status --porcelain=v1 -z: each
// record is "XY path", and a rename or copy is followed by its source path.
func handoverStatusPaths(status string) []string {
	var paths []string
	records := strings.Split(status, "\x00")
	for i := 0; i < len(records); i++ {
		record := records[i]
		if len(record) < 4 {
			continue
		}
		paths = append(paths, record[3:])
		if record[0] == 'R' || record[0] == 'C' {
			i++
		}
	}
	return paths
}

// nodeHandoverKeepLockReason is the git worktree lock reason a hand-over sets.
const nodeHandoverKeepLockReason = "panewire handover-keep"

// handoverAssumedPaths reads git ls-files -v -z and returns the paths marked
// assume-unchanged (a lowercase tag) or skip-worktree (S) and present in
// top, whose edits git status does not show. An absent skip-worktree path is
// a sparse checkout's, not work.
func handoverAssumedPaths(top, files string) []string {
	var paths []string
	for _, record := range strings.Split(files, "\x00") {
		if len(record) < 3 || record[1] != ' ' {
			continue
		}
		tag, path := record[0], record[2:]
		hidden := tag >= 'a' && tag <= 'z'
		if tag == 'S' || tag == 's' {
			_, err := os.Lstat(filepath.Join(top, filepath.FromSlash(path)))
			hidden = err == nil
		}
		if hidden {
			paths = append(paths, path)
		}
	}
	return paths
}

// handoverWorktreeLocked reports whether git worktree list --porcelain shows
// path locked with the hand-over's own reason (an earlier keep of the same
// worktree). Another holder's lock is never adopted: its owner may unlock it.
func handoverWorktreeLocked(list, path string) bool {
	for _, block := range strings.Split(list, "\n\n") {
		lines := strings.Split(block, "\n")
		if lines[0] != "worktree "+path {
			continue
		}
		for _, line := range lines[1:] {
			if line == "locked "+nodeHandoverKeepLockReason {
				return true
			}
		}
	}
	return false
}
