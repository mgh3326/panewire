package panewire

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/jobowner"
)

// worktree.handover_snapshot (hk 5259; docs/node-commands.md): the node saves
// a dirty or unpushed worktree as a commit on refs/panewire/handover/<job>/
// <commit> and reads it back from origin, before the hub closes the old pane.
// Only tracked contents and unpushed commits travel. Untracked files are never
// added or pushed: they stay in the old worktree and are listed in the result.
const (
	nodeCommandHandoverSnapshot   = "worktree.handover_snapshot"
	hubCapabilityHandoverSnapshot = "handover_snapshot_v1"

	handoverSnapshotOK       = "ok"
	handoverSnapshotRefused  = "snapshot_refused"
	handoverSnapshotPushFail = "push_failed"
	handoverSnapshotUnproven = "origin_mismatch"
	handoverEpochMismatch    = "epoch_mismatch"

	// The reasons of a snapshot_refused outcome: state the snapshot cannot
	// carry, so the worktree is not handed over.
	handoverRefusedSubmodule    = "submodule_changes"
	handoverRefusedGitlink      = "staged_gitlink"
	handoverRefusedSkipWorktree = "skip_worktree_file"
	handoverRefusedUnmerged     = "unmerged_index"
	handoverRefusedChanged      = "changed_during_snapshot"

	handoverSnapshotRefPrefix = "refs/panewire/handover/"
	// nodeHandoverSnapshotMaxListed bounds the untracked paths a result names
	// and nodeHandoverSnapshotListBytes their bytes, so the result stays well
	// inside nodeCommandMaxResultBytes.
	nodeHandoverSnapshotMaxListed = 50
	nodeHandoverSnapshotListBytes = 8 << 10
)

// nodeHandoverSnapshotRunTimeout is the budget of the whole command: shorter
// than the hub's wait for a node command (hubNodeCommandWait), so the node's
// answer arrives first. A var so a test can shorten it.
var nodeHandoverSnapshotRunTimeout = 25 * time.Second

// handoverRefusal is state the snapshot cannot carry; reason is one of the
// handoverRefused* values.
type handoverRefusal struct{ reason string }

func (r handoverRefusal) Error() string { return "snapshot refused: " + r.reason }

type nodeHandoverSnapshotArgs struct {
	Job    string `json:"job"`
	Epoch  uint64 `json:"epoch"`
	Cwd    string `json:"cwd"`
	Origin string `json:"origin"`
	Branch string `json:"branch"`
}

type nodeHandoverSnapshotResult struct {
	Outcome string `json:"outcome"`
	Reason  string `json:"reason,omitempty"`
	Top     string `json:"top,omitempty"`
	Branch  string `json:"branch,omitempty"`
	Ref     string `json:"ref,omitempty"`
	Commit  string `json:"commit,omitempty"`
	Tree    string `json:"tree,omitempty"`
	Parent  string `json:"parent,omitempty"`
	// Untracked lists the worktree's untracked, non-ignored paths (and
	// intent-to-add entries), at most nodeHandoverSnapshotMaxListed of them;
	// UntrackedMore counts the rest. They are not in the snapshot.
	Untracked     []string `json:"untracked,omitempty"`
	UntrackedMore int      `json:"untracked_more,omitempty"`
}

func validNodeHandoverSnapshotArgs(args nodeHandoverSnapshotArgs) bool {
	return validNodeHandoverCheckArgs(nodeHandoverCheckArgs{Job: args.Job, Cwd: args.Cwd, Origin: args.Origin, Branch: args.Branch}) && args.Epoch > 0
}

func handoverSnapshotRef(job, commit string) string {
	return handoverSnapshotRefPrefix + job + "/" + commit
}

// validHandoverFetchRef is the fetch a successor spec checkout may name (hk
// 5324): exactly the hand-over ref form, so a submitted checkout cannot pull
// an arbitrary ref into the successor's clone. The general fetch grammar
// (spawnargs.ValidFetchRef) stays closed to it.
func validHandoverFetchRef(ref string) bool {
	job, commit, found := strings.Cut(strings.TrimPrefix(ref, handoverSnapshotRefPrefix), "/")
	return strings.HasPrefix(ref, handoverSnapshotRefPrefix) && found && fleet.ValidJobID(job) && nodeShaPattern.MatchString(commit)
}

// validHandoverUntrackedPath is one printable relative path for the note.
func validHandoverUntrackedPath(path string) bool {
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

func validNodeHandoverSnapshotResult(result nodeHandoverSnapshotResult) bool {
	switch result.Outcome {
	case handoverSnapshotOK:
		job, _ := strings.CutPrefix(strings.TrimSuffix(result.Ref, "/"+result.Commit), handoverSnapshotRefPrefix)
		if !nodeShaPattern.MatchString(result.Commit) || !nodeShaPattern.MatchString(result.Tree) || !nodeShaPattern.MatchString(result.Parent) ||
			!validNodeJobName(job) || result.Ref != handoverSnapshotRef(job, result.Commit) || result.Reason != "" {
			return false
		}
		if len(result.Untracked) > nodeHandoverSnapshotMaxListed || result.UntrackedMore < 0 {
			return false
		}
		listed := 0
		for _, path := range result.Untracked {
			listed += len(path)
			if !validHandoverUntrackedPath(path) || listed > nodeHandoverSnapshotListBytes {
				return false
			}
		}
		return validNodeCommandPath(result.Top) && validNodeRefName(result.Branch)
	case handoverSnapshotRefused:
		switch result.Reason {
		case handoverRefusedSubmodule, handoverRefusedGitlink, handoverRefusedSkipWorktree, handoverRefusedUnmerged, handoverRefusedChanged:
			return reflect.DeepEqual(result, nodeHandoverSnapshotResult{Outcome: result.Outcome, Reason: result.Reason})
		}
		return false
	case handoverSnapshotPushFail, handoverSnapshotUnproven, handoverEpochMismatch, handoverBranchMismatch, handoverGitFailed, nodeCaptureNotAJob:
		return reflect.DeepEqual(result, nodeHandoverSnapshotResult{Outcome: result.Outcome})
	}
	return false
}

// nodeHandoverSnapshot is the command. The caller holds nodeCommandMu: the
// snapshot must not interleave with a close.
func (client *HubClient) nodeHandoverSnapshot(ctx context.Context, args nodeHandoverSnapshotArgs) (any, string) {
	ctx, cancel := context.WithTimeout(ctx, nodeHandoverSnapshotRunTimeout)
	defer cancel()
	outcome := func(name string) (any, string) {
		if ctx.Err() != nil {
			return nil, "snapshot_failed"
		}
		return nodeHandoverSnapshotResult{Outcome: name}, ""
	}
	job, _, found, err := client.openNodeJobDir(args.Job)
	switch {
	case errors.Is(err, errNodeJobsRootUnset):
		return nil, "jobs_root_unset"
	case err != nil:
		return nil, "worktree_unavailable"
	case !found:
		return outcome(nodeCaptureNotAJob)
	}
	defer job.Close()
	// A newer owner than the hub's epoch means this request is stale.
	client.assignmentMu.Lock()
	epoch := client.assignedJobs[args.Job]
	client.assignmentMu.Unlock()
	stored, err := jobowner.Epoch(filepath.Join(client.jobsInboxRoot, "jobs"), args.Job)
	if err != nil {
		return outcome(handoverEpochMismatch)
	}
	if stored > epoch {
		epoch = stored
	}
	if epoch > args.Epoch {
		return outcome(handoverEpochMismatch)
	}
	target, err := readNodeCloseTarget(job)
	if err != nil || !target.receipt || target.ended {
		return outcome(handoverGitFailed)
	}
	prepared, reason := client.handoverIdentity(ctx, nodeHandoverCheckArgs{Job: args.Job, Cwd: args.Cwd, Origin: args.Origin, Branch: args.Branch}, &target)
	if reason != "" {
		return outcome(reason)
	}
	scratch, err := os.MkdirTemp("", "panewire-handover-*")
	if err != nil {
		return outcome(handoverGitFailed)
	}
	defer os.RemoveAll(scratch)
	snapshot, err := handoverSnapshotPush(ctx, prepared.top, scratch, args.Job)
	var refused handoverRefusal
	switch {
	case ctx.Err() != nil:
		return nil, "snapshot_failed"
	case errors.As(err, &refused):
		return nodeHandoverSnapshotResult{Outcome: handoverSnapshotRefused, Reason: refused.reason}, ""
	case errors.Is(err, errHandoverSnapshotPush):
		return outcome(handoverSnapshotPushFail)
	case errors.Is(err, errHandoverSnapshotOrigin):
		return outcome(handoverSnapshotUnproven)
	case err != nil:
		return outcome(handoverGitFailed)
	}
	result := nodeHandoverSnapshotResult{Outcome: handoverSnapshotOK, Top: prepared.top, Branch: args.Branch,
		Ref: snapshot.Ref, Commit: snapshot.Commit, Tree: snapshot.Tree, Parent: snapshot.Parent}
	// Listed last, so the note names the worktree as the snapshot left it.
	untracked, err := handoverSnapshotLocalOnly(ctx, prepared.top)
	if err != nil {
		return outcome(handoverGitFailed)
	}
	for _, path := range untracked {
		if len(result.Untracked) < nodeHandoverSnapshotMaxListed && validHandoverUntrackedPath(path) && listedBytes(result.Untracked)+len(path) <= nodeHandoverSnapshotListBytes {
			result.Untracked = append(result.Untracked, path)
		} else {
			result.UntrackedMore++
		}
	}
	return result, ""
}

func listedBytes(paths []string) int {
	total := 0
	for _, path := range paths {
		total += len(path)
	}
	return total
}

var (
	errHandoverSnapshotPush   = errors.New("snapshot push failed")
	errHandoverSnapshotOrigin = errors.New("snapshot origin verification failed")
)

// handoverSnapshotGit runs one git command in top. Literal pathspecs keep a
// file name from being read as a pattern; the fsmonitor hook never runs; a
// credential prompt cannot wait on a terminal.
func handoverSnapshotGit(ctx context.Context, top string, env []string, stdin string, args ...string) (string, error) {
	cmd := nodeGitCommand(ctx, append([]string{"-C", top, "--literal-pathspecs", "-c", "core.fsmonitor=false"}, args...)...)
	cmd.Env = append(cmd.Env, "GIT_TERMINAL_PROMPT=0")
	cmd.Env = append(cmd.Env, env...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	out, err := cmd.Output()
	return string(out), err
}

func handoverNULs(out string) []string {
	var items []string
	for _, item := range strings.Split(out, "\x00") {
		if item != "" {
			items = append(items, item)
		}
	}
	return items
}

// handoverIndexEntry is one line of ls-files --stage -z.
type handoverIndexEntry struct {
	line, mode, stage string
}

func handoverIndex(ctx context.Context, top string) (map[string]handoverIndexEntry, error) {
	out, err := handoverSnapshotGit(ctx, top, nil, "", "ls-files", "--stage", "-z")
	if err != nil {
		return nil, err
	}
	entries := map[string]handoverIndexEntry{}
	for _, item := range handoverNULs(out) {
		meta, path, found := strings.Cut(item, "\t")
		fields := strings.Fields(meta)
		if !found || len(fields) != 3 {
			return nil, fmt.Errorf("unreadable index entry")
		}
		if fields[2] != "0" {
			return nil, handoverRefusal{handoverRefusedUnmerged}
		}
		entries[path] = handoverIndexEntry{line: item + "\x00", mode: fields[0], stage: fields[2]}
	}
	return entries, nil
}

// handoverSnapshotCheck refuses state a tracked-contents snapshot cannot carry
// (hk 5259): a submodule that is moved, dirty or newly staged, whose changes
// the superproject's tree never holds, and an unmerged index. Assume-unchanged
// paths need no refusal: the temporary index carries no flags, so their edits
// are read like any other. A skip-worktree path is handled by the tree build.
func handoverSnapshotCheck(ctx context.Context, top string) error {
	raw, err := handoverSnapshotGit(ctx, top, nil, "", "diff", "--cached", "--raw", "-z", "--no-renames")
	if err != nil {
		return err
	}
	items := handoverNULs(raw)
	for i := 0; i+1 < len(items); i += 2 {
		fields := strings.Fields(items[i])
		if len(fields) >= 2 && (strings.TrimPrefix(fields[0], ":") == "160000" || fields[1] == "160000") {
			return handoverRefusal{handoverRefusedGitlink}
		}
	}
	status, err := handoverSnapshotGit(ctx, top, nil, "", "submodule", "status", "--recursive")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(status, "\n") {
		if strings.HasPrefix(line, "+") || strings.HasPrefix(line, "U") {
			return handoverRefusal{handoverRefusedSubmodule}
		}
	}
	dirs, err := handoverSnapshotGit(ctx, top, nil, "", "submodule", "foreach", "--quiet", "--recursive", `printf '%s\0' "$PWD"`)
	if err != nil {
		return err
	}
	for _, dir := range handoverNULs(dirs) {
		state, err := handoverSnapshotGit(ctx, dir, nil, "", "status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignore-submodules=none")
		if err != nil {
			return err
		}
		if state != "" {
			return handoverRefusal{handoverRefusedSubmodule}
		}
		// A flagged entry hides its edits from status.
		flags, err := handoverSnapshotGit(ctx, dir, nil, "", "ls-files", "-v", "-z")
		if err != nil {
			return err
		}
		for _, item := range handoverNULs(flags) {
			if item[0] >= 'a' && item[0] <= 'z' || item[0] == 'S' {
				return handoverRefusal{handoverRefusedSubmodule}
			}
		}
	}
	return nil
}

// handoverFingerprint is what the snapshot reads from the user's repository:
// HEAD, the index with its flags, and the staged difference against head.
// The tree is built from several reads; if this differs before and after, a
// writer raced them and the lists they were derived from may disagree. HEAD
// must be the commit the snapshot takes as its parent: a commit made since it
// was read would be missing from the pushed ref.
func handoverFingerprint(ctx context.Context, top, head string) (string, error) {
	var out strings.Builder
	for _, args := range [][]string{
		{"rev-parse", "--verify", "HEAD^{commit}"},
		{"ls-files", "--stage", "-z"},
		{"ls-files", "-v", "-z"},
		{"diff", "--cached", "--raw", "-z", "--no-renames", head},
	} {
		part, err := handoverSnapshotGit(ctx, top, nil, "", args...)
		if err != nil {
			return "", err
		}
		if args[0] == "rev-parse" && strings.ToLower(strings.TrimSpace(part)) != head {
			return "", handoverRefusal{handoverRefusedChanged}
		}
		out.WriteString(part)
		out.WriteString("\x01")
	}
	return out.String(), nil
}

// handoverSkipWorktree lists the paths whose index entry carries skip-worktree
// (ls-files -v tags S, or s with assume-unchanged).
func handoverSkipWorktree(ctx context.Context, top string) ([]string, error) {
	out, err := handoverSnapshotGit(ctx, top, nil, "", "ls-files", "-v", "-z")
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, item := range handoverNULs(out) {
		if len(item) > 2 && (item[0] == 'S' || item[0] == 's') && item[1] == ' ' {
			paths = append(paths, item[2:])
		}
	}
	return paths, nil
}

// handoverSnapshotTree captures tracked worktree contents without touching the
// user's index, as of the commit head. Paths removed from that index are
// excluded before add -u: a removed path can contain a secret which is now
// untracked.
func handoverSnapshotTree(ctx context.Context, top, scratch, head string) (string, error) {
	// Read first: the submodule scans below can be slow, and whatever lands
	// during them differs from this and from the read after the build.
	before, err := handoverFingerprint(ctx, top, head)
	if err != nil {
		return "", err
	}
	if err := handoverSnapshotCheck(ctx, top); err != nil {
		return "", err
	}
	index, err := handoverIndex(ctx, top)
	if err != nil {
		return "", err
	}
	skipped, err := handoverSkipWorktree(ctx, top)
	if err != nil {
		return "", err
	}
	// A skip-worktree path is absent on a sparse checkout, and the index
	// entry is the truth for it. One present on disk may hold edits git
	// ignores: it cannot be told apart from work, so it is refused.
	for _, path := range skipped {
		if _, err := os.Lstat(filepath.Join(top, path)); err == nil {
			return "", handoverRefusal{handoverRefusedSkipWorktree}
		}
	}
	deleted, err := handoverSnapshotGit(ctx, top, nil, "", "diff", "--cached", "--name-only", "-z", "--no-renames", "--diff-filter=D", head)
	if err != nil {
		return "", err
	}
	added, err := handoverSnapshotGit(ctx, top, nil, "", "diff", "--cached", "--name-only", "-z", "--no-renames", "--diff-filter=A", head)
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
	// git creates the index itself; an empty file is not one.
	if err := os.Remove(name); err != nil {
		return "", err
	}
	env := []string{"GIT_INDEX_FILE=" + name}
	if _, err := handoverSnapshotGit(ctx, top, env, "", "read-tree", head); err != nil {
		return "", err
	}
	for _, path := range handoverNULs(deleted) {
		if _, err := handoverSnapshotGit(ctx, top, env, "", "update-index", "--force-remove", "--", path); err != nil {
			return "", err
		}
	}
	// Seed newly staged paths using their index entries, never with pathspec
	// expansion or recursive directory adds. add -u then captures their
	// current worktree content along with the paths already in HEAD.
	var seed strings.Builder
	for _, path := range handoverNULs(added) {
		entry, found := index[path]
		if !found {
			return "", fmt.Errorf("invalid staged entry")
		}
		seed.WriteString(entry.line)
	}
	if seed.Len() > 0 {
		if _, err := handoverSnapshotGit(ctx, top, env, seed.String(), "update-index", "-z", "--index-info"); err != nil {
			return "", err
		}
	}
	if _, err := handoverSnapshotGit(ctx, top, env, "", "add", "-u", "--", "."); err != nil {
		return "", err
	}
	// add -u recorded each absent skip-worktree path as a deletion.
	seed.Reset()
	for _, path := range skipped {
		if entry, found := index[path]; found {
			seed.WriteString(entry.line)
		}
	}
	if seed.Len() > 0 {
		if _, err := handoverSnapshotGit(ctx, top, env, seed.String(), "update-index", "-z", "--index-info"); err != nil {
			return "", err
		}
	}
	tree, err := handoverSnapshotGit(ctx, top, env, "", "write-tree")
	if err != nil {
		return "", err
	}
	if handoverSnapshotBeforeRecheck != nil {
		handoverSnapshotBeforeRecheck()
	}
	after, err := handoverFingerprint(ctx, top, head)
	if err != nil {
		return "", err
	}
	if after != before {
		return "", handoverRefusal{handoverRefusedChanged}
	}
	return strings.TrimSpace(tree), nil
}

// handoverSnapshotBeforeRecheck is a test seam: it runs after the tree is
// written and before the repository is read again for a racing writer.
var handoverSnapshotBeforeRecheck func()

type handoverSnapshotCommit struct{ Commit, Ref, Tree, Parent string }

// handoverSnapshotPush publishes a content-addressed ref and accepts it only
// after a read from origin confirms the exact commit.
func handoverSnapshotPush(ctx context.Context, top, scratch, job string) (handoverSnapshotCommit, error) {
	if !validNodeJobName(job) {
		return handoverSnapshotCommit{}, fmt.Errorf("invalid job")
	}
	// One HEAD for the tree, the parent and the result: a commit made while
	// the snapshot runs cannot split them.
	out, err := handoverSnapshotGit(ctx, top, nil, "", "rev-parse", "--verify", "HEAD^{commit}")
	head := strings.ToLower(strings.TrimSpace(out))
	if err != nil || !nodeShaPattern.MatchString(head) {
		return handoverSnapshotCommit{}, fmt.Errorf("head unreadable")
	}
	tree, err := handoverSnapshotTree(ctx, top, scratch, head)
	if err != nil {
		return handoverSnapshotCommit{}, err
	}
	identity := []string{"GIT_AUTHOR_NAME=panewire", "GIT_AUTHOR_EMAIL=panewire@localhost", "GIT_COMMITTER_NAME=panewire", "GIT_COMMITTER_EMAIL=panewire@localhost"}
	out, err = handoverSnapshotGit(ctx, top, identity, "", "commit-tree", "--no-gpg-sign", tree, "-p", head, "-m", "panewire hand-over snapshot of "+job)
	commit := strings.TrimSpace(out)
	if err != nil || !nodeShaPattern.MatchString(commit) {
		return handoverSnapshotCommit{}, fmt.Errorf("snapshot commit failed")
	}
	ref := handoverSnapshotRef(job, commit)
	if _, err := handoverSnapshotGit(ctx, top, nil, "", "-c", "push.recurseSubmodules=no", "push", "--no-verify", "--no-follow-tags", "--recurse-submodules=no", "origin", commit+":"+ref); err != nil {
		return handoverSnapshotCommit{}, fmt.Errorf("%w: %v", errHandoverSnapshotPush, err)
	}
	remote, err := handoverSnapshotGit(ctx, top, nil, "", "ls-remote", "origin", ref)
	if err != nil || strings.TrimSpace(remote) != commit+"\t"+ref {
		return handoverSnapshotCommit{}, errHandoverSnapshotOrigin
	}
	return handoverSnapshotCommit{Commit: commit, Ref: ref, Tree: tree, Parent: head}, nil
}

// handoverSnapshotLocalOnly names what the snapshot leaves in the worktree:
// untracked, non-ignored paths and intent-to-add entries (git add -N holds no
// staged contents). Ignored files are not preserved and not listed.
func handoverSnapshotLocalOnly(ctx context.Context, top string) ([]string, error) {
	others, err := handoverSnapshotGit(ctx, top, nil, "", "ls-files", "-z", "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	paths := handoverNULs(others)
	added, err := handoverSnapshotGit(ctx, top, nil, "", "diff", "--cached", "--name-only", "-z", "--no-renames", "--diff-filter=A")
	if err != nil {
		return nil, err
	}
	withIntent, err := handoverSnapshotGit(ctx, top, nil, "", "diff", "--cached", "--name-only", "-z", "--no-renames", "--diff-filter=A", "--ita-visible-in-index")
	if err != nil {
		return nil, err
	}
	staged := handoverNULs(added)
	for _, path := range handoverNULs(withIntent) {
		if !slices.Contains(staged, path) {
			paths = append(paths, path)
		}
	}
	return paths, nil
}

// handoverSnapshotGuardPrepare is the cost-first half of a snapshot-bound
// close recheck (hk 5324): the same worktree-identity reads handoverPrepare
// makes, then one ls-remote proving the ref still names the stored commit on
// origin. The worktree side is bound by handoverSnapshotBoundLocal, run last
// before the close.
func (client *HubClient) handoverSnapshotGuardPrepare(ctx context.Context, job string, guard *nodeHandoverCloseGuard, check nodeHandoverCheckArgs, target *nodeCloseTarget) (nodeHandoverPrepared, string) {
	prepared, reason := client.handoverIdentity(ctx, check, target)
	if reason != "" {
		return nodeHandoverPrepared{}, reason
	}
	ref := handoverSnapshotRef(job, guard.Commit)
	git := client.nodeGitRunner()
	out, err := git(ctx, "-C", prepared.top, "ls-remote", "origin", ref)
	if err != nil || ctx.Err() != nil {
		return nodeHandoverPrepared{}, handoverGitFailed
	}
	if strings.TrimSpace(string(out)) != guard.Commit+"\t"+ref {
		return nodeHandoverPrepared{}, handoverGitFailed
	}
	return prepared, ""
}

// handoverSnapshotBoundLocal is the fast local half of a snapshot-bound close
// recheck, run last before the close: the worktree still sits where the
// snapshot found it, HEAD is still the snapshot's parent, and `git diff`
// against the snapshot commit reads clean — a new commit or any tracked edit
// since then is work the snapshot does not hold. Untracked paths never
// entered the snapshot, so they do not refuse the close.
func (client *HubClient) handoverSnapshotBoundLocal(ctx context.Context, prepared nodeHandoverPrepared, guard *nodeHandoverCloseGuard) nodeHandoverCheckResult {
	refuse := func(reason string) nodeHandoverCheckResult { return nodeHandoverCheckResult{Outcome: reason} }
	git := client.nodeGitRunner()
	read := func(dir string, gitArgs ...string) (string, bool) {
		out, err := git(ctx, append([]string{"-C", dir, "-c", "core.fsmonitor=false"}, gitArgs...)...)
		return string(out), err == nil && ctx.Err() == nil
	}
	args, top, bound := prepared.args, prepared.top, guard.Snapshot
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
	if origin, ok := read(top, "remote", "get-url", "origin"); !ok || strings.TrimSpace(origin) != args.Origin {
		return refuse(handoverGitFailed)
	}
	head, ok := read(top, "rev-parse", "--verify", "HEAD^{commit}")
	head = strings.ToLower(strings.TrimSpace(head))
	if !ok || !nodeShaPattern.MatchString(head) {
		return refuse(handoverGitFailed)
	}
	if head != bound.Parent {
		return refuse(handoverUnpushed)
	}
	if _, err := git(ctx, "-C", top, "-c", "core.fsmonitor=false", "diff", "--quiet", "--exit-code", guard.Commit); err != nil {
		if ctx.Err() != nil {
			return refuse(handoverGitFailed)
		}
		if code, _ := extractGitExitAndStderr(err); code == 1 {
			return refuse(handoverDirty)
		}
		return refuse(handoverGitFailed)
	}
	return nodeHandoverCheckResult{Outcome: handoverCheckOK, Top: top, Branch: args.Branch, Pushed: guard.Commit}
}
