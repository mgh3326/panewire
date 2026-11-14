package panewire

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/harnessstate"
)

// worktree.prepare, worktree.remove and quota.refresh (docs/node-commands.md,
// hk design design/2026-10-30/cross-host-jobs §5.3–5.6). The work root comes
// only from the node's own spawn.json `work_root`; every op touching one
// clone serializes on the per-clone lock <clone>.git-op.lock.
const (
	nodeGitOpLockWait = 60 * time.Second
	nodeGitOpLockPoll = 100 * time.Millisecond

	nodeOutcomeBusy      = "busy"
	nodeOutcomeDirty     = "dirty"
	nodeOutcomeGitFailed = "git-failed"
	nodeOutcomeGitTooOld = "git_too_old"
	nodeOutcomeCancelled = "cancelled"

	nodePreparePrepared        = "prepared"
	nodePrepareReused          = "reused"
	nodePrepareBranchElsewhere = "branch-elsewhere"
	nodePrepareShaMissing      = "sha-missing"
	nodePrepareOriginMismatch  = "origin-mismatch"
)

// worktree.remove outcomes mirror the receipt reasons mergewake
// internal/reap reports today, verbatim.
const (
	nodeRemoveRemoved           = "removed"
	nodeRemoveWorktreeGone      = "worktree-gone"
	nodeRemoveWorktreeChanged   = "worktree-changed"
	nodeRemoveUnregistered      = "unregistered"
	nodeRemoveLocked            = "locked"
	nodeRemoveDetached          = "detached"
	nodeRemoveNotDetached       = "not_detached"
	nodeRemoveStatusCheckFailed = "status-check-failed"
	nodeRemoveIgnoredFiles      = "ignored-files"
	nodeRemovePaneInUse         = "pane-in-use"
	nodeRemovePaneScanFailed    = "pane-scan-failed"
	nodeRemoveUnmerged          = "unmerged-or-unpushed"
	nodeRemoveOutsideWorkRoot   = "outside-work-root"

	nodeQuotaRefreshed = "refreshed"
)

var (
	errNodeGitOpBusy      = errors.New("git-op lock held")
	errNodeGitOpCancelled = errors.New("git-op lock wait cancelled")
)

var (
	nodeReviewerDirPattern = regexp.MustCompile(`^mwr-[a-z0-9._-]+$`)
	nodeShaPattern         = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	nodeFetchPullPattern   = regexp.MustCompile(`^pull/[0-9]+/head$`)
	nodeQuotaPoolPattern   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	nodeGlobClass          = regexp.MustCompile(`\[[^\]]*\]`)
)

type nodeWorktreePrepareArgs struct {
	Origin      string `json:"origin"`
	Mode        string `json:"mode"`
	Branch      string `json:"branch"`
	Sha         string `json:"sha"`
	Fetch       string `json:"fetch"`
	Dir         string `json:"dir"`
	VerifyReuse bool   `json:"verify_reuse,omitempty"`
}

type nodeWorktreeRemoveArgs struct {
	Cwd         string   `json:"cwd"`
	ExpectHead  string   `json:"expect_head"`
	Detached    bool     `json:"detached"`
	Base        string   `json:"base"`
	Regenerable []string `json:"regenerable"`
}

type nodeQuotaRefreshArgs struct {
	Pool string `json:"pool"`
}

// Results stay close to worktree.inspect: the inspected worktree rides along
// whenever there is one worth reporting.
type nodeWorktreePrepareResult struct {
	Outcome       string        `json:"outcome"`
	Worktree      *nodeWorktree `json:"worktree"`
	RetryAfterS   int           `json:"retry_after_s,omitempty"`
	FastForwarded bool          `json:"fast_forwarded,omitempty"`
	Reason        string        `json:"reason,omitempty"`
}

type nodeWorktreeRemoveResult struct {
	Outcome     string        `json:"outcome"`
	Worktree    *nodeWorktree `json:"worktree"`
	Files       []string      `json:"files,omitempty"`
	RetryAfterS int           `json:"retry_after_s,omitempty"`
}

type nodeQuotaRefreshResult struct {
	Outcome string `json:"outcome"`
}

// validNodeRefName mirrors git's check-ref-format rules for the argument
// shapes these ops accept: no control bytes, "..", "@{", whitespace or git's
// magic chars, no segment leading with '.' or ending ".lock".
func validNodeRefName(name string) bool {
	if name == "" || name == "@" || len(name) > 200 || hasControlByte(name) {
		return false
	}
	if strings.HasPrefix(name, "/") || strings.HasSuffix(name, "/") || strings.HasSuffix(name, ".") ||
		strings.HasPrefix(name, "-") || strings.ContainsAny(name, " ~^:?*[\\") ||
		strings.Contains(name, "..") || strings.Contains(name, "@{") || strings.Contains(name, "//") {
		return false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" || strings.HasPrefix(seg, ".") || strings.HasSuffix(seg, ".lock") {
			return false
		}
	}
	return true
}

// validNodeFetchRef is the closed set of fetch shapes: a branch head or a
// pull-request head.
func validNodeFetchRef(fetch string) bool {
	if rest, ok := strings.CutPrefix(fetch, "refs/heads/"); ok {
		return validNodeRefName(rest)
	}
	return nodeFetchPullPattern.MatchString(fetch)
}

func validNodeGlob(glob string) bool {
	if glob == "" || len(glob) > 128 || hasControlByte(glob) {
		return false
	}
	// A pattern made only of wildcards ("*", "**", "*/?") would approve any
	// ignored file for deletion; it must name something literal.
	// Character classes ("[a-z]/") are wildcards too.
	return strings.Trim(nodeGlobClass.ReplaceAllString(glob, ""), "*?/") != ""
}

// nodeOriginRepo normalizes an origin argument to "owner/repo" for the clone
// identity check. Any userinfo refuses the value: URL userinfo, or an '@' in
// anything that is not an absolute local path. URL and scp-style remotes need
// exactly owner/repo path segments; a local clone source stands in with its
// last two.
func nodeOriginRepo(raw string) (string, bool) {
	// git clone receives the raw argument, so its control bytes are refused
	// before trimming could hide a trailing one.
	u := strings.TrimSpace(raw)
	if hasControlByte(raw) || !nodeOriginShapeOK(u) {
		return "", false
	}
	switch {
	case strings.Contains(u, "://"):
		return nodeURLOriginRepo(u)
	case filepath.IsAbs(u):
		seg := nodeOriginPathSegments(u)
		if len(seg) < 2 {
			return "", false
		}
		return seg[len(seg)-2] + "/" + seg[len(seg)-1], true
	case strings.Contains(u, "@"):
		return "", false
	}
	host, repoPath, ok := strings.Cut(u, ":")
	if !ok {
		return "", false
	}
	return nodeScpOriginRepo(host, repoPath)
}

// nodeCloneRemoteRepo reads owner/repo from a clone's own origin. Unlike a
// caller-supplied origin, an existing clone may use the scp-style SSH form
// user@host:owner/repo.git; its user part is dropped, never refused. The
// remote splits at its first ':' and only the host side can hold userinfo,
// so an '@' in the repo path (host:owner/re@po.git) stays part of the path.
func nodeCloneRemoteRepo(raw string) (string, bool) {
	u := strings.TrimSpace(raw)
	if !nodeOriginShapeOK(u) {
		return "", false
	}
	if strings.Contains(u, "://") {
		if v, err := url.Parse(u); err == nil && v.User != nil {
			v.User = nil
			u = v.String()
		}
		return nodeURLOriginRepo(u)
	}
	if filepath.IsAbs(u) {
		return nodeOriginRepo(u)
	}
	host, repoPath, ok := strings.Cut(u, ":")
	if !ok {
		return "", false
	}
	if at := strings.LastIndex(host, "@"); at >= 0 && !strings.Contains(host, "/") {
		host = host[at+1:]
	}
	return nodeScpOriginRepo(host, repoPath)
}

// nodeOriginShapeOK is the check every origin form shares.
func nodeOriginShapeOK(u string) bool {
	// A leading '-' would reach git as an option, never a repository.
	return u != "" && !strings.HasPrefix(u, "-") && len(u) <= 2048 && !hasControlByte(u) && !strings.Contains(u, " ")
}

func nodeURLOriginRepo(u string) (string, bool) {
	v, err := url.Parse(u)
	// A query or fragment can carry a credential that git would persist in
	// the clone's config; origins never need one.
	if err != nil || v.User != nil || v.RawQuery != "" || v.ForceQuery || v.Fragment != "" || strings.Contains(u, "#") {
		return "", false
	}
	switch v.Scheme {
	case "http", "https", "ssh", "git":
		if v.Host == "" {
			return "", false
		}
	case "file":
	default:
		return "", false
	}
	seg := nodeOriginPathSegments(v.Path)
	// A file:// origin is a local path: same last-two rule as an absolute
	// path, so a clone made through file:// matches the plain-path origin.
	if v.Scheme == "file" && len(seg) >= 2 {
		seg = seg[len(seg)-2:]
	}
	if len(seg) != 2 {
		return "", false
	}
	return seg[0] + "/" + seg[1], true
}

// nodeScpOriginRepo reads an scp-style host:owner/repo whose userinfo, if
// any, is already gone.
func nodeScpOriginRepo(host, repoPath string) (string, bool) {
	// An empty host (":owner/repo") is a local path to git, and a
	// one-character host is drive-letter shaped (C:owner/repo): a local path
	// on Windows, never a remote worth trusting. A '/' before the colon
	// makes the whole value a relative path.
	if len(host) < 2 || strings.ContainsAny(host, "/@") {
		return "", false
	}
	seg := nodeOriginPathSegments(repoPath)
	if len(seg) != 2 {
		return "", false
	}
	return seg[0] + "/" + seg[1], true
}

func nodeOriginPathSegments(p string) []string {
	p = strings.TrimSuffix(strings.Trim(p, "/"), ".git")
	var out []string
	for _, seg := range strings.Split(p, "/") {
		// A dot segment changes which repository the path names, so the
		// identity check fails closed instead of normalizing it away.
		if seg == "." || seg == ".." {
			return nil
		}
		// A segment becomes a directory name under the work root: it may
		// hold no path syntax of any platform (backslash, drive colon).
		if strings.ContainsAny(seg, "\\:") {
			return nil
		}
		if seg != "" {
			out = append(out, seg)
		}
	}
	return out
}

// nodeWorkRoot reads only `work_root` from the node's spawn.json. The file
// carries spawn's other keys too, so this decodes loosely instead of going
// through loadHubSpawnConfig — a node with spawn disabled still has one.
func (client *HubClient) nodeWorkRoot() (string, error) {
	if client.spawnConfigPath == "" {
		return "", errors.New("work root unset")
	}
	info, err := os.Lstat(client.spawnConfigPath)
	if errors.Is(err, os.ErrNotExist) && client.spawnConfigDefault {
		// No spawn.json at the default path: the node runs on defaults.
		if config, defaults, resolveErr := client.resolveHubSpawnConfig(); resolveErr == nil && defaults {
			return config.WorkRoot, nil
		}
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 64<<10 {
		return "", errors.New("work root unset")
	}
	contents, err := os.ReadFile(client.spawnConfigPath)
	if err != nil {
		return "", errors.New("work root unset")
	}
	var config struct {
		WorkRoot string `json:"work_root"`
	}
	if json.Unmarshal(contents, &config) != nil || !filepath.IsAbs(config.WorkRoot) || len(config.WorkRoot) > nodeCommandMaxPathBytes {
		return "", errors.New("work root unset")
	}
	return filepath.Clean(config.WorkRoot), nil
}

func (client *HubClient) nodeGitOpWait() time.Duration {
	if client.nodeGitLockWait > 0 {
		return client.nodeGitLockWait
	}
	return nodeGitOpLockWait
}

// acquireNodeGitOpLock polls the per-clone lock until it is taken, the wait
// budget runs out, or the caller's context ends. A wait that runs out — the
// node's own budget or the run deadline — reports busy, so a retry is legal;
// a cancelled caller reports errNodeGitOpCancelled, since nobody is left to
// retry, and never takes the lock.
func acquireNodeGitOpLock(ctx context.Context, path string, wait time.Duration) (func(), error) {
	deadline := time.Now().Add(wait)
	for {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, errNodeGitOpCancelled
		}
		release, err := tryNodeGitOpLock(path)
		if err == nil {
			return release, nil
		}
		if !errors.Is(err, errNodeGitOpBusy) {
			return nil, err
		}
		if !time.Now().Before(deadline) {
			return nil, errNodeGitOpBusy
		}
		timer := time.NewTimer(nodeGitOpLockPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(ctx.Err(), context.Canceled) {
				return nil, errNodeGitOpCancelled
			}
			return nil, errNodeGitOpBusy
		case <-timer.C:
		}
	}
}

func nodeGitOpRetryAfter(wait time.Duration) int {
	return int((wait + time.Second - time.Nanosecond) / time.Second)
}

// nodeGitVersionOK enforces the git >= 2.31 floor worktree.inspect already
// documents: prepare and remove refuse old git the same way rather than
// parsing worktree output they can't trust.
func nodeGitVersionOK(ctx context.Context, git relayCommandRunner) (bool, error) {
	out, err := git(ctx, "--version")
	if err != nil {
		return false, err
	}
	fields := strings.Fields(string(out))
	if len(fields) < 3 || fields[0] != "git" || fields[1] != "version" {
		return false, nil
	}
	parts := strings.Split(fields[2], ".")
	if len(parts) < 2 {
		return false, nil
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false, nil
	}
	return major > 2 || (major == 2 && minor >= 31), nil
}

type nodeWorktreeEntry struct {
	Path       string
	Branch     string
	Detached   bool
	Locked     bool
	LockReason string
}

// nodeListWorktrees parses `git worktree list --porcelain` for the clone.
func nodeListWorktrees(ctx context.Context, git relayCommandRunner, main string) ([]nodeWorktreeEntry, error) {
	out, err := git(ctx, "-C", main, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, err
	}
	var entries []nodeWorktreeEntry
	var cur *nodeWorktreeEntry
	flush := func() {
		if cur != nil {
			entries = append(entries, *cur)
			cur = nil
		}
	}
	for _, line := range strings.Split(string(out), "\n") {
		switch {
		case strings.HasPrefix(line, "worktree "):
			flush()
			cur = &nodeWorktreeEntry{Path: strings.TrimPrefix(line, "worktree ")}
		case cur == nil:
		case strings.HasPrefix(line, "branch "):
			cur.Branch = strings.TrimPrefix(strings.TrimPrefix(line, "branch "), "refs/heads/")
		case line == "detached":
			cur.Detached = true
		case strings.HasPrefix(line, "locked"):
			cur.Locked = true
			cur.LockReason = strings.TrimSpace(strings.TrimPrefix(line, "locked"))
		}
	}
	flush()
	return entries, nil
}

type nodeWorktreeAddIntent struct {
	Path   string         `json:"path"`
	Mode   string         `json:"mode"`
	Branch string         `json:"branch"`
	Sha    string         `json:"sha"`
	Add    nodeAddProcess `json:"add"`
}

// nodeWorktreeAddIntentPath names the intent file of one worktree path. Each
// path has its own file, so an add for one worktree never replaces, or clears,
// the intent an interrupted add for another worktree still needs to replay.
func nodeGitCommonDir(ctx context.Context, git relayCommandRunner, clone string) (string, error) {
	out, err := git(ctx, "-C", clone, "rev-parse", "--git-common-dir")
	if err != nil {
		return "", err
	}
	common := strings.TrimSpace(string(out))
	if common == "" {
		return "", errors.New("git common dir is empty")
	}
	if !filepath.IsAbs(common) {
		common = filepath.Join(clone, common)
	}
	return common, nil
}

func nodeWorktreeAddIntentPath(ctx context.Context, git relayCommandRunner, clone, dir string) (string, error) {
	common, err := nodeGitCommonDir(ctx, git, clone)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(filepath.Clean(dir)))
	return filepath.Join(common, "panewire-worktree-add-intent-"+hex.EncodeToString(sum[:])+".json"), nil
}

// nodeWriteWorktreeAddIntent resolves dir's intent file and writes it. The
// per-path file keeps an add for one worktree from replacing, or clearing, the
// intent an interrupted add for another worktree still needs to replay.
func nodeWriteWorktreeAddIntent(ctx context.Context, git relayCommandRunner, clone, dir string, intent nodeWorktreeAddIntent) (string, error) {
	name, err := nodeWorktreeAddIntentPath(ctx, git, clone, dir)
	if err != nil {
		return "", err
	}
	return name, nodeWriteWorktreeAddIntentFile(name, intent)
}

// nodeWriteWorktreeAddIntentFile replaces one intent file atomically.
func nodeWriteWorktreeAddIntentFile(name string, intent nodeWorktreeAddIntent) error {
	contents, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(name, ".panewire-worktree-add-intent-*", contents, 0o600)
}

func nodeReadWorktreeAddIntent(ctx context.Context, git relayCommandRunner, clone, dir string) (nodeWorktreeAddIntent, error) {
	name, err := nodeWorktreeAddIntentPath(ctx, git, clone, dir)
	if err != nil {
		return nodeWorktreeAddIntent{}, err
	}
	contents, err := os.ReadFile(name)
	if err != nil {
		return nodeWorktreeAddIntent{}, err
	}
	var intent nodeWorktreeAddIntent
	if err := json.Unmarshal(contents, &intent); err != nil {
		return nodeWorktreeAddIntent{}, err
	}
	return intent, nil
}

func nodeClearWorktreeAddIntent(name string) {
	if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("node worktree add intent cleanup failed", "err", err)
	}
}

func nodeGitProcessWasSignaled(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ProcessState == nil {
		return false
	}
	status, ok := exitErr.ProcessState.Sys().(interface{ Signaled() bool })
	return ok && status.Signaled()
}

// nodeWorktreeAddIntentMatches reports whether recorded describes this add. The
// identity is evidence about the recorded add, not something the caller asks for,
// so it is not part of the comparison.
func nodeWorktreeAddIntentMatches(recorded nodeWorktreeAddIntent, dir string, expected nodeWorktreeAddIntent) bool {
	return recorded.Path == filepath.Clean(dir) &&
		recorded.Mode == expected.Mode &&
		recorded.Branch == expected.Branch &&
		recorded.Sha == expected.Sha
}

// nodeReclaimInterruptedAdd removes the worktree an earlier add left locked
// initializing, and only that: the recorded intent must describe this add and
// must name the add child that ran it, and that child must have exited. The lock
// says an add is in flight, not that it is ours or that it is dead, so it never
// authorizes a forced remove on its own (lead-verify F3, hk 4589).
func nodeReclaimInterruptedAdd(ctx context.Context, git relayCommandRunner, clone, dir string, entries []nodeWorktreeEntry, expected nodeWorktreeAddIntent) bool {
	intent, err := nodeReadWorktreeAddIntent(ctx, git, clone, dir)
	if err != nil || !nodeWorktreeAddIntentMatches(intent, dir, expected) {
		return false
	}
	if !nodeAddProcessExited(intent.Add) {
		return false
	}
	for _, entry := range entries {
		if !nodeSamePath(entry.Path, dir) || !entry.Locked || entry.LockReason != "initializing" {
			continue
		}
		if _, err := git(ctx, "-C", clone, "worktree", "remove", "--force", "--force", dir); err != nil {
			slog.Warn("node worktree interrupted add reclaim failed", "path", dir, "err", err)
			return false
		}
		if path, err := nodeWorktreeAddIntentPath(ctx, git, clone, dir); err == nil {
			nodeClearWorktreeAddIntent(path)
		}
		return true
	}
	return false
}

// nodeAddWorktree records the intended path and the identity of the git child
// that runs the add, so a later prepare can reclaim this exact interrupted add
// after a node restart, and nothing else.
func nodeAddWorktree(ctx context.Context, git relayCommandRunner, add nodeAddRunner, clone, dir string, intent nodeWorktreeAddIntent, args ...string) error {
	intent.Path = filepath.Clean(dir)
	intent.Add = nodeAddProcess{}
	intentPath, err := nodeWriteWorktreeAddIntent(ctx, git, clone, dir, intent)
	if err != nil {
		return err
	}
	// The child's identity reaches the intent while the child is alive: a restart
	// may only reclaim a path whose add child has exited, never the live add the
	// CodeRabbit Major on #493 describes.
	record := func(proc nodeAddProcess) error {
		intent.Add = proc
		return nodeWriteWorktreeAddIntentFile(intentPath, intent)
	}
	addErr := add(ctx, record, args...)
	if addErr == nil {
		nodeClearWorktreeAddIntent(intentPath)
		return nil
	}
	if !nodeGitProcessWasSignaled(addErr) {
		nodeClearWorktreeAddIntent(intentPath)
		return addErr
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entries, err := nodeListWorktrees(cleanupCtx, git, clone)
	if err != nil {
		slog.Warn("node worktree interrupted add cleanup failed", "path", dir, "err", err)
		return addErr
	}
	var atDir *nodeWorktreeEntry
	for i := range entries {
		if nodeSamePath(entries[i].Path, dir) {
			atDir = &entries[i]
			break
		}
	}
	switch {
	case atDir == nil:
		// Git left nothing at this path (it rolled back, or never created it), so
		// no replay can reclaim this intent: clear it.
		nodeClearWorktreeAddIntent(intentPath)
	case atDir.Locked && atDir.LockReason == "initializing":
		// An initializing lock names no owner. Remove only the path this attempt's
		// recorded add child worked on, and only once that child has exited.
		if !nodeAddProcessExited(intent.Add) {
			return addErr
		}
		if _, err := git(cleanupCtx, "-C", clone, "worktree", "remove", "--force", "--force", dir); err != nil {
			slog.Warn("node worktree interrupted add cleanup failed", "path", dir, "err", err)
			return addErr
		}
		nodeClearWorktreeAddIntent(intentPath)
	}
	return addErr
}

// nodeCanonicalClonePath is the one name prepare and remove give a clone:
// its EvalSymlinks result. A clone that does not exist yet (prepare, before
// cloning) is named through its resolved parent, which is what EvalSymlinks
// returns once the directory exists. Any other resolution failure — a
// dangling symlink, an unreadable path — is an error, never a fallback name,
// so one clone can never be locked under two names.
func nodeCanonicalClonePath(clone string) (string, error) {
	clone = filepath.Clean(clone)
	if _, err := os.Lstat(clone); errors.Is(err, os.ErrNotExist) {
		parent, err := filepath.EvalSymlinks(filepath.Dir(clone))
		if err != nil {
			return "", err
		}
		return filepath.Join(parent, filepath.Base(clone)), nil
	} else if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(clone)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

// nodeGitOpLockPath names the per-clone lock <canonical clone>.git-op.lock
// for every op, so prepare and remove on one clone always take one file even
// when the work root, or the clone itself, is a symlink.
func nodeGitOpLockPath(clone string) (string, error) {
	canonical, err := nodeCanonicalClonePath(clone)
	if err != nil {
		return "", err
	}
	return canonical + ".git-op.lock", nil
}

func nodeResolvedPath(path string) string {
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return filepath.Clean(resolved)
	}
	return filepath.Clean(path)
}

// nodeSamePath compares paths the way git prints them: symlinks resolved
// when the filesystem allows it, clean-join equality otherwise.
func nodeSamePath(a, b string) bool {
	return nodeResolvedPath(a) == nodeResolvedPath(b)
}

func nodePathWithin(root, path string) bool {
	rel, err := filepath.Rel(nodeResolvedPath(root), nodeResolvedPath(path))
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func nodeWorktreeWithinWorkRoot(workRoot string, worktree *nodeWorktree) bool {
	return nodePathWithin(workRoot, worktree.Path) && nodePathWithin(workRoot, worktree.Common)
}

func nodeWorktreeClean(ctx context.Context, git relayCommandRunner, path string) (bool, error) {
	out, err := git(ctx, "-C", path, "status", "--porcelain")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(out)) == "", nil
}

func nodeEnsureHarnessStateExcludes(ctx context.Context, git relayCommandRunner, clone string) error {
	out, err := git(ctx, "-C", clone, "rev-parse", "--git-common-dir")
	if err != nil {
		return err
	}
	common := strings.TrimSpace(string(out))
	if !filepath.IsAbs(common) {
		common = filepath.Join(clone, common)
	}
	dir := filepath.Join(common, "info")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	name := filepath.Join(dir, "exclude")
	contents, err := os.ReadFile(name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	mode := os.FileMode(0644)
	if info, err := os.Stat(name); err == nil {
		mode = info.Mode().Perm()
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	text := string(contents)
	updated := text
	for _, pattern := range strings.Split(strings.TrimSuffix(harnessstate.Excludes, "\n"), "\n") {
		found := false
		for _, line := range strings.Split(text, "\n") {
			if strings.TrimSuffix(line, "\r") == pattern {
				found = true
				break
			}
		}
		if !found {
			if updated != "" && !strings.HasSuffix(updated, "\n") {
				updated += "\n"
			}
			updated += pattern + "\n"
		}
	}
	if updated == text {
		return nil
	}
	return replaceFileAtomically(name, ".exclude-*", []byte(updated), mode)
}

// nodeWorktreeIgnored lists every ignored path in the worktree; callers
// must refuse on any the regenerable allowlist does not cover, so the list
// is never truncated.
func nodeWorktreeIgnored(ctx context.Context, git relayCommandRunner, path string) ([]string, error) {
	// -z prints raw paths (no core.quotePath C-quoting), NUL-terminated.
	out, err := git(ctx, "-C", path, "status", "--porcelain", "--ignored", "-z")
	if err != nil {
		return nil, err
	}
	var ignored []string
	for _, entry := range strings.Split(string(out), "\x00") {
		if name, ok := strings.CutPrefix(entry, "!! "); ok && name != "" {
			ignored = append(ignored, name)
		}
	}
	return ignored, nil
}

func nodeGitHead(ctx context.Context, git relayCommandRunner, dir string) (string, error) {
	out, err := git(ctx, "-C", dir, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// nodeRegenerable reports whether an ignored path inside a worktree matches
// the caller's regenerable allowlist. A glob containing a slash must match
// the whole path; a slash-free one may match only the last segment.
func nodeRegenerable(globs []string, name string) bool {
	// git reports a wholly ignored directory as one "dir/" entry; only a
	// directory pattern (one ending in "/") may approve deleting all of it.
	dirEntry := strings.HasSuffix(name, "/")
	p := strings.Trim(name, "/")
	base := p
	if i := strings.LastIndex(p, "/"); i >= 0 {
		base = p[i+1:]
	}
	for _, raw := range globs {
		// The slash test reads the glob as given: "logs/" or "/build" is a
		// whole-path pattern, not a last-segment one.
		g := strings.Trim(raw, "/")
		if g == "" || (dirEntry && !strings.HasSuffix(raw, "/")) {
			continue
		}
		match := base
		if strings.Contains(raw, "/") {
			match = p
		}
		if ok, _ := path.Match(g, match); ok {
			return true
		}
	}
	return false
}

// nodeBranchReleased mirrors mergewake's Pushed: merged into the base ref
// (remote-tracking first, then local) or fully pushed to an upstream.
func nodeBranchReleased(ctx context.Context, git relayCommandRunner, main, branch, base string) bool {
	// Merged means reachable from the remote base. A local ref proves
	// nothing (it can be the branch itself), and a base naming the branch
	// would make every branch look merged.
	if base != branch {
		if _, err := git(ctx, "-C", main, "merge-base", "--is-ancestor", branch, "origin/"+base); err == nil {
			return true
		}
	}
	// Pushed means origin has it: only the freshly fetched origin/<branch>
	// counts, never an upstream that is local or on another remote.
	out, err := git(ctx, "-C", main, "rev-list", "--count", "--left-right", branch+"...refs/remotes/origin/"+branch)
	return err == nil && strings.TrimSpace(strings.SplitN(string(out), "\t", 2)[0]) == "0"
}

func (client *HubClient) nodeWorktreePrepare(ctx context.Context, args nodeWorktreePrepareArgs) (any, string) {
	ownerRepo, ok := nodeOriginRepo(args.Origin)
	if !ok {
		return nil, "invalid_args"
	}
	workRoot, err := client.nodeWorkRoot()
	if err != nil {
		return nil, "work_root_unset"
	}
	if err := os.MkdirAll(workRoot, 0700); err != nil {
		return nil, "work_root_unset"
	}

	tracker := newNodeGitTracker(client.nodeGitRunner())
	if ctx.Err() != nil {
		return tracker.decide(ctx, nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed})
	}
	res, code := client.nodeWorktreePrepareRun(ctx, tracker, ownerRepo, workRoot, args)
	if code != "" {
		return nil, code
	}
	return tracker.decide(ctx, res)
}

func (client *HubClient) nodeWorktreePrepareRun(ctx context.Context, tracker *nodeGitTracker, ownerRepo, workRoot string, args nodeWorktreePrepareArgs) (nodeWorktreePrepareResult, string) {
	git := tracker.git
	if ok, err := nodeGitVersionOK(ctx, git); err != nil {
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
	} else if !ok {
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitTooOld}, ""
	}
	repo := ownerRepo[strings.IndexByte(ownerRepo, '/')+1:]
	clone := filepath.Join(workRoot, repo)
	// The clone must be a direct child of the work root before anything is
	// created there.
	if filepath.Dir(clone) != filepath.Clean(workRoot) || filepath.Base(clone) != repo {
		return nodeWorktreePrepareResult{}, "invalid_args"
	}
	if strings.EqualFold(args.Dir, repo) {
		// The job worktree may never be the clone's own main worktree.
		return nodeWorktreePrepareResult{}, "invalid_args"
	}
	if _, err := os.Stat(clone); err == nil && !nodePathWithin(workRoot, clone) {
		// Refuse before the lock path resolves beside a target outside the root.
		return nodeWorktreePrepareResult{Outcome: nodePrepareOriginMismatch}, ""
	}
	lockPath, err := nodeGitOpLockPath(clone)
	if err != nil {
		tracker.step = "lock"
		tracker.exit = 1
		tracker.stderr = err.Error()
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	wait := client.nodeGitOpWait()
	release, err := acquireNodeGitOpLock(ctx, lockPath, wait)
	if err != nil {
		switch {
		case errors.Is(err, errNodeGitOpBusy):
			return nodeWorktreePrepareResult{Outcome: nodeOutcomeBusy, RetryAfterS: nodeGitOpRetryAfter(wait)}, ""
		case errors.Is(err, errNodeGitOpCancelled):
			tracker.step = "lock"
			return nodeWorktreePrepareResult{Outcome: nodeOutcomeCancelled}, ""
		}
		tracker.step = "lock"
		tracker.exit = 1
		tracker.stderr = err.Error()
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	defer release()

	switch _, err := os.Stat(clone); {
	case err == nil && !nodePathWithin(workRoot, clone):
		// A symlink at <work_root>/<repo> must not lead git outside the root.
		return nodeWorktreePrepareResult{Outcome: nodePrepareOriginMismatch}, ""
	case err == nil:
		// The clone's own origin is its identity: a same-named directory
		// tracking a different owner/repo (a fork sharing the name) is a
		// refusal, not a reuse.
		remote, err := git(ctx, "-C", clone, "remote", "get-url", "origin")
		if err != nil {
			return nodeWorktreePrepareResult{Outcome: nodePrepareOriginMismatch}, ""
		}
		existing, ok := nodeCloneRemoteRepo(string(remote))
		if !ok || !strings.EqualFold(existing, ownerRepo) {
			return nodeWorktreePrepareResult{Outcome: nodePrepareOriginMismatch}, ""
		}
	case errors.Is(err, os.ErrNotExist):
		if _, err := git(ctx, "clone", "--", args.Origin, clone); err != nil {
			return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
		}
	default:
		tracker.step = "stat"
		tracker.exit = 1
		tracker.stderr = err.Error()
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	if err := nodeEnsureHarnessStateExcludes(ctx, git, clone); err != nil {
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
	}

	dir := filepath.Join(workRoot, args.Dir)
	outcome := ""
	result := nodeWorktreePrepareResult{}
	switch args.Mode {
	case "branch":
		outcome = client.nodePrepareBranch(ctx, tracker, clone, dir, args, &result)
	case "detached":
		outcome = client.nodePrepareDetached(ctx, tracker, clone, dir, args)
	}
	result.Outcome = outcome
	if outcome == nodePreparePrepared || outcome == nodePrepareReused {
		result.Worktree = nodeWorktreeInspect(ctx, git, dir).Worktree
		if result.Worktree == nil || result.Worktree.Head == "" {
			// A success the hub would reject as malformed is reported as
			// the failure it is; a retry answers reused.
			if tracker.step == "" || tracker.step == "prepare" {
				tracker.step = "inspect"
				tracker.subcmd = "inspect"
				tracker.exit = 1
				tracker.stderr = "worktree head missing"
			}
			return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
		}
	}
	return result, ""
}

type nodeGitTracker struct {
	runner relayCommandRunner
	step   string
	subcmd string
	exit   int
	stderr string
}

func newNodeGitTracker(runner relayCommandRunner) *nodeGitTracker {
	return &nodeGitTracker{
		runner: runner,
		step:   "prepare",
		subcmd: "prepare",
	}
}

func (tracker *nodeGitTracker) git(ctx context.Context, args ...string) ([]byte, error) {
	subcmd := nodeGitSubcommand(args)
	tracker.step = subcmd
	tracker.subcmd = subcmd
	out, err := tracker.runner(ctx, args...)
	if err != nil {
		tracker.exit, tracker.stderr = extractGitExitAndStderr(err)
	}
	return out, err
}

// add runs one worktree add through the node's add runner and records a failure
// the way git records every other step.
func (tracker *nodeGitTracker) add(ctx context.Context, add nodeAddRunner, clone, dir string, intent nodeWorktreeAddIntent, args ...string) error {
	tracker.step = "worktree"
	tracker.subcmd = "worktree"
	err := nodeAddWorktree(ctx, tracker.runner, add, clone, dir, intent, args...)
	if err != nil {
		tracker.exit, tracker.stderr = extractGitExitAndStderr(err)
	}
	return err
}

func (tracker *nodeGitTracker) decide(ctx context.Context, result nodeWorktreePrepareResult) (nodeWorktreePrepareResult, string) {
	if result.Outcome == nodeOutcomeGitFailed {
		if ctx.Err() != nil {
			result.Outcome = nodeOutcomeCancelled
			logNodeCheckoutCancelled(ctx, tracker.step, tracker.subcmd)
		} else {
			logNodeCheckoutGitFailed(tracker.step, tracker.subcmd, tracker.exit, tracker.stderr)
		}
	} else if result.Outcome == nodeOutcomeCancelled {
		logNodeCheckoutCancelled(ctx, tracker.step, tracker.subcmd)
	}
	return result, ""
}

func logNodeCheckoutGitFailed(step, subcmd string, exit int, stderr string) {
	sub := subcmd
	if sub == "" {
		sub = step
	}
	if exit <= 0 {
		exit = 1
	}
	stderrTail := nodeCheckoutBoundedStderr(stderr)
	slog.Warn("node checkout git failed",
		"step", step,
		"subcommand", sub,
		"exit", exit,
		"stderr", stderrTail,
	)
}

func logNodeCheckoutCancelled(ctx context.Context, step, subcmd string) {
	err := ctx.Err()
	if subcmd != "" && subcmd != step {
		slog.Info("node checkout cancelled",
			"step", step,
			"subcommand", subcmd,
			"err", err,
			"error", err,
		)
	} else {
		slog.Info("node checkout cancelled",
			"step", step,
			"err", err,
			"error", err,
		)
	}
}

func extractGitExitAndStderr(err error) (int, string) {
	if err == nil {
		return 0, ""
	}
	exitCode := 1
	stderr := ""
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		exitCode = exitErr.ExitCode()
		stderr = string(exitErr.Stderr)
	} else {
		type coder interface {
			ExitCode() int
		}
		if c, ok := err.(coder); ok {
			exitCode = c.ExitCode()
		}
		type stderrer interface {
			Stderr() []byte
		}
		if s, ok := err.(stderrer); ok {
			stderr = string(s.Stderr())
		} else {
			type stderrerStr interface {
				Stderr() string
			}
			if s, ok := err.(stderrerStr); ok {
				stderr = s.Stderr()
			}
		}
	}
	if exitCode == 1 {
		var code int
		if _, scanErr := fmt.Sscanf(err.Error(), "exit status %d", &code); scanErr == nil {
			exitCode = code
		}
	}
	if stderr == "" {
		stderr = err.Error()
	}
	return exitCode, stderr
}

var userinfoCredentialPattern = regexp.MustCompile(`(?i)[^/\s:@]+:[^/\s@]+@`)

func nodeCheckoutBoundedStderr(raw string) string {
	if raw == "" {
		return ""
	}
	redacted := fleetEventRedact(raw)
	redacted = userinfoCredentialPattern.ReplaceAllString(redacted, "[redacted]@")
	return utf8SuffixWithin(redacted, hubSpawnStderrTail)
}

func nodeGitSubcommand(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-C" || arg == "-c" {
			i++
			continue
		}
		if strings.HasPrefix(arg, "-C") {
			continue
		}
		if arg == "--version" {
			return "version"
		}
		if strings.HasPrefix(arg, "-") {
			continue
		}
		return arg
	}
	if len(args) > 0 {
		return strings.TrimPrefix(args[0], "--")
	}
	return "git"
}

func (client *HubClient) nodePrepareBranch(ctx context.Context, tracker *nodeGitTracker, clone, dir string, args nodeWorktreePrepareArgs, result *nodeWorktreePrepareResult) string {
	git := tracker.git
	if _, err := git(ctx, "-C", clone, "fetch", "--prune", "origin"); err != nil {
		return nodeOutcomeGitFailed
	}
	if args.Sha != "" {
		if _, err := git(ctx, "-C", clone, "cat-file", "-e", args.Sha+"^{commit}"); err != nil {
			return nodePrepareShaMissing
		}
	}
	entries, err := nodeListWorktrees(ctx, git, clone)
	if err != nil {
		return nodeOutcomeGitFailed
	}
	expected := strings.ToLower(args.Sha)
	originTip := ""
	if out, err := git(ctx, "-C", clone, "rev-parse", "--verify", "refs/remotes/origin/"+args.Branch+"^{commit}"); err == nil {
		originTip = strings.TrimSpace(string(out))
	}
	if expected == "" {
		if originTip == "" {
			// No sha and no origin branch: nothing names the tip to prepare.
			return nodePrepareBranchElsewhere
		}
		expected = strings.ToLower(originTip)
	}
	if args.VerifyReuse && !strings.EqualFold(originTip, expected) {
		if originTip == "" || expected == "" {
			return nodePrepareBranchElsewhere
		}
		if _, err := git(ctx, "-C", clone, "merge-base", "--is-ancestor", expected, originTip); err != nil {
			return nodePrepareBranchElsewhere
		}
	}
	var atDir *nodeWorktreeEntry
	for i := range entries {
		if nodeSamePath(entries[i].Path, dir) {
			atDir = &entries[i]
		} else if entries[i].Branch == args.Branch {
			return nodePrepareBranchElsewhere
		}
	}
	intent := nodeWorktreeAddIntent{Mode: "branch", Branch: args.Branch, Sha: args.Sha}
	if atDir != nil && atDir.Locked {
		if args.VerifyReuse {
			return nodeRemoveLocked
		}
		if !nodeReclaimInterruptedAdd(ctx, git, clone, dir, entries, intent) {
			return nodeRemoveLocked
		}
		atDir = nil
	}
	if atDir != nil {
		if atDir.Branch != args.Branch {
			return nodePrepareBranchElsewhere
		}
		clean, err := nodeWorktreeClean(ctx, git, dir)
		if err != nil {
			return nodeOutcomeGitFailed
		}
		if !clean {
			return nodeOutcomeDirty
		}
		// Reuse needs the same expected tip as the create path below: the
		// stated sha, else the freshly fetched origin/<branch>.
		head, err := nodeGitHead(ctx, git, dir)
		if err != nil {
			return nodeOutcomeGitFailed
		}
		if !strings.EqualFold(head, expected) {
			if args.VerifyReuse {
				return nodePrepareBranchElsewhere
			}
			relation, err := nodeCommitRelation(ctx, git, clone, head, expected)
			if err != nil {
				return nodeOutcomeGitFailed
			}
			if relation != "behind" {
				result.Reason = nodeBranchRelationReason(head, expected, relation)
				return nodePrepareBranchElsewhere
			}
			collision, err := nodeIgnoredCheckoutCollision(ctx, git, dir, head, expected)
			if err != nil {
				return nodeOutcomeGitFailed
			}
			if collision {
				return nodeOutcomeDirty
			}
			if _, err := git(ctx, "-C", dir, "merge", "--ff-only", "--no-overwrite-ignore", expected); err != nil {
				return nodeOutcomeGitFailed
			}
			result.FastForwarded = true
		}
		return nodePrepareReused
	}
	if args.VerifyReuse {
		return nodePrepareBranchElsewhere
	}
	if _, err := git(ctx, "-C", clone, "show-ref", "--verify", "--quiet", "refs/heads/"+args.Branch); err == nil {
		out, err := git(ctx, "-C", clone, "rev-parse", "--verify", "refs/heads/"+args.Branch+"^{commit}")
		if err != nil {
			return nodeOutcomeGitFailed
		}
		local := strings.TrimSpace(string(out))
		if !strings.EqualFold(local, expected) {
			relation, err := nodeCommitRelation(ctx, git, clone, local, expected)
			if err != nil {
				return nodeOutcomeGitFailed
			}
			if relation != "behind" {
				result.Reason = nodeBranchRelationReason(local, expected, relation)
				return nodePrepareBranchElsewhere
			}
			if _, err := git(ctx, "-C", clone, "update-ref", "refs/heads/"+args.Branch, expected, local); err != nil {
				return nodeOutcomeGitFailed
			}
			result.FastForwarded = true
		}
		if err := tracker.add(ctx, client.nodeAddRunner(), clone, dir, nodeWorktreeAddIntent{Mode: "branch", Branch: args.Branch, Sha: args.Sha}, "-C", clone, "worktree", "add", dir, args.Branch); err != nil {
			return nodeOutcomeGitFailed
		}
		return nodePreparePrepared
	}
	if originTip == "" || !strings.EqualFold(originTip, expected) {
		return nodePrepareBranchElsewhere
	}
	if err := tracker.add(ctx, client.nodeAddRunner(), clone, dir, nodeWorktreeAddIntent{Mode: "branch", Branch: args.Branch, Sha: args.Sha}, "-C", clone, "worktree", "add", "-b", args.Branch, dir, "origin/"+args.Branch); err != nil {
		return nodeOutcomeGitFailed
	}
	if _, err := git(ctx, "-C", dir, "branch", "--set-upstream-to", "origin/"+args.Branch, args.Branch); err != nil {
		return nodeOutcomeGitFailed
	}
	return nodePreparePrepared
}

func nodeIgnoredCheckoutCollision(ctx context.Context, git relayCommandRunner, dir, head, expected string) (bool, error) {
	changed, err := git(ctx, "-C", dir, "diff", "--name-only", "-z", head, expected, "--")
	if err != nil {
		return false, err
	}
	ignored, err := git(ctx, "-C", dir, "ls-files", "--others", "--ignored", "--exclude-standard", "-z")
	if err != nil {
		return false, err
	}
	for _, path := range strings.Split(string(changed), "\x00") {
		if path == "" {
			continue
		}
		for _, local := range strings.Split(string(ignored), "\x00") {
			if local != "" && (path == local || strings.HasPrefix(path, local+"/") || strings.HasPrefix(local, path+"/")) {
				return true, nil
			}
		}
	}
	return false, nil
}

func nodeCommitRelation(ctx context.Context, git relayCommandRunner, clone, local, expected string) (string, error) {
	ancestor := func(a, b string) (bool, error) {
		_, err := git(ctx, "-C", clone, "merge-base", "--is-ancestor", a, b)
		if err == nil {
			return true, nil
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
			return false, nil
		}
		return false, err
	}
	behind, err := ancestor(local, expected)
	if err != nil {
		return "", err
	}
	if behind {
		return "behind", nil
	}
	ahead, err := ancestor(expected, local)
	if err != nil {
		return "", err
	}
	if ahead {
		return "ahead", nil
	}
	return "diverged", nil
}

func nodeBranchRelationReason(local, expected, relation string) string {
	return fmt.Sprintf("local=%s expected=%s relation=%s", utf8PrefixWithin(local, 7), utf8PrefixWithin(expected, 7), relation)
}

// nodePrepareDetached fetches the caller's closed ref, requires the commit
// to exist and to be FETCH_HEAD or an ancestor of it (a ref that contains
// the sha, see docs/node-commands.md), and reuses only a clean worktree
// detached at exactly that sha. The worktree is always detached at args.Sha.
func (client *HubClient) nodePrepareDetached(ctx context.Context, tracker *nodeGitTracker, clone, dir string, args nodeWorktreePrepareArgs) string {
	git := tracker.git
	if _, err := git(ctx, "-C", clone, "fetch", "origin", args.Fetch); err != nil {
		return nodeOutcomeGitFailed
	}
	fetched, err := git(ctx, "-C", clone, "rev-parse", "--verify", "FETCH_HEAD^{commit}")
	if err != nil {
		return nodePrepareShaMissing
	}
	if _, err := git(ctx, "-C", clone, "cat-file", "-e", args.Sha+"^{commit}"); err != nil {
		return nodePrepareShaMissing
	}
	if !strings.EqualFold(strings.TrimSpace(string(fetched)), args.Sha) {
		if _, err := git(ctx, "-C", clone, "merge-base", "--is-ancestor", args.Sha, "FETCH_HEAD"); err != nil {
			return nodePrepareShaMissing
		}
	}
	entries, err := nodeListWorktrees(ctx, git, clone)
	if err != nil {
		return nodeOutcomeGitFailed
	}
	var atDir *nodeWorktreeEntry
	for i := range entries {
		if nodeSamePath(entries[i].Path, dir) {
			atDir = &entries[i]
		}
	}
	intent := nodeWorktreeAddIntent{Mode: "detached", Sha: args.Sha}
	if atDir != nil && atDir.Locked {
		if !nodeReclaimInterruptedAdd(ctx, git, clone, dir, entries, intent) {
			return nodeRemoveLocked
		}
		atDir = nil
	}
	if atDir != nil {
		head, err := nodeGitHead(ctx, git, dir)
		if err != nil {
			return nodeOutcomeGitFailed
		}
		if !atDir.Detached || !strings.EqualFold(head, args.Sha) {
			return nodePrepareBranchElsewhere
		}
		clean, err := nodeWorktreeClean(ctx, git, dir)
		if err != nil {
			return nodeOutcomeGitFailed
		}
		if !clean {
			return nodeOutcomeDirty
		}
		return nodePrepareReused
	}
	if err := tracker.add(ctx, client.nodeAddRunner(), clone, dir, intent, "-C", clone, "worktree", "add", "--detach", dir, args.Sha); err != nil {
		return nodeOutcomeGitFailed
	}
	return nodePreparePrepared
}

func (client *HubClient) nodeWorktreeRemove(ctx context.Context, args nodeWorktreeRemoveArgs) (any, string) {
	git := client.nodeGitRunner()
	if ok, err := nodeGitVersionOK(ctx, git); err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitFailed}, ""
	} else if !ok {
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitTooOld}, ""
	}
	workRoot, err := client.nodeWorkRoot()
	if err != nil {
		return nil, "work_root_unset"
	}
	worktree := nodeWorktreeInspect(ctx, git, args.Cwd).Worktree
	if worktree == nil {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveWorktreeGone}, ""
	}
	if !nodeWorktreeWithinWorkRoot(workRoot, worktree) {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveOutsideWorkRoot}, ""
	}
	if !strings.EqualFold(worktree.Head, args.ExpectHead) {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveWorktreeChanged}, ""
	}
	main := filepath.Dir(worktree.Common)
	lockPath, err := nodeGitOpLockPath(main)
	if err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	wait := client.nodeGitOpWait()
	release, err := acquireNodeGitOpLock(ctx, lockPath, wait)
	if err != nil {
		switch {
		case errors.Is(err, errNodeGitOpBusy):
			return nodeWorktreeRemoveResult{Outcome: nodeOutcomeBusy, RetryAfterS: nodeGitOpRetryAfter(wait)}, ""
		case errors.Is(err, errNodeGitOpCancelled):
			return nodeWorktreeRemoveResult{Outcome: nodeOutcomeCancelled}, ""
		}
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	defer release()

	// The inspection above is stale by the time the lock is held; re-resolve
	// before any check that trusts it.
	worktree = nodeWorktreeInspect(ctx, git, args.Cwd).Worktree
	if worktree == nil {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveWorktreeGone}, ""
	}
	if !nodeWorktreeWithinWorkRoot(workRoot, worktree) {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveOutsideWorkRoot}, ""
	}
	if !strings.EqualFold(worktree.Head, args.ExpectHead) {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveWorktreeChanged}, ""
	}
	currentMain := filepath.Dir(worktree.Common)
	if !nodeSamePath(main, currentMain) {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveWorktreeChanged}, ""
	}
	main = currentMain
	entries, err := nodeListWorktrees(ctx, git, main)
	if err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	var entry *nodeWorktreeEntry
	for i := range entries {
		if nodeSamePath(entries[i].Path, worktree.Path) {
			entry = &entries[i]
		}
	}
	switch {
	case entry == nil:
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveUnregistered}, ""
	case entry.Locked:
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveLocked}, ""
	case args.Detached && (entry.Branch != "" || !entry.Detached):
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveNotDetached}, ""
	case !args.Detached && entry.Branch == "":
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveDetached}, ""
	case args.Detached && (!nodeSamePath(filepath.Dir(worktree.Path), workRoot) || !nodeReviewerDirPattern.MatchString(filepath.Base(worktree.Path))):
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveOutsideWorkRoot}, ""
	}
	clean, err := nodeWorktreeClean(ctx, git, worktree.Path)
	if err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveStatusCheckFailed}, ""
	}
	if !clean {
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeDirty}, ""
	}
	if !args.Detached {
		// origin/* refs are a cache: refresh them before they prove "merged" or
		// "pushed", and fail closed when the remote cannot be read.
		if _, err := git(ctx, "-C", main, "fetch", "--prune", "--no-tags", "origin"); err != nil {
			// Inconclusive, not a proven "unmerged": nothing is removed either way.
			return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitFailed}, ""
		}
		if !nodeBranchReleased(ctx, git, main, entry.Branch, args.Base) {
			return nodeWorktreeRemoveResult{Outcome: nodeRemoveUnmerged}, ""
		}
	}
	// Ignored files, then live panes, are read last, right before removal,
	// so the window for a new file or a new pane is as short as possible.
	ignored, err := nodeWorktreeIgnored(ctx, git, worktree.Path)
	if err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveStatusCheckFailed}, ""
	}
	var unsafe []string
	for _, name := range ignored {
		if !nodeRegenerable(args.Regenerable, name) {
			unsafe = append(unsafe, name)
		}
	}
	if len(unsafe) > 0 {
		// `git worktree remove` deletes ignored files too; only expendable
		// output the caller named may go with it. files names the refusing
		// paths, bounded only in what is reported — every path is checked.
		reported := unsafe
		if len(reported) > 64 {
			reported = reported[:64]
		}
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveIgnoredFiles, Files: reported}, ""
	}
	// The pane list is a stale snapshot the moment it is read, so the usage
	// check re-reads it immediately before the removal itself.
	inUse, err := client.nodePaneInUse(ctx, worktree.Path)
	if err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeRemovePaneScanFailed}, ""
	}
	if inUse {
		return nodeWorktreeRemoveResult{Outcome: nodeRemovePaneInUse}, ""
	}
	if _, err := git(ctx, "-C", main, "worktree", "remove", worktree.Path); err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	if !args.Detached {
		// -d, never -D: git itself still refuses a branch that never merged. A
		// branch that survives is logged, not hidden — the worktree is gone and
		// the receipt reads removed either way.
		if _, err := git(ctx, "-C", main, "branch", "-d", entry.Branch); err != nil {
			slog.Warn("worktree.remove: branch delete failed", "branch", entry.Branch)
		}
	}
	return nodeWorktreeRemoveResult{Outcome: nodeRemoveRemoved, Worktree: worktree}, ""
}

// nodePaneInUse reports whether any live pane — agent or plain shell — sits
// inside path, read fresh from `herdr pane list`.
func (client *HubClient) nodePaneInUse(ctx context.Context, path string) (bool, error) {
	out, err := client.nodeHerdrRunner()(ctx, "pane", "list")
	if err != nil {
		return false, err
	}
	panes, err := nodeHerdrRows(out, "panes")
	if err != nil {
		return false, err
	}
	for _, pane := range panes {
		if nodePathWithin(path, pane.Cwd) {
			return true, nil
		}
	}
	return false, nil
}

// nodeQuotaRefresh is a closed call: the pools this host measures natively are
// measured in this process, every other pool runs exactly `pacelever refresh
// <pool>` (a pool in shadow also logs the node's own measurement beside it);
// no other argv is reachable from the wire.
func (client *HubClient) nodeQuotaRefresh(ctx context.Context, args nodeQuotaRefreshArgs) (any, string) {
	ctx, cancel := context.WithTimeout(ctx, fleetQuotaTimeout)
	defer cancel()
	if !isNativeQuotaPool(args.Pool) {
		client.warnMessage("quota.refresh: " + args.Pool + " 풀에는 native 측정 소스가 없습니다")
		return nil, "refresh_failed"
	}
	source := client.nativeQuotaSource(args.Pool)
	marked := client.loginScreenMarked(args.Pool)
	reading, measuredHere, err := source.RefreshWithMeasurement(ctx)
	if measuredHere {
		client.noteOwnQuotaMeasurement(args.Pool)
	}
	if err != nil {
		return nil, "refresh_failed"
	}
	if marked && !measuredHere && source.measurableHere(ctx) {
		switch source.probeLogin(ctx) {
		case loginProbeLoginPresent:
			client.noteOwnQuotaMeasurement(args.Pool)
		case loginProbeLoginMissing:
			return nil, "refresh_failed"
		}
	}
	if reading.Key == "" {
		client.noteQuotaUnknown(args.Pool)
	} else {
		client.noteQuotaAccount(args.Pool, reading.Key)
	}
	return nodeQuotaRefreshResult{Outcome: nodeQuotaRefreshed}, ""
}
