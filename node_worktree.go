package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
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
	nodeShaPattern       = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	nodeFetchPullPattern = regexp.MustCompile(`^pull/[0-9]+/head$`)
	nodeQuotaPoolPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	nodeGlobClass        = regexp.MustCompile(`\[[^\]]*\]`)
)

type nodeWorktreePrepareArgs struct {
	Origin string `json:"origin"`
	Mode   string `json:"mode"`
	Branch string `json:"branch"`
	Sha    string `json:"sha"`
	Fetch  string `json:"fetch"`
	Dir    string `json:"dir"`
}

type nodeWorktreeRemoveArgs struct {
	Cwd         string   `json:"cwd"`
	ExpectHead  string   `json:"expect_head"`
	Base        string   `json:"base"`
	Regenerable []string `json:"regenerable"`
}

type nodeQuotaRefreshArgs struct {
	Pool string `json:"pool"`
}

// Results stay close to worktree.inspect: the inspected worktree rides along
// whenever there is one worth reporting.
type nodeWorktreePrepareResult struct {
	Outcome     string        `json:"outcome"`
	Worktree    *nodeWorktree `json:"worktree"`
	RetryAfterS int           `json:"retry_after_s,omitempty"`
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

// nodeHasControlByte reports any ASCII control byte (< 0x20, or DEL): git's
// check-ref-format refuses every one, and none belongs in an origin or glob.
func nodeHasControlByte(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}

// validNodeRefName mirrors git's check-ref-format rules for the argument
// shapes these ops accept: no control bytes, "..", "@{", whitespace or git's
// magic chars, no segment leading with '.' or ending ".lock".
func validNodeRefName(name string) bool {
	if name == "" || name == "@" || len(name) > 200 || nodeHasControlByte(name) {
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
	if glob == "" || len(glob) > 128 || nodeHasControlByte(glob) {
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
	if nodeHasControlByte(raw) || !nodeOriginShapeOK(u) {
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
	return u != "" && !strings.HasPrefix(u, "-") && len(u) <= 2048 && !nodeHasControlByte(u) && !strings.Contains(u, " ")
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

func (client *HubClient) nodePaceleverRunner() relayCommandRunner {
	if client.nodePacelever != nil {
		return client.nodePacelever
	}
	return defaultNodeCommandRunner("pacelever")
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
	Path     string
	Branch   string
	Detached bool
	Locked   bool
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
		}
	}
	flush()
	return entries, nil
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
	git := client.nodeGitRunner()
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
		return nil, "invalid_args"
	}
	if strings.EqualFold(args.Dir, repo) {
		// The job worktree may never be the clone's own main worktree.
		return nil, "invalid_args"
	}
	if _, err := os.Stat(clone); err == nil && !nodePathWithin(workRoot, clone) {
		// Refuse before the lock path resolves beside a target outside the root.
		return nodeWorktreePrepareResult{Outcome: nodePrepareOriginMismatch}, ""
	}
	lockPath, err := nodeGitOpLockPath(clone)
	if err != nil {
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	wait := client.nodeGitOpWait()
	release, err := acquireNodeGitOpLock(ctx, lockPath, wait)
	if err != nil {
		switch {
		case errors.Is(err, errNodeGitOpBusy):
			return nodeWorktreePrepareResult{Outcome: nodeOutcomeBusy, RetryAfterS: nodeGitOpRetryAfter(wait)}, ""
		case errors.Is(err, errNodeGitOpCancelled):
			return nodeWorktreePrepareResult{Outcome: nodeOutcomeCancelled}, ""
		}
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
		return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
	}

	dir := filepath.Join(workRoot, args.Dir)
	outcome := ""
	switch args.Mode {
	case "branch":
		outcome = client.nodePrepareBranch(ctx, git, clone, dir, args)
	case "detached":
		outcome = client.nodePrepareDetached(ctx, git, clone, dir, args)
	}
	result := nodeWorktreePrepareResult{Outcome: outcome}
	if outcome == nodePreparePrepared || outcome == nodePrepareReused {
		result.Worktree = nodeWorktreeInspect(ctx, git, dir).Worktree
		if result.Worktree == nil || result.Worktree.Head == "" {
			// A success the hub would reject as malformed is reported as
			// the failure it is; a retry answers reused.
			return nodeWorktreePrepareResult{Outcome: nodeOutcomeGitFailed}, ""
		}
	}
	return result, ""
}

// nodePrepareBranch is runtug#18's branch behaviour on the node: fetch, then
// reuse only a worktree already on the branch, and refuse a local branch
// that points at another commit instead of silently tracking it.
func (client *HubClient) nodePrepareBranch(ctx context.Context, git relayCommandRunner, clone, dir string, args nodeWorktreePrepareArgs) string {
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
	var atDir *nodeWorktreeEntry
	for i := range entries {
		if nodeSamePath(entries[i].Path, dir) {
			atDir = &entries[i]
		} else if entries[i].Branch == args.Branch {
			return nodePrepareBranchElsewhere
		}
	}
	if atDir != nil {
		if atDir.Locked {
			return nodeRemoveLocked
		}
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
			return nodePrepareBranchElsewhere
		}
		return nodePrepareReused
	}
	if _, err := git(ctx, "-C", clone, "show-ref", "--verify", "--quiet", "refs/heads/"+args.Branch); err == nil {
		out, err := git(ctx, "-C", clone, "rev-parse", "--verify", "refs/heads/"+args.Branch+"^{commit}")
		if err != nil {
			return nodeOutcomeGitFailed
		}
		if expected != "" && !strings.EqualFold(strings.TrimSpace(string(out)), expected) {
			return nodePrepareBranchElsewhere
		}
		if _, err := git(ctx, "-C", clone, "worktree", "add", dir, args.Branch); err != nil {
			return nodeOutcomeGitFailed
		}
		return nodePreparePrepared
	}
	if originTip == "" || !strings.EqualFold(originTip, expected) {
		return nodePrepareBranchElsewhere
	}
	if _, err := git(ctx, "-C", clone, "worktree", "add", "-b", args.Branch, dir, "origin/"+args.Branch); err != nil {
		return nodeOutcomeGitFailed
	}
	if _, err := git(ctx, "-C", dir, "branch", "--set-upstream-to", "origin/"+args.Branch, args.Branch); err != nil {
		return nodeOutcomeGitFailed
	}
	return nodePreparePrepared
}

// nodePrepareDetached fetches the caller's closed ref, requires the commit
// to exist, and reuses only a clean worktree detached at exactly that sha.
func (client *HubClient) nodePrepareDetached(ctx context.Context, git relayCommandRunner, clone, dir string, args nodeWorktreePrepareArgs) string {
	if _, err := git(ctx, "-C", clone, "fetch", "origin", args.Fetch); err != nil {
		return nodeOutcomeGitFailed
	}
	fetched, err := git(ctx, "-C", clone, "rev-parse", "--verify", "FETCH_HEAD^{commit}")
	if err != nil || !strings.EqualFold(strings.TrimSpace(string(fetched)), args.Sha) {
		return nodePrepareShaMissing
	}
	if _, err := git(ctx, "-C", clone, "cat-file", "-e", args.Sha+"^{commit}"); err != nil {
		return nodePrepareShaMissing
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
	if atDir != nil {
		if atDir.Locked {
			return nodeRemoveLocked
		}
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
	if _, err := git(ctx, "-C", clone, "worktree", "add", "--detach", dir, args.Sha); err != nil {
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
	case entry.Branch == "":
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveDetached}, ""
	}
	clean, err := nodeWorktreeClean(ctx, git, worktree.Path)
	if err != nil {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveStatusCheckFailed}, ""
	}
	if !clean {
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeDirty}, ""
	}
	// origin/* refs are a cache: refresh them before they prove "merged" or
	// "pushed", and fail closed when the remote cannot be read.
	if _, err := git(ctx, "-C", main, "fetch", "--prune", "--no-tags", "origin"); err != nil {
		// Inconclusive, not a proven "unmerged": nothing is removed either way.
		return nodeWorktreeRemoveResult{Outcome: nodeOutcomeGitFailed}, ""
	}
	if !nodeBranchReleased(ctx, git, main, entry.Branch, args.Base) {
		return nodeWorktreeRemoveResult{Outcome: nodeRemoveUnmerged}, ""
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
	// -d, never -D: git itself still refuses a branch that never merged. A
	// branch that survives is logged, not hidden — the worktree is gone and
	// the receipt reads removed either way.
	if _, err := git(ctx, "-C", main, "branch", "-d", entry.Branch); err != nil {
		slog.Warn("worktree.remove: branch delete failed", "branch", entry.Branch)
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

// nodeQuotaRefresh is a closed call: exactly `pacelever refresh <pool>`,
// no other argv reachable from the wire.
func (client *HubClient) nodeQuotaRefresh(ctx context.Context, args nodeQuotaRefreshArgs) (any, string) {
	if _, err := client.nodePaceleverRunner()(ctx, "refresh", args.Pool); err != nil {
		return nil, "refresh_failed"
	}
	return nodeQuotaRefreshResult{Outcome: nodeQuotaRefreshed}, ""
}
