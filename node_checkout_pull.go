package panewire

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// checkout.pull is the node half of `panewire fleet checkout-pull`
// (docs/checkout-pull.md). It fast-forwards one allow-listed git checkout on
// the hub's order and reports the checkout's revision in the heartbeat. The
// allow-list is the safety boundary: a name names one configured directory,
// never a path, and nothing in the op runs a shell built from input.
const (
	nodeCommandCheckoutPull = "checkout.pull"

	// hubFeatureCheckoutPull is listed in hub.features by a hub that reads the
	// heartbeat's checkouts field. An older hub rejects an unknown heartbeat
	// field, so a node sends checkouts only on a connection that lists it.
	hubFeatureCheckoutPull = "checkout_pull"

	// nodeCheckoutPullRunTimeout bounds the whole op; the hub waits longer
	// (hubCheckoutPullWait), so the node's answer arrives first.
	nodeCheckoutPullRunTimeout  = 4 * time.Minute
	nodeCheckoutFetchTimeout    = 90 * time.Second
	nodeCheckoutPullStepTimeout = 60 * time.Second
	nodeCheckoutInstallTimeout  = 2 * time.Minute
	nodeCheckoutRevTimeout      = 3 * time.Second
	// nodeCheckoutMaxHeartbeat bounds the heartbeat's checkouts map; the hub
	// refuses a heartbeat that carries more.
	nodeCheckoutMaxHeartbeat = 8

	nodeCheckoutDefaultName = "rstack"
	nodeCheckoutDefaultDir  = ".agents/rstack"

	nodeCheckoutOutcomePulled  = "pulled"
	nodeCheckoutOutcomeCurrent = "current"
	nodeCheckoutOutcomeRefused = "refused"

	// Refusal reasons. Every reason but pull-interrupted leaves the checkout's
	// branch and worktree as they were; pull-interrupted reports what the pull
	// left behind (see nodeCheckoutFailedPull).
	nodeCheckoutReasonNotAllowed  = "not-allow-listed"
	nodeCheckoutReasonNotCheckout = "not-a-checkout"
	nodeCheckoutReasonBusy        = "busy"
	nodeCheckoutReasonLockFailed  = "lock-failed"
	nodeCheckoutReasonDetached    = "detached"
	nodeCheckoutReasonNoUpstream  = "no-upstream"
	nodeCheckoutReasonDirty       = "dirty"
	nodeCheckoutReasonGitFailed   = "git-failed"
	nodeCheckoutReasonFetchFailed = "fetch-failed"
	nodeCheckoutReasonNotFF       = "not-fast-forward"
	nodeCheckoutReasonPullFailed  = "pull-failed"
	// nodeCheckoutReasonPullInterrupted: a pull failed after it may have moved
	// HEAD or the worktree. The result carries the HEAD it found in to.
	nodeCheckoutReasonPullInterrupted = "pull-interrupted"
	// nodeCheckoutReasonIgnoredCollision: an incoming path matches a local
	// ignored path, so the fast-forward would overwrite node-local data.
	nodeCheckoutReasonIgnoredCollision = "ignored-collision"
	nodeCheckoutReasonInstallGone      = "install-missing"
	nodeCheckoutReasonInstallLong      = "install-timeout"
	nodeCheckoutInstallExitKilled      = -1
	nodeCheckoutInstallPathFile        = "install.sh"
	nodeCheckoutGitTerminalPrompt      = "GIT_TERMINAL_PROMPT=0"
	nodeCheckoutDetachedBranchName     = "HEAD"
)

var (
	// nodeCheckoutNamePattern is the allow-list's name shape, shared with the
	// hub's order and heartbeat validation.
	nodeCheckoutNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	nodeCheckoutRevPattern  = regexp.MustCompile(`^[0-9a-fA-F]{7,40}$`)
	nodeCheckoutShortSHA    = regexp.MustCompile(`^[0-9a-f]{4,40}$`)
	// nodeCheckoutRefusals is the closed reason set a refusal may carry.
	nodeCheckoutRefusals = map[string]bool{
		nodeCheckoutReasonNotAllowed: true, nodeCheckoutReasonNotCheckout: true, nodeCheckoutReasonBusy: true,
		nodeCheckoutReasonLockFailed: true, nodeCheckoutReasonDetached: true, nodeCheckoutReasonNoUpstream: true,
		nodeCheckoutReasonDirty: true, nodeCheckoutReasonGitFailed: true, nodeCheckoutReasonFetchFailed: true,
		nodeCheckoutReasonNotFF: true, nodeCheckoutReasonPullFailed: true, nodeCheckoutReasonPullInterrupted: true,
		nodeCheckoutReasonIgnoredCollision: true,
	}
)

type nodeCheckoutPullArgs struct {
	Checkout string `json:"checkout"`
	// Rev is an optional target: the result says whether HEAD now contains
	// it. Empty means no target.
	Rev string `json:"rev"`
}

// nodeCheckoutPullResult is the closed shape a node answers. Install fields
// are always present: install_ran says whether install.sh started, and
// install_exit is its status (-1 when killed), zero when it did not run.
type nodeCheckoutPullResult struct {
	Outcome       string `json:"outcome"`
	Checkout      string `json:"checkout"`
	From          string `json:"from,omitempty"`
	To            string `json:"to,omitempty"`
	Reason        string `json:"reason,omitempty"`
	InstallRan    bool   `json:"install_ran"`
	InstallExit   int    `json:"install_exit"`
	Target        string `json:"target,omitempty"`
	TargetReached *bool  `json:"target_reached,omitempty"`
}

// HubCheckoutRev is one allow-listed checkout's revision in a node's
// heartbeat: the short sha and the branch, empty when HEAD is detached.
type HubCheckoutRev struct {
	Sha    string `json:"sha"`
	Branch string `json:"branch"`
}

// nodeCheckoutDefaults is the allow-list when spawn.json names none: only the
// rstack checkout under the node user's home.
func nodeCheckoutDefaults() map[string]string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return map[string]string{}
	}
	return map[string]string{nodeCheckoutDefaultName: filepath.Join(home, nodeCheckoutDefaultDir)}
}

// nodeCheckoutAllowList returns name to directory for every checkout the node
// may act on, read from the loaded spawn.json (hubSpawnConfig.Checkouts, the one
// parser). No config, or no checkouts key, gives the default rstack checkout. A
// config that does not load gives an empty list: checkout.pull fails closed.
func (client *HubClient) nodeCheckoutAllowList() map[string]string {
	if client != nil && client.nodeCheckouts != nil {
		return client.nodeCheckouts
	}
	if client == nil {
		return map[string]string{}
	}
	config, defaults, err := client.resolveHubSpawnConfig()
	if err != nil {
		// A missing spawn.json is the node on defaults, whether the path was
		// the default one or named explicitly.
		var configErr *hubSpawnConfigError
		if asConfigError(err, &configErr) && configErr.Rule == "config_missing" {
			return nodeCheckoutDefaults()
		}
		return map[string]string{}
	}
	if defaults || config.Checkouts == nil {
		return nodeCheckoutDefaults()
	}
	return config.Checkouts
}

// checkoutPeerAccepted reports whether the current hub listed
// hubFeatureCheckoutPull, so the heartbeat may carry checkouts.
func (client *HubClient) checkoutPeerAccepted() bool {
	peer := client.signalsPeer.Load()
	return peer != nil && peer.acceptsCheckoutPull.Load()
}

// nodeCheckoutGitRunner adapts runNodeCheckoutGit to the runner the shared
// ignored-path check takes. Its args carry their own -C dir.
func nodeCheckoutGitRunner(dir string) relayCommandRunner {
	return func(ctx context.Context, args ...string) ([]byte, error) {
		return runNodeCheckoutGit(ctx, dir, nodeCheckoutPullStepTimeout, args...)
	}
}

// runNodeCheckoutGit runs git in dir under its own timeout, with no terminal
// prompt, so a credential prompt fails instead of hanging the node.
func runNodeCheckoutGit(ctx context.Context, dir string, timeout time.Duration, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), nodeCheckoutGitTerminalPrompt)
	cmd.WaitDelay = 100 * time.Millisecond
	nodeCheckoutSetGroup(cmd)
	return cmd.Output()
}

// nodeCheckoutHead reads a checkout's revision for the heartbeat. A failed
// read omits the checkout rather than reporting a guess.
func nodeCheckoutHead(ctx context.Context, dir string) (HubCheckoutRev, bool) {
	sha, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--short", "HEAD")
	if err != nil {
		return HubCheckoutRev{}, false
	}
	branch, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return HubCheckoutRev{}, false
	}
	rev := HubCheckoutRev{Sha: strings.TrimSpace(string(sha)), Branch: strings.TrimSpace(string(branch))}
	if !nodeCheckoutShortSHA.MatchString(rev.Sha) {
		return HubCheckoutRev{}, false
	}
	if rev.Branch == nodeCheckoutDetachedBranchName {
		rev.Branch = ""
	}
	if rev.Branch != "" && !validNodeRefName(rev.Branch) {
		return HubCheckoutRev{}, false
	}
	return rev, true
}

// checkoutRevisionsForHeartbeat reports each allow-listed checkout's revision,
// in name order and at most nodeCheckoutMaxHeartbeat entries. It returns nil
// when there is nothing to report, which leaves the field out.
func (client *HubClient) checkoutRevisionsForHeartbeat(ctx context.Context) map[string]HubCheckoutRev {
	allow := client.nodeCheckoutAllowList()
	names := make([]string, 0, len(allow))
	for name := range allow {
		names = append(names, name)
	}
	sort.Strings(names)
	var out map[string]HubCheckoutRev
	for _, name := range names {
		if len(out) >= nodeCheckoutMaxHeartbeat {
			break
		}
		rev, ok := nodeCheckoutHead(ctx, allow[name])
		if !ok {
			continue
		}
		if out == nil {
			out = make(map[string]HubCheckoutRev)
		}
		out[name] = rev
	}
	return out
}

// nodeCheckoutPull is the checkout.pull op. An unlisted name is refused
// before any path is touched.
func (client *HubClient) nodeCheckoutPull(ctx context.Context, args nodeCheckoutPullArgs) (any, string) {
	dir, listed := client.nodeCheckoutAllowList()[args.Checkout]
	if !listed {
		return nodeCheckoutPullResult{Outcome: nodeCheckoutOutcomeRefused, Checkout: args.Checkout, Target: args.Rev, Reason: nodeCheckoutReasonNotAllowed}, ""
	}
	ctx, cancel := context.WithTimeout(ctx, nodeCheckoutPullRunTimeout)
	defer cancel()
	return client.nodeCheckoutPullRun(ctx, args.Checkout, dir, args.Rev), ""
}

// nodeCheckoutPullRun pulls one checkout with `git pull --ff-only`. Every
// refusal happens before the pull, or is a pull that did not fast-forward, so
// a refused checkout keeps its branch and worktree.
func (client *HubClient) nodeCheckoutPullRun(ctx context.Context, name, dir, rev string) nodeCheckoutPullResult {
	result := nodeCheckoutPullResult{Checkout: name, Target: rev}
	refuse := func(reason string) nodeCheckoutPullResult {
		result.Outcome, result.Reason = nodeCheckoutOutcomeRefused, reason
		return result
	}
	if info, err := os.Lstat(dir); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return refuse(nodeCheckoutReasonNotCheckout)
	}
	lockPath, err := nodeGitOpLockPath(dir)
	if err != nil {
		return refuse(nodeCheckoutReasonNotCheckout)
	}
	unlock, err := acquireNodeGitOpLock(ctx, lockPath, client.nodeGitOpWait())
	switch {
	case errors.Is(err, errNodeGitOpBusy):
		return refuse(nodeCheckoutReasonBusy)
	case err != nil:
		return refuse(nodeCheckoutReasonLockFailed)
	}
	defer unlock()

	top, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--show-toplevel")
	if err != nil || !nodeSamePath(strings.TrimSpace(string(top)), dir) {
		return refuse(nodeCheckoutReasonNotCheckout)
	}
	if _, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "symbolic-ref", "--quiet", "HEAD"); err != nil {
		return refuse(nodeCheckoutReasonDetached)
	}
	if _, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"); err != nil {
		return refuse(nodeCheckoutReasonNoUpstream)
	}
	status, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "status", "--porcelain", "--untracked-files=normal")
	if err != nil {
		return refuse(nodeCheckoutReasonGitFailed)
	}
	if len(strings.TrimSpace(string(status))) > 0 {
		return refuse(nodeCheckoutReasonDirty)
	}
	from, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--verify", "HEAD")
	if err != nil {
		return refuse(nodeCheckoutReasonGitFailed)
	}
	if _, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutFetchTimeout, "fetch", "--quiet"); err != nil {
		return refuse(nodeCheckoutReasonFetchFailed)
	}
	// The fast-forward is `merge --ff-only --no-overwrite-ignore` against the
	// fetched upstream, not `pull`: pull rejects --no-overwrite-ignore, and a plain
	// fast-forward overwrites an ignored file that the incoming commit tracks.
	expected, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--verify", "@{u}")
	if err != nil {
		return refuse(nodeCheckoutReasonGitFailed)
	}
	collision, err := nodeIgnoredCheckoutCollision(ctx, nodeCheckoutGitRunner(dir), dir, strings.TrimSpace(string(from)), strings.TrimSpace(string(expected)))
	if err != nil {
		return refuse(nodeCheckoutReasonGitFailed)
	}
	if collision {
		return refuse(nodeCheckoutReasonIgnoredCollision)
	}
	if _, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutPullStepTimeout, "merge", "--ff-only", "--no-overwrite-ignore", "@{u}"); err != nil {
		// A failed fast-forward may have moved HEAD or the worktree before it
		// died, so the state is read back and reported, not assumed clean.
		reason, head := nodeCheckoutFailedPull(ctx, dir, strings.TrimSpace(string(from)))
		if reason == nodeCheckoutReasonPullInterrupted {
			// Only an interrupted pull names the HEAD it left; an unchanged
			// refusal reports no revision, so none can reach a deploy record.
			result.To = head
		}
		return refuse(reason)
	}
	to, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--verify", "HEAD")
	if err != nil {
		// The pull succeeded, so HEAD may have moved; without a readable HEAD the
		// state cannot be confirmed, and that is reported as interrupted.
		return refuse(nodeCheckoutReasonPullInterrupted)
	}
	result.From = strings.TrimSpace(string(from))
	result.To = strings.TrimSpace(string(to))
	if result.From == result.To {
		result.Outcome = nodeCheckoutOutcomeCurrent
	} else {
		result.Outcome = nodeCheckoutOutcomePulled
		// Renames are off so a file moved out of install/ or bin/ lists its old
		// path too. A range whose file list cannot be read gets install, not
		// silence: a stale install is the worse failure.
		changed, err := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "diff", "-z", "--no-renames", "--name-only", result.From, result.To)
		if err != nil || installPathsChanged(string(changed)) {
			result.InstallRan, result.InstallExit, result.Reason = client.nodeCheckoutInstall(ctx, dir)
		}
	}
	if rev != "" {
		_, reachedErr := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "merge-base", "--is-ancestor", rev, "HEAD")
		reached := reachedErr == nil
		result.TargetReached = &reached
	}
	return result
}

// nodeCheckoutFailedPull names why a `git pull --ff-only` failed, from the
// checkout's state rather than the error. If HEAD moved or the worktree is
// dirty, the pull was interrupted mid-apply: the reason is pull-interrupted and
// head is the HEAD found (empty when it cannot be read). Otherwise the state is
// unchanged, and ancestry tells a non-fast-forward from another failure.
func nodeCheckoutFailedPull(ctx context.Context, dir, from string) (string, string) {
	headOut, headErr := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "rev-parse", "--verify", "HEAD")
	statusOut, statusErr := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "status", "--porcelain", "--untracked-files=normal")
	if headErr != nil || statusErr != nil {
		return nodeCheckoutReasonPullInterrupted, ""
	}
	head := strings.TrimSpace(string(headOut))
	if head != from || len(strings.TrimSpace(string(statusOut))) > 0 {
		return nodeCheckoutReasonPullInterrupted, head
	}
	// A pull that cannot fast-forward says so from the ancestry, not from
	// git's wording, which varies with locale.
	if _, ancestry := runNodeCheckoutGit(ctx, dir, nodeCheckoutRevTimeout, "merge-base", "--is-ancestor", "HEAD", "@{u}"); isExitStatus(ancestry, 1) {
		return nodeCheckoutReasonNotFF, head
	}
	return nodeCheckoutReasonPullFailed, head
}

// installPathsChanged reports whether a `git diff -z --name-only` list touches
// install/ or bin/, the two places install.sh depends on. The -z list is NUL
// separated and unquoted, so a name with a quote, a backslash or a non-ASCII
// byte still matches its prefix.
func installPathsChanged(names string) bool {
	for _, line := range strings.Split(names, "\x00") {
		if strings.HasPrefix(line, "install/") || strings.HasPrefix(line, "bin/") {
			return true
		}
	}
	return false
}

// nodeCheckoutInstall runs the checkout's install.sh directly, with no shell
// and no argument from the order. It returns whether the script started, its
// status and, when it did not run or ran too long, the reason.
func (client *HubClient) nodeCheckoutInstall(ctx context.Context, dir string) (bool, int, string) {
	script := filepath.Join(dir, nodeCheckoutInstallPathFile)
	info, err := os.Lstat(script)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
		return false, 0, nodeCheckoutReasonInstallGone
	}
	ctx, cancel := context.WithTimeout(ctx, client.nodeCheckoutInstallBudget())
	defer cancel()
	cmd := exec.CommandContext(ctx, script)
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	cmd.WaitDelay = 100 * time.Millisecond
	// The script's own children must not outlive the bound: the whole process
	// group is killed when the budget ends.
	nodeCheckoutSetGroup(cmd)
	err = cmd.Run()
	if cmd.ProcessState == nil {
		// The script never started (not executable, or no interpreter).
		return false, 0, nodeCheckoutReasonInstallGone
	}
	exit := cmd.ProcessState.ExitCode()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return true, nodeCheckoutInstallExitKilled, nodeCheckoutReasonInstallLong
	}
	if err != nil && exit == 0 && !errors.Is(err, exec.ErrWaitDelay) {
		exit = nodeCheckoutInstallExitKilled
	}
	if exit != 0 {
		slog.Warn("checkout install failed", "dir", filepath.Base(dir), "exit", exit)
	}
	return true, exit, ""
}

// nodeCheckoutInstallBudget bounds install.sh; a test seam shortens it.
func (client *HubClient) nodeCheckoutInstallBudget() time.Duration {
	if client != nil && client.nodeInstallWait > 0 {
		return client.nodeInstallWait
	}
	return nodeCheckoutInstallTimeout
}

// isExitStatus reports whether err is a process exit with the given status.
func isExitStatus(err error, status int) bool {
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr) && exitErr.ExitCode() == status
}

// validNodeCheckoutPullResult is the hub's check on a node's answer: a
// pulled or current result names full revisions, and a refusal names a reason
// from the closed set.
func validNodeCheckoutPullResult(result nodeCheckoutPullResult) bool {
	if !nodeCheckoutNamePattern.MatchString(result.Checkout) {
		return false
	}
	switch result.Outcome {
	case nodeCheckoutOutcomePulled:
		return nodeShaPattern.MatchString(result.From) && nodeShaPattern.MatchString(result.To) && result.From != result.To &&
			(result.Reason == "" || result.Reason == nodeCheckoutReasonInstallGone || result.Reason == nodeCheckoutReasonInstallLong)
	case nodeCheckoutOutcomeCurrent:
		return nodeShaPattern.MatchString(result.To) && result.Reason == "" && !result.InstallRan
	case nodeCheckoutOutcomeRefused:
		// A refusal names no revision, except an interrupted pull's HEAD.
		if result.Reason != nodeCheckoutReasonPullInterrupted && result.To != "" {
			return false
		}
		return nodeCheckoutRefusals[result.Reason]
	}
	return false
}
