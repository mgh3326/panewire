package fleet

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	neturl "net/url"
	"regexp"
	"strings"

	"github.com/mgh3326/panewire/internal/spawnargs"
)

// A checkout spec is a spawn that names the worktree it will run in instead of
// holding one on the submitting host (hk 4738): --checkout-origin,
// --checkout-commit and --checkout-ref. The submitting host reads no checkout
// at all; the node prepares a detached worktree of the same origin at the
// exact commit, exactly as it does for a -c spawn from a linked worktree.
// -c keeps working, and the two are exclusive: one spawn names one worktree.
//
// An implementer spec on refs/heads/<branch> asks for mode "successor" (hk
// 5118): the node prepares a local branch <branch> at the commit, tracking
// origin/<branch>, so a successor seat continues the pushed branch with a
// plain `git push`. Reviewers and consults keep the detached checkout, since a
// reader never continues the branch.
//
// The values are the worktree.prepare grammar (spawnargs), so a spec the
// submitter accepts is one the hub and the node accept, and a bad one is
// refused in the caller's terminal with the flag named.

// checkoutCommitPattern is the exact commit: 40 lowercase hex characters, the
// form worktree.prepare and the hub's pinned-checkout rule both read.
var checkoutCommitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

const checkoutSpecFlags = "--checkout-origin, --checkout-commit and --checkout-ref"

// checkoutSpec returns the pinned checkout the spawn names, with the
// repository name the hub keys the node's cwd_map by. Both are "" when the
// spawn names no spec. A spec is all three flags or a usage refusal.
func (s Spawn) checkoutSpec() (*HubCheckout, string, error) {
	given := 0
	for _, value := range []string{s.CheckoutOrigin, s.CheckoutCommit, s.CheckoutRef} {
		if value != "" {
			given++
		}
	}
	if given == 0 {
		if len(s.CheckoutExclude) != 0 || len(s.CheckoutInclude) != 0 {
			return nil, "", fail(exitUsage, "--checkout-exclude and --checkout-include need %s: a sparse checkout is a spec checkout", checkoutSpecFlags)
		}
		return nil, "", nil
	}
	if given != 3 {
		return nil, "", fail(exitUsage, "pass %s together: one pinned checkout", checkoutSpecFlags)
	}
	if s.CWD != "" {
		return nil, "", fail(exitUsage, "-c and %s are exclusive: a spec spawn holds no local checkout, a -c spawn reads its -c", checkoutSpecFlags)
	}
	if !checkoutCommitPattern.MatchString(s.CheckoutCommit) {
		return nil, "", fail(exitUsage, "--checkout-commit must be a 40-character commit id (lowercase hex), got %q", s.CheckoutCommit)
	}
	if !spawnargs.ValidFetchRef(s.CheckoutRef) {
		return nil, "", fail(exitUsage, "--checkout-ref must be pull/<n>/head or refs/heads/<branch>, got %q", s.CheckoutRef)
	}
	// The origin becomes the https form the -c path uploads. The node keeps one
	// clone per repository and reuses it for every spawn, and each later fetch
	// uses that clone's own remote, so the transport must not depend on which
	// spawn cloned it. What https cannot keep is refused by name; a credential
	// is never dropped.
	origin, name, why := specOrigin(s.CheckoutOrigin)
	if why != "" {
		return nil, "", fail(exitUsage, "%s, got %q", why, s.CheckoutOrigin)
	}
	if !validID(name) || !hubKeyRule.MatchString(RepoKey(name)) {
		return nil, "", fail(exitUsage, "--checkout-origin names repository %q, which is not a hub cwd key (lowercase letters, digits, dot, underscore or hyphen, at most 64)", name)
	}
	// The lists are refused here as at the hub and the node; the entry index
	// names the bad one rather than repeating its text (hk 5098). A branch
	// checkout never takes them, so an implementer's branch ref and the
	// sparse flags refuse each other here, before the mode is chosen.
	if reason := spawnargs.SparseSpecRefusal(checkoutMode(s.Role, s.CheckoutRef), s.CheckoutExclude, s.CheckoutInclude); reason != "" {
		return nil, "", fail(exitUsage, "%s", reason)
	}
	mode, branch := checkoutMode(s.Role, s.CheckoutRef), ""
	fetch := s.CheckoutRef
	if mode == HubCheckoutSuccessor {
		// A successor continues the pushed branch, so the branch name travels
		// on its own and the node fetches the whole remote (hk 5118).
		branch, fetch = strings.TrimPrefix(s.CheckoutRef, "refs/heads/"), ""
	}
	return &HubCheckout{
		Origin:  origin,
		Mode:    mode,
		Branch:  branch,
		Sha:     s.CheckoutCommit,
		Fetch:   fetch,
		Dir:     checkoutWorktreeName(s.Job),
		Exclude: s.CheckoutExclude,
		Include: s.CheckoutInclude,
	}, name, nil
}

// HubCheckoutSuccessor is the checkout mode of an implementer spec spawn whose
// ref is a branch: the node prepares a local branch at the commit instead of a
// detached HEAD (hk 5118).
const HubCheckoutSuccessor = "successor"

// checkoutMode names the mode a spec spawn's checkout is prepared in. A
// reviewer or consult reads the commit and never continues the branch, so only
// an implementer seat gets the local branch (hk 5118).
func checkoutMode(role, ref string) string {
	if role == "implementer" && strings.HasPrefix(ref, "refs/heads/") {
		return HubCheckoutSuccessor
	}
	return "detached"
}

// originRule is the sentence --checkout-origin refusals share.
const originRule = "--checkout-origin must be the repository's remote naming owner/repo, with no credentials: an https:// URL is used as named, and ssh://, git://, git@host:owner/repo or host:owner/repo become the same https form the -c path uploads"

// specOrigin normalizes --checkout-origin to the origin the node clones, with
// the repository name the hub keys the node's cwd_map by, or the sentence
// saying why the value cannot be carried.
//
// The node keeps one clone per repository and reuses it for every spawn, and
// each later fetch runs on that clone's own remote (nodeWorktreePrepare), so
// the transport must not depend on which spawn cloned the repository: every
// origin becomes the userinfo-free https form the -c path uploads. An endpoint
// https cannot keep — an explicit ssh or git port — is refused by name rather
// than dropped, a credential is refused rather than discarded, a one-character
// host is a Windows drive letter and not a remote, and the result must name
// owner/repo.
func specOrigin(raw string) (origin, name, why string) {
	if credentialedURL(raw) {
		return "", "", originRule
	}
	if strings.Contains(raw, "://") {
		u, err := neturl.Parse(raw)
		if err != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
			return "", "", originRule
		}
		switch u.Scheme {
		case "https", "ssh", "git":
		default:
			return "", "", originRule
		}
		if u.Scheme != "https" && u.Port() != "" {
			return "", "", fmt.Sprintf("--checkout-origin names %s port %s, which the node's https clone does not keep: pass the https URL", u.Scheme, u.Port())
		}
	}
	origin, ok := hubOrigin(raw)
	if !ok {
		return "", "", originRule
	}
	u, err := neturl.Parse(origin)
	if err != nil || len(u.Hostname()) < 2 {
		return "", "", originRule
	}
	ownerRepo, ok := spawnargs.OriginRepo(origin)
	if !ok {
		return "", "", originRule
	}
	return origin, ownerRepo[strings.IndexByte(ownerRepo, '/')+1:], ""
}

// checkoutWorktreeName is the directory the node prepares a job's checkout in,
// under its work root. A linked worktree keeps its own directory name; a
// canonical checkout and a spec spawn name one after the job id, bounded and
// disambiguated by a hash so two long job ids never share a directory.
func checkoutWorktreeName(job string) string {
	if len(job) > 110 {
		sum := sha256.Sum256([]byte(job))
		job = job[:40] + "-" + hex.EncodeToString(sum[:])
	}
	return job + "-checkout"
}

// CheckoutWorktreeName is the checkout directory a spec spawn of job gets, for
// the hub when it builds a successor's checkout itself (hk 4633).
func CheckoutWorktreeName(job string) string { return checkoutWorktreeName(job) }
