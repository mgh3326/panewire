package fleet

import (
	"crypto/sha256"
	"encoding/hex"
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
	// The spec's origin becomes the userinfo-free https form the -c path
	// uploads (hubOrigin), so an scp-style or ssh remote is accepted and
	// normalized instead of refused, and the node clones one form for both
	// paths. The normalized origin must still name owner/repo.
	const originRefusal = "--checkout-origin must be the repository's remote, naming owner/repo: https://, ssh://, git://, git@host:owner/repo or host:owner/repo, with no credentials"
	origin, ok := hubOrigin(s.CheckoutOrigin)
	if !ok {
		return nil, "", fail(exitUsage, "%s, got %q", originRefusal, s.CheckoutOrigin)
	}
	ownerRepo, ok := spawnargs.OriginRepo(origin)
	if !ok {
		return nil, "", fail(exitUsage, "%s, got %q", originRefusal, s.CheckoutOrigin)
	}
	name := ownerRepo[strings.IndexByte(ownerRepo, '/')+1:]
	if !validID(name) || !hubKeyRule.MatchString(RepoKey(name)) {
		return nil, "", fail(exitUsage, "--checkout-origin names repository %q, which is not a hub cwd key (lowercase letters, digits, dot, underscore or hyphen, at most 64)", name)
	}
	return &HubCheckout{
		Origin: origin,
		Mode:   "detached",
		Sha:    s.CheckoutCommit,
		Fetch:  s.CheckoutRef,
		Dir:    checkoutWorktreeName(s.Job),
	}, name, nil
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
