package fleet

import (
	"crypto/sha256"
	"encoding/hex"
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
	// The origin is the repository's remote in a form the node clones. A URL
	// keeps the endpoint it names, port included (a spec may name an ssh or
	// git port the -c path never sees); an scp-style remote becomes the https
	// form the -c path uploads. A credential is refused rather than dropped,
	// and the normalized origin must still name owner/repo.
	const originRefusal = "--checkout-origin must be the repository's remote, naming owner/repo, with no credentials: https://, ssh:// or git:// (cloned as named, port included), or git@host:owner/repo and host:owner/repo (become https)"
	origin, ok := specOrigin(s.CheckoutOrigin)
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

// specOrigin normalizes --checkout-origin to the origin the node clones. A URL
// keeps the endpoint it names — scheme, host and port — so ssh://host:2222 and
// git://host:9418 are cloned there, not on 443. Its userinfo is dropped,
// because the node's prepare takes none, but a credential (an https userinfo or
// any password) is refused rather than silently discarded. An scp-style remote
// becomes the userinfo-free https form the -c path uploads; a one-character
// host is a Windows drive letter, not a remote. A local path, an unlisted
// scheme or an owner/repo-less path is refused.
func specOrigin(raw string) (string, bool) {
	if credentialedURL(raw) {
		return "", false
	}
	origin := raw
	if strings.Contains(raw, "://") {
		u, err := neturl.Parse(raw)
		if err != nil || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
			return "", false
		}
		switch u.Scheme {
		case "https", "ssh", "git":
		default:
			return "", false
		}
		u.User = nil
		origin = u.String()
	} else {
		host, rest, ok := strings.Cut(raw, ":")
		if !ok || rest == "" || strings.HasPrefix(rest, "/") {
			return "", false
		}
		if at := strings.LastIndex(host, "@"); at >= 0 {
			host = host[at+1:]
		}
		if len(host) < 2 || strings.Contains(host, "/") {
			return "", false
		}
		origin = "https://" + host + "/" + rest
	}
	if _, ok := spawnargs.OriginRepo(origin); !ok {
		return "", false
	}
	return origin, true
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
