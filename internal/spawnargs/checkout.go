package spawnargs

import (
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

// The checkout spec (worktree.prepare) has one grammar: the node prepares
// what it is handed, the hub refuses a request the node would drop, and a
// submitter that names the spec itself (--checkout-origin, --checkout-commit,
// --checkout-ref) applies the same rule before anything is uploaded (hk 4738).

var fetchPullPattern = regexp.MustCompile(`^pull/[0-9]+/head$`)

// ValidRefName mirrors git's check-ref-format rules for the argument shapes
// worktree.prepare accepts: no control bytes, "..", "@{", whitespace or git's
// magic chars, no segment leading with '.' or ending ".lock".
func ValidRefName(name string) bool {
	if name == "" || name == "@" || len(name) > 200 || hasControl(name) {
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

// ValidFetchRef is the closed set of fetch shapes: a branch head or a
// pull-request head.
func ValidFetchRef(fetch string) bool {
	if rest, ok := strings.CutPrefix(fetch, "refs/heads/"); ok {
		return ValidRefName(rest)
	}
	return fetchPullPattern.MatchString(fetch)
}

// OriginRepo normalizes an origin argument to "owner/repo" for the clone
// identity check. Any userinfo refuses the value: URL userinfo, or an '@' in
// anything that is not an absolute local path. URL and scp-style remotes need
// exactly owner/repo path segments; a local clone source stands in with its
// last two.
func OriginRepo(raw string) (string, bool) {
	// git clone receives the raw argument, so its control bytes are refused
	// before trimming could hide a trailing one.
	u := strings.TrimSpace(raw)
	if hasControl(raw) || !originShapeOK(u) {
		return "", false
	}
	switch {
	case strings.Contains(u, "://"):
		return urlOriginRepo(u)
	case filepath.IsAbs(u):
		seg := originPathSegments(u)
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
	return scpOriginRepo(host, repoPath)
}

// CloneRemoteRepo reads owner/repo from a clone's own origin. Unlike a
// caller-supplied origin, an existing clone may use the scp-style SSH form
// user@host:owner/repo.git; its user part is dropped, never refused. The
// remote splits at its first ':' and only the host side can hold userinfo,
// so an '@' in the repo path (host:owner/re@po.git) stays part of the path.
func CloneRemoteRepo(raw string) (string, bool) {
	u := strings.TrimSpace(raw)
	if !originShapeOK(u) {
		return "", false
	}
	if strings.Contains(u, "://") {
		if v, err := url.Parse(u); err == nil && v.User != nil {
			v.User = nil
			u = v.String()
		}
		return urlOriginRepo(u)
	}
	if filepath.IsAbs(u) {
		return OriginRepo(u)
	}
	host, repoPath, ok := strings.Cut(u, ":")
	if !ok {
		return "", false
	}
	if at := strings.LastIndex(host, "@"); at >= 0 && !strings.Contains(host, "/") {
		host = host[at+1:]
	}
	return scpOriginRepo(host, repoPath)
}

// originShapeOK is the check every origin form shares.
func originShapeOK(u string) bool {
	// A leading '-' would reach git as an option, never a repository.
	return u != "" && !strings.HasPrefix(u, "-") && len(u) <= 2048 && !hasControl(u) && !strings.Contains(u, " ")
}

func urlOriginRepo(u string) (string, bool) {
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
	seg := originPathSegments(v.Path)
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

// scpOriginRepo reads an scp-style host:owner/repo whose userinfo, if any, is
// already gone.
func scpOriginRepo(host, repoPath string) (string, bool) {
	// An empty host (":owner/repo") is a local path to git, and a
	// one-character host is drive-letter shaped (C:owner/repo): a local path
	// on Windows, never a remote worth trusting. A '/' before the colon
	// makes the whole value a relative path.
	if len(host) < 2 || strings.ContainsAny(host, "/@") {
		return "", false
	}
	seg := originPathSegments(repoPath)
	if len(seg) != 2 {
		return "", false
	}
	return seg[0] + "/" + seg[1], true
}

func originPathSegments(p string) []string {
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

// hasControl reports a byte git would read as a line break or a delimiter.
func hasControl(s string) bool {
	for i := range len(s) {
		if s[i] < 0x20 || s[i] == 0x7f {
			return true
		}
	}
	return false
}
