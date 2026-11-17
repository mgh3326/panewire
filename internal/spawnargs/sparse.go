package spawnargs

import (
	"fmt"
	"strings"
)

// A sparse checkout is a spec spawn (--checkout-origin, --checkout-commit,
// --checkout-ref) that names the paths to keep out of the worktree and the
// paths to keep in it (hk 5095, hk 5098). The node builds the pattern list
// itself: it never accepts a pattern list from the caller, so the grammar
// here is the only one a hub and a node apply. Validation runs on the
// submitter's terminal, again at the hub, and again at the node; each refusal
// names the entry's index rather than repeating its text.

const (
	// MaxSparseExcludes bounds the exclude list; MaxSparseIncludes the include
	// list. MaxSparsePatternBytes bounds one entry of either.
	MaxSparseExcludes     = 64
	MaxSparseIncludes     = 500
	MaxSparsePatternBytes = 4096
)

// SparseRefusal returns why a sparse request is refused, or "" when both lists
// are acceptable. An entry must be a plain path or pattern: it is prefixed
// with "!/" (an exclude) or "/" (an include) as the node builds the list, so a
// value that carries one of those prefixes, a ".." segment, a NUL or a
// newline would change the list's meaning rather than name a path.
func SparseRefusal(exclude, include []string) string {
	if reason := sparseListRefusal("--checkout-exclude", exclude, MaxSparseExcludes); reason != "" {
		return reason
	}
	return sparseListRefusal("--checkout-include", include, MaxSparseIncludes)
}

func sparseListRefusal(flag string, entries []string, limit int) string {
	if len(entries) > limit {
		return fmt.Sprintf("%s carries %d entries; the limit is %d", flag, len(entries), limit)
	}
	for index, entry := range entries {
		if len(entry) > MaxSparsePatternBytes {
			return fmt.Sprintf("%s entry %d is longer than %d bytes", flag, index+1, MaxSparsePatternBytes)
		}
		if entry == "" {
			return fmt.Sprintf("%s entry %d is empty", flag, index+1)
		}
		if strings.HasPrefix(entry, "!") || strings.HasPrefix(entry, "/") {
			return fmt.Sprintf("%s entry %d starts with %q: state the path or pattern without the list prefix", flag, index+1, entry[:1])
		}
		if strings.Contains(entry, "..") || strings.ContainsAny(entry, "\x00\r\n") {
			return fmt.Sprintf("%s entry %d contains %s, which a sparse pattern cannot carry", flag, index+1, sparseBadText(entry))
		}
	}
	return ""
}

// sparseBadText names what is wrong with one entry without repeating it.
func sparseBadText(entry string) string {
	switch {
	case strings.Contains(entry, ".."):
		return `".."`
	case strings.Contains(entry, "\x00"):
		return "a NUL"
	case strings.Contains(entry, "\n"):
		return "a newline"
	default:
		return "a carriage return"
	}
}

// SparseSpecRefusal is SparseRefusal plus the rule that a pattern list belongs
// to a detached spec checkout: branch mode prepares a full worktree, so the
// lists there would be silently dropped.
func SparseSpecRefusal(mode string, exclude, include []string) string {
	if mode != "detached" && (len(exclude) != 0 || len(include) != 0) {
		return "--checkout-exclude and --checkout-include are valid only for a detached checkout spec"
	}
	return SparseRefusal(exclude, include)
}

// SparsePatterns is the pattern list the node feeds
// `git sparse-checkout set --no-cone --stdin`: everything, then one exclusion
// per exclude, then one literal path per include. Includes are literal paths
// (a changed file is always checked out, even inside an excluded tree), so
// their gitignore metacharacters are escaped.
func SparsePatterns(exclude, include []string) []string {
	patterns := make([]string, 0, 1+len(exclude)+len(include))
	patterns = append(patterns, "/*")
	for _, entry := range exclude {
		patterns = append(patterns, "!/"+entry)
	}
	for _, entry := range include {
		patterns = append(patterns, "/"+EscapeSparseInclude(entry))
	}
	return patterns
}

// EscapeSparseInclude renders one include path as the literal pattern gitignore
// reads: a backslash, the glob characters, and a leading "!" or "#" are
// escaped so the path is matched exactly as written.
func EscapeSparseInclude(path string) string {
	var b strings.Builder
	b.Grow(len(path) + 4)
	for i := range len(path) {
		c := path[i]
		if c == '\\' || c == '*' || c == '?' || c == '[' || i == 0 && (c == '!' || c == '#') {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return b.String()
}
