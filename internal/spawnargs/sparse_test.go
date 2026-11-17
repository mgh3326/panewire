package spawnargs

import (
	"slices"
	"strings"
	"testing"
)

// hk 5098: the sparse checkout's grammar, stated once for the submitter, the
// hub and the node.

func TestSparseRefusalAcceptsPlainPaths(t *testing.T) {
	if reason := SparseRefusal([]string{"Assets/Bin", "docs/generated"}, []string{"Assets/Changed.prefab", "a[1].txt"}); reason != "" {
		t.Fatalf("a plain plain list was refused: %s", reason)
	}
	if reason := SparseRefusal(nil, nil); reason != "" {
		t.Fatalf("empty lists were refused: %s", reason)
	}
}

func TestSparseRefusalNamesTheBadEntryIndex(t *testing.T) {
	for _, tc := range []struct {
		name    string
		exclude []string
		include []string
		want    string
	}{
		{"exclude starts with !", []string{"ok", "!Assets"}, nil, "--checkout-exclude entry 2 starts with"},
		{"exclude starts with /", []string{"/Assets"}, nil, "--checkout-exclude entry 1 starts with"},
		{"include starts with !", nil, []string{"ok", "!a.txt"}, "--checkout-include entry 2 starts with"},
		{"include starts with /", nil, []string{"/a.txt"}, "--checkout-include entry 1 starts with"},
		{"parent segment", []string{"Assets/../etc"}, nil, "--checkout-exclude entry 1 contains"},
		{"NUL", nil, []string{"a\x00b"}, "--checkout-include entry 1 contains"},
		{"newline", []string{"a\nb"}, nil, "--checkout-exclude entry 1 contains"},
		{"carriage return", nil, []string{"a\rb"}, "--checkout-include entry 1 contains"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason := SparseRefusal(tc.exclude, tc.include)
			if !strings.HasPrefix(reason, tc.want) {
				t.Fatalf("reason %q does not start with %q", reason, tc.want)
			}
			// The refusal names the entry, never its text.
			for _, entry := range append(append([]string(nil), tc.exclude...), tc.include...) {
				if strings.Contains(entry, "..") || len(entry) > 1 {
					if strings.Contains(reason, entry) {
						t.Fatalf("reason %q repeats the entry %q", reason, entry)
					}
				}
			}
		})
	}
}

func TestSparseRefusalBounds(t *testing.T) {
	excludes := make([]string, MaxSparseExcludes)
	includes := make([]string, MaxSparseIncludes)
	for i := range excludes {
		excludes[i] = "dir" + strings.Repeat("x", i%7)
	}
	for i := range includes {
		includes[i] = "file" + strings.Repeat("y", i%7)
	}
	if reason := SparseRefusal(excludes, includes); reason != "" {
		t.Fatalf("the exact bounds were refused: %s", reason)
	}
	if reason := SparseRefusal(append(excludes, "extra"), includes); !strings.Contains(reason, "limit is 64") {
		t.Fatalf("one exclude over the bound: %q", reason)
	}
	if reason := SparseRefusal(excludes, append(includes, "extra")); !strings.Contains(reason, "limit is 500") {
		t.Fatalf("one include over the bound: %q", reason)
	}
	long := strings.Repeat("a", MaxSparsePatternBytes+1)
	if reason := SparseRefusal(nil, []string{"short", long}); !strings.Contains(reason, "entry 2 is longer than") {
		t.Fatalf("an over-long include: %q", reason)
	}
}

func TestSparseSpecRefusalNeedsDetachedMode(t *testing.T) {
	if reason := SparseSpecRefusal("branch", []string{"Assets"}, nil); !strings.Contains(reason, "detached") {
		t.Fatalf("branch mode with lists: %q", reason)
	}
	if reason := SparseSpecRefusal("branch", nil, nil); reason != "" {
		t.Fatalf("branch mode without lists was refused: %s", reason)
	}
	if reason := SparseSpecRefusal("detached", []string{"Assets"}, nil); reason != "" {
		t.Fatalf("detached mode with a plain exclude was refused: %s", reason)
	}
}

func TestSparsePatternsStageExcludesThenLiteralIncludes(t *testing.T) {
	got := SparsePatterns([]string{"Assets/Bin", "docs/generated"}, []string{"Assets/Changed.prefab", "a[1].txt", "b*.txt", "q?.md", `back\slash`, "!bang", "#hash"})
	want := []string{
		"/*",
		"!/Assets/Bin",
		"!/docs/generated",
		"/Assets/Changed.prefab",
		`/a\[1].txt`,
		`/b\*.txt`,
		`/q\?.md`,
		`/back\\slash`,
		`/\!bang`,
		`/\#hash`,
	}
	if !slices.Equal(got, want) {
		t.Fatalf("patterns = %#v\nwant %#v", got, want)
	}
}
