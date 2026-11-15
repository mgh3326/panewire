package teststate

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// TestRemoveAllHandlesReadOnlyTree proves removal survives a tree whose
// directories are 0555, as a Go module cache is: the mode must be raised before
// the entries can be unlinked (hk 4733).
func TestRemoveAllHandlesReadOnlyTree(t *testing.T) {
	root := filepath.Join(t.TempDir(), "tree")
	inner := filepath.Join(root, "pkg", "mod", "cache")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "module.zip"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{inner, filepath.Dir(inner), root} {
		if err := os.Chmod(path, 0o555); err != nil {
			t.Fatal(err)
		}
	}
	if err := RemoveAll(root); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if _, err := os.Lstat(root); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("tree survived: %v", err)
	}
}

// TestRemoveAllMissingPathIsNotAnError keeps the cleanup idempotent: two runs
// racing on the same leftover dir both succeed. The tolerance has to hold at
// every level, so removeDir must not report a directory that vanished between
// its own stat and its listing.
func TestRemoveAllMissingPathIsNotAnError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone")
	for name, remove := range map[string]func(string) error{"RemoveAll": RemoveAll, "removeDir": removeDir} {
		if err := remove(missing); err != nil {
			t.Fatalf("%s on a missing path: %v", name, err)
		}
	}
}

// TestRemoveAllDoesNotFollowSymlink removes a link, never the tree it names.
func TestRemoveAllDoesNotFollowSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "keep")
	if err := os.WriteFile(outsideFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := RemoveAll(root); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if _, err := os.Stat(outsideFile); err != nil {
		t.Fatalf("RemoveAll followed a symlink: %v", err)
	}
}
