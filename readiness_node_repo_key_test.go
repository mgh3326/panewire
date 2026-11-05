package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

// A node on defaults lists a work-root checkout that keeps its remote's
// spelling (ParkingArcade) under its lower-case key, and resolves that key.
func TestDefaultRepoDirResolvesKeyCaseInsensitively(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "ParkingArcade")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got, ok := defaultRepoDir(root, "parkingarcade")
	if !ok {
		t.Fatal("parkingarcade did not resolve")
	}
	if info, err := os.Stat(got); err != nil || !info.IsDir() || !sameFile(t, got, dir) {
		t.Fatalf("resolved %q, want the ParkingArcade directory", got)
	}
	if _, ok := defaultRepoDir(root, "ParkingArcade"); ok {
		t.Fatal("an upper-case key is not a valid key")
	}
}

// Two directories that differ only in case are ambiguous: neither resolves.
func TestDefaultRepoDirRefusesAmbiguousCase(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "Repo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "REPO"), 0o755); err != nil || countEntries(t, root) != 2 {
		t.Skip("case-insensitive filesystem: two directories differing only in case cannot exist")
	}
	if _, ok := defaultRepoDir(root, "repo"); ok {
		t.Fatal("an ambiguous key resolved")
	}
}

func sameFile(t *testing.T, a, b string) bool {
	t.Helper()
	ia, ea := os.Stat(a)
	ib, eb := os.Stat(b)
	return ea == nil && eb == nil && os.SameFile(ia, ib)
}

func countEntries(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
