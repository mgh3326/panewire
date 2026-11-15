package teststate

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// TestSweepStaleRemovesOnlyStaleOwnedStateDirs pins what a sweep may touch: a
// directory with the prefix, older than the bound and owned by this user. A
// fresh run, another prefix, a file and a symlink each stay, and a read-only
// tree inside a stale dir goes with it (hk 4733).
func TestSweepStaleRemovesOnlyStaleOwnedStateDirs(t *testing.T) {
	base := t.TempDir()
	now := time.Now()
	stale := now.Add(-2 * StaleAfter)
	mkdir := func(name string, mode fs.FileMode) string {
		path := filepath.Join(base, name)
		if err := os.Mkdir(path, mode); err != nil {
			t.Fatal(err)
		}
		return path
	}
	age := func(path string) {
		if err := os.Chtimes(path, stale, stale); err != nil {
			t.Fatal(err)
		}
	}

	staleDir := mkdir(StatePrefix+"stale", 0o700)
	if err := os.WriteFile(filepath.Join(staleDir, "fleet-events.jsonl"), []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	age(staleDir)

	freshDir := mkdir(StatePrefix+"fresh", 0o700)

	otherDir := mkdir("panewire-test-other-7", 0o700)
	age(otherDir)

	file := filepath.Join(base, StatePrefix+"file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	age(file)

	// A go module cache is 0555 all the way down; removing it needs a writable
	// parent at every level.
	readOnly := mkdir(StatePrefix+"readonly", 0o700)
	inner := filepath.Join(readOnly, "pkg", "mod")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inner, "module.zip"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(inner, 0o555); err != nil {
		t.Fatal(err)
	}
	age(readOnly)

	// A symlink with the prefix names a tree outside base; it is not a
	// directory of ours, so neither the link nor its target is touched.
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "keep")
	if err := os.WriteFile(outsideFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, StatePrefix+"link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	removed, err := SweepStale(base, now, StaleAfter, StatePrefix)
	if err != nil {
		t.Fatalf("SweepStale: %v", err)
	}
	want := []string{readOnly, staleDir}
	slices.Sort(want)
	slices.Sort(removed)
	if !slices.Equal(removed, want) {
		t.Fatalf("removed %v, want %v", removed, want)
	}
	for _, path := range []string{freshDir, otherDir, file, link} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("%s must survive the sweep: %v", path, err)
		}
	}
	if _, err := os.Stat(outsideFile); err != nil {
		t.Fatalf("sweep followed a symlink: %v", err)
	}
}
