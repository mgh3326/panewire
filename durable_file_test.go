package panewire

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReplaceFileAtomicallyPublishesModeAndLeavesNoTemporary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := replaceFileAtomically(path, ".state-*", []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != "new\n" {
		t.Fatalf("contents=%q err=%v", contents, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("directory entries=%v err=%v", entries, err)
	}
}

func TestWriteSyncedTemporaryFileFailureLeavesNothing(t *testing.T) {
	if _, err := writeSyncedTemporaryFile(filepath.Join(t.TempDir(), "missing"), ".x-*", []byte("x"), 0o600); err == nil {
		t.Fatal("expected an error for a missing directory")
	}
}
