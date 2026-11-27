package laneevent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidID(t *testing.T) {
	for value, want := range map[string]bool{
		"emit-1": true, strings.Repeat("a", idMax): true,
		"": false, strings.Repeat("a", idMax+1): false,
		"a\nb": false, "a\x7f": false, "a\u0085b": false, "\xff": false,
	} {
		if got := ValidID(value); got != want {
			t.Errorf("ValidID(%.20q)=%t, want %t", value, got, want)
		}
	}
}

// An entry holds no NUL, and one job's prefix does not match a job whose id
// starts the same way.
func TestCompletionKeys(t *testing.T) {
	key := MergedCompletionKey("job.completed\x00j\x001\x00r.md\x00\x0000001-job.completed.json")
	if strings.ContainsRune(key, 0) || !strings.HasPrefix(key, CompletionKeyPrefix("j")) || strings.HasPrefix(key, CompletionKeyPrefix("j-2")) {
		t.Fatalf("key=%q", key)
	}
	if other := MergedCompletionKey("job.completed\x00j-2\x001\x00r.md\x00\x0000001-job.completed.json"); strings.HasPrefix(other, CompletionKeyPrefix("j")) {
		t.Fatalf("j's prefix matches %q", other)
	}
}

// The READY record round-trips, the latest write wins, and content that is
// not a lane.event id reads as no record. Mutants: append instead of replace,
// or return unvalidated content.
func TestReadyEventRecord(t *testing.T) {
	dir := t.TempDir()
	if got := ReadReadyEvent(dir); got != "" {
		t.Fatalf("no record reads %q", got)
	}
	if err := WriteReadyEvent(dir, "ready-1"); err != nil {
		t.Fatal(err)
	}
	if err := WriteReadyEvent(dir, "ready-2"); err != nil {
		t.Fatal(err)
	}
	if got := ReadReadyEvent(dir); got != "ready-2" {
		t.Fatalf("latest record reads %q", got)
	}
	info, err := os.Stat(filepath.Join(dir, ReadyEventFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode %o, want 0600", info.Mode().Perm())
	}
	for _, bad := range []string{"", "  \n", "bad\nid", strings.Repeat("r", idMax+1)} {
		if err := os.WriteFile(filepath.Join(dir, ReadyEventFile), []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if got := ReadReadyEvent(dir); got != "" {
			t.Fatalf("%.20q reads %q", bad, got)
		}
	}
}
