package laneevent

import (
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
