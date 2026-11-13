package fleet

import (
	"os"
	"path/filepath"
	"testing"
)

// A pool-order file with a key this release does not read still loads; a
// malformed value for a known key still refuses.
func TestPoolPolicyUnknownKeyLoadsMalformedKnownKeyRefuses(t *testing.T) {
	dir := t.TempDir()
	unknown := filepath.Join(dir, "pool-unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"version":3,"order":["waste","free","ranked"],"credit_pools":["anthropic-api","openai-api"],"retired_key":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPoolPolicy(unknown); err != nil {
		t.Fatalf("pool policy with an unknown key refused: %v", err)
	}
	malformed := filepath.Join(dir, "pool-malformed.json")
	if err := os.WriteFile(malformed, []byte(`{"version":"one"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPoolPolicy(malformed); err == nil {
		t.Fatal("pool policy with a non-integer version was accepted")
	}
}
