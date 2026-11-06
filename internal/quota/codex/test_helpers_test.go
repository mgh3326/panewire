package codex

import (
	"testing"
	"time"
)

// Wait helpers for this package's tests. They mirror `eventually` and
// `waitUntil` in the root package's test_helpers_test.go, which an internal
// package's tests cannot import. Use them instead of a fixed time.Sleep or a
// new deadline loop.

// eventually polls condition every millisecond until it holds, failing the
// test with label once timeout passes.
func eventually(t *testing.T, timeout time.Duration, label string, condition func() bool) {
	t.Helper()
	if !waitUntil(timeout, condition) {
		t.Fatalf("timed out waiting for %s", label)
	}
}

// waitUntil polls condition every millisecond until it holds or timeout
// passes, and reports whether it held.
func waitUntil(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond) // pacing a poll of asynchronous work
	}
	return true
}
