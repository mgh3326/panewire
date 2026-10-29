package panewire

import (
	"testing"
	"time"
)

// Shared in-package test helpers. Reach for these before writing another
// deadline loop or a ticket-prefixed copy of one.

// eventually polls condition every millisecond until it holds, failing the
// test with label once timeout passes. Use it instead of a fixed time.Sleep
// whenever a test waits for asynchronous work to land.
func eventually(t *testing.T, timeout time.Duration, label string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		time.Sleep(time.Millisecond)
	}
}
