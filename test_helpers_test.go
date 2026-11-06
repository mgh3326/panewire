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
	if !waitUntil(timeout, condition) {
		t.Fatalf("timed out waiting for %s", label)
	}
}

// waitUntil polls condition every millisecond until it holds or timeout
// passes, and reports whether it held. Use it when the failure message needs
// the state the condition observed; otherwise use eventually.
func waitUntil(timeout time.Duration, condition func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// awaitClientEvent returns the next event of kind from a node client's event
// stream, skipping other kinds, and fails the test once timeout passes.
func awaitClientEvent(t *testing.T, timeout time.Duration, events <-chan hubClientEvent, kind string) hubClientEvent {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case event := <-events:
			if event.Kind == kind {
				return event
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", kind)
		}
	}
}
