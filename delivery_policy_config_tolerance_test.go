package panewire

import (
	"slices"
	"testing"
)

// An unknown delivery key is ignored and recorded for the startup warning; the
// policy still loads with the rest of its rules.
func TestDeliveryPolicyUnknownKeyIgnoredAndRecorded(t *testing.T) {
	p, err := parseDeliveryPolicy([]byte("default: interrupt\nunknown: true"))
	if err != nil {
		t.Fatalf("delivery policy with an unknown key refused: %v", err)
	}
	if !slices.Equal(p.ignoredKeys, []string{"unknown"}) {
		t.Fatalf("ignoredKeys=%v, want [unknown]", p.ignoredKeys)
	}
}
