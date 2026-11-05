package panewire

import "testing"

func TestHubContainmentArgs(t *testing.T) {
	if !validHubSpawnArgsForMode([]string{"--containment", "hk:12"}, true) {
		t.Fatal("operator hub spawn must accept containment")
	}
	if validHubSpawnArgs([]string{"--containment", "hk:12"}) {
		t.Fatal("non-operator hub spawn must not accept containment")
	}
}
