package panewire

import (
	"slices"
	"testing"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4659: the clients learn the design's request states (waiting, pending,
// parked, launching, landed, refused, given_up, cancelled) before any hub
// emits them. hk 4660 emitted given_up, and hk 4736 emits waiting, refused and
// cancelled for start conditions. The rest stay unemitted until their parts,
// and every state the hub emits is one the clients know.
func TestHubSpawnExternalStatesUnchanged(t *testing.T) {
	want := []string{"accepted", "cancelled", "deadline_exceeded", "dispatching", "failed", "given_up", "reconciling", "refused", "retryable", "succeeded", "waiting"}
	// Every status a spawn_operations row can hold today, each also in its
	// grant-required stored form (hub_spawn_grant.go). A read maps the stored
	// value back (spawnLogicalStatus) and the API shows spawnPublicStatus.
	stored := []string{"accepted", "bound", "dispatching", "reconciling", "deadline_exceeded", "succeeded", "failed", "retryable", "given_up", "waiting", "refused", "cancelled"}
	for _, status := range slices.Clone(stored) {
		stored = append(stored, spawnStoredStatus(status, &hubSpawnRequest{GrantRequired: true}))
	}
	seen := map[string]bool{}
	for _, status := range stored {
		seen[spawnPublicStatus(spawnLogicalStatus(status))] = true
	}
	var got []string
	for status := range seen {
		got = append(got, status)
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("hub external states = %v, want %v", got, want)
	}
	for _, status := range got {
		if !fleet.SpawnRequestStateKnown(status) {
			t.Errorf("hub emits %q, which the clients do not know", status)
		}
	}
	for _, status := range []string{"pending", "parked", "launching", "landed"} {
		if seen[status] {
			t.Errorf("hub emits design state %q before its part", status)
		}
		if !fleet.SpawnRequestStateKnown(status) {
			t.Errorf("clients do not know design state %q", status)
		}
	}
}
