package panewire

import (
	"errors"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"

	"github.com/mgh3326/panewire/internal/fleet"
)

// Login sightings can reach the notice worker out of time order. The need keeps
// its latest sighting (SightedAt), so an older sighting that lands last cannot
// move the clear boundary back: a clean report computed between the two
// sightings must not clear the need (hk 5778, PR #768 review).
func TestLoginNeedSightedAtKeepsLatestWhenSightingsArriveOutOfOrder(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	hub.oneTimeNotify = func(*sentry.Event) (bool, error) { return false, errors.New("sentry unavailable") }
	older := hub.now().UTC()
	newer := older.Add(2 * oneTimeActionClearCycle)
	// The newer sighting is applied first; the older one lands last.
	for _, at := range []time.Time{newer, older} {
		hub.noteReadinessActions("node-a", readinessWith(at, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginMissing}), at)
	}
	settleOneTimeNotices(t, hub)
	key := oneTimeActionKey{machine: "node-a", kind: "agy", need: oneTimeLoginMissing}
	hub.oneTimeMu.Lock()
	sightedAt := hub.oneTimeActions[key].SightedAt
	hub.oneTimeMu.Unlock()
	if !sightedAt.Equal(newer) {
		t.Fatalf("SightedAt = %v after out-of-order sightings, want the newer %v", sightedAt, newer)
	}
	// Clean reports checked after the older sighting but before the newer one,
	// one full cycle apart, are stale: they must not clear the need.
	clean := func(checked time.Time) {
		t.Helper()
		hub.noteReadinessActions("node-a", readinessWith(checked, "agy", fleet.HarnessReadiness{Installed: true, Login: fleet.ReadinessLoginPresent}), checked)
		settleOneTimeNotices(t, hub)
	}
	clean(older.Add(time.Second))
	clean(older.Add(time.Second + oneTimeActionClearCycle))
	if got := loginNeedsOf(t, hub, "node-a"); len(got) != 1 || got[0] != "agy" {
		t.Fatalf("a stale clean report cleared the need: LoginNeeds=%v want [agy]", got)
	}
}
