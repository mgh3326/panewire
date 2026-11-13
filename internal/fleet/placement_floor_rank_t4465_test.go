package fleet

import (
	"strings"
	"testing"
)

// hk 4465 (operator decision 231): the free floor is an eligibility rule that
// runs before the load bucket. A host under the floor ranks after every host
// at or above it, whatever its bucket; with every host under the floor the
// highest free*power ranks first.

func floorHost(name string, over bool, free, power, score float64) selHost {
	terms := &PlacementTerms{CPU: free, Mem: 1, IO: 1, Load: 1, Jobs: 1}
	sig := PlacementSignals{Terms: terms, Power: ptrF(power), BelowFreeFloor: free < 0.05}
	h := selHost{name: name, awake: true, over: over, load: ptrF(0.5), score: ptrF(score), view: &HostView{Name: name, Signals: sig}}
	h.below, h.freePower = floorKey(sig)
	return h
}

func names(hosts []selHost) string {
	var out []string
	for _, h := range hosts {
		out = append(out, h.name)
	}
	return strings.Join(out, ",")
}

func TestRankHostsFloorOutranksLoadBucket(t *testing.T) {
	// below is under the load threshold (bucket 0) with the best score;
	// above is over it (bucket 2) with free at the floor.
	below := floorHost("below", false, 0.02, 100, 90)
	above := floorHost("above", true, 0.2, 12, -160)
	if got := names(rankHosts([]selHost{below, above})); got != "above,below" {
		t.Fatalf("rank=%s: the below-floor host in the better bucket must lose to the above-floor host", got)
	}
	// Bucket ordering still applies among the hosts at the floor.
	near := floorHost("near", false, 0.3, 100, 5)
	far := floorHost("far", true, 0.9, 100, 80)
	if got := names(rankHosts([]selHost{far, below, near, above})); got != "near,far,above,below" && got != "near,above,far,below" {
		t.Fatalf("rank=%s: bucket order among above-floor hosts, then the below-floor host last", got)
	}
	// The below-floor host stays in the list as a last resort.
	if got := names(rankHosts([]selHost{below, above})); !strings.HasSuffix(got, ",below") {
		t.Fatalf("rank=%s", got)
	}
	// Every host under the floor: highest free*power first, bucket ignored
	// (the 04:00:57Z shape: oci's tiny free times power 30 vs rpi's 0.04*12).
	a := floorHost("a", false, 0.04, 12, 1)
	b := floorHost("b", true, 0.03, 100, -5)
	c := floorHost("c", false, 0.001, 30, 9)
	if got := names(rankHosts([]selHost{a, c, b})); got != "b,a,c" {
		t.Fatalf("rank=%s, want b (3), a (.48), c (.03)", got)
	}
	// A host the hub sent no terms for is never under the floor.
	blind := selHost{name: "blind", awake: true, over: true, load: ptrF(0.9), score: ptrF(0)}
	if got := names(rankHosts([]selHost{below, blind})); got != "blind,below" {
		t.Fatalf("rank=%s", got)
	}
}

func TestSortPlaceCandidatesFloorOutranksLoadBucket(t *testing.T) {
	mk := func(name string, order int, load, free, power, score float64) *placeCandidate {
		sig := PlacementSignals{Terms: &PlacementTerms{CPU: free, Mem: 1, IO: 1, Load: 1, Jobs: 1}, Power: ptrF(power), BelowFreeFloor: free < 0.05}
		return &placeCandidate{name: name, order: order, awake: true, load: ptrF(load), score: ptrF(score), signals: sig, login: loginUnknown}
	}
	// below: load .1 (bucket 1, under the threshold), best score; above: load .9
	// (bucket 3).
	below, above := mk("below", 0, 0.1, 0.02, 100, 90), mk("above", 1, 0.9, 0.2, 12, -160)
	ordered := []*placeCandidate{below, above}
	sortPlaceCandidates(ordered, 0.5, "")
	if ordered[0].name != "above" {
		t.Fatalf("order %s,%s: the floor must rank before the load bucket", ordered[0].name, ordered[1].name)
	}
	x, y := mk("x", 0, 0.1, 0.04, 12, 1), mk("y", 1, 0.9, 0.03, 100, -5)
	ordered = []*placeCandidate{x, y}
	sortPlaceCandidates(ordered, 0.5, "")
	if ordered[0].name != "y" {
		t.Fatalf("all below the floor: highest free*power first, got %s", ordered[0].name)
	}
}
