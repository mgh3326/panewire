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
	h := selHost{name: name, awake: true, over: over, load: ptrF(0.5), score: ptrF(score), view: &HostView{Name: name, Connected: true, Signals: sig}}
	h.below, h.floorValue = floorKey(sig, h.score)
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
	// Every host under the floor: the highest score (free*power less the
	// bridge cost of the decided jobs) first, bucket ignored.
	a := floorHost("a", false, 0.04, 12, 0.48)
	b := floorHost("b", true, 0.03, 100, 3)
	c := floorHost("c", false, 0.001, 30, 0.03)
	if got := names(rankHosts([]selHost{a, c, b})); got != "b,a,c" {
		t.Fatalf("rank=%s, want b (3), a (.48), c (.03)", got)
	}
	// Jobs already decided onto b lower its score, so a burst moves on to a
	// instead of piling onto the host with the best free*power.
	b.score, b.floorValue = ptrF(3-30), 3-30
	if got := names(rankHosts([]selHost{a, c, b})); got != "a,c,b" {
		t.Fatalf("rank=%s: three decided jobs on b must move the next one off it", got)
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
	x, y := mk("x", 0, 0.1, 0.04, 12, 0.48), mk("y", 1, 0.9, 0.03, 100, 3)
	ordered = []*placeCandidate{x, y}
	sortPlaceCandidates(ordered, 0.5, "")
	if ordered[0].name != "y" {
		t.Fatalf("all below the floor: highest score first, got %s", ordered[0].name)
	}
	// Three decided jobs on y: its score drops and x goes first.
	y.score = ptrF(3 - 30)
	ordered = []*placeCandidate{y, x}
	sortPlaceCandidates(ordered, 0.5, "")
	if ordered[0].name != "x" {
		t.Fatalf("a burst must move off the host that carries the decided jobs, got %s", ordered[0].name)
	}
	// A throttled host at free .5 does not displace a usable one.
	t1 := mk("throttled", 0, 0.1, 0.5, 100, -50)
	t1.unusable = true
	u1 := mk("usable", 1, 0.1, 0.04, 100, 4)
	ordered = []*placeCandidate{t1, u1}
	sortPlaceCandidates(ordered, 0.5, "")
	if ordered[0].name != "usable" {
		t.Fatalf("an unusable host displaced the usable one: %s", ordered[0].name)
	}
}

// A host that is asleep has no metrics, so its free reads 1; it must stay behind
// every awake host, below the floor or not.
func TestRankHostsFloorKeepsSleepersBehindAwakeHosts(t *testing.T) {
	below := floorHost("below", false, 0.02, 100, 2)
	sleeper := selHost{name: "sleeper", awake: false, wakeable: true, score: ptrF(100)}
	if got := names(rankHosts([]selHost{sleeper, below})); got != "below,sleeper" {
		t.Fatalf("rank=%s: the sleeper must stay behind the awake below-floor host", got)
	}
	above := floorHost("above", true, 0.5, 100, 50)
	if got := names(rankHosts([]selHost{sleeper, below, above})); got != "above,below,sleeper" {
		t.Fatalf("rank=%s", got)
	}
}

// The hub's usable verdict comes before the floor: a throttled host with free
// 0.5 must not displace a usable host at free 0.04... and with no eligible host
// the order is the ordinary one.
func TestRankHostsFloorRespectsTheHubsUsableVerdict(t *testing.T) {
	usable := floorHost("usable", false, 0.04, 100, 4)
	throttled := floorHost("throttled", false, 0.5, 100, -50)
	throttled.view.Reason, throttled.view.Source = "throttled", "prometheus"
	capped := floorHost("capped", false, 0.5, 100, 50)
	capped.view.Reason = "active_jobs>=max"
	for _, hosts := range [][]selHost{{throttled, usable}, {usable, throttled}} {
		if got := names(rankHosts(hosts)); !strings.HasPrefix(got, "usable") {
			t.Fatalf("rank=%s: an unusable host displaced the usable one", got)
		}
	}
	// With no eligible host the floor changes nothing: a host at its job cap
	// still ranks on score as it did before the floor (the live engine does not
	// read the hub's usable verdict; shadow arm "fixed" does).
	if got := names(rankHosts([]selHost{usable, capped})); got != "capped,usable" {
		t.Fatalf("rank=%s: the floor must not reorder hosts when none is eligible", got)
	}
	// A usable host at the floor still outranks both.
	good := floorHost("good", true, 0.3, 100, 30)
	if got := names(rankHosts([]selHost{throttled, capped, usable, good})); !strings.HasPrefix(got, "good,") {
		t.Fatalf("rank=%s", got)
	}
}
