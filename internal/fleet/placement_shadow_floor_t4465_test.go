package fleet

import "testing"

// hk 4465: shadowDecide mirrors the live decision (placementScoreK.decide in
// the root package): the free floor, the all-below pick by free*power, and a
// hysteresis margin that is real for a negative winner.
func TestShadowDecideMirrorsFreeFloorRules(t *testing.T) {
	pol := shadowPolicy{hysteresis: 1.25, freeFloor: 0.05, bridgeCost: 10}
	type host struct {
		score, free, power float64
	}
	run := func(hs ...host) (string, []selHost) {
		var hosts []selHost
		for i, h := range hs {
			hosts = append(hosts, selHost{name: string(rune('a' + i)), awake: true, score: ptrF(h.score),
				view: &HostView{Signals: PlacementSignals{Power: ptrF(h.power), Terms: &PlacementTerms{CPU: h.free, Mem: 1, IO: 1, Load: 1, Jobs: 1}}}})
		}
		shadowDecide(hosts, shadowReq{}, pol, func(h selHost) (float64, float64, bool) {
			t := h.view.Signals.Terms
			return t.CPU * t.Mem * t.IO * t.Load * t.Jobs, *h.view.Signals.Power, true
		}, false)
		for _, h := range hosts {
			if h.decided {
				return h.name, hosts
			}
		}
		return "", hosts
	}
	cases := []struct {
		name string
		want string
		in   []host
	}{
		// The 2026-11-13 04:00:57Z record: only c has free >= 0.05.
		{"floor skips the winner under it", "c", []host{{-27.9, .021, 100}, {-39.1, .018, 50}, {-164, .218, 12}}},
		{"a below-floor rival with the best score cannot take it", "a", []host{{3, .06, 12}, {90, .049, 100}}},
		{"all below: highest free*power", "b", []host{{30, .04, 50}, {-20, .03, 100}, {0, .0017, 30}}},
		{"negative winner: merely ahead keeps it", "a", []host{{-14, .5, 100}, {-13, .5, 100}}},
		{"negative winner: the margin takes it", "b", []host{{-10, .5, 100}, {-8, .5, 100}}},
		{"typical moment", "a", []host{{40, .5, 100}, {27, .9, 30}}},
	}
	for _, tc := range cases {
		if got, _ := run(tc.in...); got != tc.want {
			t.Errorf("%s: decided %q, want %q", tc.name, got, tc.want)
		}
	}
}
