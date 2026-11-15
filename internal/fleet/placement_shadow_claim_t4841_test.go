package fleet

import (
	"context"
	"math"
	"strings"
	"testing"
)

// hk 4841: the claim arm charges a claim as a share of the host's own
// free*power (claim_share), not as bridge_cost*100/power points. It is recorded
// as one more arm of placement_shadow; the live path does not read it.

// claimHost is a connected, awake host whose free*power is gross, with bridged
// claims in the bridge window. Its live score is score.
func claimHost(name string, gross, power float64, bridged int, score float64) selHost {
	return selHost{name: name, awake: true, score: ptrF(score), view: &HostView{Connected: true, Signals: PlacementSignals{
		Terms:       &PlacementTerms{CPU: gross / power, Mem: 1, IO: 1, Load: 1, Jobs: 1},
		Power:       ptrF(power),
		BridgedJobs: ptrI(bridged),
	}}}
}

func claimPolicy() shadowPolicy {
	return shadowPolicy{hysteresis: shadowDefaultHysteresis, freeFloor: shadowDefaultFreeFloor, bridgeCost: shadowDefaultBridgeCost, claimShare: shadowDefaultClaimShare}
}

// selNames is the host order of a ranked list, best first.
func selNames(hs []selHost) string {
	var names []string
	for _, h := range hs {
		names = append(names, h.name)
	}
	return strings.Join(names, ",")
}

// mustFixed is shadowFixed without its measure, for the tests here.
func mustFixed(hosts []selHost, pol shadowPolicy) []selHost {
	out, _ := shadowFixed(hosts, shadowReq{}, pol)
	return out
}

// The worked example of the hk 3350 evidence (findings/2026-11-15/host-outcomes-
// vs-power, section 6): one claim on desktop (power 50, free*power 19.86) took
// its live score to -0.14, and the job went to rpi (power 12, free*power 5.08).
// The fixed arm has the live scores and names rpi; the claim arm halves
// desktop's own headroom for its one claim (9.93) and names desktop.
func TestShadowClaimWorkedExample_T4841(t *testing.T) {
	pol := claimPolicy()
	desktop := claimHost("desktop", 19.86, 50, 1, -0.14)
	rpi := claimHost("rpi", 5.08, 12, 0, 5.08)
	if got := pol.claimScore(desktop); math.Abs(*got-9.93) > 1e-9 {
		t.Fatalf("desktop claim score %v, want 9.93", *got)
	}
	for _, order := range [][]selHost{{desktop, rpi}, {rpi, desktop}} {
		live := rankHosts(mustFixed(order, pol))
		claim := rankHosts(shadowClaimed(order, shadowReq{}, pol))
		if live[0].name != "rpi" {
			t.Fatalf("fixed (live) first %s, want rpi: %s", live[0].name, selNames(live))
		}
		if claim[0].name != "desktop" {
			t.Fatalf("claim first %s, want desktop: %s", claim[0].name, selNames(claim))
		}
	}
}

// A decision with no claims is the same under the claim arm as under the live
// rules: with nothing bridged a host's claim score is its free*power, which is
// its live score when the live score carries no claim charge.
func TestShadowClaimNoClaimsMatchesLive_T4841(t *testing.T) {
	pol := claimPolicy()
	hosts := []selHost{
		claimHost("a", 50, 100, 0, 50),
		claimHost("b", 45, 50, 0, 45),
		claimHost("c", 3.6, 12, 0, 3.6),
	}
	live := selNames(rankHosts(hosts))
	if fixed := selNames(rankHosts(mustFixed(hosts, pol))); fixed != live {
		t.Fatalf("no claims: fixed %s, live %s", fixed, live)
	}
	if claim := selNames(rankHosts(shadowClaimed(hosts, shadowReq{}, pol))); claim != live {
		t.Fatalf("no claims: claim %s, live %s", claim, live)
	}
}

// claimFixture is the host-first worker fleet with terms: mac-b has one claim in
// the bridge window, cloud-small none, and the hub's live scores favour mac-b.
// The claim arm names cloud-small.
func claimFixture(t *testing.T) (*App, Spawn, *fakePlacementSource) {
	t.Helper()
	a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 1, "pi": 1})
	for i := range src.hosts {
		switch src.hosts[i].Name {
		case "mac-b":
			sig := sigs(100)
			sig.Terms = &PlacementTerms{CPU: 1, Mem: 1, IO: 1, Load: 1, Jobs: 1}
			sig.BridgedJobs = ptrI(1)
			src.hosts[i].Signals = sig
		case "cloud-small":
			sig := sigs(100)
			sig.Terms = &PlacementTerms{CPU: 1, Mem: 1, IO: 1, Load: 1, Jobs: 1}
			src.hosts[i].Signals = sig
		}
	}
	return a, s, src
}

// The event records both winners: live.first is the host the live path took,
// and arms.claim.first the host the claim arm names.
func TestShadowClaimRecordsBothWinners_T4841(t *testing.T) {
	a, s, _ := claimFixture(t)
	events := shadowEvents(a)
	if _, err := a.selectProfile(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	sh := *shadowEventOf(t, *events).Shadow
	claim, ok := sh.Arms[shadowArmClaim]
	if !ok {
		t.Fatalf("placement_shadow has no %s arm: %v", shadowArmClaim, sh.Arms)
	}
	if sh.Live.First != "mac-b" || claim.First != "cloud-small" || claim.Outcome != shadowDisagree {
		t.Fatalf("live %s, claim %s (%s), want live mac-b and claim cloud-small disagree", sh.Live.First, claim.First, claim.Outcome)
	}
}

// Live placement does not read the claim arm: the same spawn with the shadow on
// (claim arm recorded, disagreeing) and off chooses the same host and records
// the same fleet events other than placement_shadow.
func TestShadowClaimLiveUnchanged_T4841(t *testing.T) {
	aOn, sOn, _ := claimFixture(t)
	eventsOn := shadowEvents(aOn)
	on := liveRecord(t, aOn, sOn, eventsOn)
	if !hasShadow(*eventsOn) {
		t.Fatal("shadow on recorded no placement_shadow event")
	}
	if got := shadowEventOf(t, *eventsOn).Shadow.Arms[shadowArmClaim]; got.Outcome != shadowDisagree {
		t.Fatalf("fixture does not exercise the claim arm: outcome %s", got.Outcome)
	}

	aOff, sOff, _ := claimFixture(t)
	aOff.Config.Placement = map[string]any{"shadow": map[string]any{"enabled": false}}
	eventsOff := shadowEvents(aOff)
	off := liveRecord(t, aOff, sOff, eventsOff)
	if hasShadow(*eventsOff) {
		t.Fatal("shadow off still recorded an event")
	}
	if on != off {
		t.Fatalf("live path differs with the claim arm on:\n on: %s\noff: %s", on, off)
	}
}

// claim_share is one key of [placement.shadow]: default 0.5, below 1.
func TestShadowClaimShareConfig_T4841(t *testing.T) {
	a, _, _ := claimFixture(t)
	if got := a.shadowPolicy().claimShare; got != shadowDefaultClaimShare {
		t.Fatalf("default claim_share %v, want %v", got, shadowDefaultClaimShare)
	}
	a.Config.Placement = map[string]any{"shadow": map[string]any{"claim_share": 0.25}}
	if got := a.shadowPolicy().claimShare; got != 0.25 {
		t.Fatalf("claim_share 0.25 read as %v", got)
	}
	if err := validShadowPolicy(map[string]any{"shadow": map[string]any{"claim_share": 1.0}}); err == nil {
		t.Fatal("claim_share 1 accepted; one claim would remove the whole host")
	}
	if err := validShadowPolicy(map[string]any{"shadow": map[string]any{"claim_share": 0.25}}); err != nil {
		t.Fatalf("claim_share 0.25 rejected: %v", err)
	}
}
