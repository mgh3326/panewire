package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

func shadowEventOf(t *testing.T, events []fleetevent.Event) fleetevent.Event {
	t.Helper()
	var found []fleetevent.Event
	for _, e := range events {
		if e.Kind == "placement_shadow" {
			found = append(found, e)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want one placement_shadow event, got %d in %d events", len(found), len(events))
	}
	return found[0]
}

func rankNames(o []fleetevent.ShadowHostRank) string {
	var names []string
	for _, r := range o {
		names = append(names, r.Host)
	}
	return strings.Join(names, ",")
}

// (a) The live path is byte for byte the same with the shadow on and off, on
// the host-first fixtures, including scenarios where an arm orders hosts
// differently from the live path (so a leak of an arm's order into the live
// path turns this red).
func TestPlacementShadowLiveUnchanged_T3650(t *testing.T) {
	refuseA := func(a *App) {
		q := a.Quota.(*fakeQuota)
		gate := q.GateDo
		q.GateDo = func(r QuotaRequest) (string, error) {
			if r.AccountKey == testClaudeA {
				return "", fail(3, "quota exhausted")
			}
			return gate(r)
		}
	}
	scenarios := []struct {
		name  string
		build func(t *testing.T) (*App, Spawn)
	}{
		{"account only on a busy host", func(t *testing.T) (*App, Spawn) {
			a, s, _, _ := hostFirstFixture(t, "counsel", "A", consultRows,
				map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB}, map[string]float64{"cloud-small": 1.6, "mac-b": 0.2})
			return a, s
		}},
		{"refused accounts skip the host", func(t *testing.T) (*App, Spawn) {
			a, s, _, _ := hostFirstFixture(t, "counsel", "A", consultRows,
				map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB}, map[string]float64{"mac-b": 0.1, "cloud-small": 0.5})
			refuseA(a)
			return a, s
		}},
		{"unity cap", func(t *testing.T) (*App, Spawn) {
			a, s, _, _ := hostFirstFixture(t, "counsel", "A", consultRows,
				map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB}, map[string]float64{"cloud-small": 0.1, "mac-b": 0.5})
			s.Tags = "unity"
			return a, s
		}},
		{"local load5 over threshold, remote busy fraction under it (Defect A)", func(t *testing.T) (*App, Spawn) {
			a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 40, "pi": 30})
			a.Config.Local["spawn"] = true
			a.LoadRatio = func(context.Context) (float64, error) { return 1.4, nil }
			src.hosts[0].LoadRatio = ptrF(0.5)
			src.hosts = append(src.hosts, HostView{Name: a.machine(), Connected: true, LoadRatio: ptrF(0.3), Score: ptrF(300), Signals: sigs(100)})
			src.presence = append(src.presence, PresenceRecord{HostID: a.machine(), AccountKey: testClaudeB, LastSeen: a.Now()})
			return a, s
		}},
		{"challenger inside the hysteresis margin (Defect B)", func(t *testing.T) (*App, Spawn) {
			a, s, _ := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 120, "cloud-tiny": 50, "pi": 40})
			return a, s
		}},
		{"host at its job cap stays a live candidate (usable verdict)", func(t *testing.T) (*App, Spawn) {
			a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 40, "pi": 30})
			src.hosts[0].Reason = "active_jobs>=max"
			return a, s
		}},
		{"hub memory floor drops a host the measured signal admits", func(t *testing.T) (*App, Spawn) {
			a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 90, "cloud-tiny": 1, "pi": 1})
			src.hosts[0].Reason = "not_accepting(memory_pressure)"
			src.hosts[0].Signals = sigs(100)
			src.hosts[0].Extra = HostExtra{MemFreePct: ptrF(25), Pressure: ptrF(0)}
			return a, s
		}},
		{"macOS warn pressure with high swap (memory)", func(t *testing.T) (*App, Spawn) {
			a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 90, "cloud-tiny": 1, "pi": 1})
			src.hosts[0].Signals, src.hosts[1].Signals = sigs(100), sigs(100)
			src.hosts[0].Extra = HostExtra{MemFreePct: ptrF(60), Pressure: ptrF(1), SwapInPerS: ptrF(20000)}
			return a, s
		}},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			aOn, sOn := sc.build(t)
			eventsOn := shadowEvents(aOn)
			on := liveRecord(t, aOn, sOn, eventsOn)
			if len(*eventsOn) == 0 || !hasShadow(*eventsOn) {
				t.Fatalf("shadow on recorded no placement_shadow event")
			}

			aOff, sOff := sc.build(t)
			aOff.Config.Placement = map[string]any{"shadow": map[string]any{"enabled": false}}
			eventsOff := shadowEvents(aOff)
			off := liveRecord(t, aOff, sOff, eventsOff)
			if hasShadow(*eventsOff) {
				t.Fatalf("shadow off still recorded an event")
			}
			if on != off {
				t.Fatalf("live path differs with the shadow on:\n on: %s\noff: %s", on, off)
			}
		})
	}
}

func hasShadow(events []fleetevent.Event) bool {
	for _, e := range events {
		if e.Kind == "placement_shadow" {
			return true
		}
	}
	return false
}

// (b) Defect A: the local host's load5 per core (1.4) and a remote host's busy
// fraction (0.5) were compared with one threshold. The live order puts the
// remote first because the local host is "over"; fixed measures every host by
// its CPU-busy fraction, and the local host (0.3) is not over.
func TestPlacementShadowDefectAUnitMismatch_T3650(t *testing.T) {
	a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 40, "pi": 30})
	a.Config.Local["spawn"] = true
	a.LoadRatio = func(context.Context) (float64, error) { return 1.4, nil }
	src.hosts[0].LoadRatio = ptrF(0.5)
	src.hosts = append(src.hosts, HostView{Name: a.machine(), Connected: true, LoadRatio: ptrF(0.3), Score: ptrF(300), Signals: sigs(100)})
	events := shadowEvents(a)
	if _, err := a.selectProfile(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	ev := shadowEventOf(t, *events)
	sh := ev.Shadow
	if sh.Live.First != "mac-b" {
		t.Fatalf("live first %s, want the remote mac-b (local is over at load5 1.4): %s", sh.Live.First, rankNames(sh.Live.Order))
	}
	fixed := sh.Arms["fixed"]
	if fixed.First != a.machine() || fixed.Outcome != "disagree" || sh.LoadMeasure != "cpu_busy" {
		t.Fatalf("fixed %+v measure %q, want the local host first by CPU busy: %v", fixed, sh.LoadMeasure, sh)
	}
	if ev.Outcome != "disagree" || ev.Kind != "placement_shadow" {
		t.Fatalf("event outcome %q", ev.Outcome)
	}
	// The local host's bucket moved from over (2) to under the threshold (0).
	if b := fixed.Order[0].Bucket; b != 0 {
		t.Fatalf("fixed bucket of the local host = %d, want 0", b)
	}
}

// With no CPU-busy reading anywhere the fixed arm measures load5 per core for
// every host alike: the remote host has none and ranks as unknown, never as a
// busy fraction compared against load5.
func TestPlacementShadowLoadMeasureNeverMixed_T3650(t *testing.T) {
	pol := shadowPolicy{threshold: 1, cpuBusyOver: 1}
	local := selHost{name: "local", local: true, awake: true, local5: ptrF(1.4), load: ptrF(1.4), over: true, score: ptrF(10)}
	remote := selHost{name: "remote", awake: true, load: ptrF(0.5), score: ptrF(20)}
	out, measure := shadowFixed([]selHost{local, remote}, shadowReq{}, pol)
	if measure != "load5_per_core" || out[1].load != nil || out[0].load == nil || !out[0].over || out[1].over {
		t.Fatalf("measure %s, local %+v remote %+v", measure, out[0], out[1])
	}
	if got := rankNames(shadowRank(rankHosts(out))); got != "remote,local" {
		t.Fatalf("order %s: the unknown remote ranks before the over local", got)
	}
}

// (c) Defect B: the hub's hysteresis keeps the policy-order winner while a
// rival scores less than 1.25 times as much. The live fleet-state path lost
// that (Decided is never set) and ranks the rival first; fixed keeps the
// incumbent. A rival that clears the margin takes the decision in both.
func TestPlacementShadowDefectBHysteresis_T3650(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rival        float64
		wantFixed    string
		wantOutcome  string
		wantDecision bool
	}{
		{"inside the margin", 120, "mac-b", "disagree", true},
		{"challenger clears the margin", 130, "cloud-small", "agree", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _ := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": tc.rival, "cloud-tiny": 50, "pi": 40})
			events := shadowEvents(a)
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			sh := shadowEventOf(t, *events).Shadow
			if sh.Live.First != "cloud-small" || sel.chosen.host != "cloud-small" {
				t.Fatalf("live must stay on the score order: first %s chose %s", sh.Live.First, sel.chosen.host)
			}
			if got := sh.Arms["fixed"]; got.First != tc.wantFixed || got.Outcome != tc.wantOutcome {
				t.Fatalf("fixed %+v, want first %s %s", got, tc.wantFixed, tc.wantOutcome)
			}
		})
	}
}

// Defect B also covers the hub's usable verdict: a host at its job cap stays a
// live candidate and is dropped from the fixed order.
func TestPlacementShadowUsableVerdict_T3650(t *testing.T) {
	a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 40, "pi": 30})
	src.hosts[0].Reason = "active_jobs>=max"
	events := shadowEvents(a)
	if _, err := a.selectProfile(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	sh := shadowEventOf(t, *events).Shadow
	if sh.Live.First != "mac-b" || strings.Contains(rankNames(sh.Arms["fixed"].Order), "mac-b") || sh.Arms["fixed"].First != "cloud-small" {
		t.Fatalf("live %s, fixed %s", rankNames(sh.Live.Order), rankNames(sh.Arms["fixed"].Order))
	}
}

// (d) The node reports 60% free by memory_pressure but its pressure level is
// warn and it swaps hard. Live and fixed keep the host first (free 60 is over
// the 30 floor); measured demotes it. The 25%-free / normal-pressure host the
// hub refused is admitted again by measured, a critical one is refused.
func TestPlacementShadowMemorySignal_T3650(t *testing.T) {
	a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 90, "cloud-tiny": 1, "pi": 1})
	src.hosts[0].Signals, src.hosts[1].Signals = sigs(100), sigs(100)
	src.hosts[0].Extra = HostExtra{MemFreePct: ptrF(60), Pressure: ptrF(1), SwapInPerS: ptrF(20000)}
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.host != "mac-b" {
		t.Fatalf("live chose %s: the shadow must not change it", sel.chosen.host)
	}
	ev := shadowEventOf(t, *events)
	sh := ev.Shadow
	if sh.Live.First != "mac-b" || sh.Arms["fixed"].First != "mac-b" || sh.Arms["fixed"].Outcome != "agree" {
		t.Fatalf("live %s fixed %+v", sh.Live.First, sh.Arms["fixed"])
	}
	if m := sh.Arms["measured"]; m.First != "cloud-small" || m.Outcome != "disagree" || ev.Outcome != "disagree" {
		t.Fatalf("measured %+v, want cloud-small demoting mac-b", m)
	}
	var in fleetevent.ShadowInput
	for _, x := range sh.Inputs {
		if x.Host == "mac-b" {
			in = x
		}
	}
	if in.MemorySrc != "pressure" || in.Pressure == nil || *in.Pressure != 1 || in.SwapInPerS == nil || in.MemFreePct == nil {
		t.Fatalf("mac-b inputs %+v", in)
	}
}

func TestPlacementShadowMemoryAdmission_T3650(t *testing.T) {
	pol := shadowPolicy{threshold: 1, cpuBusyOver: 1, hysteresis: 1.25, freeFloor: 0.05, memRefuseBelowPct: 30, pressureRefuseLevel: 2, pressureWarnMem: 0.5,
		memFloor: 0.2, memSpan: 0.2, swapInHalf: 5000, loadKnee: 0.75, loadCap: 4, jobsHalf: 8, bridgeCost: 10}
	mk := func(name, reason string, pressure, free *float64) selHost {
		hv := &HostView{Name: name, Connected: true, Reason: reason, Signals: sigs(100), Extra: HostExtra{Pressure: pressure, MemFreePct: free}}
		return selHost{name: name, awake: true, view: hv, score: ptrF(50), cfg: map[string]any{"tags": []any{"macos"}}}
	}
	// refusedByHub: the hub dropped it on its 30% floor (free 25, pressure normal).
	refusedByHub := mk("hub-refused", "not_accepting(memory_pressure)", ptrF(0), ptrF(25))
	refusedByHub.excluded, refusedByHub.memExcluded, refusedByHub.fit = true, true, "excluded"
	critical := mk("critical", "available", ptrF(2), ptrF(60))
	critical.view.Signals.MemAvailable = ptrF(0.6)
	healthy := mk("healthy", "available", ptrF(0), ptrF(60))
	hosts := []selHost{refusedByHub, critical, healthy}
	// With no memory reading the hub's refusal stands.
	blind := refusedByHub
	blind.name = "blind"
	blind.view = &HostView{Name: "blind", Connected: true, Reason: "not_accepting(memory_pressure)"}
	if got := rankNames(shadowRank(rankHosts(shadowMeasured([]selHost{blind, healthy}, shadowReq{}, pol)))); got != "healthy" {
		t.Fatalf("a hub memory refusal with no replacement reading was lifted: %s", got)
	}
	if got := rankNames(shadowRank(rankHosts(hosts))); got != "critical,healthy" {
		t.Fatalf("live order %s", got)
	}
	if got := rankNames(shadowRank(rankHosts(shadowMeasured(hosts, shadowReq{}, pol)))); got != "hub-refused,healthy" && got != "healthy,hub-refused" {
		t.Fatalf("measured order %s: hub-refused is admitted again, critical is refused", got)
	}
	// A hub refusal for another reason stays a refusal.
	other := refusedByHub
	other.view = &HostView{Name: "other", Connected: true, Reason: "not_accepting,not_accepting(memory_pressure)", Signals: sigs(100)}
	other.memExcluded = onlyMemoryNotAccepting(other.view.Reason)
	if other.memExcluded {
		t.Fatal("an operator not_accepting is not a memory-only refusal")
	}
}

// (e) The capability map replaces power per work class, and power is the
// fallback when a host has no entry.
func TestPlacementShadowCapability_T3650(t *testing.T) {
	build := func(caps map[string]any) fleetevent.PlacementShadow {
		a, s, src := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 90, "cloud-tiny": 1, "pi": 1})
		src.hosts[0].Signals, src.hosts[1].Signals = sigs(100), sigs(100)
		if caps != nil {
			a.Config.Placement = map[string]any{"capability": caps}
		}
		events := shadowEvents(a)
		if _, err := a.selectProfile(context.Background(), s); err != nil {
			t.Fatal(err)
		}
		return *shadowEventOf(t, *events).Shadow
	}
	absent := build(nil)
	if absent.WorkClass != "heavy" || absent.Role != "worker" || absent.Arms["measured"].First != "mac-b" {
		t.Fatalf("absent map: %+v", absent)
	}
	for _, in := range absent.Inputs {
		if in.Capability != nil {
			t.Fatalf("absent map recorded a capability for %s", in.Host)
		}
	}
	present := build(map[string]any{"heavy": map[string]any{"cloud-small": int64(400)}, "light": map[string]any{"mac-b": int64(1000)}})
	if present.Arms["measured"].First != "cloud-small" || present.Live.First != "mac-b" {
		t.Fatalf("heavy capability: measured %s live %s", present.Arms["measured"].First, present.Live.First)
	}
	for _, in := range present.Inputs {
		if (in.Host == "cloud-small") != (in.Capability != nil) {
			t.Fatalf("capability recorded for %s: %v", in.Host, in.Capability)
		}
	}
	// The light class reads its own table.
	pol := shadowPolicy{capability: map[string]map[string]float64{"light": {"x": 7}}}
	h := selHost{name: "x", view: &HostView{Signals: sigs(100)}}
	if c, ok := pol.capabilityOf("light", "x", h); !ok || c != 7 {
		t.Fatalf("light capability %v %v", c, ok)
	}
	if c, ok := pol.capabilityOf("heavy", "x", h); ok || c != 100 {
		t.Fatalf("heavy falls back to power: %v %v", c, ok)
	}
}

func TestPlacementShadowWorkClass_T3650(t *testing.T) {
	for role, want := range map[string]string{"reviewer": "light", "counsel": "light", "consult": "light", "design": "light", "design-lead": "light", "worker": "heavy", "tester": "heavy", "": "heavy", "other": "heavy"} {
		if got := workClassOf(role); got != want {
			t.Errorf("workClassOf(%q) = %s, want %s", role, got, want)
		}
	}
}

// (f) A panicking arm is recorded as shadow_error; the spawn still succeeds
// with the live choice, and the other arm is still recorded.
func TestPlacementShadowArmPanic_T3650(t *testing.T) {
	a, s, _ := workerFleet(t, map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 40, "pi": 30})
	a.shadowProbe = func(arm string) {
		if arm == "measured" {
			panic("boom in " + arm)
		}
	}
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.host != "mac-b" {
		t.Fatalf("spawn must succeed on the live choice: %v %+v", err, sel)
	}
	ev := shadowEventOf(t, *events)
	m := ev.Shadow.Arms["measured"]
	if ev.Outcome != "shadow_error" || m.Outcome != "shadow_error" || !strings.Contains(m.Error, "boom") || len(m.Order) != 0 {
		t.Fatalf("event %q measured %+v", ev.Outcome, m)
	}
	if f := ev.Shadow.Arms["fixed"]; f.Outcome != "agree" {
		t.Fatalf("fixed arm %+v", f)
	}
}

// (g) The event stays bounded however many hosts the fleet has.
func TestPlacementShadowEventBounded_T3650(t *testing.T) {
	a, s, _, src := hostFirstFixture(t, "worker", "B", workerRows, map[string]string{"mac-b": testClaudeB}, nil)
	for i := 0; i < 26; i++ {
		name := fmt.Sprintf("extra-%02d", i)
		a.Config.Hosts[name] = map[string]any{"tags": []any{"linux"}}
		a.Config.HostOrder = append(a.Config.HostOrder, name)
		src.hosts = append(src.hosts, HostView{Name: name, Connected: true, LoadRatio: ptrF(0.1), Score: ptrF(float64(10 + i)), Signals: sigs(100)})
	}
	for i := range src.hosts {
		if src.hosts[i].Score == nil {
			src.hosts[i].Score = ptrF(5)
		}
	}
	events := shadowEvents(a)
	if _, err := a.selectProfile(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	sh := shadowEventOf(t, *events).Shadow
	if len(a.Config.HostOrder) != 30 || !sh.Truncated {
		t.Fatalf("hosts %d truncated %v", len(a.Config.HostOrder), sh.Truncated)
	}
	if len(sh.Live.Order) != fleetevent.MaxCandidates || len(sh.Inputs) > fleetevent.MaxCandidates || len(sh.Arms) > fleetevent.MaxShadowArms {
		t.Fatalf("live %d inputs %d arms %d", len(sh.Live.Order), len(sh.Inputs), len(sh.Arms))
	}
	for name, arm := range sh.Arms {
		if len(arm.Order) > fleetevent.MaxCandidates {
			t.Fatalf("arm %s has %d hosts", name, len(arm.Order))
		}
	}
	line, err := json.Marshal(shadowEventOf(t, *events))
	if err != nil || len(line) > 24<<10 {
		t.Fatalf("event line %d bytes: %v", len(line), err)
	}
}

// The shadow policy keys are validated and the policy defaults to the live
// constants.
func TestPlacementShadowPolicyConfig_T3650(t *testing.T) {
	for _, bad := range []map[string]any{
		{"shadow": "x"},
		{"shadow": map[string]any{"hysteresis": 0.5}},
		{"shadow": map[string]any{"nope": 1.0}},
		{"shadow": map[string]any{"enabled": "yes"}},
		{"capability": map[string]any{"medium": map[string]any{"h": 1.0}}},
		{"capability": map[string]any{"light": map[string]any{"h": -1.0}}},
		{"capability": map[string]any{"light": "x"}},
	} {
		if err := validShadowPolicy(bad); err == nil {
			t.Errorf("accepted %v", bad)
		}
	}
	if err := validShadowPolicy(map[string]any{"shadow": map[string]any{"hysteresis": 1.5, "swap_in_half_per_s": int64(100)}, "capability": map[string]any{"heavy": map[string]any{"mac-b": int64(150)}}}); err != nil {
		t.Fatal(err)
	}
	// The retired keys still load; the new ones are range checked.
	if err := validShadowPolicy(map[string]any{"shadow": map[string]any{"all_loaded_below": 15.0, "claim_cost": 10.0, "free_floor": 0.1, "bridge_cost": 20.0}}); err != nil {
		t.Fatal(err)
	}
	if err := validShadowPolicy(map[string]any{"shadow": map[string]any{"free_floor": 1.5}}); err == nil {
		t.Error("accepted free_floor 1.5")
	}
	a := testApp(t)
	pol := a.shadowPolicy()
	if !pol.enabled || pol.hysteresis != 1.25 || pol.freeFloor != 0.05 || pol.bridgeCost != 10 || pol.memRefuseBelowPct != 30 || pol.cpuBusyOver != pol.threshold || len(pol.capability) != 0 {
		t.Fatalf("defaults %+v", pol)
	}
}

// The shadow-only fields never make a hub reply fail to parse: a value of the
// wrong type reads as absent, and the live fields decode as before.
func TestPlacementShadowParseStaysLenient_T3650(t *testing.T) {
	for _, raw := range []string{
		`{"candidates":[{"machine":"a","pressure":"warn","memory_free_pct":"x","swap_in_per_s":[1],"swap_used_mb":{}}]}`,
		`{"candidates":[{"machine":"a","signals":{"swap_in_linux":1,"cpu_busy_1m":"0.1"}}]}`,
		`{"candidates":[{"machine":"a","pressure":null,"signals":{"cpu_busy_1m":null}}]}`,
	} {
		views, err := parseHubHosts(raw)
		if err != nil || len(views) != 1 || views[0].Name != "a" || views[0].Extra.Pressure != nil || views[0].Extra.CPUBusy1m != nil || views[0].Extra.SwapInLinux {
			t.Fatalf("%s: %v %+v", raw, err, views)
		}
	}
	views, err := parseHubHosts(`{"candidates":[{"machine":"a","load_ratio":0.4,"pressure":1,"swap_in_per_s":9,"signals":{"cpu_busy_1m":0.2,"swap_in_linux":true,"mem_available":0.5}}]}`)
	if err != nil || *views[0].LoadRatio != 0.4 || *views[0].Extra.Pressure != 1 || *views[0].Extra.SwapInPerS != 9 || *views[0].Extra.CPUBusy1m != 0.2 || !views[0].Extra.SwapInLinux || *views[0].Signals.MemAvailable != 0.5 {
		t.Fatalf("%v %+v", err, views)
	}
	var m FleetStateMessage
	if e := json.Unmarshal([]byte(`{"host":"a","kind":"load","load":{"load_ratio":0.1,"usable":"maybe","signals":{"swap_in_linux":"y"}}}`), &m); e != nil || m.Load == nil || m.Load.Usable.v != nil {
		t.Fatalf("%v %+v", e, m)
	}
}

// A NaN capability is refused at load and never reaches the event.
func TestPlacementShadowNaNCapability_T3650(t *testing.T) {
	nan := math.NaN()
	if err := validShadowPolicy(map[string]any{"capability": map[string]any{"heavy": map[string]any{"mac-b": nan}}}); err == nil {
		t.Fatal("NaN capability accepted")
	}
	pol := shadowPolicy{capability: map[string]map[string]float64{"heavy": {"mac-b": nan}}}
	in := pol.shadowInput(selHost{name: "mac-b"}, shadowReq{class: "heavy"}, time.Time{})
	if in.Capability != nil {
		t.Fatalf("NaN reached the event: %v", *in.Capability)
	}
	if _, err := json.Marshal(in); err != nil {
		t.Fatal(err)
	}
}

// A Linux PSI percent (1 or 2 are ordinary values there) is never read as a
// macOS pressure level: the wire carries no OS, so only a host declaring the
// macos capability has its pressure read as a level.
func TestPlacementShadowPressureNeedsMacOS_T3650(t *testing.T) {
	pol := shadowPolicy{pressureRefuseLevel: 2, pressureWarnMem: 0.5, memRefuseBelowPct: 30, memFloor: 0.2, memSpan: 0.2, swapInHalf: 5000}
	mk := func(tags ...any) selHost {
		return selHost{name: "h", awake: true, cfg: map[string]any{"tags": tags}, view: &HostView{Connected: true, Signals: sigs(100), Extra: HostExtra{Pressure: ptrF(2)}}}
	}
	if m := pol.memory(mk("linux")); m.refuse || m.src != "available" {
		t.Fatalf("linux PSI 2 read as critical: %+v", m)
	}
	if m := pol.memory(mk()); m.refuse || m.src != "available" {
		t.Fatalf("ambiguous pressure used: %+v", m)
	}
	if m := pol.memory(mk("macos")); !m.refuse || m.src != "pressure" {
		t.Fatalf("macOS critical not refused: %+v", m)
	}
}

// A host the hub reports slot-full is left out of the fleet-state host list
// (hubHosts), so it has no view; the live order still contains it, and the
// arms apply the hub's verdict and leave it out.
func TestPlacementShadowSlotFullHost_T3650(t *testing.T) {
	hosts := []string{"mac-b", "cloud-small", "cloud-tiny", "pi"}
	fixture := func(t *testing.T, full ...string) (*App, Spawn, *fakeStateReader) {
		a, s, _, src := hostFirstFixture(t, "worker", "B", workerRows,
			map[string]string{"mac-b": testClaudeB, "cloud-small": testClaudeB, "cloud-tiny": testClaudeB, "pi": testClaudeB}, nil)
		a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
		reader := &fakeStateReader{}
		a.State = reader
		a.Source = stateSource{a: a, inner: src}
		reader.snap = slotFullSnapshot(t, a, full...)
		return a, s, reader
	}
	armsExclude := func(t *testing.T, sh *fleetevent.PlacementShadow, host string, excluded bool) {
		t.Helper()
		for _, arm := range []string{"fixed", "measured"} {
			if got := strings.Contains(rankNames(sh.Arms[arm].Order), host); got == excluded {
				t.Fatalf("%s order %s: %s excluded=%v, want %v", arm, rankNames(sh.Arms[arm].Order), host, !got, excluded)
			}
		}
	}
	run := func(t *testing.T, a *App, s Spawn) *fleetevent.PlacementShadow {
		t.Helper()
		events := shadowEvents(a)
		if sel, err := a.selectProfile(context.Background(), s); err != nil || sel == nil {
			t.Fatal(err)
		}
		return shadowEventOf(t, *events).Shadow
	}

	t.Run("one full host", func(t *testing.T) {
		a, s, _ := fixture(t, "mac-b")
		sh := run(t, a, s)
		if !strings.Contains(rankNames(sh.Live.Order), "mac-b") {
			t.Fatalf("live order %s must still list the slot-full host", rankNames(sh.Live.Order))
		}
		armsExclude(t, sh, "mac-b", true)
	})
	t.Run("every host full", func(t *testing.T) {
		a, s, _ := fixture(t, hosts...)
		sh := run(t, a, s)
		for _, h := range hosts {
			armsExclude(t, sh, h, true)
		}
	})
	t.Run("a wakeable full host is not a sleeper fallback", func(t *testing.T) {
		a, s, _ := fixture(t, "mac-b")
		a.Config.Hosts["mac-b"]["wake"] = "panewire"
		sh := run(t, a, s)
		armsExclude(t, sh, "mac-b", true)
	})
	t.Run("the next read forgets the previous one", func(t *testing.T) {
		a, s, reader := fixture(t, "mac-b")
		clock := a.Now()
		a.Now = func() time.Time { return clock }
		reader.snap = slotFullSnapshot(t, a, "mac-b")
		run(t, a, s)
		clock = clock.Add(stateSnapshotMaxAge)
		reader.snap, reader.err = FleetSnapshot{}, fmt.Errorf("hub down")
		sh := run(t, a, s)
		armsExclude(t, sh, "mac-b", false)
	})
}

// slotFullSnapshot is a fleet-state snapshot with every placement host
// connected and the named ones slot-full.
func slotFullSnapshot(t *testing.T, a *App, full ...string) FleetSnapshot {
	t.Helper()
	now := a.Now()
	var states []map[string]any
	for _, host := range []string{"mac-b", "cloud-small", "cloud-tiny", "pi"} {
		m := fsLoad(host, true, 50, 0.1, now.Add(-5*time.Second))
		m["load"].(map[string]any)["slot_full"] = slices.Contains(full, host)
		states = append(states, fsPublisher(host, true, now), m)
	}
	return fsSnapshot(t, now, states...)
}
