package fleet

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// threeHostWasteFixture is a three-machine fleet for a grade-B worker: mac-b (lightest),
// cloud-small and cloud-tiny (heaviest), so the ranking is mac-b, cloud-small,
// cloud-tiny. place maps host -> account. readings maps account -> quota; an
// account with no reading is one the live run never observed.
//
// testClaudeA is at waste risk: 40% projected at a reset 8 h away, so 60%
// unused. testClaudeB is in the ranked tier (40% blocking, no waste).
func threeHostWasteFixture(t *testing.T, place map[string]string, readings map[string]PoolView) (*App, Spawn, *fakeQuota) {
	t.Helper()
	a, s, q := selFixture(t, "B", workerRows, map[string]PoolView{"claude": riskPool(40, 8)}, nil)
	serveRouting(t, routingDocV1)
	q.GateDo = gatePools(map[string]string{"sonnet": "claude"})
	a.Config.Local["spawn"] = false
	a.Config.Hosts = map[string]map[string]any{
		"mac-b":       {"tags": []any{"macos"}},
		"cloud-small": {"tags": []any{"linux"}},
		"cloud-tiny":  {"tags": []any{"linux"}},
	}
	a.Config.HostOrder = []string{"mac-b", "cloud-small", "cloud-tiny"}
	load := map[string]float64{"mac-b": 0.1, "cloud-small": 0.3, "cloud-tiny": 0.5}
	src := &fakePlacementSource{accounts: map[string]SharedAccount{}}
	for _, name := range a.Config.HostOrder {
		src.hosts = append(src.hosts, HostView{Name: name, Connected: true, LoadRatio: ptrF(load[name])})
	}
	for host, key := range place {
		src.presence = append(src.presence, PresenceRecord{HostID: host, AccountKey: key, LastSeen: a.Now()})
	}
	for key, v := range readings {
		v := v
		src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &v}
	}
	a.Source = src
	return a, s, q
}

// threeHostReadings gives both accounts a reading: A at waste risk, B ranked.
func threeHostReadings() map[string]PoolView {
	return map[string]PoolView{
		testClaudeA: riskPool(40, 8),
		testClaudeB: {Status: "ok", Class: "spend", Blocking: ptrF(40)},
	}
}

// lookupCountingSource counts account lookups, the path a refresh would take.
type lookupCountingSource struct {
	*fakePlacementSource
	lookups int
}

func (c *lookupCountingSource) SharedAccount(ctx context.Context, key string) (SharedAccount, error) {
	c.lookups++
	return c.fakePlacementSource.SharedAccount(ctx, key)
}

// (a) The live walk takes the ranked account on the top host. The waste-risk
// account sits only on the lowest-ranked host: waste_first picks it, the
// outcome is differs, and the unspent waste account is named.
func TestWasteFirstDiffersOnWasteAccountLowerRanked_T3651(t *testing.T) {
	a, s, _ := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeB, "cloud-tiny": testClaudeA}, threeHostReadings())
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.host != "mac-b" || sel.chosen.accountKey != testClaudeB {
		t.Fatalf("live must choose the ranked account on mac-b: %v %+v", err, sel)
	}
	w := shadowEventOf(t, *events).Shadow.WasteFirst
	if w == nil {
		t.Fatal("placement_shadow carries no waste_first arm")
	}
	if w.Outcome != "differs" {
		t.Fatalf("outcome %q, want differs: %+v", w.Outcome, w)
	}
	if w.Live == nil || w.Live.Host != "mac-b" || w.Live.AccountKey != testClaudeB || w.Live.Pass != 1 || w.Live.Pool != "claude" || w.Live.Profile != "sonnet" {
		t.Fatalf("live choice %+v", w.Live)
	}
	c := w.Choice
	if c == nil || c.Host != "cloud-tiny" || c.AccountKey != testClaudeA || !c.WasteRisk || c.UnusedAtResetPct == nil || *c.UnusedAtResetPct != 60 {
		t.Fatalf("waste_first choice %+v", c)
	}
	if u := w.LeftUnspent; u == nil || u.AccountKey != testClaudeA || u.UnusedAtResetPct == nil || *u.UnusedAtResetPct != 60 {
		t.Fatalf("left_unspent %+v, want the waste account at 60%% unused", u)
	}
}

// (b) The waste-risk account sits on the top host: the live walk spends it and
// the waste-first order agrees.
func TestWasteFirstAgreesOnTopHost_T3651(t *testing.T) {
	a, s, _ := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeA, "cloud-tiny": testClaudeB}, threeHostReadings())
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.accountKey != testClaudeA {
		t.Fatalf("live must choose the waste account on mac-b: %v %+v", err, sel)
	}
	w := shadowEventOf(t, *events).Shadow.WasteFirst
	if w == nil || w.Outcome != "agree" || w.Choice == nil || w.Choice.AccountKey != testClaudeA || w.Choice.Host != "mac-b" {
		t.Fatalf("want agree on the waste account on mac-b: %+v", w)
	}
	if w.LeftUnspent != nil {
		t.Fatalf("agree names no unspent account: %+v", w.LeftUnspent)
	}
}

// (c) The waste-risk account has no reading, so the live run never observed its
// quota. It is recorded as unknown and never chosen: the waste-first order falls
// to the ranked account that was observed.
func TestWasteFirstNeverChoosesUnobservedAccount_T3651(t *testing.T) {
	readings := map[string]PoolView{testClaudeB: threeHostReadings()[testClaudeB]}
	a, s, _ := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeA, "cloud-tiny": testClaudeB}, readings)
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.accountKey != testClaudeB {
		t.Fatalf("live must fall through to the observed account: %v %+v", err, sel)
	}
	w := shadowEventOf(t, *events).Shadow.WasteFirst
	if w == nil || w.Choice == nil || w.Choice.AccountKey == testClaudeA {
		t.Fatalf("an unobserved account was chosen: %+v", w)
	}
	if w.Outcome != "agree" || w.Choice.AccountKey != testClaudeB {
		t.Fatalf("want agree on the observed account, got %q %+v", w.Outcome, w.Choice)
	}
	if !slices.Contains(w.Unknown, testClaudeA) {
		t.Fatalf("unknown %v does not name %s", w.Unknown, testClaudeA)
	}
}

// (d) The arm makes no quota-gate call, no account lookup (refresh path) and no
// runner call: with the arm on and off the counts are the same.
func TestWasteFirstMakesNoCall_T3651(t *testing.T) {
	count := func(shadowOn bool) (gates, lookups, runs int) {
		a, s, q := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeB, "cloud-tiny": testClaudeA}, threeHostReadings())
		src := &lookupCountingSource{fakePlacementSource: a.Source.(*fakePlacementSource)}
		a.Source = src
		gate := q.GateDo
		q.GateDo = func(r QuotaRequest) (string, error) {
			gates++
			return gate(r)
		}
		if !shadowOn {
			a.Config.Placement = map[string]any{"shadow": map[string]any{"enabled": false}}
		}
		events := shadowEvents(a)
		if _, err := a.selectProfile(context.Background(), s); err != nil {
			t.Fatal(err)
		}
		if hasShadow(*events) != shadowOn {
			t.Fatalf("shadow recorded %v with enabled=%v", hasShadow(*events), shadowOn)
		}
		return gates, src.lookups, len(a.Runner.(*fakeRunner).Calls)
	}
	g1, l1, r1 := count(true)
	g0, l0, r0 := count(false)
	if g1 != g0 || l1 != l0 || r1 != r0 {
		t.Fatalf("calls differ with the arm on: gate %d/%d lookups %d/%d runner %d/%d", g1, g0, l1, l0, r1, r0)
	}
}

// (e) The live decision is the same with the shadow on and off, and a failure in
// the arm leaves every existing field of the event as it was.
func TestWasteFirstLiveUnchanged_T3651(t *testing.T) {
	place := map[string]string{"mac-b": testClaudeB, "cloud-tiny": testClaudeA}
	aOn, sOn, _ := threeHostWasteFixture(t, place, threeHostReadings())
	evOn := shadowEvents(aOn)
	on := liveRecord(t, aOn, sOn, evOn)
	if !hasShadow(*evOn) {
		t.Fatal("shadow on recorded no placement_shadow event")
	}

	aOff, sOff, _ := threeHostWasteFixture(t, place, threeHostReadings())
	aOff.Config.Placement = map[string]any{"shadow": map[string]any{"enabled": false}}
	evOff := shadowEvents(aOff)
	off := liveRecord(t, aOff, sOff, evOff)
	if hasShadow(*evOff) {
		t.Fatal("shadow off still recorded an event")
	}
	if on != off {
		t.Fatalf("live path differs with the shadow on:\n on: %s\noff: %s", on, off)
	}

	aPanic, sPanic, _ := threeHostWasteFixture(t, place, threeHostReadings())
	aPanic.shadowProbe = func(arm string) {
		if arm == shadowArmWasteFirst {
			panic("boom")
		}
	}
	evPanic := shadowEvents(aPanic)
	liveRecord(t, aPanic, sPanic, evPanic)
	base, panicked := shadowEventOf(t, *evOn), shadowEventOf(t, *evPanic)
	if base.Outcome != panicked.Outcome || base.Reason != panicked.Reason || base.Detail != panicked.Detail {
		t.Fatalf("event header differs with the arm failing: %q/%q vs %q/%q", base.Outcome, base.Detail, panicked.Outcome, panicked.Detail)
	}
	strip := func(sh *fleetevent.PlacementShadow) string {
		c := *sh
		c.WasteFirst = nil
		b, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	if strip(base.Shadow) != strip(panicked.Shadow) {
		t.Fatalf("existing shadow fields differ with the arm failing:\n%s\n%s", strip(base.Shadow), strip(panicked.Shadow))
	}
}

// (f) placement.shadow.enabled = false turns the whole event, the waste_first
// arm included, off.
func TestWasteFirstDisabledOmitsArm_T3651(t *testing.T) {
	a, s, _ := threeHostWasteFixture(t, map[string]string{"mac-b": testClaudeB, "cloud-tiny": testClaudeA}, threeHostReadings())
	a.Config.Placement = map[string]any{"shadow": map[string]any{"enabled": false}}
	events := shadowEvents(a)
	if _, err := a.selectProfile(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	for _, e := range *events {
		if e.Shadow != nil || e.Kind == "placement_shadow" {
			t.Fatalf("disabled shadow still wrote %+v", e)
		}
	}
}

// (g) A panic in the arm is shadow_error with the reason, the event outcome is
// the host arms' own, and the spawn proceeds on the live choice.
func TestWasteFirstPanicIsShadowError_T3651(t *testing.T) {
	place := map[string]string{"mac-b": testClaudeB, "cloud-tiny": testClaudeA}
	aRef, sRef, _ := threeHostWasteFixture(t, place, threeHostReadings())
	evRef := shadowEvents(aRef)
	if _, err := aRef.selectProfile(context.Background(), sRef); err != nil {
		t.Fatal(err)
	}
	refOutcome := shadowEventOf(t, *evRef).Outcome

	a, s, _ := threeHostWasteFixture(t, place, threeHostReadings())
	a.shadowProbe = func(arm string) {
		if arm == shadowArmWasteFirst {
			panic("boom in " + arm)
		}
	}
	events := shadowEvents(a)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil || sel.chosen.host != "mac-b" || sel.chosen.accountKey != testClaudeB {
		t.Fatalf("spawn must proceed on the live choice: %v %+v", err, sel)
	}
	ev := shadowEventOf(t, *events)
	w := ev.Shadow.WasteFirst
	if w == nil || w.Outcome != shadowError || !strings.Contains(w.Error, "boom") || w.Choice != nil {
		t.Fatalf("waste_first %+v, want shadow_error with the panic", w)
	}
	if ev.Outcome != refOutcome {
		t.Fatalf("event outcome %q changed by the arm's failure, want %q", ev.Outcome, refOutcome)
	}
}
