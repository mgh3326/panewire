package fleet

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// hookedSource runs a hook before each host read answers.
type hookedSource struct {
	loggingSource
	before func()
}

func (s hookedSource) Hosts(ctx context.Context, cwd string) ([]HostView, error) {
	s.before()
	return s.loggingSource.Hosts(ctx, cwd)
}

// loggingSource records the host read in an event log.
type loggingSource struct {
	*fakePlacementSource
	log *[]string
}

func (s loggingSource) Hosts(ctx context.Context, cwd string) ([]HostView, error) {
	*s.log = append(*s.log, "read")
	return s.fakePlacementSource.Hosts(ctx, cwd)
}

func gateFixture(t *testing.T, log *[]string) (*App, Spawn) {
	return gateFixtureSSH(t, log, func(args string) Result {
		if strings.Contains(args, "_spawn-stdin") {
			*log = append(*log, "launch")
			return Result{Out: "OK pane=w:p1\n"}
		}
		return Result{}
	})
}

func gateFixtureSSH(t *testing.T, log *[]string, ssh func(string) Result) (*App, Spawn) {
	t.Helper()
	src := &fakePlacementSource{}
	a, s, _ := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", ssh)
	a.Source = loggingSource{src, log}
	a.Config.Hosts = map[string]map[string]any{"desk-1": {}}
	a.Config.HostOrder = []string{"desk-1"}
	a.PlacementGate = func(context.Context) (func(), error) {
		*log = append(*log, "gate")
		return func() { *log = append(*log, "release") }, nil
	}
	a.PlacementTry = func(host string) { *log = append(*log, "try:"+host) }
	return a, s
}

// hk 4448: the decision holds the gate from the host read until it has told
// the hub its host, and not through the slow launch after it.
func TestPlacementAutoHoldsTheGateFromReadToTry(t *testing.T) {
	var log []string
	a, s := gateFixture(t, &log)
	if handled, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil || !handled {
		t.Fatalf("%v", e)
	}
	want := []string{"gate", "read", "try:desk-1", "release", "launch"}
	if !slices.Equal(log, want) {
		t.Fatalf("events %v, want %v", log, want)
	}
}

// A gate that cannot be taken refuses the placement: it never reads the view
// ungated.
func TestPlacementAutoRefusesWhenTheGateFails(t *testing.T) {
	var log []string
	a, s := gateFixture(t, &log)
	a.PlacementGate = func(context.Context) (func(), error) { return nil, errors.New("deadline") }
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e == nil {
		t.Fatalf("placement went ahead without the gate: %v", log)
	}
	if slices.Contains(log, "read") {
		t.Fatalf("host view read without the gate: %v", log)
	}
}

// A pinned host is a decision too, and counts.
func TestPlacementForcedHostTellsTry(t *testing.T) {
	var log []string
	a, s := gateFixture(t, &log)
	s.Host = "desk-1"
	if handled, _, e := a.place(context.Background(), s, "brief"); e != nil || !handled {
		t.Fatalf("%v", e)
	}
	if !slices.Contains(log, "try:desk-1") {
		t.Fatalf("pinned host not counted: %v", log)
	}
}

// A wake waits for a node for up to wake_wait. The gate is not held through
// it, so no other decision queues behind a sleeping host.
func TestPlacementAutoReleasesTheGateBeforeAWake(t *testing.T) {
	var log []string
	a, s := gateFixture(t, &log)
	src := a.Source.(loggingSource).fakePlacementSource
	src.hosts = []HostView{{Name: "desk-1", Connected: false}}
	a.Config.Hosts = map[string]map[string]any{"desk-1": {"wake": "panewire"}}
	a.Wake = &fakeWaker{onWake: func(host string) {
		log = append(log, "wake:"+host)
		src.hosts[0].Connected = true
	}}
	if handled, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil || !handled {
		t.Fatalf("%v", e)
	}
	wake, release := slices.Index(log, "wake:desk-1"), slices.Index(log, "release")
	if wake < 0 || release < 0 || release > wake {
		t.Fatalf("the gate must be released before the wake: %v", log)
	}
}

// A refusal moves on to the next candidate. That candidate is ranked again,
// under the gate, on fresh hub scores, so it sees the jobs other decisions
// counted since the plan was made.
func TestPlacementAutoReranksAfterARefusal(t *testing.T) {
	var log []string
	a, s := gateFixtureSSH(t, &log, func(args string) Result {
		if !strings.Contains(args, "_spawn-stdin") {
			return Result{}
		}
		if strings.Contains(args, "hot-a") {
			log = append(log, "refused")
			return Result{Code: 3, Err: "full"}
		}
		log = append(log, "launch")
		return Result{Out: "OK pane=w:p2\n"}
	})
	src := a.Source.(loggingSource).fakePlacementSource
	score := func(v float64) *float64 { return &v }
	src.hosts = []HostView{{Name: "hot-a", Connected: true, Score: score(10)}, {Name: "hot-b", Connected: true, Score: score(5)}, {Name: "hot-c", Connected: true, Score: score(1)}}
	a.Config.Hosts = map[string]map[string]any{"hot-a": {}, "hot-b": {}, "hot-c": {}}
	a.Config.HostOrder = []string{"hot-a", "hot-b", "hot-c"}
	reads := 0
	a.Source = hookedSource{loggingSource{src, &log}, func() {
		// Between the plan and the fallback, other decisions counted onto b
		// and a; c is now the best place.
		if reads++; reads == 2 {
			src.hosts[0].Score, src.hosts[1].Score, src.hosts[2].Score = score(-9), score(-5), score(1)
		}
	}}
	if handled, rec, e := a.placeAuto(context.Background(), s, "brief"); e != nil || !handled || rec["chose"] != "hot-c" {
		t.Fatalf("handled=%v err=%v chose=%v log=%v", handled, e, rec["chose"], log)
	}
	want := []string{"gate", "read", "try:hot-a", "release", "refused", "gate", "read", "try:hot-c", "release", "launch"}
	if !slices.Equal(log, want) {
		t.Fatalf("events %v, want %v", log, want)
	}
}

// afterARefusal runs an auto placement over hot-a, hot-b and hot-c in which
// hot-a refuses; change edits the hub's answer between the plan and the
// fallback. It returns the hosts launched on, in order.
func afterARefusal(t *testing.T, initial []HostView, change func(hosts []HostView) []HostView) []string {
	t.Helper()
	var log []string
	a, s := gateFixtureSSH(t, &log, func(args string) Result {
		if !strings.Contains(args, "_spawn-stdin") {
			return Result{}
		}
		for _, host := range []string{"hot-a", "hot-b", "hot-c"} {
			if strings.Contains(args, host) {
				log = append(log, "launch:"+host)
				if host == "hot-a" {
					return Result{Code: 3, Err: "full"}
				}
			}
		}
		return Result{Out: "OK pane=w:p2\n"}
	})
	a.Config.Placement = map[string]any{"load_threshold": 0.8}
	src := a.Source.(loggingSource).fakePlacementSource
	src.hosts = initial
	a.Config.Hosts = map[string]map[string]any{"hot-a": {}, "hot-b": {}, "hot-c": {}}
	a.Config.HostOrder = []string{"hot-a", "hot-b", "hot-c"}
	reads := 0
	a.Source = hookedSource{loggingSource{src, &log}, func() {
		if reads++; reads == 2 {
			src.hosts = change(src.hosts)
		}
	}}
	if handled, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil || !handled {
		t.Fatalf("%v (log %v)", e, log)
	}
	var launched []string
	for _, event := range log {
		if host, ok := strings.CutPrefix(event, "launch:"); ok {
			launched = append(launched, host)
		}
	}
	return launched
}

func hostView(name string, score, load float64) HostView {
	return HostView{Name: name, Connected: true, Score: &score, LoadRatio: &load}
}

// The fallback is chosen from a new plan, so what changed while the first
// launch was refused counts: a host that filled its task slots, one the
// operator stopped accepting, or one whose load fell under the threshold.
func TestPlacementFallbackPlansAgainOnCurrentEligibility(t *testing.T) {
	initial := func() []HostView {
		return []HostView{hostView("hot-a", 10, .1), hostView("hot-b", 5, .1), hostView("hot-c", 1, .1)}
	}
	cases := []struct {
		name   string
		change func(hosts []HostView) []HostView
		want   []string
	}{
		{"nothing changed", func(h []HostView) []HostView { return h }, []string{"hot-a", "hot-b"}},
		{"the next host filled its task slots", func(h []HostView) []HostView {
			return []HostView{h[0], {Name: "hot-b", Connected: true, SlotFull: true}, h[2]}
		}, []string{"hot-a", "hot-c"}},
		{"the next host is no longer accepting", func(h []HostView) []HostView {
			h[1].Reason = "not_accepting"
			return h
		}, []string{"hot-a", "hot-c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := afterARefusal(t, initial(), tc.change); !slices.Equal(got, tc.want) {
				t.Fatalf("launched on %v, want %v", got, tc.want)
			}
		})
	}
	// A host over the threshold at the plan, under it at the fallback, ranks
	// by its new reading.
	t.Run("over then under", func(t *testing.T) {
		over := []HostView{hostView("hot-a", 10, .1), hostView("hot-b", 5, .95), hostView("hot-c", 1, .5)}
		got := afterARefusal(t, over, func(h []HostView) []HostView {
			load := .1
			h[1].LoadRatio = &load
			return h
		})
		if want := []string{"hot-a", "hot-b"}; !slices.Equal(got, want) {
			t.Fatalf("launched on %v, want %v: the host that fell under the threshold stayed in the worst bucket", got, want)
		}
	})
}

func ptr[T any](v T) *T { return &v }

// changingState answers each snapshot read from the next of its snapshots, the
// last one repeating.
type changingState struct {
	snaps []func() FleetSnapshot
	reads int
}

func (c *changingState) Snapshot(context.Context) (FleetSnapshot, error) {
	i := min(c.reads, len(c.snaps)-1)
	c.reads++
	return c.snaps[i](), nil
}

// Through the hub's fleet state, a host reported slot-full is left out of the
// host list (hubHosts). It must stay out of the fallback too, not come back
// as a host the hub did not report; and every decision reads the state again.
func TestPlacementFallbackKeepsSlotFullHostsOutOfTheFleetState(t *testing.T) {
	var log []string
	a, s := gateFixtureSSH(t, &log, func(args string) Result {
		if !strings.Contains(args, "_spawn-stdin") {
			return Result{}
		}
		for _, host := range []string{"hot-a", "hot-b", "hot-c"} {
			if strings.Contains(args, host) {
				log = append(log, "launch:"+host)
				if host == "hot-a" {
					return Result{Code: 3, Err: "full"}
				}
			}
		}
		return Result{Out: "OK pane=w:p2\n"}
	})
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid"}
	a.Config.Hosts = map[string]map[string]any{"hot-a": {}, "hot-b": {}, "hot-c": {}}
	a.Config.HostOrder = []string{"hot-a", "hot-b", "hot-c"}
	snapshot := func(bFull bool) func() FleetSnapshot {
		return func() FleetSnapshot {
			now := a.Now()
			var states []map[string]any
			for i, host := range []string{"hot-a", "hot-b", "hot-c"} {
				m := fsLoad(host, true, float64(10-4*i), 0.1, now.Add(-time.Second))
				m["load"].(map[string]any)["slot_full"] = bFull && host == "hot-b"
				states = append(states, fsPublisher(host, true, now), m)
			}
			return fsSnapshot(t, now, states...)
		}
	}
	reader := &changingState{snaps: []func() FleetSnapshot{snapshot(false), snapshot(true)}}
	a.State = reader
	a.Source = stateSource{a: a, inner: a.Source}
	if handled, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil || !handled {
		t.Fatalf("%v (log %v)", e, log)
	}
	if want := []string{"launch:hot-a", "launch:hot-c"}; !slices.Equal(log, want) {
		t.Fatalf("events %v, want %v (reads %d)", log, want, reader.reads)
	}
	if reader.reads < 2 {
		t.Fatalf("the fallback did not read the state again: %d reads", reader.reads)
	}
}
