package fleet

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

// hookedSource runs a hook after each host read.
type hookedSource struct {
	loggingSource
	after func()
}

func (s hookedSource) Hosts(ctx context.Context, cwd string) ([]HostView, error) {
	views, err := s.loggingSource.Hosts(ctx, cwd)
	s.after()
	return views, err
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
