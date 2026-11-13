package fleet

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

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
	t.Helper()
	src := &fakePlacementSource{}
	a, s, _ := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
		if strings.Contains(args, "_spawn-stdin") {
			*log = append(*log, "launch")
			return Result{Out: "OK pane=w:p1\n"}
		}
		return Result{}
	})
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
