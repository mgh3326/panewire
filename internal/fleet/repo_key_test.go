package fleet

import (
	"context"
	"testing"
	"time"
)

// A checkout keeps its remote's spelling (ParkingArcade) while node repo keys
// are lower case by contract; readiness compares keys, not spellings.
func TestReadinessRepoKeyIgnoresCase(t *testing.T) {
	now := time.Date(2026, 11, 5, 9, 0, 0, 0, time.UTC)
	r := readyNode()
	r.Repos = []string{"parkingarcade"}
	state := ReadinessState{Readiness: r}
	if got := state.Drop(ReadinessNeed{Kind: "claude", Repo: "ParkingArcade"}, now); got != "" {
		t.Fatalf("ParkingArcade on a node mapping parkingarcade: drop=%q", got)
	}
	if got := state.Drop(ReadinessNeed{Kind: "claude", Repo: "OtherRepo"}, now); got != ReadyRepoUnmapped {
		t.Fatalf("an unmapped repo: drop=%q", got)
	}
}

// The hub cwd_key must match ^[a-z0-9._-]{1,64}$, so it is the lower-case key.
func TestHubSpawnSendsLowerCaseRepoKey(t *testing.T) {
	src := &fakePlacementSource{hosts: []HostView{{Name: "h1", Connected: true, LoadRatio: ptrF(0.1)}}}
	a, s, _ := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(string) Result { return Result{} })
	hub := &fakeHub{t: t, events: &[]string{}}
	a.Hub = hub
	a.Config.Hosts = map[string]map[string]any{"h1": {"via": "hub"}}
	a.Config.HostOrder = []string{"h1"}
	a.Repository = &Repository{Name: "ParkingArcade", Origin: "https://git.invalid/org/ParkingArcade.git", LocalRoot: a.Home + "/work"}
	if _, _, e := a.placeAuto(context.Background(), s, "brief"); e != nil {
		t.Fatal(e)
	}
	if len(hub.spawns) != 1 || hub.spawns[0].CWDKey != "parkingarcade" {
		t.Fatalf("spawns=%+v", hub.spawns)
	}
	if got := a.placementRepo(context.Background(), s.CWD); got != "parkingarcade" {
		t.Fatalf("placement repo=%q", got)
	}
}
