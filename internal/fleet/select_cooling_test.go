package fleet

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestSelectPoolCoolingUntilReset(t *testing.T) {
	rows := riskRows()
	a, s, q := selFixture(t, "A", rows, map[string]PoolView{"claude": riskPool(20, 100)}, []string{"devin", "claude"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude"})
	now := a.Now()
	clock := now
	a.Now = func() time.Time { return clock }
	a.State = &fakeStateReader{snap: FleetSnapshot{Cooling: map[string]time.Time{"devin-free": now.Add(time.Hour)}}}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	row := candOutcome(sel.record(), "devin-swe2", "high")
	if row["outcome"] != "dropped:pool_cooling" {
		t.Fatalf("cooling outcome: %v", row)
	}
	clock = now.Add(time.Hour)
	sel, err = a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	row = candOutcome(sel.record(), "devin-swe2", "high")
	if row["outcome"] != "chosen" && row["outcome"] != "below_choice" {
		t.Fatalf("pool not offered at reset: %v", row)
	}
}

func TestSelectRecordsUnavailableCoolingState(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state FleetStateReader
		want  string
	}{
		{"disabled", nil, "pool_cooling_hub_state_off"},
		{"unavailable", &fakeStateReader{err: errors.New("offline")}, "pool_cooling_hub_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, q := selFixture(t, "A", riskRows(), map[string]PoolView{"claude": riskPool(20, 100)}, []string{"devin", "claude"})
			a.State = tc.state
			q.GateDo = gatePools(map[string]string{"devin-swe2": "devin", "sonnet": "claude"})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(sel.degraded, tc.want) {
				t.Fatalf("degraded: %v", sel.degraded)
			}
		})
	}
}
