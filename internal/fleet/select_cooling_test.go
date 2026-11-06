package fleet

import (
	"context"
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
