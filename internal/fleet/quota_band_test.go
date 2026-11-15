package fleet

import (
	"testing"
	"time"
)

// QuotaView is the reduction the selection order ranks on, and Band names the
// pool-order band the fleet status snapshot publishes. Together they let the
// hub label a stored reading without running a selection.
func TestQuotaViewAndBandClassifyPools(t *testing.T) {
	t.Setenv("PANEWIRE_POOL_POLICY", "")
	policy, err := LoadPoolPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	window := func(name string, pct *float64, resets time.Time) QuotaWindow {
		return QuotaWindow{Window: name, UsedPct: pct, ResetsAt: resets.Format(time.RFC3339)}
	}

	cases := []struct {
		name   string
		pool   string
		view   PoolView
		window []QuotaWindow
		band   string
	}{
		{name: "fresh short window ranked", pool: "claude", window: []QuotaWindow{window("5h", new(float64(10)), now.Add(3*time.Hour))}, band: BandRanked},
		{name: "leftover at the long reset is waste", pool: "claude", window: []QuotaWindow{window("7d", new(float64(10)), now.Add(48*time.Hour))}, band: BandWaste},
		{name: "a full long window stays ranked", pool: "claude", window: []QuotaWindow{window("7d", new(float64(70)), now.Add(84*time.Hour))}, band: BandRanked},
		{name: "credit pool", pool: "anthropic-api", view: PoolView{Class: "spend"}, band: BandCredit},
		{name: "free set", pool: "devin", view: PoolView{Class: "spend"}, band: BandFree},
		{name: "excluded pool", pool: "codex", view: PoolView{Class: "exclude"}, band: BandExcluded},
	}
	for _, c := range cases {
		view := c.view
		if c.window != nil {
			view = QuotaView(c.window, now)
		}
		if got := policy.Band(c.pool, view); got != c.band {
			t.Errorf("%s: band %q, want %q (view %+v)", c.name, got, c.band, view)
		}
	}
}

// QuotaView paces its projection at the reading's measurement time, the same
// rule CLIQuota.Pools applies, and takes the longest account window.
func TestQuotaViewProjectsAtMeasuredTime(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	view := QuotaView([]QuotaWindow{
		{Window: "5h", UsedPct: new(float64(50)), ResetsAt: now.Add(2 * time.Hour).Format(time.RFC3339)},
		{Window: "7d", UsedPct: new(float64(50)), ResetsAt: now.Add(84 * time.Hour).Format(time.RFC3339)},
	}, now)
	if view.Projected == nil || *view.Projected < 99 || *view.Projected > 101 {
		t.Fatalf("projected %v, want about 100", view.Projected)
	}
	if view.RankProjected == nil || *view.RankProjected < 99 || *view.RankProjected > 101 {
		t.Fatalf("rank projected %v, want about 100", view.RankProjected)
	}
	if view.ShortUsed == nil || *view.ShortUsed != 50 {
		t.Fatalf("short-used %v, want 50", view.ShortUsed)
	}
}
