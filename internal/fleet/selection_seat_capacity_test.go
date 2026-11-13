package fleet

import (
	"context"
	"fmt"
	"testing"
)

// A recent placement lowers its host's score before load metrics catch up.
// Auto selection must then move to a seat held by the host with more free
// capacity, even while the hub still names the first host as its decision.
func TestAutoSelectionMovesToSeatOnHostWithMoreFreeCapacity(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	}
	a, s, q, src := hostFirstFixture(t, "implementer", "B", rows,
		map[string]string{"mac-b": testClaudeB, "cloud-small": testCodexA}, nil)
	src.accounts[testCodexA] = SharedAccount{Key: testCodexA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(40)}}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "codex"})

	claims := 0
	for i := range 6 {
		s.Job = fmt.Sprintf("job-%d", i+1)
		for j := range src.hosts {
			h := &src.hosts[j]
			free, power := 0.01, 100.0
			bridged := 0
			if h.Name == "mac-b" {
				free, power = 0.8, 100
				bridged = claims
			} else if h.Name == "cloud-small" {
				free, power = 0.6, 100
			}
			terms := &PlacementTerms{CPU: free, Mem: 1, IO: 1, Load: 1, Jobs: 1}
			h.Signals = PlacementSignals{Terms: terms, Power: &power, BridgedJobs: &bridged, BelowFreeFloor: free < 0.05}
			h.Score = ptrF(free*power - float64(bridged)*10)
			h.Decided = h.Name == "mac-b"
		}

		sel, err := a.selectProfile(context.Background(), s)
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		want := "sonnet"
		if i >= 3 {
			want = "codex-sol"
		}
		if got := sel.chosen.row.Profile; got != want {
			t.Fatalf("request %d chose %s on %s, want %s after recent-job capacity cost; trace=%+v", i+1, got, sel.chosen.host, want, sel.hosts)
		}
		if sel.chosen.host == "mac-b" {
			claims++
		}
	}
}
