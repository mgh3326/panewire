package arbiter

import (
	"context"
	"testing"
	"time"
)

// An unverified job is skipped before its identity and status are judged, so an
// unknown status on its own label does not refuse the pass. Without the skip
// ahead of the status check, GCWithJobs returns ExitStorage.
func TestGCWithJobsUnverifiedJobSkipsBeforeUnknownStatusRefusal(t *testing.T) {
	s, clk := newStore(t)
	quotaJob(t, s, "unverified", "unverified-label")
	clk.advance(11 * time.Minute)
	r, e := s.GCWithJobs(ctx, func(context.Context, []string) (AgentSnapshot, error) {
		return AgentSnapshot{
			Agents:     []Agent{{Status: "unknown", Name: "unverified-label"}},
			Unverified: map[string]string{"unverified": "unnamed herdr row on the job's pane"},
		}, nil
	})
	if e != nil {
		t.Fatalf("unknown status on an unverified job's label refused the pass: %v", e)
	}
	if len(r.Skipped) != 1 || r.Skipped[0].Job != "unverified" {
		t.Fatalf("skipped %+v, want the unverified job", r.Skipped)
	}
	if pools, _ := s.QuotaPools(ctx, "unverified"); len(pools) != 1 {
		t.Fatal("unverified job lost its quota record")
	}
}
