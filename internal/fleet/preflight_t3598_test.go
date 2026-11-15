package fleet

import (
	"context"
	"strings"
	"testing"
	"time"
)

// A probe whose SSH session stalls after connecting must not hold the command
// for the runner's per-call limit once per remaining host: preflight is a
// diagnostic that has to answer inside the wait its callers give it (hk 3598).
// The stalled probe reports its checks failed instead of hanging the run.
func TestPreflightCommandBoundStalledProbeT3598(t *testing.T) {
	prior := preflightBudget
	preflightBudget = 300 * time.Millisecond
	t.Cleanup(func() { preflightBudget = prior })

	a, _, _, out := preflightFixture(t)
	a.Runner = runnerFunc(func(ctx context.Context, c Command) Result {
		if c.Name != "ssh" {
			return Result{}
		}
		select {
		case <-ctx.Done():
			return Result{Code: exitTimeout}
		case <-time.After(3 * time.Second):
			return Result{Out: goodRemote().output()}
		}
	})
	start := time.Now()
	e := a.preflightCmd(context.Background(), nil)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("preflight took %s with a stalled probe; the budget is %s", elapsed, preflightBudget)
	}
	if exitCode(e) != exitPreflight {
		t.Fatalf("want exit %d, got %v", exitPreflight, e)
	}
	for _, want := range []string{"review_config:probe_failed_rc124", "version_skew:probe_failed_rc124"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("%s missing from output:\n%s", want, out)
		}
	}
}
