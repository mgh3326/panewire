package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// hk 5480 (scope cut of hk 5456): a spawn waits for a shared quota refresh only
// while the request has budget for the measurement. With less than one
// measurement window left, the seat is dropped for the request with
// refresh_unavailable and placement moves to the next candidate.
func TestSelectionSkipsARefreshItCannotWaitFor_5480(t *testing.T) {
	a, s, calls := refreshWaitFixture5480(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := a.selectProfile(ctx, s)
	if len(*calls) != 0 {
		t.Fatalf("selection queued %v of refresh, want none: 20s left cannot cover a %s measurement", *calls, a.measureTimeout())
	}
	if errors.Is(err, ErrQuotaRefreshPending) {
		t.Fatal("selection kept the request pending on a refresh it cannot wait for")
	}
}

// With budget to spare the same account queues exactly one measurement, with
// the window the fleet reads from [quota].measure_timeout_s.
func TestSelectionQueuesARefreshThatFitsTheBudget_5480(t *testing.T) {
	a, s, calls := refreshWaitFixture5480(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_, err := a.selectProfile(ctx, s)
	if !errors.Is(err, ErrQuotaRefreshPending) {
		t.Fatalf("err = %v, want the request left pending on its refresh", err)
	}
	if len(*calls) != 1 || (*calls)[0] != a.measureTimeout() {
		t.Fatalf("queued %v, want one refresh with a %s budget", *calls, a.measureTimeout())
	}
}

// The dropped pool is named refresh_unavailable and the next candidate lands in
// the same request.
func TestSelectionDropsThePoolAndTakesTheNextCandidate_5480(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(50)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)},
	}
	a, s, q := selFixture(t, "B", rows, pools, []string{"codex", "claude"})
	q.GateDo = gatePools(map[string]string{"codex-sol": "codex", "sonnet": "claude"})
	source, ok := a.Source.(*fakePlacementSource)
	if !ok {
		t.Fatalf("unexpected placement source %T", a.Source)
	}
	codexKey := "codex:0000000000000001"
	source.accounts[codexKey] = SharedAccount{Key: codexKey, Observed: true, Fresh: false, MeasuredAt: a.Now().Add(-time.Hour)}
	var calls []time.Duration
	a.ScheduleQuotaRefresh = func(machine, pool string, timeout time.Duration) bool {
		calls = append(calls, timeout)
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sel, err := a.selectProfile(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Fatalf("queued %v of refresh with 20s left, want none", calls)
	}
	if sel.chosen == nil || sel.chosen.row.Pool != "claude" {
		t.Fatalf("chosen = %+v, want the claude seat", sel.chosen)
	}
	got := ""
	for _, c := range sel.cands {
		if c.row.Profile == "codex-sol" {
			got = c.outcome
		}
	}
	if got != "dropped:refresh_unavailable" {
		t.Fatalf("codex outcome = %q, want dropped:refresh_unavailable", got)
	}
}

// refreshWaitFixture5480 is one stale codex account on one host, with the
// refresh calls selection queues recorded.
func refreshWaitFixture5480(t *testing.T) (*App, Spawn, *[]time.Duration) {
	t.Helper()
	rows := []CatalogRow{{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"}}
	a, s, q := selFixture(t, "B", rows, nil, []string{"codex"})
	q.GateDo = gatePools(map[string]string{"codex-sol": "codex"})
	source, ok := a.Source.(*fakePlacementSource)
	if !ok {
		t.Fatalf("unexpected placement source %T", a.Source)
	}
	key := "codex:0000000000000001"
	source.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: false, MeasuredAt: a.Now().Add(-time.Hour)}
	var calls []time.Duration
	a.ScheduleQuotaRefresh = func(machine, pool string, timeout time.Duration) bool {
		calls = append(calls, timeout)
		return true
	}
	return a, s, &calls
}

// spawn status prints the tried list of a request that ended without a
// candidate, so the requester reads what was asked and why it was dropped.
func TestSpawnStatusPrintsTheTriedList_5480(t *testing.T) {
	record := SpawnStatusRecord{
		Request: "req-1",
		State:   "given_up",
		Reason:  "expired: no candidate could take it",
		Tried: []SpawnStatusTried{
			{Pool: "codex", Host: "oci", Reason: "refresh_unavailable"},
			{Host: "mac-dev", Reason: "quota_exhausted"},
		},
	}
	line := record.line()
	for _, want := range []string{"tried=[", "codex/oci: refresh_unavailable", "mac-dev: quota_exhausted"} {
		if !strings.Contains(line, want) {
			t.Fatalf("status line %q does not carry %q", line, want)
		}
	}
}
