package fleet

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// hk 5456: a spawn never waits past its budget for a shared quota refresh. A
// request with less budget left than one measurement takes does not start the
// wait at all: the seat is dropped for that request and placement moves on.
func TestSelectionSkipsARefreshItCannotWaitFor_t5456(t *testing.T) {
	a, s, calls := refreshWaitFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := a.selectProfile(ctx, s)
	if len(*calls) != 0 {
		t.Fatalf("selection queued %v of refresh, want none: 20s left cannot cover a %s measurement", *calls, a.measureTimeout())
	}
	if errors.Is(err, ErrQuotaRefreshPending) {
		t.Fatal("selection kept the request pending on a refresh that cannot finish in its budget")
	}
}

// The same account with budget to spare still queues exactly one measurement,
// with the window the fleet reads from [quota].measure_timeout_s.
func TestSelectionQueuesARefreshThatFitsTheBudget_t5456(t *testing.T) {
	a, s, calls := refreshWaitFixture(t)
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

// A key this attempt already waited on, whose attempt finished without making
// the reading usable, is not waited on again: the seat drops instead.
func TestSelectionDoesNotWaitTwiceForOneRefresh_t5456(t *testing.T) {
	a, s, calls := refreshWaitFixture(t)
	a.QuotaRefreshSettled = func(machine, pool string) (string, bool) {
		return "its shared quota refresh finished without a usable reading", machine == "local" && pool == "codex"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_, err := a.selectProfile(ctx, s)
	if len(*calls) != 0 {
		t.Fatalf("queued %v of refresh for a key it already waited on without a usable reading, want none", *calls)
	}
	if errors.Is(err, ErrQuotaRefreshPending) {
		t.Fatal("the request was parked on a second refresh for the same key")
	}
}

// A refresh the hub did not queue (no budget for it) never leaves the request
// pending: selection drops the seat and moves on (hk 5456).
func TestSelectionDoesNotWaitOnAnUnqueuedRefresh_t5456(t *testing.T) {
	a, s, calls := refreshWaitFixture(t)
	a.ScheduleQuotaRefresh = func(machine, pool, accountKey string, timeout time.Duration) bool {
		*calls = append(*calls, timeout)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	_, err := a.selectProfile(ctx, s)
	if len(*calls) != 1 {
		t.Fatalf("selection asked for %v of refresh, want one attempt", *calls)
	}
	if errors.Is(err, ErrQuotaRefreshPending) {
		t.Fatal("selection parked the request on a refresh the hub did not queue")
	}
}

// refreshWaitFixture is one stale codex account on one host, with the refresh
// calls selection queues recorded.
func refreshWaitFixture(t *testing.T) (*App, Spawn, *[]time.Duration) {
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
	a.ScheduleQuotaRefresh = func(machine, pool, accountKey string, timeout time.Duration) bool {
		calls = append(calls, timeout)
		return true
	}
	return a, s, &calls
}

// hk 5456 (wanted 5): a pool at or above its usage cutoff is not selected and
// then waited on for a refresh that can only confirm it. commandcode refuses
// from 90, every other pool from 99, the cutoff the admission gate uses.
func TestSelectionDropsAPoolAtItsCutoff_t5456(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "B", Pool: "codex", Gate: "default", ModelID: "m"},
		{Profile: "sonnet", Effort: "high", Grade: "B", Pool: "claude", Gate: "default", ModelID: "m"},
	}
	pools := map[string]PoolView{
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(99)},
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(55.5)},
	}
	a, s, q := selFixture(t, "B", rows, pools, []string{"codex", "claude"})
	q.GateDo = gatePools(map[string]string{"codex-sol": "codex", "sonnet": "claude"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen == nil || sel.chosen.row.Pool != "claude" {
		t.Fatalf("chosen = %+v, want the pool with room", sel.chosen)
	}
	got := fmt.Sprint(candOutcome(sel.record(), "codex-sol", "high")["outcome"])
	if !strings.HasPrefix(got, "dropped:window_exhausted") {
		t.Fatalf("codex at 99%% dropped as %q, want window_exhausted", got)
	}
}

// The cutoff rule itself: a pool at its cutoff is excluded, the same pooling
// under a 100 cutoff is not, and a pool with room is never excluded.
func TestExhaustReasonAppliesThePoolCutoff_t5456(t *testing.T) {
	policy := PoolPolicy{}
	at99 := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(99)}
	if got := policy.exhaustReason(at99, false, 99); got != "window_exhausted" {
		t.Fatalf("99%% under a 99 cutoff = %q, want window_exhausted", got)
	}
	if got := policy.exhaustReason(at99, false, 100); got != "" {
		t.Fatalf("99%% under a 100 cutoff = %q, want a candidate", got)
	}
	at55 := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(55)}
	if got := policy.exhaustReason(at55, false, 90); got != "" {
		t.Fatalf("55%% under a 90 cutoff = %q, want a candidate", got)
	}
}

// spawn status prints the tried list of a request that ended without a
// candidate, so the requester reads what was asked and why it was dropped.
func TestSpawnStatusPrintsTheTriedList_t5456(t *testing.T) {
	record := SpawnStatusRecord{
		Request: "req-1",
		State:   "given_up",
		Reason:  "expired: no candidate could take it",
		Tried: []SpawnStatusTried{
			{Pool: "codex", Host: "oci", Reason: "refresh finished without a usable reading"},
			{Host: "mac-dev", Reason: "quota_exhausted"},
		},
	}
	line := record.line()
	for _, want := range []string{"tried=[", "codex/oci: refresh finished without a usable reading", "mac-dev: quota_exhausted"} {
		if !strings.Contains(line, want) {
			t.Fatalf("status line %q does not carry %q", line, want)
		}
	}
}
