package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// hk 5456: the hub's scheduler admits a measuremet or refuses it. A refused
// demand is never waited on: the seat is dropped for that request and
// placement moves on.
func TestSelectionDoesNotWaitOnARefusedRefresh_t5456(t *testing.T) {
	a, s, calls := refreshWaitFixture(t)
	a.ScheduleQuotaRefresh = func(machine, pool, accountKey string, window time.Duration) (string, QuotaRefreshAdmission) {
		*calls = append(*calls, window)
		return "", QuotaRefreshRefused
	}
	_, err := a.selectProfile(context.Background(), s)
	if len(*calls) != 1 {
		t.Fatalf("selection asked for %v of refresh, want one attempt", *calls)
	}
	if errors.Is(err, ErrQuotaRefreshPending) {
		t.Fatal("selection kept the request pending on a refresh the scheduler refused")
	}
}

// An admitted attempt leaves the request pending on it, with the window the
// fleet reads from [quota].measure_timeout_s, and one ask per demand.
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
	a.QuotaRefreshSettled = func(machine, pool, accountKey string) (string, bool) {
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
	a.ScheduleQuotaRefresh = func(machine, pool, accountKey string, window time.Duration) (string, QuotaRefreshAdmission) {
		calls = append(calls, window)
		return "qr-1", QuotaRefreshAdmitted
	}
	return a, s, &calls
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

// hk 5456 review: a seat whose own refresh measured another account is dropped
// for the request with that reason, and the next candidate is taken.
func TestSelectionMovesPastAnAccountItCannotMeasure_t5456(t *testing.T) {
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
	a.ScheduleQuotaRefresh = func(machine, pool, accountKey string, window time.Duration) (string, QuotaRefreshAdmission) {
		return "", QuotaRefreshRefused
	}
	a.QuotaRefreshSettled = func(machine, pool, accountKey string) (string, bool) {
		if pool != "codex" {
			return "", false
		}
		return "its shared quota refresh measured another account on " + machine + ", so waiting again cannot supply the selected one", true
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen == nil || sel.chosen.row.Pool != "claude" {
		t.Fatalf("chosen = %+v, want the claude seat", sel.chosen)
	}
	var refusal string
	for _, c := range sel.cands {
		if c.row.Profile == "codex-sol" && len(c.judgments) != 0 {
			refusal = c.judgments[0].Reason
		}
	}
	if !strings.Contains(refusal, "another account") {
		t.Fatalf("codex refusal = %q, want the settled account-mismatch reason", refusal)
	}
}
