package fleet

import (
	"context"
	"strings"
	"testing"
)

const (
	freeReceipt = `{"schema":"scopefuel.gate.v1","provider_id":"devin","ok":true,"exit_code":0,"gate_basis":"free-bucket:swe-2-medium"}`
	paidReceipt = `{"schema":"scopefuel.gate.v1","provider_id":"devin","ok":true,"exit_code":0,"gate_basis":"projection"}`
)

// remoteFreeFixture selects a free SWE-2 rung whose account projects past
// reset, on a remote host that holds its own devin account.
func remoteFreeFixture(t *testing.T, gate string) (*App, Spawn, *fakeQuota, *fakePlacementSource) {
	t.Helper()
	rows := []CatalogRow{{Profile: "devin-swe2-medium", Effort: "medium", Grade: "A", Pool: "devin", Gate: "default", ModelID: "swe-2-medium"}}
	pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(91), Projected: ptrF(130)}}
	a, s, q := selFixture(t, "B", rows, pools, []string{"devin"})
	const key = "devin:fedcba9876543210"
	src := a.Source.(*fakePlacementSource)
	src.hosts = []HostView{{Name: "host-b", Connected: true}}
	src.presence = append(src.presence, PresenceRecord{HostID: "presence-b", AccountKey: key, LastSeen: a.Now()})
	src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true}
	a.Config.Hosts = map[string]map[string]any{"host-b": {"presence_id": "presence-b"}}
	a.Config.HostOrder = []string{"host-b"}
	s.Host = "host-b"
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey != key {
			t.Fatalf("judged %q, want the remote account", r.AccountKey)
		}
		return gate, nil
	}
	return a, s, q, src
}

func TestSelectFreeAdmitPlacesRemotelyDespiteProjection(t *testing.T) {
	a, s, _, _ := remoteFreeFixture(t, freeReceipt)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("free admit not placed: %v", err)
	}
	if sel.chosen.tierName != "free" {
		t.Fatalf("tier=%q", sel.chosen.tierName)
	}
	if g := sel.chosen.judgments; len(g) != 1 || g[0].Host != "host-b" || g[0].Basis != "free-bucket:swe-2-medium" {
		t.Fatalf("judgments: %+v", g)
	}
	rec := sel.record()
	if got := candOutcome(rec, "devin-swe2-medium", "medium"); got["outcome"] != "chosen" || got["projected_pct"] != 130.0 {
		t.Fatalf("record: %v", got)
	}
}

func TestSelectExhaustedFreeRungNeedsFreeBasis(t *testing.T) {
	a, s, _, _ := remoteFreeFixture(t, paidReceipt)
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || sel.chosen != nil {
		t.Fatal("a non-free basis must not rescue an exhausted projection")
	}
	if got := candOutcome(sel.record(), "devin-swe2-medium", "medium"); got["outcome"] != "dropped:projected_exhaust" {
		t.Fatalf("record: %v", got)
	}
	// The same basis is fine while the projection stays below the threshold.
	a, s, _, _ = remoteFreeFixture(t, paidReceipt)
	a.Quota.(*fakeQuota).PoolViews["devin"] = PoolView{Status: "ok", Class: "spend", Projected: ptrF(40)}
	if sel, err = a.selectProfile(context.Background(), s); err != nil || sel.chosen == nil {
		t.Fatalf("unexhausted free rung refused: %v", err)
	}
}

func TestSelectMeasuresDifferentRemoteAccountThenAdmits(t *testing.T) {
	a, s, q, src := remoteFreeFixture(t, freeReceipt)
	const key = "devin:fedcba9876543210"
	src.accounts[key] = SharedAccount{Key: key, Observed: true}
	var ssh []string
	a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
		if c.Name != "ssh" {
			return Result{}, false
		}
		ssh = append(ssh, strings.Join(c.Args, " "))
		src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true}
		return Result{}, true
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen == nil {
		t.Fatalf("measured account not admitted: %v", err)
	}
	if len(ssh) != 1 || !strings.Contains(ssh[0], "host-b pacelever refresh 'devin'") {
		t.Fatalf("measurement calls: %v", ssh)
	}
	if g := sel.chosen.judgments; len(g) != 1 || g[0].Measured != "ok" || g[0].Outcome != "admitted" {
		t.Fatalf("judgments: %+v", g)
	}
	if len(q.Calls) != 1 {
		t.Fatalf("gate ran before the measurement landed: %+v", q.Calls)
	}
}

func TestSelectMeasurementFailureRefuses(t *testing.T) {
	for name, res := range map[string]Result{"transport": {Code: 255, Err: "unreachable"}, "timeout": {Code: exitTimeout}, "unpublished": {}} {
		t.Run(name, func(t *testing.T) {
			a, s, q, src := remoteFreeFixture(t, freeReceipt)
			const key = "devin:fedcba9876543210"
			src.accounts[key] = SharedAccount{Key: key, Observed: true}
			a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) { return res, c.Name == "ssh" })
			sel, err := a.selectProfile(context.Background(), s)
			if err == nil || sel.chosen != nil {
				t.Fatal("unmeasurable target admitted")
			}
			if len(q.Calls) != 0 {
				t.Fatalf("gate ran without fresh evidence: %+v", q.Calls)
			}
			c := sel.cands[0]
			wantStatus, wantCode := exitEvictionUnconfirmed, outcomeQuotaUnknown
			if name != "unpublished" {
				wantStatus, wantCode = res.Code, outcomeNoEligibleHost
			}
			if len(c.judgments) != 1 || c.judgments[0].Outcome != "unknown" || c.judgments[0].ExitCode != wantStatus || sel.outcome != wantCode {
				t.Fatalf("judgments: %+v", c.judgments)
			}
			if name == "unpublished" && c.judgments[0].Reason != "measure_not_observed" && c.judgments[0].Reason != "measure_stale" {
				t.Fatalf("reason: %q", c.judgments[0].Reason)
			}
			if name != "unpublished" && (c.judgments[0].Measured != "failed" || c.judgments[0].Reason != "measure_failed") {
				t.Fatalf("judgment: %+v", c.judgments[0])
			}
		})
	}
}

func TestSelectPinnedRefreshMeasuresOnAnyHolder(t *testing.T) {
	a, s, _, src := remoteFreeFixture(t, freeReceipt)
	const key = "devin:fedcba9876543210"
	src.accounts[key] = SharedAccount{Key: key, Observed: true}
	// The host pin decides placement; the local holder of the same account
	// still measures it, as the unrestricted plan does (hk 2647).
	src.presence = append(src.presence, PresenceRecord{HostID: "local", AccountKey: key, LastSeen: a.Now()})
	measured := 0
	a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
		if c.Name == "ssh" {
			t.Fatal("pinned refresh skipped the local holder")
		}
		if c.Name == "pacelever" {
			measured++
			src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true}
			return Result{}, true
		}
		return Result{}, false
	})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || measured != 1 || sel.chosen.host != "host-b" {
		t.Fatalf("pinned holder: %v; measurements=%d", err, measured)
	}
}

// wrapRunner lets a test answer some commands itself and defer the rest.
func wrapRunner(next Runner, intercept func(Command) (Result, bool)) Runner {
	return runnerFunc(func(ctx context.Context, c Command) Result {
		if r, ok := intercept(c); ok {
			return r
		}
		return next.Run(ctx, c)
	})
}

type runnerFunc func(context.Context, Command) Result

func (f runnerFunc) Run(ctx context.Context, c Command) Result { return f(ctx, c) }
