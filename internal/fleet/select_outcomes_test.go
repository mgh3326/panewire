package fleet

import (
	"context"
	"fmt"
	"testing"
)

func TestSelectRetainsEveryProfileAccountCandidate(t *testing.T) {
	a, s, q, _ := accountConsultFixture(t)
	s.Role = "consult"
	q.CatalogRows = q.CatalogRows[1:]
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey == testClaudeB {
			return "", fail(3, "quota exhausted")
		}
		if r.AccountKey != testCodexA {
			t.Fatalf("wrong admission account: %+v", r)
		}
		return "pool=codex\n", nil
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.row.Profile != "codex-sol" {
		t.Fatalf("later-row account was lost: %v", err)
	}
	want := map[string]int{
		"opus/high/" + testClaudeA: 1, "opus/high/" + testClaudeB: 1,
		"fable/high/" + testClaudeA: 1, "fable/high/" + testClaudeB: 1,
		"codex-sol/high/" + testCodexA: 1,
	}
	got := map[string]int{}
	for _, c := range sel.cands {
		got[c.row.Profile+"/"+c.row.Effort+"/"+c.accountKey]++
	}
	if len(got) != len(want) {
		t.Fatalf("candidate identities: got %v, want %v", got, want)
	}
	for key, count := range want {
		if got[key] != count {
			t.Fatalf("candidate %s count=%d, want %d", key, got[key], count)
		}
	}
	// Both claude rungs retain their candidate records, but measured account
	// exhaustion is authoritative for both, so the second is not gated again.
	if len(sel.ranked) != 3 || len(q.Calls) != 2 {
		t.Fatalf("account admission must gate exhausted quota once: ranked=%d, gates=%d", len(sel.ranked), len(q.Calls))
	}
	if rows := sel.record()["candidates"].([]any); len(rows) != 5 {
		t.Fatalf("record lost account candidates: %v", rows)
	}
}

func TestSelectBoundAccountOutcomesPrecedeMissingBindings(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
		exit int
	}{
		{"exhausted-plus-no-login", outcomeQuotaExhausted, exitRoleWait},
		{"exhausted-plus-no-capable-host", outcomeQuotaExhausted, exitRoleWait},
		{"unknown-plus-no-login", outcomeQuotaUnknown, exitQuotaUnknown},
		{"exhausted-plus-bound-auth-failure", outcomeAuthUnavailable, exitAuthUnavailable},
		{"exhausted-plus-bound-transport-failure", outcomeNoEligibleHost, exitPlacement},
		{"exhausted-plus-bound-policy-failure", outcomePolicyUnavailable, exitConfig},
		{"no-logins", outcomeAuthUnavailable, exitAuthUnavailable},
		{"no-hosts", outcomeNoEligibleHost, exitPlacement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, q, src := accountConsultFixture(t)
			q.CatalogRows = []CatalogRow{q.CatalogRows[1], q.CatalogRows[3]}
			src.presence = src.presence[:1]
			switch tc.name {
			case "unknown-plus-no-login":
				src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA}
			case "no-logins":
				src.presence = nil
			case "no-hosts":
				a.Config.Hosts = nil
				a.Config.HostOrder = nil
			case "exhausted-plus-no-capable-host":
				for name, seat := range a.Policy.Profiles {
					if seat.Canon == "codex-sol" {
						seat.Capability = "codex_only"
						a.Policy.Profiles[name] = seat
					}
				}
			case "exhausted-plus-bound-auth-failure", "exhausted-plus-bound-transport-failure", "exhausted-plus-bound-policy-failure":
				src.presence = append(src.presence, PresenceRecord{HostID: "host-b", AccountKey: testCodexA, LastSeen: a.Now()})
			}
			q.GateDo = func(r QuotaRequest) (string, error) {
				if r.AccountKey != testCodexA {
					t.Fatalf("exhausted or unknown account reached admission: %+v", r)
				}
				status := 6
				if tc.name == "exhausted-plus-bound-transport-failure" {
					status = exitUnreachable
				}
				if tc.name == "exhausted-plus-bound-policy-failure" {
					status = exitConfig
				}
				return "", fail(status, "bound account failure")
			}
			sel, err := a.selectProfile(context.Background(), s)
			if exitCode(err) != tc.exit || sel.outcome != tc.want || sel.chosen != nil {
				t.Fatalf("got %v/%s, want %s/%d", err, sel.outcome, tc.want, tc.exit)
			}
		})
	}
}

func TestSelectRefreshedClassificationKeepsItsOutcome(t *testing.T) {
	for _, reason := range []string{"window_exhausted", "quota_unknown"} {
		t.Run(reason, func(t *testing.T) {
			a, s, q, src := accountConsultFixture(t)
			q.CatalogRows = q.CatalogRows[1:2]
			src.presence = src.presence[1:2]
			calls := 0
			q.GateDo = func(QuotaRequest) (string, error) {
				calls++
				return "", fail(exitEvictionUnconfirmed, "refresh required")
			}
			a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
				if c.Name != "ssh" {
					return Result{}, false
				}
				v := PoolView{Status: "ok", Class: "spend"}
				if reason == "window_exhausted" {
					v.Blocking = ptrF(100)
				}
				src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, Quota: &v}
				return Result{}, true
			})
			sel, err := a.selectProfile(context.Background(), s)
			if err == nil || sel.chosen != nil || calls != 1 {
				t.Fatalf("unexpected admission after refresh: %v; calls=%d", err, calls)
			}
			row := sel.record()["candidates"].([]any)[0].(map[string]any)
			if got := row["outcome"]; got != "dropped:"+reason {
				t.Fatalf("classification became a gate refusal: got %v, want dropped:%s", got, reason)
			}
			if row["reason"] != reason {
				t.Fatalf("classification reason changed: %v", row)
			}
		})
	}
}

func TestSelectAccountTiersFollowPolicyOrder(t *testing.T) {
	rows := []CatalogRow{
		{Profile: "devin-swe2", Effort: "high", Grade: "S", Pool: "devin", Gate: "default", ModelID: "swe-2"},
		{Profile: "sonnet", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
		{Profile: "opus", Effort: "high", Grade: "S", Pool: "anthropic-api", Gate: "default", ModelID: "opus-m"},
		{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"},
	}
	pools := map[string]PoolView{
		"devin":         {Status: "ok", Class: "spend", Blocking: ptrF(5)},
		"claude":        {Status: "ok", Class: "spend", Projected: ptrF(40)},
		"anthropic-api": {Status: "ok", Class: "spend", Blocking: ptrF(20)},
		"codex":         {Status: "ok", Class: "spend", Blocking: ptrF(30)},
	}
	a, s, q := selFixture(t, "S", rows, pools, []string{"devin", "claude", "anthropic-api", "codex"})
	q.GateDo = gatePools(map[string]string{"devin-swe2": "devin"})
	serveRouting(t, routingDocV1)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range sel.ranked {
		got = append(got, c.row.Profile+":"+c.tierName)
	}
	if fmt.Sprint(got) != "[sonnet:waste devin-swe2:free opus:credit codex-sol:ranked]" {
		t.Fatalf("account tier order changed: %v", got)
	}
}
