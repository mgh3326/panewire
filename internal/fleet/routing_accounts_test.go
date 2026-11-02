package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

const (
	testClaudeA = "claude:00000000000000a1"
	testClaudeB = "claude:00000000000000b2"
	testCodexA  = "codex:00000000000000c3"
)

func exhaustedGatePools(pools map[string]string) func(QuotaRequest) (string, error) {
	gate := gatePools(pools)
	return func(r QuotaRequest) (string, error) {
		out, err := gate(r)
		if err != nil {
			err = fail(3, "quota exhausted")
		}
		return out, err
	}
}

func accountLeadFixture(t *testing.T) (*App, Spawn, *fakeQuota, *fakePlacementSource) {
	t.Helper()
	a, s, q := leadFixture(t)
	s.Role = "builder"
	a.Config.Local["spawn"] = false
	a.Config.Hosts = map[string]map[string]any{"host-a": {}, "host-b": {}}
	a.Config.HostOrder = []string{"host-a", "host-b"}
	src := &fakePlacementSource{
		hosts: []HostView{{Name: "host-a", Connected: true}, {Name: "host-b", Connected: true}},
		presence: []PresenceRecord{
			{HostID: "host-a", AccountKey: testClaudeA, LastSeen: a.Now()},
			{HostID: "host-b", AccountKey: testClaudeB, LastSeen: a.Now()},
			{HostID: "host-b", AccountKey: testCodexA, LastSeen: a.Now()},
		},
		accounts: map[string]SharedAccount{},
	}
	for _, key := range []string{testClaudeA, testClaudeB, testCodexA} {
		v := riskPool(30, 8)
		if key == testClaudeA {
			v = riskPool(120, 8)
		}
		if key == testCodexA {
			v = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(40)}
		}
		src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &v}
	}
	a.Source = src
	return a, s, q, src
}

func TestAccountLeadTriesHealthyAccountOnAnotherHost(t *testing.T) {
	a, s, q, _ := accountLeadFixture(t)
	q.PoolViews["claude"] = riskPool(120, 8)
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey == testClaudeB {
			return "pool=claude\n", nil
		}
		return "", fail(3, "quota exhausted")
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatalf("healthy account B must admit opus on host-b: %v", err)
	}
	if sel.chosen.row.Profile != "opus" || sel.chosen.judgments[len(sel.chosen.judgments)-1].Host != "host-b" {
		t.Fatalf("want opus on host-b, got %+v", sel.chosen)
	}
}

func TestAccountLeadIgnoresTaskGrade(t *testing.T) {
	a, s, q := leadFixture(t)
	s.Role = "builder"
	// The task grade is metadata; it must not promote fable over opus.
	q.CatalogRows[1].Grade = "C"
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.row.Profile != "opus" {
		t.Fatalf("task grade must neither filter nor rank lead profiles: %v, %+v", err, sel)
	}
}

func TestLeadConsultRejectExplicitGrade(t *testing.T) {
	for _, role := range []string{"builder", "counsel"} {
		a, _, _, _ := cmdFixture(t)
		err := a.Run(context.Background(), []string{"spawn", "--role", role, "--grade", "A"})
		if exitCode(err) != exitConfig || !strings.Contains(err.Error(), "lead_consult_take_no_grade") {
			t.Fatalf("%s: explicit grade must name lead_consult_take_no_grade, exit 70: %v", role, err)
		}
	}
}

func TestLeadConsultRejectEmptyGradeFlag(t *testing.T) {
	for _, args := range [][]string{
		{"spawn", "--role", "builder", "--grade="},
		{"spawn", "--role", "counsel", "--grade="},
		{"plan", "--role", "builder", "--t", "T1", "--grade="},
		{"plan", "--role", "counsel", "--t", "T1", "--grade="},
		{"resolve", "--role", "lead", "--grade="},
		{"resolve", "--role", "consult", "--grade="},
	} {
		a, _, _, _ := accountLeadFixture(t)
		err := a.Run(context.Background(), args)
		if exitCode(err) != exitConfig || !strings.Contains(err.Error(), noLeadGradeRule) {
			t.Fatalf("%v: an explicit empty grade must still refuse: %v", args, err)
		}
	}
}

func TestExplicitProfileCannotBypassLeadOrRetirement(t *testing.T) {
	for _, retired := range []bool{false, true} {
		a, s, fr, _ := cmdFixture(t)
		serveRouting(a, routingDocV1)
		q := a.Quota.(*fakeQuota)
		q.CatalogRows = []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"}}
		s.Profile, s.Role, s.OverrideReason = "sonnet", "builder", "test placement override"
		s.Lane, s.Parent = "test-lane", "test-parent"
		if retired {
			at := "2026-01-01T00:00:00Z"
			q.CatalogRows[0].RetiredAt = &at
			s.Role, s.Lane, s.Parent = "worker", "", ""
		}
		err := a.Spawn(context.Background(), s)
		if exitCode(err) != exitSeatRefused {
			t.Fatalf("retired=%v: explicit profile must refuse before placement: %v", retired, err)
		}
		for _, c := range fr.Calls {
			if c.Name == "herdr" {
				t.Fatalf("refused profile reached launch: %+v", c)
			}
		}
	}
}

func TestAccountLeadSharedLoginFallsBackToCodex(t *testing.T) {
	a, s, q, src := accountLeadFixture(t)
	src.presence[1].AccountKey = testClaudeA
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey != testCodexA {
			t.Fatalf("exhausted Claude account reached a gate: %+v", r)
		}
		return "pool=codex\n", nil
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.row.Profile != "codex-sol" {
		t.Fatalf("same exhausted account on two hosts must try codex-sol: %v", err)
	}
	seen := 0
	for _, c := range sel.cands {
		if c.row.Profile == "opus" && c.accountKey == testClaudeA {
			seen++
		}
	}
	if seen != 1 {
		t.Fatalf("one account became %d opus candidates", seen)
	}
	chosen := sel.record()["chosen"].(map[string]any)
	if chosen["account_key"] != testCodexA || chosen["host"] != "host-b" || chosen["outcome_code"] != outcomeAdmitted {
		t.Fatalf("incomplete selection receipt: %v", chosen)
	}
}

func TestAccountLeadExhaustionAndUnknownAreDistinct(t *testing.T) {
	for _, kind := range []string{"exhausted", "unobserved", "model-only"} {
		t.Run(kind, func(t *testing.T) {
			a, s, q, src := accountLeadFixture(t)
			for key, acc := range src.accounts {
				v := riskPool(120, 8)
				acc.Quota = &v
				src.accounts[key] = acc
			}
			if kind != "exhausted" {
				acc := src.accounts[testClaudeA]
				if kind == "unobserved" {
					acc.Observed, acc.Fresh, acc.Quota = false, false, nil
				} else {
					acc.Quota = &PoolView{Status: "ok", Class: "spend"}
				}
				src.accounts[testClaudeA] = acc
			}
			refreshes := 0
			next := a.Runner
			a.Runner = runnerFunc(func(ctx context.Context, c Command) Result {
				if c.Name == "ssh" && strings.Contains(strings.Join(c.Args, " "), "pacelever refresh") {
					refreshes++
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("account refresh has no deadline")
					}
					return Result{}
				}
				return next.Run(ctx, c)
			})
			q.GateDo = func(QuotaRequest) (string, error) { t.Fatal("unusable quota reached a launch gate"); return "", nil }
			sel, err := a.selectProfile(context.Background(), s)
			want, exit := outcomeQuotaExhausted, exitRoleWait
			if kind != "exhausted" {
				want, exit = outcomeQuotaUnknown, exitQuotaUnknown
			}
			if exitCode(err) != exit || sel.chosen != nil || sel.record()["outcome_code"] != want {
				t.Fatalf("want %s/%d, got %v %+v", want, exit, err, sel)
			}
			if kind == "exhausted" && refreshes != 0 || kind != "exhausted" && refreshes != 1 {
				t.Fatalf("account refresh count=%d for %s", refreshes, kind)
			}
		})
	}
}

func TestLeadConsultNativeTopOnly(t *testing.T) {
	for _, role := range []string{"builder", "counsel"} {
		for _, profile := range []string{"agy-opus46", "pi-opus55", "opus"} {
			t.Run(role+"/"+profile, func(t *testing.T) {
				a, s, q := leadFixture(t)
				s.Role = role
				block := strings.Replace(routingBlockV1, `["opus", "fable", "codex-sol", "codex-astra"]`, `["*"]`, 1)
				serveRouting(a, routingDoc(block))
				q.CatalogRows = []CatalogRow{{Profile: profile, Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "anthropic-model"}}
				for name, seat := range a.Policy.Profiles {
					if seat.Canon == profile {
						seat.Kind, seat.Capability, seat.Roles = "agy", "standard", []string{"builder", "counsel", "worker"}
						a.Policy.Profiles[name] = seat
					}
				}
				sel, err := a.selectProfile(context.Background(), s)
				if exitCode(err) != exitSeatRefused || sel.chosen != nil || sel.outcome != outcomeNoRoleCandidate {
					t.Fatalf("non-native top refused incorrectly: %v", err)
				}
			})
		}
	}
}

func TestLeadConsultResolveAndPlanWithoutGrade(t *testing.T) {
	for _, role := range []string{"lead", "consult"} {
		a, _, _ := leadFixture(t)
		if err := a.resolveCmd(context.Background(), []string{"--role", role, "--json"}); err != nil {
			t.Fatal(err)
		}
		var result resolveResult
		if err := json.Unmarshal([]byte(a.Out.(interface{ String() string }).String()), &result); err != nil {
			t.Fatal(err)
		}
		if result.RoutingRole != role || result.Profile != "opus" || result.Grade != "" || result.AccountKey == "" || result.Host == "" || result.OutcomeCode != outcomeAdmitted {
			t.Fatalf("no-grade %s receipt: %+v", role, result)
		}
	}
	for _, role := range []string{"builder", "counsel"} {
		a, q, _, _, _ := planFixture(t)
		q.CatalogRows = []CatalogRow{{Profile: "opus", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "opus-m"}}
		q.GateDo = gatePools(map[string]string{"opus": "claude"})
		serveRouting(a, routingDocV1)
		if err := a.planCmd(context.Background(), []string{"--role", role, "--t", "T1", "--json"}); err != nil {
			t.Fatalf("plan %s without grade: %v", role, err)
		}
	}
}

func TestAccountHostPinDoesNotClaimFleetExhaustion(t *testing.T) {
	a, s, _, _ := accountLeadFixture(t)
	s.Host = "host-a"
	_, err := a.selectProfile(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "--host host-a restricts accounts") || !strings.Contains(err.Error(), "not fleet exhaustion") {
		t.Fatalf("pinned-host refusal: %v", err)
	}
}

func TestDelegateCannotBypassRoleEligibility(t *testing.T) {
	rows := []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "sonnet-m"}}
	e := newDelegateEnvWith(t, rows, map[string]string{"sonnet": "claude"}, map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(10)}})
	e.routingBody = routingDocV1
	err := e.a.delegate(context.Background(), delegateArgs(e, "--role", "verifier", "--headless"))
	if exitCode(err) != exitSeatRefused || !strings.Contains(err.Error(), outcomeNoRoleCandidate) || len(e.a.JobStore.(*fakeJobStore).records) != 0 {
		t.Fatalf("ineligible delegate reached a launch: %v", err)
	}
}

func TestLaunchKeepsSelectedAccountAndJSONReceipt(t *testing.T) {
	a, s, q, src := accountLeadFixture(t)
	a.Config.Local["spawn"] = true
	src.presence = append(src.presence, PresenceRecord{HostID: "local", AccountKey: testClaudeB, LastSeen: a.Now()})
	s.Role, s.RoutingRole, s.AccountKey = "worker", "lead", testClaudeB
	q.LaunchValue = Launch{Profile: "opus", ModelID: "opus-m", Effort: "high", Grade: "C", Gate: "default", Pool: "claude"}
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey != testClaudeB {
			t.Fatalf("selected account was lost at launch: %+v", r)
		}
		return `{"schema":"scopefuel.gate.v1","provider_id":"claude","profile":"opus","account_key":"` + testClaudeB + `","ok":true,"exit_code":0,"requested_by":"test-caller"}`, nil
	}
	_, receipt, err := a.admitLaunch(context.Background(), s, a.Policy.Profiles["opus"], "high", "high", "S+", "", true)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := admissionGate("opus", s.AccountKey, "", receipt)
	if err != nil || lease.Pool != "claude" || lease.Audit["requested_by"] != "test-caller" {
		t.Fatalf("JSON receipt cannot supply the local lease: %v %+v", err, lease)
	}
}

func TestLeadConsultSpawnWithoutTaskGrade(t *testing.T) {
	for _, role := range []string{"builder", "counsel"} {
		a, s, fr, _ := cmdFixture(t)
		s.Profile, s.Role, s.DryRun = "auto", role, true
		if role == "builder" {
			s.Lane, s.Parent = "test-lane", "test-parent"
		}
		q := a.Quota.(*fakeQuota)
		q.CatalogRows = []CatalogRow{{Profile: "opus", Effort: "high", Grade: "C", Pool: "claude", Gate: "default", ModelID: "opus-m"}}
		q.GateDo = gatePools(map[string]string{"opus": "claude"})
		a.Source = &fakePlacementSource{presence: []PresenceRecord{{HostID: "local", AccountKey: testClaudeA, LastSeen: a.Now()}}, accounts: map[string]SharedAccount{testClaudeA: {Key: testClaudeA, Observed: true, Fresh: true, Quota: &PoolView{Status: "ok", Class: "spend", Blocking: ptrF(10)}}}}
		serveRouting(a, routingDocV1)
		a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
			if c.Name == "handoffkeep" && strings.HasPrefix(strings.Join(c.Args, " "), "tasks show") {
				return Result{Out: `{"id":42,"title":"ungraded request","state":"ready","refs":{}}`}, true
			}
			return Result{}, false
		})
		if err := a.Spawn(context.Background(), s); err != nil {
			t.Fatalf("%s requires no task grade: %v", role, err)
		}
		for _, c := range fr.Calls {
			if c.Name == "herdr" {
				t.Fatal("dry run launched a process")
			}
		}
	}
}

func TestSelectionRefusalCodes(t *testing.T) {
	for _, tc := range []struct {
		kind, outcome string
		exit          int
	}{
		{"auth", outcomeAuthUnavailable, exitAuthUnavailable},
		{"host", outcomeNoEligibleHost, exitPlacement},
		{"role", outcomeNoRoleCandidate, exitSeatRefused},
		{"policy", outcomePolicyUnavailable, exitConfig},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			a, _, q, src := accountLeadFixture(t)
			switch tc.kind {
			case "auth":
				src.presence = nil
			case "host":
				a.Config.Hosts = nil
				a.Config.HostOrder = nil
			case "role":
				q.CatalogRows = q.CatalogRows[:1]
			case "policy":
				serveRouting(a, "")
			}
			err := a.resolveCmd(context.Background(), []string{"--role", "lead", "--json"})
			var result resolveResult
			if parseErr := json.Unmarshal([]byte(a.Out.(interface{ String() string }).String()), &result); parseErr != nil {
				t.Fatal(parseErr)
			}
			if exitCode(err) != tc.exit || result.OutcomeCode != tc.outcome || !strings.Contains(err.Error(), tc.outcome) {
				t.Fatalf("want %s/%d, got %v %+v", tc.outcome, tc.exit, err, result)
			}
		})
	}
}

func TestSharedAccountReadingsRejectExpiredOrInvalidBuckets(t *testing.T) {
	a := testApp(t)
	for _, buckets := range [][]accountBucket{
		nil,
		{{Scope: "account", UsedPct: ptrF(10), Window: "7d", ResetsAt: "2025-12-31T00:00:00Z"}},
		{{Scope: "account", UsedPct: ptrF(101), Window: "7d"}},
		{{Scope: "account", Window: "7d"}},
	} {
		if got := accountPoolView(buckets, a.Now()); got != nil {
			t.Fatalf("unusable shared buckets supplied quota: %+v", got)
		}
	}
	v := accountPoolView([]accountBucket{{Scope: "account", UsedPct: ptrF(40), Window: "7d", ResetsAt: "2026-01-04T12:00:00Z"}}, a.Now())
	if v == nil || v.Projected == nil || *v.Projected != 80 || v.Urgency == nil || v.ResetHours == nil {
		t.Fatalf("account reading was not ranked at its own reset: %+v", v)
	}
}
