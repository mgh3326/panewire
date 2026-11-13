package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type refusalQuota struct {
	*fakeQuota
	gate CLIQuota
}

func (q refusalQuota) Gate(ctx context.Context, req QuotaRequest) (string, error) {
	return q.gate.Gate(ctx, req)
}

func TestRawGateExitThreeUsesCodeInsteadOfProse(t *testing.T) {
	for _, message := range []string{"window_exhausted", "weekly limit reached", "사용량 한도 초과"} {
		t.Run(message, func(t *testing.T) {
			a, s, q, src := accountConsultFixture(t)
			q.CatalogRows = q.CatalogRows[1:2]
			src.presence = src.presence[1:2]
			q.GateDo = func(r QuotaRequest) (string, error) { return "", fail(3, "%s", message) }
			sel, err := a.selectProfile(context.Background(), s)
			if exitCode(err) != exitRoleWait || sel.outcome != outcomeQuotaExhausted {
				t.Fatalf("gate prose changed exhaustion: %v/%s", err, sel.outcome)
			}
		})
	}
}

func TestStructuredGateRefusalsAreIndependentOfMessages(t *testing.T) {
	for _, tc := range []struct {
		name, outcome, reasonCode, message, want string
		exit                                     int
		roleDenied                               bool
	}{
		{"typed exhaustion", outcomeQuotaExhausted, "", "weekly limit reached", outcomeQuotaExhausted, exitRoleWait, false},
		{"reason code", "", "window_exhausted", "사용량 한도 초과", outcomeQuotaExhausted, exitRoleWait, false},
		{"typed policy", outcomeNoRoleCandidate, "", "quota exhausted is not the reason", outcomeNoRoleCandidate, exitSeatRefused, false},
		{"role flag", "", "", "role_restricted: removed astra seat alias", outcomeNoRoleCandidate, exitSeatRefused, true},
		{"ambiguous measured block", "", "", "weekly limit reached", outcomeQuotaUnknown, exitQuotaUnknown, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, q, src := accountConsultFixture(t)
			q.CatalogRows = q.CatalogRows[1:2]
			src.presence = src.presence[1:2]
			calls := 0
			gateRunner := &fakeRunner{Do: func(c Command) Result {
				calls++
				d := map[string]any{"schema": "scopefuel.gate.v1", "provider_id": "claude", "account_key": testClaudeB, "ok": false, "exit_code": 3, "reason": tc.message, "used_pct": 95, "role_denied": tc.roleDenied}
				if tc.outcome != "" {
					d["outcome_code"] = tc.outcome
				}
				if tc.reasonCode != "" {
					d["reason_code"] = tc.reasonCode
				}
				b, _ := json.Marshal(d)
				return Result{Out: string(b), Code: 3}
			}}
			a.Quota = refusalQuota{fakeQuota: q, gate: CLIQuota{Runner: gateRunner, Binary: "fake-quota"}}
			sel, err := a.selectProfile(context.Background(), s)
			if exitCode(err) != tc.exit || sel.outcome != tc.want {
				t.Fatalf("structured refusal misclassified: %v/%s, want %s/%d", err, sel.outcome, tc.want, tc.exit)
			}
			wantCalls := 1
			if tc.want == outcomeQuotaUnknown {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("gate calls=%d want=%d", calls, wantCalls)
			}
			if tc.roleDenied && !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("role reason lost: %v", err)
			}
		})
	}
}

func TestRefreshExitThreeIsNotGateExhaustion(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	q.CatalogRows = q.CatalogRows[1:2]
	src.presence = src.presence[1:2]
	src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB}
	a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
		if c.Name == "ssh" {
			return Result{Code: 3, Err: "quota exhausted"}, true
		}
		return Result{}, false
	})
	sel, err := a.selectProfile(context.Background(), s)
	if exitCode(err) != exitConfig || sel.outcome != outcomePolicyUnavailable {
		t.Fatalf("refresh process exit became measured gate exhaustion: %v/%s", err, sel.outcome)
	}
}

func TestAllHostsRoleDeniedReturnsRoleRestricted(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	q.CatalogRows = []CatalogRow{{Profile: "codex-astra", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "astra"}}
	secondAccount := "codex:00000000000000d4"
	src.presence = []PresenceRecord{{HostID: "host-a", AccountKey: testCodexA, LastSeen: a.Now()}, {HostID: "host-b", AccountKey: secondAccount, LastSeen: a.Now()}}
	src.accounts[secondAccount] = SharedAccount{Key: secondAccount, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: ptrPool(riskPool(30, 8))}
	reason := "role_restricted: removed astra seat alias"
	calls := 0
	q.GateDo = func(r QuotaRequest) (string, error) { calls++; return "", fail(exitSeatRefused, "%s", reason) }
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || !strings.HasPrefix(err.Error(), "role_restricted:") || !strings.Contains(err.Error(), reason) || strings.Contains(err.Error(), "quota_unknown") || exitCode(err) != exitSeatRefused {
		t.Fatalf("role denial was masked: %v (%s)", err, sel.outcome)
	}
	if calls != 2 {
		t.Fatalf("role denied %d accounts, want each host account", calls)
	}
	for _, host := range []string{"host-a", "host-b"} {
		row := hostRow(t, sel, host)
		if row["admission_outcome"] == "quota_unknown" {
			t.Fatalf("role denial was recorded as quota_unknown on %s: %v", host, row)
		}
	}
}

func TestMixedQuotaUnknownAndRoleDenialPrefersUnknownQuota(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	q.CatalogRows = []CatalogRow{{Profile: "codex-astra", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "astra"}}
	secondAccount := "codex:00000000000000d4"
	src.presence = []PresenceRecord{{HostID: "host-a", AccountKey: testCodexA, LastSeen: a.Now()}, {HostID: "host-b", AccountKey: secondAccount, LastSeen: a.Now()}}
	src.accounts[secondAccount] = SharedAccount{Key: secondAccount, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: ptrPool(riskPool(30, 8))}
	denial := "role_restricted: removed astra seat alias"
	calls := 0
	visited := map[string]bool{}
	q.GateDo = func(r QuotaRequest) (string, error) {
		calls++
		visited[r.AccountKey] = true
		if r.AccountKey == testCodexA {
			return "", fail(exitSeatRefused, "%s", denial)
		}
		return "", fail(exitEvictionUnconfirmed, "quota unknown")
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || !strings.Contains(err.Error(), "QUOTA_UNKNOWN") || sel.outcome != outcomeQuotaUnknown || exitCode(err) != exitQuotaUnknown {
		t.Fatalf("mixed denials should report unknown quota: %v (%s)", err, sel.outcome)
	}
	if len(visited) != 2 {
		t.Fatalf("mixed outcome fixture reached accounts %v, want both", visited)
	}
}

func TestRoleDeniedTraceBoundsAndFlattensGateReason(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	q.CatalogRows = []CatalogRow{{Profile: "codex-astra", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "astra"}}
	src.presence = []PresenceRecord{{HostID: "host-a", AccountKey: testCodexA, LastSeen: a.Now()}}
	longReason := "role_restricted: " + strings.Repeat("x", 230) + "\nhost=mac-personal"
	q.GateDo = func(QuotaRequest) (string, error) { return "", fail(exitSeatRefused, "%s", longReason) }
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || exitCode(err) != exitSeatRefused {
		t.Fatalf("role denial not returned: %v", err)
	}
	tried := hostRow(t, sel, "host-a")["tried"].([]any)
	if len(tried) != 1 {
		t.Fatalf("expected one attempted gate row, got %v", tried)
	}
	reason := tried[0].(map[string]any)["reason"].(string)
	if len(reason) > 200 || strings.ContainsAny(reason, "\r\n") || !strings.HasPrefix(reason, "role_restricted:") || !strings.HasSuffix(reason, "host=mac-personal") {
		t.Fatalf("role gate trace is not bounded and flattened: %d bytes %q", len(reason), reason)
	}
}

func TestGateReasonClipBoundIncludesEllipsis(t *testing.T) {
	got := clipGateReason("role_restricted:"+strings.Repeat("x", 250), 200)
	if len(got) > 200 || !strings.HasPrefix(got, "role_restricted:") {
		t.Fatalf("clipped role reason exceeds bound or lost prefix: %d bytes %q", len(got), got)
	}
}

func TestNonRoleNoRoleCandidateDoesNotBeatAuthFailure(t *testing.T) {
	a := testApp(t)
	p := &selPlan{cands: []*selCand{
		{accountKey: "credit:one", code: outcomeNoRoleCandidate, reason: "pool_excluded"},
		{accountKey: "claude:two", code: outcomeAuthUnavailable, reason: "auth_unavailable"},
	}}
	_, err := a.selectionRefused(context.Background(), Spawn{}, selReq{role: "implementer"}, p)
	if exitCode(err) != exitAuthUnavailable || !strings.Contains(err.Error(), "AUTH_UNAVAILABLE") || strings.Contains(err.Error(), "role_restricted") {
		t.Fatalf("pool exclusion masked sign-in refusal: %v", err)
	}
}

func TestRoleRestrictedRefusalKeepsDiagnosticSuffixAndPoolHints(t *testing.T) {
	a := testApp(t)
	p := &selPlan{poolCaps: map[string]int{"codex": 2}, cands: []*selCand{
		{accountKey: "codex:one", code: outcomeNoRoleCandidate, reason: "role_restricted: removed astra seat alias"},
		{accountKey: "codex:two", reason: reasonPoolAtCap, cap: "codex"},
	}}
	_, err := a.selectionRefused(context.Background(), Spawn{Profile: "codex-astra", Host: "host-a"}, selReq{role: "consult"}, p)
	for _, want := range []string{"role_restricted: removed astra seat alias", "routing role consult", "--host host-a restricts accounts", "pool_at_cap: codex=2"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("refusal lost %q: %v", want, err)
		}
	}
}

func TestGateRefusalTruncationKeepsHostNameWhole(t *testing.T) {
	got := clipGateReason(strings.Repeat("x", 185)+" mac-personal quota_unknown", 200)
	if strings.Contains(got, "mac-persona…") || !strings.Contains(got, "mac-personal") {
		t.Fatalf("truncated refusal split host name: %q", got)
	}
}

func TestAllHostsQuotaUnknownKeepsUnknownRefusalAndExit(t *testing.T) {
	a, s, q, src := accountConsultFixture(t)
	q.CatalogRows = []CatalogRow{q.CatalogRows[1]}
	src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true}
	src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true}
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || exitCode(err) != exitQuotaUnknown || sel.outcome != outcomeQuotaUnknown || !strings.Contains(err.Error(), "QUOTA_UNKNOWN") {
		t.Fatalf("unknown quota refusal changed: err=%v outcome=%s", err, sel.outcome)
	}
}

func TestUnreadableGateExitThreeGetsOneRefresh(t *testing.T) {
	bound := `{"schema":"scopefuel.gate.v1","provider_id":"claude","account_key":"` + testClaudeB + `","ok":false,"exit_code":3,"outcome_code":"QUOTA_EXHAUSTED"}`
	admitted := `{"schema":"scopefuel.gate.v1","provider_id":"claude","account_key":"` + testClaudeB + `","ok":true,"exit_code":0}`
	for _, tc := range []struct {
		name, out string
		recovers  bool
	}{
		{"empty", "", false},
		{"prose", "window_exhausted", false},
		{"invalid JSON", `{"schema":`, false},
		{"missing fields", `{}`, false},
		{"wrong schema", strings.ReplaceAll(bound, "scopefuel.gate.v1", "scopefuel.gate.v2"), false},
		{"duplicate field", strings.ReplaceAll(bound, `"ok":false`, `"ok":false,"ok":false`), false},
		{"conflicting codes", strings.ReplaceAll(bound, `"ok":false`, `"ok":false,"role_denied":true`), false},
		{"recovers", "window_exhausted", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, q, src := accountConsultFixture(t)
			q.CatalogRows = q.CatalogRows[1:2]
			// Account A is measured exhausted; B's unreadable gate must
			// stay unknown rather than turn their aggregate into policy failure.
			src.presence = src.presence[:2]
			gates, refreshes := 0, 0
			a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
				if c.Name == "ssh" {
					refreshes++
					return Result{}, true
				}
				return Result{}, false
			})
			gateRunner := &fakeRunner{Do: func(Command) Result {
				gates++
				if tc.recovers && gates > 1 {
					return Result{Out: admitted}
				}
				return Result{Out: tc.out, Code: quotaGateBlocked}
			}}
			a.Quota = refusalQuota{fakeQuota: q, gate: CLIQuota{Runner: gateRunner, Binary: "fake-quota"}}
			sel, err := a.selectProfile(context.Background(), s)
			if tc.recovers {
				if err != nil || sel.chosen.accountKey != testClaudeB || sel.chosen.judgments[len(sel.chosen.judgments)-1].Host != "host-b" {
					t.Fatalf("refreshed valid receipt did not admit account B: %v", err)
				}
			} else if exitCode(err) != exitQuotaUnknown || sel.outcome != outcomeQuotaUnknown {
				t.Fatalf("unreadable exit-3 receipt became %s/%d, want QUOTA_UNKNOWN/89: %v", sel.outcome, exitCode(err), err)
			}
			if gates != 2 || refreshes != 1 {
				t.Fatalf("gate calls=%d refreshes=%d, want one refresh and one gate retry", gates, refreshes)
			}
		})
	}
}

func TestPlanKeepsTypedGateRefusalOutcome(t *testing.T) {
	a, _, q, src := accountConsultFixture(t)
	a.Config.Local["spawn"] = true
	a.Config.Hosts, a.Config.HostOrder = nil, nil
	src.presence = src.presence[1:2]
	src.presence[0].HostID = "local"
	q.CatalogRows = q.CatalogRows[1:2]
	q.LaunchValue = Launch{Profile: "opus", ModelID: "opus-m", Effort: "high", Grade: "S", Gate: "default", Pool: "claude"}
	gates := 0
	q.GateDo = func(r QuotaRequest) (string, error) {
		gates++
		if gates == 1 {
			return "pool=claude\n", nil
		}
		return "", &gateRefusal{cause: fail(quotaGateBlocked, "unreadable receipt"), outcome: outcomeQuotaUnknown}
	}
	out := &bytes.Buffer{}
	a.Out = out
	err := a.planCmd(context.Background(), []string{"--role", "consult", "--t", "T1", "--grade", "A", "--json"})
	var doc map[string]any
	if parseErr := json.Unmarshal(out.Bytes(), &doc); parseErr != nil {
		t.Fatalf("plan JSON: %v", parseErr)
	}
	if gates != 2 || exitCode(err) != quotaGateBlocked || doc["outcome_code"] != outcomeQuotaUnknown {
		t.Fatalf("plan lost typed gate outcome: %v/%v, gate calls=%d", err, doc["outcome_code"], gates)
	}
}

func TestStructuredGateRefusalMustConfirmItsAccountAndCode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
		want   string
		status int
	}{
		{"different account", func(r map[string]any) { r["account_key"] = testClaudeA }, outcomeQuotaUnknown, 3},
		{"different pool", func(r map[string]any) { r["provider_id"] = "codex" }, outcomeQuotaUnknown, 3},
		{"different exit", func(r map[string]any) { r["exit_code"] = 0 }, outcomeQuotaUnknown, 3},
		{"contradictory ok", func(r map[string]any) { r["ok"] = true }, outcomeQuotaUnknown, 3},
		{"missing account", func(r map[string]any) { delete(r, "account_key") }, outcomeQuotaUnknown, 3},
		{"pool from account", func(r map[string]any) { delete(r, "provider_id") }, outcomeQuotaExhausted, 3},
		{"missing exit", func(r map[string]any) { delete(r, "exit_code") }, outcomeQuotaUnknown, 3},
		{"missing ok", func(r map[string]any) { delete(r, "ok") }, outcomeQuotaUnknown, 3},
		{"invalid field type", func(r map[string]any) { r["ok"] = "false" }, outcomeQuotaUnknown, 3},
		{"conflicting flags", func(r map[string]any) { r["role_denied"] = true }, outcomeQuotaUnknown, 3},
		{"conflicting reason", func(r map[string]any) { r["reason_code"] = "role_denied" }, outcomeQuotaUnknown, 3},
		{"unknown outcome", func(r map[string]any) { r["outcome_code"] = "future_outcome" }, outcomeQuotaUnknown, 3},
		{"ambiguous full usage", func(r map[string]any) { delete(r, "outcome_code"); r["used_pct"] = 100 }, outcomeQuotaUnknown, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := map[string]any{"schema": "scopefuel.gate.v1", "provider_id": "claude", "account_key": testClaudeB, "ok": false, "exit_code": 3, "outcome_code": outcomeQuotaExhausted, "reason": "quota exhausted"}
			tc.change(r)
			b, _ := json.Marshal(r)
			q := CLIQuota{Binary: "fake-quota", Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: string(b), Code: 3} }}}
			_, err := q.Gate(context.Background(), QuotaRequest{AccountKey: testClaudeB, Profile: "opus"})
			if exitCode(err) != tc.status || gateOutcome(err) != tc.want {
				t.Fatalf("unverified refusal trusted: %v/%s", err, gateOutcome(err))
			}
		})
	}
}

func TestRefreshNeverAddsIneligibleLocalHolder(t *testing.T) {
	for _, mode := range []string{"host-pin", "excluded", "disabled", "capability", "tag", "eligible-local"} {
		t.Run(mode, func(t *testing.T) {
			a, s, _, src := remoteFreeFixture(t, freeReceipt)
			const key = "devin:fedcba9876543210"
			s.Host = "auto"
			src.accounts[key] = SharedAccount{Key: key}
			src.presence = append(src.presence, PresenceRecord{HostID: "local", AccountKey: key, LastSeen: a.Now()})
			switch mode {
			case "host-pin":
				s.Host = "host-b"
			case "excluded":
				s.ExcludeHosts = "local"
			case "disabled":
				a.Config.Local["spawn"] = false
			case "capability":
				for name, seat := range a.Policy.Profiles {
					if seat.Canon == "devin-swe2-medium" {
						seat.Capability = "fixture_capability"
						a.Policy.Profiles[name] = seat
					}
				}
				a.Config.Hosts["host-b"]["fixture_capability"] = true
			case "tag":
				s.Tags = "fixture-tag"
				a.Config.Hosts["host-b"]["tags"] = "fixture-tag"
			}
			local, remote := 0, 0
			a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
				if c.Name == "panewire" {
					local++
				} else if c.Name == "ssh" {
					remote++
				} else {
					return Result{}, false
				}
				src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true}
				return Result{}, true
			})
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "eligible-local":
				if local != 1 || remote != 0 {
					t.Fatalf("eligible local preference: local=%d remote=%d", local, remote)
				}
			case "host-pin", "excluded":
				// A placement restriction keeps the local holder out of
				// placement, not out of measuring the account (hk 2647).
				if local != 1 || remote != 0 || sel.chosen.host != "host-b" {
					t.Fatalf("restricted local holder: local=%d remote=%d host=%s", local, remote, sel.chosen.host)
				}
			default:
				if local != 0 || remote != 1 {
					t.Fatalf("refresh escaped eligible holders: local=%d remote=%d", local, remote)
				}
			}
		})
	}
}
