package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestAccountReceiptValidationAtBothBoundaries(t *testing.T) {
	good := `{"schema":"scopefuel.gate.v1","provider_id":"claude","account_key":"` + testClaudeB + `","ok":true,"exit_code":0}`
	for _, tc := range []struct {
		name, receipt string
		want          int
	}{
		{"bound success", good, 0},
		{"other account", strings.ReplaceAll(good, testClaudeB, testClaudeA), exitConfig},
		{"other pool", strings.ReplaceAll(good, `"provider_id":"claude"`, `"provider_id":"codex"`), exitConfig},
		{"missing account", strings.ReplaceAll(good, `"account_key":"`+testClaudeB+`",`, ""), exitConfig},
		{"pool from account", strings.ReplaceAll(good, `"provider_id":"claude",`, ""), 0},
		{"other schema", strings.ReplaceAll(good, "scopefuel.gate.v1", "scopefuel.gate.v2"), exitConfig},
		{"missing schema", strings.ReplaceAll(good, `"schema":"scopefuel.gate.v1",`, ""), exitConfig},
		{"duplicate account", strings.ReplaceAll(good, `"ok":true`, `"account_key":"`+testClaudeB+`","ok":true`), exitConfig},
		{"denied", strings.ReplaceAll(good, `"ok":true`, `"ok":false`), exitConfig},
		{"missing ok", strings.ReplaceAll(good, `"ok":true,`, ""), exitConfig},
		{"missing exit", strings.ReplaceAll(good, `,"exit_code":0`, ""), exitConfig},
		{"nonzero exit", strings.ReplaceAll(good, `"exit_code":0`, `"exit_code":3`), exitConfig},
		{"wrong field type", strings.ReplaceAll(good, `"ok":true`, `"ok":"true"`), exitConfig},
		{"malformed JSON", `{"schema":`, exitConfig},
		{"text cannot bind", "pool=claude profile=opus\n", exitConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli := CLIQuota{Binary: "fake-quota", Runner: &fakeRunner{Do: func(Command) Result { return Result{Out: tc.receipt} }}}
			_, gateErr := cli.Gate(context.Background(), QuotaRequest{Profile: "opus", AccountKey: testClaudeB})
			_, leaseErr := admissionGate("opus", testClaudeB, "", tc.receipt)
			if exitCode(gateErr) != tc.want || exitCode(leaseErr) != tc.want {
				t.Fatalf("receipt boundary mismatch: cli=%v lease=%v want=%d", gateErr, leaseErr, tc.want)
			}
		})
	}
}

func TestAdmissionKeepsProfileAndAuditValidation(t *testing.T) {
	good := `{"schema":"scopefuel.gate.v1","provider_id":"claude","account_key":"` + testClaudeB + `","ok":true,"exit_code":0,"profile":"opus","requested_by":"test-caller","escalation_override":true}`
	g, err := admissionGate("opus", testClaudeB, "", good)
	if err != nil || g.Pool != "claude" || g.Audit["requested_by"] != "test-caller" || g.Audit["escalation_override"] != "true" {
		t.Fatalf("bound lease lost pool or audit: %v %+v", err, g)
	}
	for _, tc := range []struct{ name, account, receipt string }{
		{"JSON profile", testClaudeB, strings.ReplaceAll(good, `"profile":"opus"`, `"profile":"sonnet"`)},
		{"JSON audit type", testClaudeB, strings.ReplaceAll(good, `"requested_by":"test-caller"`, `"requested_by":42`)},
		{"JSON audit duplicate", testClaudeB, strings.ReplaceAll(good, `"requested_by":"test-caller"`, `"requested_by":"test-caller","requested_by":"other"`)},
		{"text profile", "", "pool=claude profile=sonnet\n"},
		{"text missing pool", "", "profile=opus\n"},
		{"text audit conflict", "", "pool=claude profile=opus requested_by=test-caller requested_by=other\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := admissionGate("opus", tc.account, "", tc.receipt); err == nil {
				t.Fatal("receipt lost a profile or audit guard")
			}
		})
	}
	g, err = admissionGate("opus", "", "", "pool=claude profile=opus requested_by=test-caller\n")
	if err != nil || g.Pool != "claude" || g.Audit["requested_by"] != "test-caller" {
		t.Fatalf("unbound legacy text changed: %v %+v", err, g)
	}
}

func TestSpawnRejectsMismatchedAccountReceipt(t *testing.T) {
	const key = "example:0123456789abcdef"
	for _, tc := range []struct{ name, account, pool, refusal string }{
		{"other account", "example:fedcba9876543210", "example", "quota gate did not confirm the target account"},
		{"other pool", key, "other", "quota gate pool mismatch"},
		{"missing account", "", "example", "quota gate did not confirm the target account"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, runner, _ := cmdFixture(t)
			s.AccountKey = key
			q := a.Quota.(*fakeQuota)
			q.LaunchValue.Pool = "example"
			a.Source = &fakePlacementSource{
				presence: []PresenceRecord{{HostID: "local", AccountKey: key, LastSeen: a.Now()}},
				accounts: map[string]SharedAccount{key: {Key: key, Observed: true, Fresh: true}},
			}
			gates := 0
			q.GateDo = func(r QuotaRequest) (string, error) {
				gates++
				if r.AccountKey != key {
					t.Fatalf("selected account lost before gate: %+v", r)
				}
				b, _ := json.Marshal(map[string]any{"schema": "scopefuel.gate.v1", "provider_id": tc.pool, "account_key": tc.account, "profile": s.Profile, "ok": true, "exit_code": 0})
				return string(b), nil
			}
			err := a.Spawn(context.Background(), s)
			if err == nil {
				t.Fatalf("spawn admitted a valid v1 receipt for %s", tc.name)
			}
			if gates != 1 || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("spawn refused before receipt validation: gates=%d err=%v", gates, err)
			}
			for _, c := range runner.Calls {
				if c.Name == "herdr" && len(c.Args) > 1 && c.Args[0] == "tab" && c.Args[1] == "create" {
					t.Fatal("unbound receipt reached pane launch")
				}
			}
		})
	}
}
