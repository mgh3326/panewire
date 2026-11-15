package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestLaunchPoolSources(t *testing.T) {
	for _, tc := range []struct {
		name, launchPool, accountPool, receiptPool, wantPool string
		rawText                                              string
		native, hub, asJSON                                  bool
		want                                                 int
	}{
		{name: "launch only subscription", launchPool: "example", wantPool: "example", native: true},
		{name: "launch only credit", launchPool: "anthropic-api", wantPool: "anthropic-api"},
		{name: "launch only JSON", launchPool: "anthropic-api", wantPool: "anthropic-api", asJSON: true},
		{name: "account only", accountPool: "anthropic-api", wantPool: "anthropic-api"},
		{name: "receipt only", receiptPool: "anthropic-api", wantPool: "anthropic-api"},
		{name: "launch only with hub", launchPool: "anthropic-api", wantPool: "anthropic-api", hub: true},
		{name: "account only with hub", accountPool: "anthropic-api", wantPool: "anthropic-api", hub: true},
		{name: "matching sources", launchPool: "anthropic-api", accountPool: "anthropic-api", receiptPool: "anthropic-api", wantPool: "anthropic-api"},
		{name: "launch receipt mismatch", launchPool: "anthropic-api", receiptPool: "openai-api", want: exitConfig},
		{name: "account receipt mismatch", accountPool: "anthropic-api", receiptPool: "openai-api", want: exitConfig},
		{name: "launch account mismatch", launchPool: "anthropic-api", accountPool: "openai-api", want: exitConfig},
		{name: "all absent", want: exitConfig},
		{name: "wrong profile with context", launchPool: "anthropic-api", rawText: "profile=other", want: exitConfig},
		{name: "conflicting audit with context", launchPool: "anthropic-api", rawText: "profile=omp-opus requested_by=fixture requested_by=other", want: exitConfig},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, runner := ompSpawnFixture(t)
			if tc.native {
				a, s, runner, _ = cmdFixture(t)
			}
			q := a.Quota.(*fakeQuota)
			q.LaunchValue.Pool = tc.launchPool
			q.PoolViews = map[string]PoolView{"anthropic-api": {Class: "credit"}, "openai-api": {Class: "credit"}}
			if tc.accountPool != "" {
				s.AccountKey = tc.accountPool + ":0000000000000001"
				a.Source = &fakePlacementSource{presence: []PresenceRecord{{HostID: "local", AccountKey: s.AccountKey, LastSeen: a.Now()}}, accounts: map[string]SharedAccount{s.AccountKey: {Key: s.AccountKey}}}
			}
			if tc.hub {
				a.Config.Hub = map[string]any{"hub_url": "https://hub.example.invalid", "hub_token_env": "fixture-token"}
			}
			q.GateDo = func(r QuotaRequest) (string, error) {
				if tc.rawText != "" {
					return tc.rawText, nil
				}
				if s.AccountKey == "" && !tc.asJSON {
					text := "profile=" + q.LaunchValue.Profile + " requested_by=fixture"
					if tc.receiptPool != "" {
						text += " pool=" + tc.receiptPool
					}
					return text, nil
				}
				receipt := map[string]any{"schema": "scopefuel.gate.v1", "account_key": s.AccountKey, "profile": q.LaunchValue.Profile, "ok": true, "exit_code": 0, "requested_by": "fixture"}
				if tc.receiptPool != "" {
					receipt["provider_id"] = tc.receiptPool
				}
				b, _ := json.Marshal(receipt)
				return string(b), nil
			}
			err := a.Spawn(context.Background(), s)
			if code(err) != tc.want {
				t.Fatalf("pool source %s: want exit %d, got %v", tc.name, tc.want, err)
			}
			if err != nil {
				for _, c := range runner.Calls {
					if c.Name == "herdr" || c.Name == "panewire" {
						t.Fatal("unresolved or mismatched pool reached execution")
					}
				}
				if hkClaimed(t) {
					t.Fatal("unresolved or mismatched pool claimed the task")
				}
				return
			}
			pools, err := a.ledger().QuotaPools(context.Background(), s.Job)
			if err != nil || len(pools) != 1 || pools[0] != tc.wantPool {
				t.Fatalf("lease lost resolved pool %s: %v %+v", tc.wantPool, err, pools)
			}
			if tc.hub {
				found := false
				for _, c := range runner.Calls {
					if c.Name == "panewire" && len(c.Args) > 0 && c.Args[0] == "place" {
						found = strings.Contains(strings.Join(c.Args, " "), "--pool "+tc.wantPool)
					}
				}
				if !found {
					t.Fatal("hub did not receive resolved pool")
				}
			}
		})
	}
}

func TestAccountReceiptWithoutPool(t *testing.T) {
	const key = "claude:0000000000000001"
	for _, status := range []int{0, quotaGateBlocked} {
		t.Run(fmt.Sprintf("exit=%d", status), func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"schema": "scopefuel.gate.v1", "account_key": key, "ok": status == 0, "exit_code": status, "outcome_code": outcomeQuotaExhausted})
			q := CLIQuota{Binary: "fake-quota", Runner: &fakeRunner{Do: func(Command) Result { return Result{Code: status, Out: string(body)} }}}
			_, err := q.Gate(context.Background(), QuotaRequest{Profile: "opus", AccountKey: key})
			if status == 0 && err != nil || status != 0 && (err == nil || gateOutcome(err) != outcomeQuotaExhausted) {
				t.Fatalf("exact account should supply omitted pool without losing status: %v", err)
			}
		})
	}
}
