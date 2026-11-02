package fleet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type failedPoolStatusQuota struct{ *fakeQuota }

func (q failedPoolStatusQuota) Pools(context.Context) (map[string]PoolView, error) {
	return nil, fmt.Errorf("pool status unavailable")
}

func TestOMPCreditPoolReadFailure(t *testing.T) {
	for _, pool := range []string{"anthropic-api", "openai-api"} {
		for _, subscription := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/subscription=%t", pool, subscription), func(t *testing.T) {
				profile, model := "omp-opus", "anthropic/claude-opus-5-5"
				if pool == "openai-api" {
					profile, model = "omp-sol", "openai/gpt-6.1-sol"
				}
				rows := []CatalogRow{ompRow(profile, model, pool)}
				if subscription {
					rows = append(rows, ompRow("sonnet", "claude-sonnet-5-5", "claude"))
				}
				a, s, q := selFixture(t, "A", rows, map[string]PoolView{"claude": {Status: "ok", Class: "spend", Blocking: ptrF(90)}, pool: {Class: "exclude"}}, []string{pool, "claude"})
				serveOMPRouting(t, a)
				a.Quota = failedPoolStatusQuota{q}
				var gates []string
				admit := gatePools(map[string]string{profile: pool, "sonnet": "claude"})
				q.GateDo = func(r QuotaRequest) (string, error) {
					gates = append(gates, r.Profile)
					return admit(r)
				}
				sel, err := a.selectProfile(context.Background(), s)
				if subscription {
					if err != nil || sel.chosen == nil || sel.chosen.row.Profile != "sonnet" {
						t.Fatalf("read failure must leave healthy subscription usable: %v", err)
					}
				} else if code(err) != exitQuotaUnknown || sel.chosen != nil {
					t.Fatalf("credit selected after pool-class read failure: err=%v chosen=%+v", err, sel.chosen)
				}
				candidate := candOutcome(sel.record(), profile, "high")
				if candidate["outcome_code"] != outcomeQuotaUnknown || candidate["reason"] != "quota_unknown" || !slices.Contains(sel.degraded, "credit_pool_status_unavailable") || slices.Contains(gates, profile) {
					t.Fatalf("unknown credit state must be recorded and never gated: candidate=%v degraded=%v gates=%v", candidate, sel.degraded, gates)
				}
			})
		}
	}
}

func TestPoolPolicyV2Migration(t *testing.T) {
	for _, tc := range []struct {
		name, body, errorText string
	}{
		{"legacy v1", `{"version":1,"order":["free","waste","credit","ranked"]}`, "migrate"},
		{"v2 credit before ranked", `{"version":2,"order":["free","waste","credit","ranked"]}`, "credit"},
		{"v2 credit last", `{"version":2,"order":["free","waste","ranked","credit"],"credit_pools":["anthropic-api","openai-api"]}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pool-order.json")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			policy, err := LoadPoolPolicy(path)
			if tc.errorText == "" {
				if err != nil || policy.Version != 2 {
					t.Fatalf("v2 credit-last policy refused: %v", err)
				}
			} else if code(err) != exitConfig || !strings.Contains(err.Error(), tc.errorText) {
				t.Fatalf("unsafe policy must refuse with %q migration evidence: %v", tc.errorText, err)
			}
		})
	}
	policy, err := LoadPoolPolicy("")
	if err != nil || policy.Version != 2 {
		t.Fatalf("embedded policy must be v2: version=%d err=%v", policy.Version, err)
	}
}

func TestOMPCreditLaunchRechecksStatus(t *testing.T) {
	for _, selected := range []bool{false, true} {
		for _, state := range []string{"credit", "exclude", "read failure"} {
			t.Run(fmt.Sprintf("selected=%t/%s", selected, state), func(t *testing.T) {
				a, s, q := selFixture(t, "A", nil, nil, []string{"anthropic-api"})
				q.LaunchValue = Launch{Profile: "omp-opus", Effort: "high", ModelID: "anthropic/claude-opus-5-5", Grade: "A", Pool: "anthropic-api", Gate: "default"}
				if selected {
					s.AccountKey = a.Source.(*fakePlacementSource).presence[0].AccountKey
				}
				q.PoolViews = map[string]PoolView{"anthropic-api": {Class: state}}
				if state == "read failure" {
					q.PoolsErr = fmt.Errorf("unreadable classes")
				}
				gates := 0
				admit := gatePools(map[string]string{"omp-opus": "anthropic-api"})
				q.GateDo = func(r QuotaRequest) (string, error) { gates++; return admit(r) }
				_, _, err := a.admitLaunch(context.Background(), s, a.Policy.Profiles["omp-opus"], "high", "high", "A", "", selected)
				if state == "credit" {
					if err != nil || gates != 1 {
						t.Fatalf("balance-less launch must reach gate: %v gates=%d", err, gates)
					}
				} else if err == nil || gates != 0 {
					t.Fatalf("unreadable or excluded credit reached launch gate: %v gates=%d", err, gates)
				}
			})
		}
	}
}

func TestOMPExplicitCreditOmittedLaunchPool(t *testing.T) {
	for _, state := range []string{"credit", "exclude", "read failure"} {
		t.Run(state, func(t *testing.T) {
			a, s, fr := ompSpawnFixture(t)
			q := a.Quota.(*fakeQuota)
			q.LaunchValue.Pool = ""
			q.PoolViews = map[string]PoolView{"anthropic-api": {Class: state}}
			if state == "read failure" {
				q.PoolsErr = fmt.Errorf("classes unavailable")
			}
			err := a.Spawn(context.Background(), s)
			if state == "credit" {
				if err != nil {
					t.Fatalf("gate pool should resolve balance-less credit: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("unreadable or excluded OMP credit launched when launch response omitted pool")
			}
			for _, c := range fr.Calls {
				if c.Name == "herdr" && strings.HasPrefix(strings.Join(c.Args, " "), "agent start") {
					t.Fatal("unreadable or excluded credit reached OMP")
				}
			}
		})
	}
}

func TestOMPSpawnReceiptPoolBinding(t *testing.T) {
	for _, binding := range []string{"legacy", "unbound-json", "selected", "selected-missing-launch-pool"} {
		for _, receiptPool := range []string{"anthropic-api", "openai-api", "claude"} {
			t.Run(binding+"/"+receiptPool, func(t *testing.T) {
				a, s, fr := ompSpawnFixture(t)
				q := a.Quota.(*fakeQuota)
				q.PoolViews = map[string]PoolView{"anthropic-api": {Class: "credit"}, "openai-api": {Class: "credit"}, "claude": {Status: "ok", Class: "spend"}}
				if strings.HasPrefix(binding, "selected") {
					s.AccountKey = "anthropic-api:0000000000000001"
					a.Source = &fakePlacementSource{presence: []PresenceRecord{{HostID: "local", AccountKey: s.AccountKey, LastSeen: a.Now()}}, accounts: map[string]SharedAccount{s.AccountKey: {Key: s.AccountKey}}}
				}
				if binding == "selected-missing-launch-pool" {
					q.LaunchValue.Pool = ""
				}
				q.GateDo = func(r QuotaRequest) (string, error) {
					if binding == "legacy" {
						return fmt.Sprintf("pool=%s profile=omp-opus\n", receiptPool), nil
					}
					receipt := map[string]any{"schema": "scopefuel.gate.v1", "provider_id": receiptPool, "profile": "omp-opus", "ok": true, "exit_code": 0}
					if s.AccountKey != "" {
						receipt["account_key"] = s.AccountKey
					}
					b, _ := json.Marshal(receipt)
					return string(b), nil
				}
				err := a.Spawn(context.Background(), s)
				if receiptPool == "anthropic-api" {
					if err != nil {
						t.Fatalf("matching pool must launch: %v", err)
					}
					return
				}
				if code(err) != exitConfig || !strings.Contains(err.Error(), "quota gate pool mismatch") {
					t.Fatalf("swapped receipt pool must refuse before execution: receipt=%s err=%v", receiptPool, err)
				}
				for _, c := range fr.Calls {
					args := strings.Join(c.Args, " ")
					if c.Name == "herdr" || c.Name == "panewire" || c.Name == "handoffkeep" && strings.HasPrefix(args, "tasks claim") {
						t.Fatalf("swapped pool reached a launch side effect: %s %s", c.Name, args)
					}
				}
			})
		}
	}
}

func TestOMPCreditRemotePlacement(t *testing.T) {
	for _, mode := range []string{"auto", "forced", "plan"} {
		for _, change := range []string{"none", "pool omitted", "account", "excluded", "status failure", "gate denied"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				const key = "anthropic-api:0000000000000001"
				src := &fakePlacementSource{hosts: []HostView{{Name: "host-b", Connected: true}}}
				spawns := 0
				a, s, fr := remoteFixture(t, src, &fakeWaker{}, "2026-01-01T00:00:00Z", func(args string) Result {
					if strings.Contains(args, "_spawn-stdin") {
						spawns++
						return Result{Out: "OK pane=w:p1 job=job-a\n"}
					}
					return Result{}
				})
				src.presenceDo = nil
				src.presence = []PresenceRecord{{HostID: "presence-b", AccountKey: key, LastSeen: a.Now()}}
				src.accounts = map[string]SharedAccount{key: {Key: key}}
				a.Config.Hosts = map[string]map[string]any{"host-b": {"presence_id": "presence-b", "cwd_map": map[string]any{s.CWD: "/srv/repo"}}}
				a.Config.HostOrder = []string{"host-b"}
				row := ompRow("omp-opus", "anthropic/claude-opus-5-5", "anthropic-api")
				q := &fakeQuota{CatalogRows: []CatalogRow{row}, PoolViews: map[string]PoolView{"anthropic-api": {Class: "credit"}}}
				q.LaunchValue = Launch{Profile: row.Profile, Effort: row.Effort, ModelID: row.ModelID, Grade: row.Grade, Pool: row.Pool, Gate: row.Gate}
				var gateKeys []string
				admit := gatePools(map[string]string{"omp-opus": "anthropic-api"})
				q.GateDo = func(r QuotaRequest) (string, error) {
					gateKeys = append(gateKeys, r.AccountKey)
					return admit(r)
				}
				a.Quota = q
				serveOMPRouting(t, a)
				s.Profile, s.T = "auto", "T1"
				sel, err := a.selectProfile(context.Background(), s)
				if err != nil || sel.chosen == nil || sel.chosen.tierName != "credit" {
					t.Fatalf("initial balance-less selection failed: %v", err)
				}
				s.Profile, s.Effort, s.AccountKey, s.RoutingRole = sel.chosen.seatName, sel.chosen.row.Effort, sel.chosen.accountKey, sel.req.route.role
				s.Sel = sel.record()
				switch change {
				case "pool omitted":
					q.GateDo = func(r QuotaRequest) (string, error) {
						gateKeys = append(gateKeys, r.AccountKey)
						body, _ := json.Marshal(map[string]any{"schema": "scopefuel.gate.v1", "account_key": r.AccountKey, "ok": true, "exit_code": 0})
						return string(body), nil
					}
				case "account":
					src.presence[0].AccountKey = "anthropic-api:0000000000000002"
				case "excluded":
					q.PoolViews["anthropic-api"] = PoolView{Class: "exclude"}
				case "status failure":
					q.PoolsErr = fmt.Errorf("status unavailable")
				case "gate denied":
					q.GateDo = func(r QuotaRequest) (string, error) {
						gateKeys = append(gateKeys, r.AccountKey)
						return "", fail(3, "credit exhausted")
					}
				}
				if mode == "plan" {
					_, host, planErr := a.planHosts(context.Background(), s, "A")
					err = planErr
					if (change == "none" || change == "pool omitted") && host != "host-b" {
						t.Fatalf("selected remote credit must remain usable in plan: host=%q err=%v", host, err)
					}
				} else {
					if mode == "forced" {
						s.Host = "host-b"
					}
					_, _, err = a.place(context.Background(), s, "fixture brief")
				}
				if change == "none" || change == "pool omitted" {
					if err != nil || mode != "plan" && spawns != 1 || len(gateKeys) != 2 {
						t.Fatalf("remote placement lost balance-less credit admission: err=%v spawns=%d gates=%v", err, spawns, gateKeys)
					}
				} else if err == nil || spawns != 0 {
					t.Fatalf("changed credit authority must refuse: change=%s err=%v spawns=%d", change, err, spawns)
				}
				for _, got := range gateKeys {
					if got != key {
						t.Fatalf("gate evaluated a different account: %q", got)
					}
				}
				for _, c := range fr.Calls {
					if strings.Contains(strings.Join(c.Args, " "), "pacelever refresh") {
						t.Fatal("balance-less credit must never trigger a balance refresh")
					}
				}
			})
		}
	}
}
