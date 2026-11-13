package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

func scoredAccountFixture(t *testing.T, rows []CatalogRow, shared bool) (*App, Spawn, *fakeQuota, *fakePlacementSource) {
	t.Helper()
	place := map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB}
	if shared {
		place["cloud-small"] = testClaudeA
		place["pi"] = testClaudeB
	}
	a, s, q, src := hostFirstFixture(t, "implementer", "B", rows, place,
		map[string]float64{"mac-b": 0.1, "cloud-small": 0.2, "cloud-tiny": 0.3, "pi": 0.4})
	// Both accounts are waste. Account A appears first in configuration and
	// has greater waste urgency, while B's host has the higher placement score.
	v := riskPool(50, 16)
	src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, Quota: &v}
	for i := range src.hosts {
		score := 10.0
		if src.hosts[i].Name == "mac-b" {
			score = 20
		}
		if src.hosts[i].Name == "cloud-small" {
			score = 35
		}
		if shared && src.hosts[i].Name == "mac-b" {
			score = 40
		}
		src.hosts[i].Score = ptrF(score)
	}
	return a, s, q, src
}

func TestHostFirstT2997HigherScoreAcrossAccounts(t *testing.T) {
	a, s, _, _ := scoredAccountFixture(t, workerRows, false)
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	if sel.chosen.host != "cloud-small" || sel.chosen.accountKey != testClaudeB || sel.chosen.tierName != "waste" {
		t.Fatalf("higher-scored host must win across waste accounts: host=%s account=%s tier=%s", sel.chosen.host, sel.chosen.accountKey, sel.chosen.tierName)
	}
	if rank := hostRow(t, sel, "cloud-small")["rank"]; rank != 1 {
		t.Fatalf("higher-scored host rank=%v, want 1", rank)
	}
}

func TestHostFirstT2997QuotaTrace(t *testing.T) {
	for _, source := range []string{"gate", "observation", "unknown"} {
		t.Run(source, func(t *testing.T) {
			a, s, q, src := scoredAccountFixture(t, workerRows, false)
			want := "quota_exhausted"
			switch source {
			case "gate":
				gate := q.GateDo
				q.GateDo = func(r QuotaRequest) (string, error) {
					if r.AccountKey == testClaudeB {
						return "", fail(3, "quota exhausted")
					}
					return gate(r)
				}
			case "observation":
				v := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(100)}
				src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, Quota: &v}
			case "unknown":
				want = "quota_unknown"
				src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true}
			}
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil || sel.chosen.host != "mac-b" {
				t.Fatalf("next host must win: %v, %+v", err, sel)
			}
			row := hostRow(t, sel, "cloud-small")
			if row["admission_outcome"] != want || row["rank"] != 1 {
				t.Fatalf("first host must record %s: %v", want, row)
			}
			tried := row["tried"].([]any)
			if len(tried) != 1 || tried[0].(map[string]any)["profile"] != "sonnet" || tried[0].(map[string]any)["outcome"] != want {
				t.Fatalf("first host must record the rung and outcome: %v", tried)
			}
			s.Sel = sel.record()
			var events []fleetevent.Event
			a.EventSink = func(e fleetevent.Event) { events = append(events, e) }
			a.placementEvent(s, map[string]any{"chose": "mac-b", "ranking": "score"}, nil)
			if len(events) != 1 || len(events[0].Selection) < 2 {
				t.Fatalf("placement event lost selection: %+v", events)
			}
			first, second := events[0].Selection[0], events[0].Selection[1]
			if first.Host != "cloud-small" || first.Rank != 1 || first.Outcome != want || len(first.Tried) != 1 || first.Tried[0].Profile != "sonnet" || first.Tried[0].Outcome != want || second.Host != "mac-b" || second.Outcome != "admitted" {
				t.Fatalf("event must explain host order and admission: %+v", events[0].Selection)
			}
		})
	}
}

func TestHostFirstT2997WasteBeatsRankedInsideHost(t *testing.T) {
	rows := append([]CatalogRow{}, workerRows...)
	rows = append(rows, CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "S+", Pool: "codex", Gate: "default", ModelID: "sol-m"})
	a, s, q, src := scoredAccountFixture(t, rows, false)
	// The first host holds ranked codex and waste claude. Its own waste wins.
	src.presence = append(src.presence, PresenceRecord{HostID: "cloud-small", AccountKey: testCodexA, LastSeen: a.Now()})
	v := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(5), Waste: new(bool)}
	src.accounts[testCodexA] = SharedAccount{Key: testCodexA, Observed: true, Fresh: true, Quota: &v}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "codex"})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.accountKey != testClaudeB || sel.chosen.tierName != "waste" || len(q.Calls) != 1 {
		t.Fatalf("host's waste must beat its ranked account: %v, %+v, gates=%v", err, sel, q.Calls)
	}
}

func TestHostFirstT2997PacingUsesOnlyPresentCredit(t *testing.T) {
	paced := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(60), RankProjected: ptrF(120), Projected: ptrF(120), ShortUsed: ptrF(40)}
	a, s := creditFixture(t, balance(80, 30*24*time.Hour), paced, "", nil)
	src := a.Source.(*fakePlacementSource)
	q := a.Quota.(*fakeQuota)
	q.CatalogRows = append(q.CatalogRows[1:], CatalogRow{Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"})
	q.PoolViews["codex"] = PoolView{Status: "ok", Class: "spend", Blocking: ptrF(90)}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-sol": "codex", "omp-opus": "anthropic-api"})
	src.presence = append(src.presence, PresenceRecord{HostID: a.machine(), AccountKey: testCodexA, LastSeen: a.Now()})
	src.accounts[testCodexA] = SharedAccount{Key: testCodexA, Observed: true, Fresh: true}
	// Move the credit account to a lower-scored remote host. The local
	// subscription keeps its grade fit ahead of the other ranked subscription.
	for i := range src.presence {
		if strings.HasPrefix(src.presence[i].AccountKey, "anthropic-api:") {
			src.presence[i].HostID = "remote"
		}
	}
	a.Config.Hosts = map[string]map[string]any{"remote": {}}
	src.hosts = []HostView{{Name: a.machine(), Connected: true, Score: ptrF(35)}, {Name: "remote", Connected: true, Score: ptrF(10), LoadRatio: ptrF(0.1)}}
	a.LoadRatio = func(context.Context) (float64, error) { return 0.2, nil }
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.seatName != "sonnet" || sel.chosen.behindCredit {
		t.Fatalf("credit on another host must not reorder this host's rungs: %v, %+v", err, sel)
	}
}

func TestHostFirstT2997RefusalEventAndNoRung(t *testing.T) {
	a, s, q, _ := scoredAccountFixture(t, workerRows, false)
	q.GateDo = func(QuotaRequest) (string, error) { return "", fail(3, "quota exhausted") }
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil {
		t.Fatal("all accounts must refuse")
	}
	s.Sel = sel.record()
	var events []fleetevent.Event
	a.EventSink = func(e fleetevent.Event) { events = append(events, e) }
	a.spawnEvents(s, err)
	if len(events) != 2 || events[1].Kind != "placement" || events[1].Outcome != "refused" || len(events[1].Selection) != 4 {
		t.Fatalf("refused placement must keep the host trace: %+v", events)
	}
	for _, h := range events[1].Selection {
		want := "quota_exhausted"
		if h.Host == "cloud-tiny" || h.Host == "pi" {
			want = "no_rung"
		}
		if h.Outcome != want {
			t.Fatalf("host %s outcome=%s, want %s", h.Host, h.Outcome, want)
		}
	}
	b, _ := json.Marshal(events[1])
	if strings.Contains(string(b), "account_key") || strings.Contains(string(b), testClaudeA) {
		t.Fatalf("fleet event must omit account identities: %s", b)
	}
}

func TestHostFirstT2997SharedExhaustionAcrossRungs(t *testing.T) {
	rows := append([]CatalogRow{}, workerRows...)
	rows = append(rows, CatalogRow{Profile: "opus", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "opus-m"})
	a, s, q, _ := scoredAccountFixture(t, rows, true)
	gate := q.GateDo
	calls := map[string]int{}
	q.GateDo = func(r QuotaRequest) (string, error) {
		calls[r.AccountKey]++
		if r.AccountKey == testClaudeA {
			return "", fail(3, "quota exhausted")
		}
		return gate(r)
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.host != "pi" {
		t.Fatalf("healthy account on next host must win: %v, %+v", err, sel)
	}
	if calls[testClaudeA] != 1 || calls[testClaudeB] != 1 {
		t.Fatalf("shared exhausted account must be gated once across hosts and rungs: %v", calls)
	}
	if row := hostRow(t, sel, "cloud-small"); row["admission_outcome"] != "quota_exhausted" {
		t.Fatalf("second holder must retain exhaustion: %v", row)
	}
}
