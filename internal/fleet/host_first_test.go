package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func hostRow(t *testing.T, sel *selPlan, host string) map[string]any {
	t.Helper()
	hosts, _ := sel.record()["hosts"].([]any)
	for _, v := range hosts {
		if row := v.(map[string]any); row["host"] == host {
			return row
		}
	}
	t.Fatalf("no host trace for %s", host)
	return nil
}

// Host-first: the machine is chosen first, then the account on it; each
// host's trace says why it was chosen, skipped or not fit.
func TestHostFirstPlacement(t *testing.T) {
	refuseA := func(q *fakeQuota) {
		gate := q.GateDo
		q.GateDo = func(r QuotaRequest) (string, error) {
			if r.AccountKey == testClaudeA {
				return "", fail(3, "quota exhausted")
			}
			return gate(r)
		}
	}
	refuseAll := func(q *fakeQuota) {
		q.GateDo = func(QuotaRequest) (string, error) { return "", fail(3, "quota exhausted") }
	}
	for _, tc := range []struct {
		name, role, grade, tags string
		rows                    []CatalogRow
		place                   map[string]string
		load                    map[string]float64
		gate                    func(*fakeQuota)
		wantHost, wantKey       string
		wantCode                int
		wantErr                 string
		hostOutcome             map[string]string
	}{
		{
			// Account A ranks first in pool order but its only holder is
			// saturated; the idle machine's account B wins.
			name: "account only on a busy host", role: "consult", grade: "A", rows: consultRows,
			place:    map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
			load:     map[string]float64{"cloud-small": 1.6, "mac-b": 0.2},
			wantHost: "mac-b", wantKey: testClaudeB,
			hostOutcome: map[string]string{"mac-b": "chosen", "cloud-small": "not_tried", "cloud-tiny": "role_not_accepted:consult", "pi": "role_not_accepted:consult"},
		},
		{
			name: "unity lead skips the waste account on a non-unity host", role: "consult", grade: "A", tags: "unity", rows: consultRows,
			place:    map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
			load:     map[string]float64{"cloud-small": 0.1, "mac-b": 0.5},
			wantHost: "mac-b", wantKey: testClaudeB,
			hostOutcome: map[string]string{"mac-b": "chosen", "cloud-small": "missing_caps:unity"},
		},
		{
			// The best host's own accounts are refused: skip it, not fatal.
			name: "refused accounts skip the host", role: "consult", grade: "A", rows: consultRows,
			place:    map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB},
			load:     map[string]float64{"mac-b": 0.1, "cloud-small": 0.5},
			gate:     refuseA,
			wantHost: "cloud-small", wantKey: testClaudeB,
			hostOutcome: map[string]string{"mac-b": "all_refused", "cloud-small": "chosen"},
		},
		{
			name: "all hosts refused", role: "consult", grade: "A", rows: consultRows,
			place:       map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
			load:        map[string]float64{"cloud-small": 0.1, "mac-b": 0.5},
			gate:        refuseAll,
			wantErr:     "QUOTA_EXHAUSTED",
			hostOutcome: map[string]string{"mac-b": "all_refused", "cloud-small": "all_refused"},
		},
		{
			name: "no host declares the cap", role: "consult", grade: "A", tags: "gpu", rows: consultRows,
			place:    map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
			wantCode: exitPlacement, wantErr: "NO_ELIGIBLE_HOST: no account admitted routing role consult; no host declares cap gpu",
			hostOutcome: map[string]string{"mac-b": "missing_caps:gpu", "cloud-small": "missing_caps:gpu"},
		},
		{
			name: "pi never takes a lead", role: "consult", grade: "B", rows: consultRows,
			// Fitting hosts exist but none holds a login: AUTH_UNAVAILABLE.
			place:    map[string]string{"pi": testClaudeB},
			wantCode: exitAuthUnavailable, wantErr: "AUTH_UNAVAILABLE",
			hostOutcome: map[string]string{"pi": "role_not_accepted:consult", "mac-b": "no_candidate_present"},
		},
		{
			name: "one-core vm never takes a heavy grade", role: "implementer", grade: "S", rows: workerRows,
			place:    map[string]string{"cloud-tiny": testClaudeB},
			wantCode: exitAuthUnavailable, wantErr: "AUTH_UNAVAILABLE",
			hostOutcome: map[string]string{"cloud-tiny": "grade_not_accepted:S", "pi": "grade_not_accepted:S"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, q, _ := hostFirstFixture(t, tc.role, tc.grade, tc.rows, tc.place, tc.load)
			s.Tags = tc.tags
			if tc.gate != nil {
				tc.gate(q)
			}
			sel, err := a.selectProfile(context.Background(), s)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want refusal %q, got %v", tc.wantErr, err)
				}
				if tc.wantCode != 0 && exitCode(err) != tc.wantCode {
					t.Fatalf("exit %d, want %d: %v", exitCode(err), tc.wantCode, err)
				}
			} else {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				if sel.chosen.host != tc.wantHost || sel.chosen.accountKey != tc.wantKey {
					t.Fatalf("placed %s with %s, want %s with %s", sel.chosen.host, sel.chosen.accountKey, tc.wantHost, tc.wantKey)
				}
			}
			for host, want := range tc.hostOutcome {
				if got := hostRow(t, sel, host)["outcome"]; got != want {
					t.Fatalf("host %s outcome %v, want %s", host, got, want)
				}
			}
		})
	}
}

// A refusal about the account itself holds on every host: the account is not
// judged again on the next host that holds it.
func TestHostFirstAccountRefusalNotRepeated(t *testing.T) {
	a, s, q, _ := hostFirstFixture(t, "implementer", "B", workerRows,
		map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeA, "pi": testClaudeB},
		map[string]float64{"mac-b": 0.1, "cloud-small": 0.2, "pi": 0.3})
	gate := q.GateDo
	judged := map[string]int{}
	q.GateDo = func(r QuotaRequest) (string, error) {
		judged[r.AccountKey]++
		if r.AccountKey == testClaudeA {
			return "", fail(3, "quota exhausted")
		}
		return gate(r)
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.host != "pi" {
		t.Fatalf("want pi with B: %v %+v", err, sel)
	}
	if judged[testClaudeA] != 1 {
		t.Fatalf("account A judged %d times, want once", judged[testClaudeA])
	}
	tried := hostRow(t, sel, "cloud-small")["tried"].([]any)
	if len(tried) != 1 || tried[0].(map[string]any)["result"] != "skipped" || !strings.HasPrefix(tried[0].(map[string]any)["reason"].(string), "refused_on:mac-b:") {
		t.Fatalf("cloud-small tries: %v", tried)
	}
}

// The placement plan tries the host selection chose first inside its load
// bucket; a saturated preferred host stays a last resort.
func TestPlacementPlanPrefersSelectedHost(t *testing.T) {
	a, _, _, _ := hostFirstFixture(t, "implementer", "B", workerRows, nil, map[string]float64{"mac-b": 0.5, "cloud-small": 0.2, "cloud-tiny": 0.1, "pi": 0.1})
	p, _ := a.plan(context.Background(), Spawn{Role: "implementer", TaskGrade: "B", SelHost: "mac-b"}, "", 0)
	if len(p.ordered) == 0 || p.ordered[0].name != "mac-b" {
		t.Fatalf("selection's host not first in its bucket: %v", p.ordered)
	}
	src := a.Source.(*fakePlacementSource)
	src.hosts[0].LoadRatio = ptrF(1.5)
	p, _ = a.plan(context.Background(), Spawn{Role: "implementer", TaskGrade: "B", SelHost: "mac-b"}, "", 0)
	if p.ordered[len(p.ordered)-1].name != "mac-b" {
		t.Fatalf("a saturated preferred host must stay last: %v", p.ordered[0].name)
	}
}

// Acceptance 4: `panewire plan --json` explains every host.
func TestPlanJSONExplainsHosts(t *testing.T) {
	a, _, _, _ := hostFirstFixture(t, "consult", "A", consultRows,
		map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
		map[string]float64{"cloud-small": 0.1, "mac-b": 0.5})
	out := a.Out.(interface{ String() string })
	if err := a.planCmd(context.Background(), []string{"--t", "T2", "--role", "consult", "--tags", "unity", "--grade", "A", "--json"}); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	var doc struct {
		Host      string `json:"host"`
		Selection struct {
			Hosts []map[string]any `json:"hosts"`
		} `json:"selection"`
	}
	if err := json.Unmarshal([]byte(out.String()), &doc); err != nil {
		t.Fatal(err)
	}
	// Four remote hosts and the local machine (spawn disabled: excluded).
	if doc.Host != "mac-b" || len(doc.Selection.Hosts) != 5 {
		t.Fatalf("host=%s hosts=%v", doc.Host, doc.Selection.Hosts)
	}
	first := doc.Selection.Hosts[0]
	tried, _ := first["tried"].([]any)
	if first["host"] != "mac-b" || first["caps_match"] != true || first["load_ratio"] != 0.5 || len(tried) != 1 || tried[0].(map[string]any)["account_key"] != testClaudeB {
		t.Fatalf("mac-b explanation %v", first)
	}
}

// The hub scores this machine too; without its score the whole ranking fell
// back to raw load and an idle Pi outranked a much stronger machine.
func TestHostFirstRanksByHubScoreWithLocal(t *testing.T) {
	a, s, _, src := hostFirstFixture(t, "implementer", "B", workerRows,
		map[string]string{"pi": testClaudeB, "mac-b": testClaudeB}, nil)
	a.Config.Local["spawn"] = true
	for i := range src.hosts {
		switch src.hosts[i].Name {
		case "pi":
			src.hosts[i].LoadRatio, src.hosts[i].Score = ptrF(0.0), ptrF(10)
		case "mac-b":
			src.hosts[i].LoadRatio, src.hosts[i].Score = ptrF(0.3), ptrF(300)
		default:
			src.hosts[i].Score = ptrF(50)
		}
	}
	src.hosts = append(src.hosts, HostView{Name: a.machine(), Connected: true, LoadRatio: ptrF(0.1), Score: ptrF(100)})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.host != "mac-b" {
		t.Fatalf("want the high-score host: %v %+v", err, sel)
	}
	if hostRow(t, sel, a.machine())["score"] != 100.0 {
		t.Fatalf("local trace lost its hub score: %v", hostRow(t, sel, a.machine()))
	}
}

// Only a transport failure of a refresh marks the host down. An exit 81
// (NO_ELIGIBLE_HOST) is also a capability- or account-scoped refusal; it
// fails that account's measurement but must not skip the host's other
// accounts.
func TestHostFirstRefreshRefusalKeepsHostUp(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     int
		wantHost string
		wantDown bool
	}{
		{"scoped refusal (81)", exitPlacement, "mac-b", false},
		{"ssh unreachable (255)", exitUnreachable, "", true},
		{"timeout (124)", exitTimeout, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []CatalogRow{consultRows[0], {Profile: "codex-sol", Effort: "high", Grade: "S", Pool: "codex", Gate: "default", ModelID: "sol-m"}}
			a, s, q, src := hostFirstFixture(t, "consult", "A", rows, nil, map[string]float64{"mac-b": 0.1})
			// mac-b holds claude A (unmeasured: needs a refresh) and codex C.
			src.presence = []PresenceRecord{{HostID: "mac-b", AccountKey: testClaudeA, LastSeen: a.Now()}, {HostID: "mac-b", AccountKey: testCodexA, LastSeen: a.Now()}}
			src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true}
			c := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(40)}
			src.accounts[testCodexA] = SharedAccount{Key: testCodexA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &c}
			q.GateDo = gatePools(map[string]string{"opus": "claude", "codex-sol": "codex"})
			a.Runner = wrapRunner(a.Runner, func(cmd Command) (Result, bool) {
				if cmd.Name != "ssh" || !strings.Contains(strings.Join(cmd.Args, " "), "panewire quota refresh 'claude'") {
					return Result{}, false
				}
				return Result{Code: tc.code, Err: "refused"}, true
			})
			sel, err := a.selectProfile(context.Background(), s)
			row := hostRow(t, sel, "mac-b")
			if tc.wantDown {
				if err == nil || row["outcome"] != "host_unavailable:measure_failed" {
					t.Fatalf("an unreachable host must be skipped: %v %v", err, row)
				}
				return
			}
			if err != nil || sel.chosen.host != tc.wantHost || sel.chosen.accountKey != testCodexA {
				t.Fatalf("a scoped refresh refusal took the host down: %v %v", err, row)
			}
		})
	}
}
