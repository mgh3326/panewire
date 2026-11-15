package fleet

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hostFirstFixture is a four-machine fleet with local spawning disabled:
// a macOS machine with unity, a small Linux VM, a one-core VM limited to
// light roles and grades up to A, and a Pi limited to the implementer role at C/B.
// Account A (claude, waste risk) ranks ahead of account B (claude, spend) in
// pool order. place maps host → account key; load maps host → load ratio.
func hostFirstFixture(t *testing.T, role, grade string, rows []CatalogRow, place map[string]string, load map[string]float64) (*App, Spawn, *fakeQuota, *fakePlacementSource) {
	t.Helper()
	pools := map[string]PoolView{"claude": riskPool(30, 8)}
	a, s, q := selFixture(t, grade, rows, pools, nil)
	serveRouting(t, routingDocV1)
	q.GateDo = gatePools(map[string]string{"opus": "claude", "sonnet": "claude"})
	s.Role = role
	a.Config.Local["spawn"] = false
	a.Config.Hosts = map[string]map[string]any{
		"mac-b":       {"tags": []any{"macos", "unity", "xcode"}},
		"cloud-small": {"tags": []any{"linux"}},
		"cloud-tiny":  {"caps": []any{"linux"}, "roles": []any{"implementer", "reviewer"}, "grades": []any{"C", "B", "A"}},
		"pi":          {"tags": []any{"linux"}, "roles": []any{"implementer"}, "grades": []any{"C", "B"}},
	}
	a.Config.HostOrder = []string{"mac-b", "cloud-small", "cloud-tiny", "pi"}
	src := &fakePlacementSource{accounts: map[string]SharedAccount{}}
	for _, name := range a.Config.HostOrder {
		hv := HostView{Name: name, Connected: true}
		if l, ok := load[name]; ok {
			hv.LoadRatio = ptrF(l)
		}
		src.hosts = append(src.hosts, hv)
	}
	for host, key := range place {
		src.presence = append(src.presence, PresenceRecord{HostID: host, AccountKey: key, LastSeen: a.Now()})
	}
	a2 := riskPool(30, 8)
	b := PoolView{Status: "ok", Class: "spend", Blocking: ptrF(40)}
	src.accounts[testClaudeA] = SharedAccount{Key: testClaudeA, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &a2}
	src.accounts[testClaudeB] = SharedAccount{Key: testClaudeB, Observed: true, Fresh: true, MeasuredAt: a.Now(), Quota: &b}
	a.Source = src
	return a, s, q, src
}

var (
	consultRows = []CatalogRow{{Profile: "opus", Effort: "high", Grade: "S", Pool: "claude", Gate: "default", ModelID: "opus-m"}}
	workerRows  = []CatalogRow{{Profile: "sonnet", Effort: "high", Grade: "S+", Pool: "claude", Gate: "default", ModelID: "sonnet-m"}}
)

func TestHostFitPlacement(t *testing.T) {
	for _, tc := range []struct {
		name, role, grade, tags string
		rows                    []CatalogRow
		place                   map[string]string
		load                    map[string]float64
		wantHost, wantKey       string
		wantCode                int
		wantErr                 string
	}{
		{
			name: "unity lead skips the waste account on a non-unity host", role: "consult", grade: "A", tags: "unity", rows: consultRows,
			place:    map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
			load:     map[string]float64{"cloud-small": 0.1, "mac-b": 0.5},
			wantHost: "mac-b", wantKey: testClaudeB,
		},
		{
			name: "no host declares the cap", role: "consult", grade: "A", tags: "gpu", rows: consultRows,
			place:    map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
			wantCode: exitPlacement, wantErr: "NO_ELIGIBLE_HOST: no account admitted routing role consult; no host declares cap gpu",
		},
		{
			name: "no single host declares every cap", role: "consult", grade: "A", tags: "unity,linux", rows: consultRows,
			place:    map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB},
			wantCode: exitPlacement, wantErr: "no single host declares all caps unity,linux",
		},
		{
			name: "pi takes a light worker task", role: "implementer", grade: "B", rows: workerRows,
			place:    map[string]string{"pi": testClaudeB},
			wantHost: "pi", wantKey: testClaudeB,
		},
		{
			// Fitting hosts exist but none holds a login: AUTH_UNAVAILABLE.
			name: "pi never takes a lead", role: "consult", grade: "B", rows: consultRows,
			place:    map[string]string{"pi": testClaudeB},
			wantCode: exitAuthUnavailable, wantErr: "AUTH_UNAVAILABLE",
		},
		{
			name: "an idle pi does not draw a lead from a busier host", role: "consult", grade: "B", rows: consultRows,
			place:    map[string]string{"pi": testClaudeB, "mac-b": testClaudeB},
			load:     map[string]float64{"pi": 0.0, "mac-b": 0.8},
			wantHost: "mac-b", wantKey: testClaudeB,
		},
		{
			name: "one-core vm never takes a heavy grade", role: "implementer", grade: "S", rows: workerRows,
			place:    map[string]string{"cloud-tiny": testClaudeB},
			wantCode: exitAuthUnavailable, wantErr: "AUTH_UNAVAILABLE",
		},
		{
			name: "heavy grade goes to a capable host", role: "implementer", grade: "S", rows: workerRows,
			place:    map[string]string{"cloud-tiny": testClaudeB, "mac-b": testClaudeB},
			load:     map[string]float64{"cloud-tiny": 0.0, "mac-b": 0.9},
			wantHost: "mac-b", wantKey: testClaudeB,
		},
		{
			name: "one-core vm takes light work", role: "implementer", grade: "B", rows: workerRows,
			place:    map[string]string{"cloud-tiny": testClaudeB, "mac-b": testClaudeB},
			load:     map[string]float64{"cloud-tiny": 0.1, "mac-b": 0.9},
			wantHost: "cloud-tiny", wantKey: testClaudeB,
		},
		{
			name: "heavy-go work needs the cap", role: "implementer", grade: "B", tags: "heavy-go", rows: workerRows,
			place:    map[string]string{"cloud-tiny": testClaudeB},
			wantCode: exitPlacement, wantErr: "no host declares cap heavy-go",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s, _, _ := hostFirstFixture(t, tc.role, tc.grade, tc.rows, tc.place, tc.load)
			s.Tags = tc.tags
			sel, err := a.selectProfile(context.Background(), s)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want refusal %q, got %v", tc.wantErr, err)
				}
				if tc.wantCode != 0 && exitCode(err) != tc.wantCode {
					t.Fatalf("exit %d, want %d: %v", exitCode(err), tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if sel.chosen.host != tc.wantHost || sel.chosen.accountKey != tc.wantKey {
				t.Fatalf("placed %s with %s, want %s with %s", sel.chosen.host, sel.chosen.accountKey, tc.wantHost, tc.wantKey)
			}
		})
	}
}

// Acceptance 1: caps parse, merge with tags, and an undeclared cap excludes
// the host for work needing it while it stays eligible for other work.
func TestHostFitConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts.toml")
	write := func(body string) (Config, error) {
		if e := os.WriteFile(path, []byte(body), 0o600); e != nil {
			t.Fatal(e)
		}
		return LoadConfig(path)
	}
	c, err := write(`[local]
caps = ["macos", "unity"]

[hosts.pi]
tags = ["linux"]
caps = ["heavy-go"]
roles = ["implementer"]
grades = ["C", "B"]

[hosts.cloud-small]
tags = ["linux"]
`)
	if err != nil {
		t.Fatal(err)
	}
	if got := hostCaps(c.Hosts["pi"]); strings.Join(got, ",") != "linux,heavy-go" {
		t.Fatalf("pi caps %v", got)
	}
	if got := hostCaps(c.Local); strings.Join(got, ",") != "macos,unity" {
		t.Fatalf("local caps %v", got)
	}
	if r := roleRefusal(c.Hosts["pi"], "consult", ""); r != "role_not_accepted:consult" {
		t.Fatalf("pi lead: %q", r)
	}
	if r := roleRefusal(c.Hosts["pi"], "implementer", "A"); r != "grade_not_accepted:A" {
		t.Fatalf("pi grade A: %q", r)
	}
	if r := roleRefusal(c.Hosts["pi"], "implementer", "B"); r != "" {
		t.Fatalf("pi implementer B: %q", r)
	}
	if r := roleRefusal(c.Hosts["cloud-small"], "consult", ""); r != "" {
		t.Fatalf("unlimited host refused: %q", r)
	}
	for name, body := range map[string]string{
		"role":       "[hosts.pi]\nroles = [\"lead\"]\n",
		"grade":      "[hosts.pi]\ngrades = [\"D\"]\n",
		"empty":      "[hosts.pi]\nroles = []\n",
		"scalar":     "[hosts.pi]\ngrades = \"B\"\n",
		"cap":        "[hosts.pi]\ncaps = [\"Unity\"]\n",
		"local role": "[local]\nroles = [\"captain\"]\n",
	} {
		if _, err := write(body); exitCode(err) != exitConfig {
			t.Fatalf("%s: want exit 70, got %v", name, err)
		}
	}

	place := map[string]string{"cloud-small": testClaudeB}
	load := map[string]float64{"cloud-small": 0.1}
	a, s, _, _ := hostFirstFixture(t, "implementer", "B", workerRows, place, load)
	s.Tags = "unity"
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil {
		t.Fatal("a host without the unity cap took unity work")
	}
	// mac-b declares unity but holds no login; cloud-small holds one but
	// does not declare unity.
	if sel.chosen != nil || exitCode(err) != exitAuthUnavailable {
		t.Fatalf("want AUTH_UNAVAILABLE: %v", err)
	}
	a, s, _, _ = hostFirstFixture(t, "implementer", "B", workerRows, place, load)
	if sel, err = a.selectProfile(context.Background(), s); err != nil || sel.chosen.host != "cloud-small" {
		t.Fatalf("cloud-small refused work it can do: %v", err)
	}
}

// The placement plan applies the same limits.
func TestPlacementPlanHostFit(t *testing.T) {
	a, _, _, _ := hostFirstFixture(t, "implementer", "B", workerRows, nil, map[string]float64{"mac-b": 0.5, "cloud-small": 0.2, "cloud-tiny": 0.1, "pi": 0.1})
	p, err := a.plan(context.Background(), Spawn{Role: "consult"}, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for host, want := range map[string]string{"pi": "role_not_accepted:consult", "cloud-tiny": "role_not_accepted:consult"} {
		if c := p.candidate(host); c.eligible || c.outcome != want {
			t.Fatalf("%s: eligible=%t outcome=%q", host, c.eligible, c.outcome)
		}
	}
	if c := p.candidate("mac-b"); !c.eligible {
		t.Fatalf("mac-b refused a lead: %q", c.outcome)
	}
	p, _ = a.plan(context.Background(), Spawn{Role: "implementer", TaskGrade: "S"}, "", 0)
	if c := p.candidate("cloud-tiny"); c.outcome != "grade_not_accepted:S" {
		t.Fatalf("cloud-tiny outcome %q", c.outcome)
	}
	p, _ = a.plan(context.Background(), Spawn{Role: "implementer", TaskGrade: "B", Tags: "unity"}, "", 0)
	if c := p.candidate("cloud-small"); c.eligible || c.outcome != "missing_tags:unity" {
		t.Fatalf("cloud-small outcome %q", c.outcome)
	}
}

// A lead or consult has no grade: the task title's grade marker must not let
// a grade-limited host accept it (placement clears it the same way).
func TestHostFitLeadIgnoresStaleTaskGrade(t *testing.T) {
	for _, tc := range []struct {
		name     string
		place    map[string]string
		load     map[string]float64
		wantHost string
	}{
		{"lead only on the grade-limited host", map[string]string{"cloud-tiny": testClaudeB}, nil, ""},
		{"lead goes to the unlimited host", map[string]string{"cloud-tiny": testClaudeB, "mac-b": testClaudeB}, map[string]float64{"cloud-tiny": 0.0, "mac-b": 0.9}, "mac-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Task title "(grade A, T1)": A is in cloud-tiny's grades list.
			a, s, _, _ := hostFirstFixture(t, "consult", "A", consultRows, tc.place, tc.load)
			delete(a.Config.Hosts["cloud-tiny"], "roles")
			sel, err := a.selectProfile(context.Background(), s)
			if tc.wantHost == "" {
				if err == nil {
					t.Fatalf("a lead was admitted on a grade-limited host: %s", sel.chosen.host)
				}
				return
			}
			if err != nil || sel.chosen.host != tc.wantHost {
				t.Fatalf("want %s: %v %+v", tc.wantHost, err, sel)
			}
		})
	}
}
