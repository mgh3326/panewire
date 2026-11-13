package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func readyClaude() HarnessReadiness {
	return HarnessReadiness{Installed: true, Version: "2.1.0", HerdrIntegration: true, Login: ReadinessLoginPresent, Account: "claude:0123456789abcdef"}
}

// readyNode is a node that passes every check for a claude spawn in repo
// "panewire": other kinds are reported too, so a drop on one kind can be told
// from a drop on the host.
func readyNode() *Readiness {
	return &Readiness{
		Harnesses:  map[string]HarnessReadiness{"claude": readyClaude(), "codex": {Installed: true, Version: "0.166.0", HerdrIntegration: true, Login: ReadinessLoginPresent}},
		Repos:      []string{"panewire", "other"},
		Brief:      ReadinessBrief{HandoffkeepEnv: true},
		Toolchains: map[string]string{"go": "1.26"},
	}
}

func TestReadinessChecksDropOnlyTheirPair(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		mutate   func(*Readiness)
		need     ReadinessNeed
		want     string
		sibling  ReadinessNeed // a pair on the same host the check must not touch
		touchSib bool
	}{
		{"harness missing", func(r *Readiness) { h := r.Harnesses["claude"]; h.Installed = false; r.Harnesses["claude"] = h }, ReadinessNeed{Kind: "claude"}, ReadyHarnessMissing, ReadinessNeed{Kind: "codex"}, false},
		{"harness version", func(r *Readiness) { h := r.Harnesses["codex"]; h.Version = "0.100.0"; r.Harnesses["codex"] = h }, ReadinessNeed{Kind: "codex", MinVersion: "0.166.0"}, ReadyHarnessVersion, ReadinessNeed{Kind: "claude", MinVersion: "2.0"}, false},
		{"unparseable version", func(r *Readiness) { h := r.Harnesses["codex"]; h.Version = ""; r.Harnesses["codex"] = h }, ReadinessNeed{Kind: "codex", MinVersion: "0.166.0"}, ReadyHarnessVersion, ReadinessNeed{Kind: "claude"}, false},
		{"dialog pending", func(r *Readiness) {
			h := r.Harnesses["codex"]
			h.Dialog = DialogCodexHooksReview
			r.Harnesses["codex"] = h
		}, ReadinessNeed{Kind: "codex"}, ReadyDialogPending, ReadinessNeed{Kind: "claude"}, false},
		{"login missing", func(r *Readiness) {
			h := r.Harnesses["claude"]
			h.Login = ReadinessLoginMissing
			r.Harnesses["claude"] = h
		}, ReadinessNeed{Kind: "claude"}, ReadyLoginMissing, ReadinessNeed{Kind: "codex"}, false},
		{"repo unmapped", func(r *Readiness) { r.Repos = []string{"other"} }, ReadinessNeed{Kind: "claude", Repo: "panewire"}, ReadyRepoUnmapped, ReadinessNeed{Kind: "claude", Repo: "other"}, false},
		{"brief unavailable", func(r *Readiness) { r.Brief.HandoffkeepEnv = false }, ReadinessNeed{Kind: "claude", Brief: true}, ReadyBriefUnavailable, ReadinessNeed{Kind: "claude", Brief: false}, false},
		{"toolchain missing", func(r *Readiness) { delete(r.Toolchains, "go") }, ReadinessNeed{Kind: "claude", Toolchains: []string{"go"}}, ReadyToolchainMissing, ReadinessNeed{Kind: "claude", Toolchains: nil}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := readyNode()
			state := ReadinessState{Readiness: r}
			if got := state.Drop(tc.need, now); got != "" {
				t.Fatalf("a ready node was dropped: %s", got)
			}
			tc.mutate(r)
			if got := state.Drop(tc.need, now); got != tc.want {
				t.Fatalf("drop=%q want %q", got, tc.want)
			}
			if got := state.Drop(tc.sibling, now); got != "" {
				t.Fatalf("the check reached the sibling pair %+v: %s", tc.sibling, got)
			}
		})
	}
}

// cmd has no login; a harness the node does not report on is not judged.
func TestReadinessCmdAndUnreportedKinds(t *testing.T) {
	r := readyNode()
	r.Harnesses["cmd"] = HarnessReadiness{Installed: true, Login: ReadinessLoginMissing}
	state := ReadinessState{Readiness: r}
	if got := state.Drop(ReadinessNeed{Kind: "cmd"}, time.Now()); got != "" {
		t.Fatalf("cmd needs no login: %s", got)
	}
	if got := state.Drop(ReadinessNeed{Kind: "kiro"}, time.Now()); got != "" {
		t.Fatalf("a kind the node does not report on was dropped: %s", got)
	}
	// None of the repo, brief or toolchain checks judge it either; only the
	// hub-owned backoff does.
	need := ReadinessNeed{Kind: "kiro", Repo: "not-mapped", Brief: true, Toolchains: []string{"cobol"}}
	r.Brief.HandoffkeepEnv = false
	if got := state.Drop(need, time.Now()); got != "" {
		t.Fatalf("an unreported kind was judged by a repo-level check: %s", got)
	}
	now := time.Now()
	state.BackoffUntil = map[string]time.Time{"kiro": now.Add(time.Minute)}
	if got := state.Drop(need, now); got != ReadyLaunchBackoff {
		t.Fatalf("backoff on an unreported kind: %q", got)
	}
}

func TestReadinessUnknownNodeKeepsToday(t *testing.T) {
	now := time.Now()
	failing := readyNode()
	failing.Harnesses = map[string]HarnessReadiness{} // would fail every check
	failing.Repos = nil
	for name, state := range map[string]ReadinessState{
		"flagged":                  {Unknown: true},
		"no report":                {},
		"flagged with stale ghost": {Unknown: true, Readiness: failing},
	} {
		if got := state.Drop(ReadinessNeed{Kind: "claude", Repo: "panewire", Brief: true, Toolchains: []string{"go"}, MinVersion: "9"}, now); got != "" {
			t.Errorf("%s: dropped %q", name, got)
		}
	}
}

func TestReadinessBackoffAppliesToItsKindUntilExpiry(t *testing.T) {
	now := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	state := ReadinessState{Readiness: readyNode(), BackoffUntil: map[string]time.Time{"claude": now.Add(time.Minute)}}
	if got := state.Drop(ReadinessNeed{Kind: "claude"}, now); got != ReadyLaunchBackoff {
		t.Fatalf("inside the window: %q", got)
	}
	if got := state.Drop(ReadinessNeed{Kind: "codex"}, now); got != "" {
		t.Fatalf("another kind on the host was backed off: %q", got)
	}
	if got := state.Drop(ReadinessNeed{Kind: "claude"}, now.Add(time.Minute)); got != "" {
		t.Fatalf("at expiry: %q", got)
	}
	// The hub-observed backoff applies even to a node that never reported.
	old := ReadinessState{Unknown: true, BackoffUntil: state.BackoffUntil}
	if got := old.Drop(ReadinessNeed{Kind: "claude"}, now); got != ReadyLaunchBackoff {
		t.Fatalf("old node: %q", got)
	}
}

func TestVersionAtLeast(t *testing.T) {
	for _, tc := range []struct {
		have, want string
		ok         bool
	}{
		{"0.166.0", "0.166.0", true}, {"0.167", "0.166.9", true}, {"v2.1", "2.1.0", true}, {"0.99.0", "0.166.0", false},
		{"1.0.0-beta", "1.0.0", true}, {"", "1", false}, {"abc", "1", false}, {"1", "x", false}, {"1.2", "1.10", false},
	} {
		if got := versionAtLeast(tc.have, tc.want); got != tc.ok {
			t.Errorf("versionAtLeast(%q,%q)=%t", tc.have, tc.want, got)
		}
	}
}

// readinessSnapshot is a hub state with each host's publisher and readiness.
func readinessSnapshot(t *testing.T, a *App, states map[string]ReadinessState, at time.Time) *fakeStateReader {
	t.Helper()
	var rows []map[string]any
	for host, state := range states {
		rows = append(rows, fsPublisher(host, true, at))
		var body map[string]any
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, map[string]any{"host": host, "kind": "readiness", "key": "", "measured_at": at, "readiness": body})
	}
	return &fakeStateReader{snap: fsSnapshot(t, at, rows...)}
}

func readinessFixture(t *testing.T, states map[string]ReadinessState) (*App, Spawn) {
	t.Helper()
	// mac-b is the better host and holds account A, which ranks first; the
	// other host holds B. A drop of mac-b's pair must move the choice, not
	// refuse the spawn.
	a, s, _, _ := hostFirstFixture(t, "consult", "A", consultRows, map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB}, map[string]float64{"mac-b": 0.1, "cloud-small": 0.5})
	a.Repository = &Repository{Name: "panewire"}
	a.Config.Hosts["mac-b"]["via"] = "hub"
	a.Config.Hosts["cloud-small"]["via"] = "hub"
	a.State = readinessSnapshot(t, a, states, a.Now())
	return a, s
}

func TestReadinessFilterDropsThePairBeforeRanking(t *testing.T) {
	good := func() ReadinessState { return ReadinessState{Readiness: readyNode()} }
	for _, tc := range []struct {
		name   string
		mutate func(*ReadinessState)
		config func(*App)
		want   string
	}{
		{"harness_missing", func(s *ReadinessState) {
			h := s.Readiness.Harnesses["claude"]
			h.Installed = false
			s.Readiness.Harnesses["claude"] = h
		}, nil, ReadyHarnessMissing},
		{"harness_version", func(s *ReadinessState) {
			h := s.Readiness.Harnesses["claude"]
			h.Version = "1.0.0"
			s.Readiness.Harnesses["claude"] = h
		}, func(a *App) {
			a.Config.Placement = map[string]any{"harness_min_version": map[string]any{"claude": "2.0"}}
		}, ReadyHarnessVersion},
		{"dialog_pending", func(s *ReadinessState) {
			h := s.Readiness.Harnesses["claude"]
			h.Dialog = DialogClaudeTrust
			s.Readiness.Harnesses["claude"] = h
		}, nil, ReadyDialogPending},
		{"login_missing", func(s *ReadinessState) {
			h := s.Readiness.Harnesses["claude"]
			h.Login = ReadinessLoginMissing
			s.Readiness.Harnesses["claude"] = h
		}, nil, ReadyLoginMissing},
		{"repo_unmapped", func(s *ReadinessState) { s.Readiness.Repos = []string{"other"} }, nil, ReadyRepoUnmapped},
		{"brief_unavailable", func(s *ReadinessState) { s.Readiness.Brief.HandoffkeepEnv = false }, nil, ReadyBriefUnavailable},
		{"toolchain_missing", func(s *ReadinessState) { delete(s.Readiness.Toolchains, "bun") }, func(a *App) {
			a.Config.Placement = map[string]any{"repo_toolchains": map[string]any{"panewire": []any{"bun"}}}
		}, ReadyToolchainMissing},
		{"launch_backoff", func(s *ReadinessState) {
			s.BackoffUntil = map[string]time.Time{"claude": time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)}
		}, nil, ReadyLaunchBackoff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad, ok := good(), good()
			ok.Readiness.Toolchains["bun"] = "1.2"
			bad.Readiness.Toolchains["bun"] = "1.2"
			if tc.mutate != nil {
				tc.mutate(&bad)
			}
			a, s := readinessFixture(t, map[string]ReadinessState{"mac-b": bad, "cloud-small": ok})
			if tc.config != nil {
				tc.config(a)
			}
			sel, err := a.selectProfile(context.Background(), s)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if sel.chosen.host != "cloud-small" || sel.chosen.accountKey != testClaudeB {
				t.Fatalf("placed %s with %s, want cloud-small with %s", sel.chosen.host, sel.chosen.accountKey, testClaudeB)
			}
			row := hostRow(t, sel, "mac-b")
			if row["outcome"] != tc.want {
				t.Fatalf("mac-b outcome=%v want %s", row["outcome"], tc.want)
			}
			raw, _ := json.Marshal(row["readiness_drops"])
			if !strings.Contains(string(raw), `"code":"`+tc.want+`"`) {
				t.Fatalf("drops=%s", raw)
			}
			if other := hostRow(t, sel, "cloud-small"); other["readiness_drops"] != nil || other["outcome"] != "chosen" {
				t.Fatalf("the ready host was touched: %v", other)
			}
		})
	}
}

// The same check on the only host holding the account names the check as the
// candidate's reason, instead of reading as a missing login.
func TestReadinessDropNamesTheCheckWhenNoHostIsLeft(t *testing.T) {
	bad := ReadinessState{Readiness: readyNode()}
	bad.Readiness.Repos = nil
	a, s, _, _ := hostFirstFixture(t, "consult", "A", consultRows, map[string]string{"mac-b": testClaudeA}, map[string]float64{"mac-b": 0.1})
	a.Repository = &Repository{Name: "panewire"}
	a.State = readinessSnapshot(t, a, map[string]ReadinessState{"mac-b": bad}, a.Now())
	sel, err := a.selectProfile(context.Background(), s)
	if err == nil || sel == nil {
		t.Fatalf("expected a refusal with a record, got %v", err)
	}
	raw, _ := json.Marshal(sel.record())
	if !strings.Contains(string(raw), `"reason":"`+ReadyRepoUnmapped+`"`) || !strings.Contains(string(raw), `"outcome":"`+ReadyRepoUnmapped+`"`) {
		t.Fatalf("record does not name the check: %s", raw)
	}
}

func TestReadinessUnknownHostsKeepTodaysChoice(t *testing.T) {
	// mac-b reports nothing usable (flagged unknown); cloud-small is not in
	// the snapshot at all. Today's ranking picks mac-b.
	a, s := readinessFixture(t, map[string]ReadinessState{"mac-b": {Unknown: true}})
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.host != "mac-b" {
		t.Fatalf("chosen=%v err=%v", sel, err)
	}
	// No state at all behaves the same.
	a, s = readinessFixture(t, nil)
	if sel, err = a.selectProfile(context.Background(), s); err != nil || sel.chosen.host != "mac-b" {
		t.Fatalf("chosen=%v err=%v", sel, err)
	}
}

// A value from a dead publisher or past the TTL is not trusted: it reads as
// unknown, not as a verdict.
func TestReadinessStaleValueIsUnknown(t *testing.T) {
	bad := ReadinessState{Readiness: readyNode()}
	bad.Readiness.Repos = nil
	a, s := readinessFixture(t, map[string]ReadinessState{"mac-b": bad})
	reader := a.State.(*fakeStateReader)
	for i := range reader.snap.States {
		if reader.snap.States[i].Kind == "readiness" {
			reader.snap.States[i].MeasuredAt = a.Now().Add(-10 * time.Minute)
		}
	}
	sel, err := a.selectProfile(context.Background(), s)
	if err != nil || sel.chosen.host != "mac-b" {
		t.Fatalf("a stale readiness value dropped the pair: %v %v", sel, err)
	}
	a, s = readinessFixture(t, map[string]ReadinessState{"mac-b": bad})
	reader = a.State.(*fakeStateReader)
	for i := range reader.snap.States {
		if reader.snap.States[i].Kind == "publisher" {
			reader.snap.States[i].Publisher.Connected = false
		}
	}
	if sel, err = a.selectProfile(context.Background(), s); err != nil || sel.chosen.host != "mac-b" {
		t.Fatalf("a dead publisher's readiness dropped the pair: %v %v", sel, err)
	}
}

// A profile of a custom seat policy resolves to its kind, so launch failures
// of that profile reach the backoff.
func TestProfileKindReadsTheActivePolicy(t *testing.T) {
	custom := filepath.Join(t.TempDir(), "policy.json")
	body := `{"version":1,"profiles":{"house-seat":{"canon":"house-seat","kind":"omp","family":"house","capability":"standard","roles":["implementer"]}}}`
	if err := os.WriteFile(custom, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PANEWIRE_POLICY", custom)
	if got := ProfileKind("house-seat"); got != "omp" {
		t.Fatalf("custom profile kind=%q", got)
	}
	t.Setenv("PANEWIRE_POLICY", filepath.Join(t.TempDir(), "missing.json"))
	if got := ProfileKind("house-seat"); got != "" {
		t.Fatalf("an unreadable policy still answered %q", got)
	}
}

func TestProfileKind(t *testing.T) {
	if ProfileKind("no-such-profile") != "" {
		t.Fatal("an unknown profile has no kind")
	}
	policy, err := LoadPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	for name, seat := range policy.Profiles {
		if got := ProfileKind(name); got != seat.Kind {
			t.Fatalf("%s: %q want %q", name, got, seat.Kind)
		}
	}
}

// Selection chooses among ready pairs, and placement must not walk past a
// dropped host to it later: the plan's candidate list drops the same pair.
func TestReadinessPlacementPlanDropsTheSamePair(t *testing.T) {
	bad := ReadinessState{Readiness: readyNode()}
	h := bad.Readiness.Harnesses["claude"]
	h.Installed = false
	bad.Readiness.Harnesses["claude"] = h
	a, s := readinessFixture(t, map[string]ReadinessState{"mac-b": bad, "cloud-small": {Readiness: readyNode()}})
	s.Profile = "opus"
	plan, err := a.plan(context.Background(), s, "claude", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range plan.candidates {
		if c.name == "mac-b" && c.outcome != ReadyHarnessMissing {
			t.Fatalf("mac-b outcome=%q", c.outcome)
		}
	}
	for _, c := range plan.ordered {
		if c.name == "mac-b" {
			t.Fatal("a host not ready for the harness stayed a fallback candidate")
		}
	}
	if !slices.ContainsFunc(plan.ordered, func(c *placeCandidate) bool { return c.name == "cloud-small" }) {
		t.Fatal("the ready host was dropped too")
	}
	// A seat of another kind on the same host is not touched.
	s.Profile = "builder-sol-low"
	if plan, err = a.plan(context.Background(), s, "codex", 0); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(plan.ordered, func(c *placeCandidate) bool { return c.name == "mac-b" }) {
		t.Fatal("a codex pair was dropped for a claude check")
	}
}
