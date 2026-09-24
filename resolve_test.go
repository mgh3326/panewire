package panewire_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	panewire "github.com/mgh3326/panewire"
)

// waitFixtureClient builds a herdr fixture serving one fleet and returns a
// connected client. agents/tabs are the exact payloads agent.list/tab.list
// answer with, so the wrk oracle below and the resolver see the same fleet.
func waitFixtureClient(t *testing.T, agents []any, tabs []any) (*herdrFixture, *panewire.HerdrClient) {
	t.Helper()
	fixture := newHerdrFixture(t, fixtureSchema(true, true))
	t.Cleanup(fixture.Close)
	fixture.On("agent.list", func() any {
		return map[string]any{"type": "agent_list", "agents": agents}
	})
	fixture.On("tab.list", func() any {
		return map[string]any{"tabs": tabs}
	})
	fixture.On("events.subscribe", func() any {
		return map[string]any{"type": "subscription_started"}
	})
	client, err := panewire.NewHerdrClient(fixture.Path())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return fixture, client
}

func sessionField(value string) map[string]any {
	return map[string]any{"agent": "claude", "kind": "id", "source": "herdr:claude", "value": value}
}

// AC-1/AC-2: wait --agent accepts the same inputs prompt --to does — name,
// tab label, and pane_id each resolve to the same single pane.
func TestAgentWaitResolvesNameLabelAndPaneID(t *testing.T) {
	agents := []any{map[string]any{
		"agent": "claude", "name": "lane-a", "pane_id": "w1:p1", "workspace_id": "w1",
		"tab_id": "w1:t1", "agent_status": "idle", "agent_session": sessionField("sess-1"),
	}}
	tabs := []any{map[string]any{"tab_id": "w1:t1", "label": "ops-label", "workspace_id": "w1"}}
	_, client := waitFixtureClient(t, agents, tabs)
	for _, target := range []string{"lane-a", "ops-label", "w1:p1"} {
		result, err := panewire.WaitAgent(context.Background(), client, target, "idle", 10*time.Millisecond, 2*time.Second)
		if err != nil {
			t.Fatalf("target %q: err=%v", target, err)
		}
		if result.Target != target || result.Status != "idle" {
			t.Fatalf("target %q: result=%+v", target, result)
		}
	}
}

// AC-3: multiple candidates never auto-select — the error lists every one.
func TestAgentWaitRejectsAmbiguousTargetListingCandidates(t *testing.T) {
	agents := []any{
		map[string]any{"agent": "claude", "name": "dup", "pane_id": "w1:p1", "agent_status": "idle"},
		map[string]any{"agent": "codex", "name": "dup", "pane_id": "w1:p2", "agent_status": "idle"},
	}
	_, client := waitFixtureClient(t, agents, nil)
	_, err := panewire.WaitAgent(context.Background(), client, "dup", "idle", 0, 2*time.Second)
	if panewire.ExitCode(err) != panewire.ExitConditionInvalid {
		t.Fatalf("err=%v code=%d", err, panewire.ExitCode(err))
	}
	if !strings.Contains(err.Error(), "w1:p1") || !strings.Contains(err.Error(), "w1:p2") {
		t.Fatalf("ambiguous error must list every candidate, got: %v", err)
	}
}

// AC-4: a name vanishing mid-wait does not sever the wait — the observation
// is pinned on pane_id + agent_session, not on the name that resolved it.
func TestAgentWaitSurvivesMidWaitNameLoss(t *testing.T) {
	fixture := newHerdrFixture(t, fixtureSchema(true, true))
	defer fixture.Close()
	var lists atomic.Int32
	fixture.On("agent.list", func() any {
		agent := map[string]any{
			"agent": "claude", "pane_id": "w1:p1", "tab_id": "w1:t1",
			"agent_status": "working", "agent_session": sessionField("sess-1"),
		}
		if lists.Add(1) == 1 {
			agent["name"] = "vanishing"
		}
		return map[string]any{"type": "agent_list", "agents": []any{agent}}
	})
	fixture.On("events.subscribe", func() any {
		go func() {
			time.Sleep(30 * time.Millisecond)
			fixture.Event(map[string]any{"event": map[string]any{"type": "pane_agent_status_changed", "pane_id": "w1:p1", "agent_status": "idle"}, "data": map[string]any{"agent_status": "idle"}})
		}()
		return map[string]any{"type": "subscription_started"}
	})
	client, err := panewire.NewHerdrClient(fixture.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	result, err := panewire.WaitAgent(context.Background(), client, "vanishing", "idle", 40*time.Millisecond, 2*time.Second)
	if err != nil {
		t.Fatalf("name loss mid-wait must not break the wait: %v", err)
	}
	if result.Status != "idle" {
		t.Fatalf("result=%+v", result)
	}
}

// AC-4: a different agent_session occupying the resolved pane fails the wait
// instead of silently reporting the new occupant's status as the target's.
func TestAgentWaitFailsOnOccupantSessionSwap(t *testing.T) {
	fixture := newHerdrFixture(t, fixtureSchema(true, true))
	defer fixture.Close()
	var lists atomic.Int32
	fixture.On("agent.list", func() any {
		session := "sess-1"
		if lists.Add(1) > 1 {
			session = "sess-2"
		}
		return map[string]any{"type": "agent_list", "agents": []any{map[string]any{
			"agent": "claude", "name": "victim", "pane_id": "w1:p1",
			"agent_status": "working", "agent_session": sessionField(session),
		}}}
	})
	fixture.On("events.subscribe", func() any {
		go func() {
			time.Sleep(30 * time.Millisecond)
			fixture.Event(map[string]any{"event": map[string]any{"type": "pane_agent_status_changed", "pane_id": "w1:p1", "agent_status": "idle"}, "data": map[string]any{"agent_status": "idle"}})
		}()
		return map[string]any{"type": "subscription_started"}
	})
	client, err := panewire.NewHerdrClient(fixture.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = panewire.WaitAgent(context.Background(), client, "victim", "idle", 40*time.Millisecond, 2*time.Second)
	if panewire.ExitCode(err) != panewire.ExitConditionInvalid || !strings.Contains(err.Error(), "replaced") {
		t.Fatalf("occupant swap must fail with replaced, got: %v", err)
	}
}

// AC-4: the pane losing its agent entirely fails the wait with an explicit
// gone error rather than hanging until timeout.
func TestAgentWaitFailsWhenPaneOccupantGone(t *testing.T) {
	fixture := newHerdrFixture(t, fixtureSchema(true, true))
	defer fixture.Close()
	var lists atomic.Int32
	fixture.On("agent.list", func() any {
		if lists.Add(1) == 1 {
			return map[string]any{"type": "agent_list", "agents": []any{map[string]any{
				"agent": "claude", "name": "doomed", "pane_id": "w1:p1",
				"agent_status": "working", "agent_session": sessionField("sess-1"),
			}}}
		}
		return map[string]any{"type": "agent_list", "agents": []any{}}
	})
	fixture.On("events.subscribe", func() any {
		return map[string]any{"type": "subscription_started"}
	})
	client, err := panewire.NewHerdrClient(fixture.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	_, err = panewire.WaitAgent(context.Background(), client, "doomed", "idle", 40*time.Millisecond, 2500*time.Millisecond)
	if panewire.ExitCode(err) != panewire.ExitConditionInvalid || !strings.Contains(err.Error(), "gone") {
		t.Fatalf("lost occupant must fail with gone, got: %v", err)
	}
}

// wrkFindOracle is a Go port of bin/wrk find_cmd's matching (agent-skills
// repo, intentionally unmodified for #448): exact name match deduped by
// pane_id first, then exact tab-label match via tab.list deduped the same
// way. It returns the surviving pane ids — 0 means "no exact name or label
// match", 1 resolves, >1 is the ambiguous refusal wrk find exits 1 on.
func wrkFindOracle(agents []map[string]any, tabs []map[string]any, target string) []string {
	uniquePanes := func(items []map[string]any) []string {
		seen := map[string]bool{}
		var out []string
		for _, a := range items {
			p, _ := a["pane_id"].(string)
			if p != "" && !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
		return out
	}
	var named []map[string]any
	for _, a := range agents {
		if name, _ := a["name"].(string); name == target {
			named = append(named, a)
		}
	}
	if panes := uniquePanes(named); len(panes) > 0 {
		return panes
	}
	labelTabs := map[string]bool{}
	for _, tab := range tabs {
		if label, _ := tab["label"].(string); label == target {
			if id, _ := tab["tab_id"].(string); id != "" {
				labelTabs[id] = true
			}
		}
	}
	var labeled []map[string]any
	for _, a := range agents {
		if id, _ := a["tab_id"].(string); labelTabs[id] {
			labeled = append(labeled, a)
		}
	}
	return uniquePanes(labeled)
}

// AC-5 (option ②, test-pinned equivalence): on wrk find's input domain —
// exact names and exact tab labels — the unified resolver must produce the
// same result as wrk find's algorithm, including the name-beats-label tier,
// pane_id dedupe, the ambiguous refusal, and the not-found failure. wrk
// name-sync's taken-set is the same name lookup: a label is "taken" exactly
// when some agent name equals it, which is the resolver's identity tier.
func TestWrkFindEquivalenceOnSharedDomain(t *testing.T) {
	agents := []map[string]any{
		{"agent": "claude", "name": "solo", "pane_id": "p1", "tab_id": "t1", "agent_status": "idle", "agent_session": sessionField("s1")},
		{"agent": "codex", "name": "dup-name", "pane_id": "p2", "tab_id": "t2", "agent_status": "idle", "agent_session": sessionField("s2")},
		{"agent": "claude", "name": "dup-name", "pane_id": "p3", "tab_id": "t3", "agent_status": "idle", "agent_session": sessionField("s3")},
		{"agent": "claude", "name": "shared", "pane_id": "p4", "tab_id": "t4", "agent_status": "idle", "agent_session": sessionField("s4")},
		{"agent": "claude", "pane_id": "p5", "tab_id": "t5", "agent_status": "working", "agent_session": sessionField("s5")},
		{"agent": "claude", "pane_id": "p6", "tab_id": "t6", "agent_status": "idle", "agent_session": sessionField("s6")},
		{"agent": "codex", "pane_id": "p7", "tab_id": "t7", "agent_status": "idle", "agent_session": sessionField("s7")},
		{"agent": "claude", "name": "dupe-row", "pane_id": "p8", "tab_id": "t8", "agent_status": "idle", "agent_session": sessionField("s8")},
		{"agent": "claude", "name": "dupe-row", "pane_id": "p8", "tab_id": "t8", "agent_status": "idle", "agent_session": sessionField("s8")},
		{"agent": "codex", "name": "other", "pane_id": "p9", "tab_id": "t9", "agent_status": "working", "agent_session": sessionField("s9")},
		// record label and tab.list label disagree: wrk reads tab.list only,
		// so "tab-drift" must still resolve while "rec-label" is extension.
		{"agent": "claude", "name": "drift", "label": "rec-label", "pane_id": "p10", "tab_id": "t10", "agent_status": "idle", "agent_session": sessionField("s10")},
	}
	tabs := []map[string]any{
		{"tab_id": "t5", "label": "lab-single"},
		{"tab_id": "t6", "label": "lab-dup"},
		{"tab_id": "t7", "label": "lab-dup"},
		{"tab_id": "t9", "label": "shared"},
		{"tab_id": "t10", "label": "tab-drift"},
	}
	agentAny := make([]any, 0, len(agents))
	for _, a := range agents {
		agentAny = append(agentAny, a)
	}
	tabAny := make([]any, 0, len(tabs))
	for _, tab := range tabs {
		tabAny = append(tabAny, tab)
	}
	_, client := waitFixtureClient(t, agentAny, tabAny)

	cases := []struct {
		input       string
		wantPanes   []string // wrk oracle result; nil means "no exact match"
		waitStatus  string   // status the single resolved pane reports
		wantAmbig   bool
		wantMissing bool
	}{
		{input: "solo", wantPanes: []string{"p1"}, waitStatus: "idle"},
		{input: "lab-single", wantPanes: []string{"p5"}, waitStatus: "working"},
		// name-beats-label: "shared" is A4's name and t9's label. wrk resolves
		// the name tier only; if the resolver wrongly picked the label pane
		// (status working), waiting for idle would hang instead of succeeding.
		{input: "shared", wantPanes: []string{"p4"}, waitStatus: "idle"},
		// two agent rows on one pane_id dedupe to a single candidate.
		{input: "dupe-row", wantPanes: []string{"p8"}, waitStatus: "idle"},
		{input: "dup-name", wantPanes: []string{"p2", "p3"}, waitStatus: "idle", wantAmbig: true},
		{input: "lab-dup", wantPanes: []string{"p6", "p7"}, waitStatus: "idle", wantAmbig: true},
		// tab.list label wins even when the agent record carries a different
		// label field — wrk only ever reads tab.list.
		{input: "tab-drift", wantPanes: []string{"p10"}, waitStatus: "idle"},
		{input: "ghost-target", wantPanes: nil, waitStatus: "idle", wantMissing: true},
	}
	for _, tc := range cases {
		oracle := wrkFindOracle(agents, tabs, tc.input)
		if len(oracle) != len(tc.wantPanes) {
			t.Fatalf("input %q: oracle=%v want %v — test fixture drifted", tc.input, oracle, tc.wantPanes)
		}
		_, err := panewire.WaitAgent(context.Background(), client, tc.input, tc.waitStatus, 0, 2*time.Second)
		switch {
		case tc.wantAmbig:
			if panewire.ExitCode(err) != panewire.ExitConditionInvalid {
				t.Fatalf("input %q: ambiguous must fail, err=%v", tc.input, err)
			}
			for _, pane := range tc.wantPanes {
				if !strings.Contains(err.Error(), pane) {
					t.Fatalf("input %q: ambiguous error must list candidate %s, got %v", tc.input, pane, err)
				}
			}
		case tc.wantMissing:
			if panewire.ExitCode(err) != panewire.ExitConditionInvalid || !strings.Contains(err.Error(), "exact") {
				t.Fatalf("input %q: miss must fail with an exact-match message, err=%v", tc.input, err)
			}
		default:
			if err != nil {
				t.Fatalf("input %q: resolver diverged from wrk (want pane %v): %v", tc.input, tc.wantPanes, err)
			}
		}
	}
}

// AC-5 boundary, pinned honestly: panewire resolves inputs outside wrk's
// name/label domain (pane_id here; tab_id, title, harness name are the other
// extension keys). wrk find reports those as "no exact name or label match" —
// a truthful narrower contract, not a contradictory answer. This test exists
// so the superset boundary is fixed, not accidental.
func TestWrkExtensionDomainResolvesPaneID(t *testing.T) {
	agents := []map[string]any{
		{"agent": "claude", "name": "lane-a", "pane_id": "w1:p1", "tab_id": "w1:t1", "agent_status": "idle", "agent_session": sessionField("s1")},
	}
	agentAny := make([]any, 0, len(agents))
	for _, a := range agents {
		agentAny = append(agentAny, a)
	}
	_, client := waitFixtureClient(t, agentAny, nil)
	if oracle := wrkFindOracle(agents, nil, "w1:p1"); len(oracle) != 0 {
		t.Fatalf("oracle should not resolve pane_id input, got %v", oracle)
	}
	if _, err := panewire.WaitAgent(context.Background(), client, "w1:p1", "idle", 0, 2*time.Second); err != nil {
		t.Fatalf("pane_id extension input must resolve: %v", err)
	}
}

// AC-1/AC-2, prompt side: --to accepts a bare pane_id through the same
// resolver, and the delivery record pins the resolved pane_id.
func TestPromptResolvesPaneIDTarget(t *testing.T) {
	fixture := newHerdrFixture(t, promptFixtureSchema(false))
	defer fixture.Close()
	fixture.On("agent.list", func() any {
		return map[string]any{"agents": []any{map[string]any{
			"agent": "claude", "name": "orch", "pane_id": "w:p9", "workspace_id": "w1",
			"tab_id": "w:t9", "cwd": "/work", "revision": 10, "agent_status": "idle",
			"agent_session": sessionField("sess-9"),
		}}}
	})
	reads := 0
	fixture.On("agent.read", func() any {
		reads++
		if reads == 1 {
			return map[string]any{"text": "idle pane\n", "revision": 10}
		}
		return map[string]any{"text": promptEchoScreen, "revision": 11}
	})
	fixture.On("agent.prompt", func() any { return map[string]any{"accepted": true} })
	d, db := startPromptDaemon(t, fixture)
	defer d.Stop()
	path := filepath.Join(t.TempDir(), "prompt.md")
	if err := os.WriteFile(path, []byte("expect: name=orch cwd=/work\n\nR2-MARKER\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := panewire.RunCLI([]string{"prompt", "--from", "sender", "--to", "w:p9", "--file", path}, panewire.CLIConfig{SocketPath: dSocket(d)}); got != panewire.ExitOK {
		t.Fatalf("exit=%d want %d", got, panewire.ExitOK)
	}
	delivery, ok, err := db.LatestDelivery(t.Context())
	if err != nil || !ok {
		t.Fatalf("delivery=%+v ok=%v err=%v", delivery, ok, err)
	}
	if delivery.ResolvedPaneID != "w:p9" {
		t.Fatalf("resolved pane=%q want w:p9", delivery.ResolvedPaneID)
	}
}
