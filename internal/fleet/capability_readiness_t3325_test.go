package fleet

import (
	"context"
	"strings"
	"testing"
)

// hk 3325: a harness seat capability (cmd_dsflash41) is a fact about the
// executing node, taken from its readiness report. The hosts.toml boolean is
// only an explicit override, so a submitter whose config lacks [hosts.X] still
// places the seat on a node that reports the harness installed and logged in.

func cmdHarness(installed bool, login string) *Readiness {
	return &Readiness{Harnesses: map[string]HarnessReadiness{
		"cmd": {Installed: installed, HerdrIntegration: true, Login: login},
	}}
}

func TestCapabilityComesFromReadiness(t *testing.T) {
	present := map[string]ReadinessState{"node-a": {Readiness: cmdHarness(true, ReadinessLoginPresent)}}
	missing := map[string]ReadinessState{"node-a": {Readiness: cmdHarness(true, ReadinessLoginMissing)}}
	unknownLogin := map[string]ReadinessState{"node-a": {Readiness: cmdHarness(true, ReadinessLoginUnknown)}}
	for _, tc := range []struct {
		name   string
		states map[string]ReadinessState
		cfg    map[string]any
		want   bool
	}{
		{"ready node, no flag", present, map[string]any{}, true},
		{"ready node, override false wins", present, map[string]any{"cmd_dsflash41": false}, false},
		{"not logged in", missing, map[string]any{}, false},
		{"override true wins over a missing login", missing, map[string]any{"cmd_dsflash41": true}, true},
		{"unmeasured login keeps the config flag", unknownLogin, map[string]any{"cmd_dsflash41": true}, true},
		{"unmeasured login without a flag is off", unknownLogin, map[string]any{}, false},
		{"no readiness report keeps the config flag", nil, map[string]any{"cmd_dsflash41": true}, true},
		{"no readiness report without a flag is off", nil, map[string]any{}, false},
		{"unknown readiness keeps the config flag", map[string]ReadinessState{"node-a": {Unknown: true, Readiness: cmdHarness(true, ReadinessLoginPresent)}}, map[string]any{"cmd_dsflash41": true}, true},
		{"harness not installed", map[string]ReadinessState{"node-a": {Readiness: cmdHarness(false, ReadinessLoginPresent)}}, map[string]any{}, false},
		{"nonboolean override fails closed", present, map[string]any{"cmd_dsflash41": "true"}, false},
		{"operator-configured flag is not a harness fact", present, map[string]any{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flag := "cmd_dsflash41"
			if tc.name == "operator-configured flag is not a harness fact" {
				flag = "clinepass"
			}
			if got := capability(tc.states, "node-a", tc.cfg, flag); got != tc.want {
				t.Fatalf("capability(%s)=%t want %t", flag, got, tc.want)
			}
		})
	}
}

// placementPlanFor plans the cmd seat against one remote host whose hosts.toml
// entry is cfg and whose readiness report is state (nil: no report at all).
func placementPlanFor(t *testing.T, cfg map[string]any, state *ReadinessState) *placePlan {
	t.Helper()
	a := placementApp(t, &fakePlacementSource{hosts: []HostView{{Name: "node-a", Connected: true, LoadRatio: ptrF(0.1)}}})
	a.Config.Local = map[string]any{"spawn": false}
	a.Config.Hosts = map[string]map[string]any{"node-a": cfg}
	a.Config.HostOrder = []string{"node-a"}
	if state != nil {
		a.State = readinessSnapshot(t, a, map[string]ReadinessState{"node-a": *state}, a.Now())
	}
	p, e := a.plan(context.Background(), Spawn{Profile: "cmd-dsflash41"}, "", 0)
	if e != nil {
		t.Fatal(e)
	}
	return p
}

func TestPlacementHarnessCapabilityFromReadiness(t *testing.T) {
	ready := &ReadinessState{Readiness: cmdHarness(true, ReadinessLoginPresent)}
	notLoggedIn := &ReadinessState{Readiness: cmdHarness(true, ReadinessLoginMissing)}
	for _, tc := range []struct {
		name   string
		cfg    map[string]any
		state  *ReadinessState
		orOutc string // "" means the host is ordered
	}{
		// The submitter's config has no [hosts.node-a] flag: only the node report
		// names the capability. This case goes RED if placement reads the
		// placing host's config again.
		{"readiness says installed and logged in", map[string]any{}, ready, ""},
		{"override false wins over readiness", map[string]any{"cmd_dsflash41": false}, ready, "missing_capability:cmd_dsflash41"},
		{"readiness says not logged in", map[string]any{}, notLoggedIn, "missing_capability:cmd_dsflash41"},
		{"override true wins over a missing login", map[string]any{"cmd_dsflash41": true}, notLoggedIn, ""},
		{"unknown readiness keeps the config flag", map[string]any{"cmd_dsflash41": true}, nil, ""},
		{"unknown readiness without a flag is ineligible", map[string]any{}, nil, "missing_capability:cmd_dsflash41"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := placementPlanFor(t, tc.cfg, tc.state)
			if tc.orOutc == "" {
				if got := strings.Join(orderNames(p), ","); got != "node-a" {
					t.Fatalf("eligible host not ordered: %q (candidates %+v)", got, p.candidates)
				}
				return
			}
			if got := strings.Join(orderNames(p), ","); got != "" {
				t.Fatalf("ineligible host was ordered: %q", got)
			}
			c := p.candidate("node-a")
			if c == nil || c.outcome != tc.orOutc {
				t.Fatalf("outcome=%+v want %s", c, tc.orOutc)
			}
		})
	}
}
