package fleet

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestSeatCheckRunglessLaunch(t *testing.T) {
	p, e := LoadPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	host := map[string]any{"clinepass": true}
	for _, tt := range []struct {
		seat, model string
		want        int
	}{
		{"agy-flash38-high", "gemini-3.8-flash-high", 0},
		{"devin-swe2-medium", "swe-2-medium", 0},
		{"cc-qwen38", "qwen3.8-max", 0},
		{"opus", "claude-opus-5-5", 70},     // the claude launcher passes the rung
		{"codex-luna", "gpt-6-luna", 70},    // so does codex
		{"cmd-dsflash41", "provider/m", 70}, // and cmd
	} {
		seat := p.Profiles[tt.seat]
		l := Launch{Profile: seat.Canon, ModelID: tt.model, Grade: "A", Gate: "default"}
		if got := code(seat.Check(l, "implementer", "B", "", "", host)); got != tt.want {
			t.Errorf("%s: code %d want %d", tt.seat, got, tt.want)
		}
	}
}

func TestSeatCheckRoutedSeatNeedsCapability(t *testing.T) {
	p, _ := LoadPolicy("")
	seat := p.Profiles["cc-glm"]
	l := Launch{Profile: "cc-glm", ModelID: "glm-5.2", Grade: "B", Gate: "default"}
	if got := code(seat.Check(l, "implementer", "B", "", "", map[string]any{})); got != 70 {
		t.Fatalf("a routed seat on a host without the route capability must refuse 70, got %d", got)
	}
}

func TestPolicyAgyAndCCSeats(t *testing.T) {
	p, e := LoadPolicy("")
	if e != nil {
		t.Fatal(e)
	}
	for _, n := range []string{"agy-flash38-high", "agy-flash38-medium", "agy-flash38-low", "agy-sonnet46", "agy-opus46", "agy-oss120"} {
		s, ok := p.Profiles[n]
		if !ok || s.Kind != "agy" || s.Gate != "" || s.gateName() != n || s.Canon != n || s.Effort != "" || s.needsEffort() {
			t.Errorf("%s: %+v", n, s)
		}
		if slices.Contains(s.Roles, "builder") {
			t.Errorf("%s must not take builder", n)
		}
	}
	for _, n := range []string{"cc-qwen38", "cc-glm"} {
		s := p.Profiles[n]
		if s.Kind != "claude" || s.Route != "clinepass" || s.Capability != "clinepass" || s.needsEffort() {
			t.Errorf("%s: %+v", n, s)
		}
	}
}

func TestLoadPolicyRejectsBadAliasAndRoute(t *testing.T) {
	dir := t.TempDir()
	for name, seat := range map[string]string{
		"gate":  `"kind":"agy","gate":"../x"`,
		"route": `"kind":"agy","route":"clinepass"`,
		"nope":  `"kind":"claude","route":"nowhere"`,
		"rung":  `"kind":"claude","route":"clinepass","effort":"high"`,
	} {
		path := filepath.Join(dir, name+".json")
		doc := `{"version":1,"profiles":{"x":{"canon":"x","family":"f","capability":"standard","roles":["implementer"],` + seat + `}}}`
		if e := os.WriteFile(path, []byte(doc), 0600); e != nil {
			t.Fatal(e)
		}
		if _, e := LoadPolicy(path); code(e) != 70 {
			t.Errorf("%s: must refuse 70: %v", name, e)
		}
	}
}

func TestCLIQuotaGateUsesGateName(t *testing.T) {
	f := &fakeRunner{Do: func(Command) Result { return Result{Out: `{}`} }}
	q := CLIQuota{Runner: f, Binary: "quota"}
	if _, e := q.Gate(context.Background(), QuotaRequest{Profile: "agy-sonnet46", GateProfile: "agy-flash38-high", Role: "implementer"}); e != nil {
		t.Fatal(e)
	}
	if got := strings.Join(f.Calls[0].Args, " "); got != "gate -m agy-flash38-high --role implementer --json" {
		t.Fatalf("gate must be called by its alias and without --effort: %s", got)
	}
	f.Calls = nil
	q.Launch(context.Background(), QuotaRequest{Profile: "agy-sonnet46", GateProfile: "agy-flash38-high"})
	if got := strings.Join(f.Calls[0].Args, " "); got != "policy launch agy-sonnet46 --json" {
		t.Fatalf("launch resolves the canon, not the alias: %s", got)
	}
}

func TestLaunchArgsRungless(t *testing.T) {
	a := testApp(t)
	p, _ := LoadPolicy("")
	argv, env, e := a.launchArgs(p.Profiles["agy-flash38-low"], Launch{ModelID: "gemini-3.8-flash-low"})
	if e != nil || strings.Contains(strings.Join(argv, " "), "effort") || len(env) != 0 {
		t.Fatalf("agy passes no effort: %v %v %v", argv, env, e)
	}
	// A routed claude harness passes no --effort and reads its key at spawn.
	keyFile := filepath.Join(a.Home, "gate-key")
	t.Setenv("CLINEPASS_GATE_KEY_FILE", keyFile)
	t.Setenv("CLINEPASS_GATE_URL", "http://route.invalid:1")
	seat := p.Profiles["cc-glm"]
	if _, _, e = a.launchArgs(seat, Launch{ModelID: "glm-5.2"}); code(e) != 70 {
		t.Fatalf("a missing key file must refuse 70: %v", e)
	}
	if e = os.WriteFile(keyFile, []byte("secret-key\n"), 0600); e != nil {
		t.Fatal(e)
	}
	argv, env, e = a.launchArgs(seat, Launch{ModelID: "glm-5.2"})
	if e != nil || strings.Join(argv, " ") != "--model glm-5.2 --dangerously-skip-permissions" {
		t.Fatalf("%v %v", argv, e)
	}
	want := []string{"ANTHROPIC_BASE_URL=http://route.invalid:1", "ANTHROPIC_AUTH_TOKEN=secret-key", "CLAUDE_CODE_MAX_CONTEXT_TOKENS=1000000"}
	if !slices.Equal(env, want) {
		t.Fatalf("%v", env)
	}
	// A claude seat with a rung still passes it.
	argv, _, _ = a.launchArgs(p.Profiles["opus"], Launch{ModelID: "m", Effort: "high"})
	if !slices.Contains(argv, "--effort") {
		t.Fatalf("%v", argv)
	}
	if got := launchProfile("agy-flash38-low", ""); got != "agy-flash38-low" {
		t.Fatal(got)
	}
}

func agyRows() []CatalogRow {
	return []CatalogRow{
		{Profile: "agy-flash38-high", Grade: "A", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-high"},
		{Profile: "agy-flash38-medium", Grade: "B", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-medium"},
		{Profile: "agy-flash38-low", Grade: "C", Pool: "agy", Gate: "default", ModelID: "gemini-3.8-flash-low"},
		{Profile: "agy-opus46", Grade: "S", Pool: "agy", Gate: "escalation", ModelID: "claude-opus-4-6-thinking"},
		{Profile: "fable", Grade: "S+", Pool: "claude", Gate: "consult_only", ModelID: "claude-fable-5-1"},
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "m"},
	}
}

func TestSelectAdmitsAgyRungless(t *testing.T) {
	waste := true
	pools := map[string]PoolView{"agy": {Status: "ok", Class: "spend", Waste: &waste}, "claude": {Status: "ok", Class: "spend"}}
	a, s, q := selFixture(t, "B", agyRows(), pools, []string{"agy", "claude"})
	q.GateDo = gatePools(withAgyGates(map[string]string{"sonnet": "claude"}))
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil {
		t.Fatal(e)
	}
	c := sel.chosen
	if c.seatName != "agy-flash38-medium" || c.row.Effort != "" {
		t.Fatalf("the cheapest covering waste-pool rung must win: %+v", c.row)
	}
	rec := sel.record()
	for profile, want := range map[string]string{"agy-flash38-high": "below_choice", "agy-flash38-low": "dropped:grade_insufficient", "agy-opus46": "dropped:gate_class", "fable": "dropped:gate_class"} {
		if got := candOutcome(rec, profile, "")["outcome"]; got != want {
			t.Errorf("%s: %v want %s", profile, got, want)
		}
	}
	last := q.Calls[len(q.Calls)-1]
	if last.GateProfile != "agy-flash38-medium" || last.Profile != "agy-flash38-medium" || last.Effort != "" {
		t.Fatalf("the gate is called by the profile's own name with no effort: %+v", last)
	}
}

func TestSelectEscalationAgyNeedsAllowEscalation(t *testing.T) {
	waste := true
	pools := map[string]PoolView{"agy": {Status: "ok", Class: "spend", Waste: &waste}}
	rows := []CatalogRow{agyRows()[3]}
	a, s, q := selFixture(t, "S", rows, pools, []string{"agy"})
	q.GateDo = gatePools(withAgyGates(nil))
	if _, e := a.selectProfile(context.Background(), s); code(e) != 77 {
		t.Fatalf("an escalation rung must not be selected by default: %v", e)
	}
	s.AllowEscalation = true
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "agy-opus46" {
		t.Fatalf("%v", e)
	}
}

func TestSelectRunglessSeatNeedingEffortStaysDropped(t *testing.T) {
	rows := []CatalogRow{{Profile: "kiro-cheap", Grade: "C", Pool: "kiro", Gate: "default", ModelID: "m"}, {Profile: "oc-glm", Grade: "B", Pool: "clinepass", Gate: "default", ModelID: "glm-5.2"}}
	a, s, q := selFixture(t, "C", rows, map[string]PoolView{"kiro": {Status: "ok", Class: "spend"}, "clinepass": {Status: "ok", Class: "spend"}}, []string{"kiro", "clinepass"})
	q.GateDo = gatePools(map[string]string{"kiro-cheap": "kiro", "oc-glm": "clinepass"})
	sel, e := a.selectProfile(context.Background(), s)
	if e != nil || sel.chosen.seatName != "oc-glm" {
		t.Fatalf("an opencode seat has no effort to pass: %v", e)
	}
	if got := candOutcome(sel.record(), "kiro-cheap", "")["outcome"]; got != "dropped:effort_empty" {
		t.Fatalf("the kiro launcher passes an effort, so its rung-less row stays dropped: %v", got)
	}
}

func TestSpawnEscalationAgySeatRefusesWithoutFlag(t *testing.T) {
	a, s, f, _ := cmdFixture(t)
	s.Profile = "agy-opus46"
	q := a.Quota.(*fakeQuota)
	q.LaunchValue = Launch{Profile: "agy-opus46", ModelID: "claude-opus-4-6-thinking", Grade: "S", Gate: "escalation", Pool: "agy"}
	// An operator request admits an escalation row at the gate, but never
	// stands in for --allow-escalation (hk 3859 review finding).
	s.OperatorRequest = "hk:1"
	e := a.Spawn(context.Background(), s)
	if code(e) != 77 || !strings.Contains(e.Error(), "--allow-escalation") {
		t.Fatalf("-m on an escalation rung needs --allow-escalation: %v", e)
	}
	for _, c := range q.Calls {
		if c.GateProfile != "" && c.GateProfile != "agy-opus46" {
			t.Fatalf("%+v", c)
		}
	}
	if _, e := os.Stat(a.ledger().DB); e == nil {
		t.Fatal("a refused spawn must claim nothing")
	}
	_ = f
	s.AllowEscalation = true
	a.Spawn(context.Background(), s)
	last := q.Calls[len(q.Calls)-1]
	if last.GateProfile != "agy-opus46" || last.Effort != "" {
		t.Fatalf("an allowed spawn gates by its own name with no effort: %+v", last)
	}
}

func TestRoutedSeatTakesNoEffort(t *testing.T) {
	a := testApp(t)
	p, _ := LoadPolicy("")
	seat := p.Profiles["cc-glm"]
	// Check refuses a launch that resolved a rung for a routed seat.
	l := Launch{Profile: "cc-glm", ModelID: "glm-5.2", Effort: "high", Grade: "B", Gate: "default"}
	if got := code(seat.Check(l, "implementer", "B", "", "", map[string]any{"clinepass": true})); got != 70 {
		t.Fatalf("a routed seat with a rung must refuse 70, got %d", got)
	}
	// Even if one reached the launcher, no --effort flag is passed.
	keyFile := filepath.Join(a.Home, "gate-key")
	t.Setenv("CLINEPASS_GATE_KEY_FILE", keyFile)
	if e := os.WriteFile(keyFile, []byte("k\n"), 0600); e != nil {
		t.Fatal(e)
	}
	argv, _, e := a.launchArgs(seat, l)
	if e != nil || slices.Contains(argv, "--effort") {
		t.Fatalf("%v %v", argv, e)
	}
	// Auto selection keeps a routed seat off a runged catalog row.
	rows := []CatalogRow{{Profile: "cc-glm", Effort: "high", Grade: "B", Pool: "clinepass", Gate: "default", ModelID: "glm-5.2"}}
	a2, s, q := selFixture(t, "B", rows, map[string]PoolView{"clinepass": {Status: "ok", Class: "spend"}}, []string{"clinepass"})
	q.GateDo = gatePools(map[string]string{"cc-glm": "clinepass"})
	sel, e := a2.selectProfile(context.Background(), s)
	if code(e) != 77 || candOutcome(sel.record(), "cc-glm", "high")["outcome"] != "dropped:seat_rung" {
		t.Fatalf("%v", e)
	}
}

func TestRouteKeyFileExpandsHome(t *testing.T) {
	a := testApp(t)
	p, _ := LoadPolicy("")
	if e := os.WriteFile(filepath.Join(a.Home, "key"), []byte("k\n"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("CLINEPASS_GATE_KEY_FILE", "~/key")
	_, env, e := a.launchArgs(p.Profiles["cc-glm"], Launch{ModelID: "glm-5.2"})
	if e != nil || !slices.Contains(env, "ANTHROPIC_AUTH_TOKEN=k") {
		t.Fatalf("a leading ~/ must expand against the home dir: %v %v", env, e)
	}
}
