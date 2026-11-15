package fleet

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A host the hub reports connected while ssh cannot reach it (hk 2647):
// it holds a codex account only it has and shares a claude account with
// the local host. The claude reading is stale, so it needs a refresh; the
// local holder can do it, the hub-only host over ssh cannot.
const (
	hubOnlyHost   = "host-hub"
	hubOnlyClaude = "claude:00000000000000a1"
	hubOnlyCodex  = "codex:00000000000000c3"
)

type hubOnlyEnv struct {
	a    *App
	s    Spawn
	q    *fakeQuota
	src  *fakePlacementSource
	ssh  []string
	pace []string
}

func hubOnlyFixture(t *testing.T) *hubOnlyEnv {
	t.Helper()
	rows := []CatalogRow{
		{Profile: "sonnet", Effort: "high", Grade: "A", Pool: "claude", Gate: "default", ModelID: "sonnet-m"},
		{Profile: "codex-terra61", Effort: "high", Grade: "A+", Pool: "codex", Gate: "default", ModelID: "terra-m"},
	}
	pools := map[string]PoolView{
		"claude": {Status: "ok", Class: "spend", Blocking: ptrF(20)},
		"codex":  {Status: "ok", Class: "spend", Blocking: ptrF(30)},
	}
	a, s, q := selFixture(t, "C", rows, pools, nil)
	e := &hubOnlyEnv{a: a, s: s, q: q}
	e.src = a.Source.(*fakePlacementSource)
	e.src.hosts = []HostView{{Name: hubOnlyHost, Connected: true, Score: ptrF(10)}, {Name: "local", Connected: true, Score: ptrF(1)}}
	e.src.presence = []PresenceRecord{
		{HostID: "local", AccountKey: hubOnlyClaude, LastSeen: a.Now()},
		{HostID: hubOnlyHost, AccountKey: hubOnlyClaude, LastSeen: a.Now()},
		{HostID: hubOnlyHost, AccountKey: hubOnlyCodex, LastSeen: a.Now()},
	}
	e.src.accounts[hubOnlyClaude] = SharedAccount{Key: hubOnlyClaude, Observed: true, MeasuredAt: a.Now().Add(-2 * time.Hour)}
	e.src.accounts[hubOnlyCodex] = SharedAccount{Key: hubOnlyCodex, Observed: true, Fresh: true, MeasuredAt: a.Now(), State: &StateUse{Source: stateSourceHub}}
	a.Config.Hosts = map[string]map[string]any{hubOnlyHost: {"ssh": "hub-only-ssh"}, "other": {}}
	a.Config.HostOrder = []string{hubOnlyHost, "other"}
	q.GateDo = gatePools(map[string]string{"sonnet": "claude", "codex-terra61": "codex"})
	a.Runner = wrapRunner(a.Runner, func(c Command) (Result, bool) {
		switch {
		case c.Name == "ssh":
			e.ssh = append(e.ssh, strings.Join(c.Args, " "))
			return Result{Code: exitUnreachable, Err: "ssh: connect to host hub-only-ssh: Operation timed out"}, true
		case c.Name == "panewire" && len(c.Args) == 3 && c.Args[0] == "quota" && c.Args[1] == "refresh", c.Name == "pacelever" && len(c.Args) == 2 && c.Args[0] == "refresh":
			pool := c.Args[len(c.Args)-1]
			e.pace = append(e.pace, pool)
			if pool == "claude" {
				e.src.accounts[hubOnlyClaude] = SharedAccount{Key: hubOnlyClaude, Observed: true, Fresh: true, MeasuredAt: a.Now()}
			}
			return Result{}, true
		}
		return Result{}, false
	})
	return e
}

func writeBrief(t *testing.T, a *App) string {
	t.Helper()
	p := filepath.Join(a.Home, "brief.md")
	if e := os.WriteFile(p, []byte("Implement the requested change.\n"), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}

// planChoice runs `panewire plan` and reads the account and host it admits.
func planChoice(t *testing.T, e *hubOnlyEnv) (string, string, string) {
	t.Helper()
	buf := e.a.Out.(interface{ String() string })
	before := len(buf.String())
	if err := e.a.planCmd(context.Background(), []string{"--grade", "C", "--t", "T1", "--role", "implementer", "--json"}); err != nil {
		t.Fatalf("plan refused: %v", err)
	}
	var out struct {
		Chosen struct {
			Seat       string `json:"seat"`
			AccountKey string `json:"account_key"`
			Host       string `json:"host"`
		} `json:"chosen"`
	}
	if err := json.Unmarshal([]byte(buf.String()[before:]), &out); err != nil {
		t.Fatal(err)
	}
	return out.Chosen.Seat, out.Chosen.AccountKey, out.Chosen.Host
}

func chosenOn(t *testing.T, sel *selPlan, err error) (string, string, string) {
	t.Helper()
	if err != nil || sel == nil || sel.chosen == nil {
		t.Fatalf("not admitted: %v", err)
	}
	return sel.chosen.seatName, sel.chosen.accountKey, sel.chosen.host
}

// Refusals 1 and 2: `-m auto --host HOST` and `--host auto --exclude-host
// <every other host>` refused NO_ELIGIBLE_HOST while the unrestricted plan
// admitted HOST. The restriction left HOST the claude account's only
// measurer; its failed ssh refresh took the host down for every account,
// the hub-fresh codex account included.
func TestHubOnlyHostRestrictedSpawnAdmitsLikePlan(t *testing.T) {
	planSeat, planKey, planHost := planChoice(t, hubOnlyFixture(t))
	if planHost != hubOnlyHost {
		t.Fatalf("plan chose %s/%s on %s, want %s", planSeat, planKey, planHost, hubOnlyHost)
	}
	for name, restrict := range map[string]func(*Spawn){
		"host":         func(s *Spawn) { s.Host = hubOnlyHost },
		"exclude_host": func(s *Spawn) { s.ExcludeHosts = "local,other" },
	} {
		t.Run(name, func(t *testing.T) {
			e := hubOnlyFixture(t)
			restrict(&e.s)
			sel, err := e.a.selectProfile(context.Background(), e.s)
			seat, key, host := chosenOn(t, sel, err)
			if seat != planSeat || key != planKey || host != planHost {
				t.Fatalf("spawn admitted %s/%s on %s; plan admitted %s/%s on %s", seat, key, host, planSeat, planKey, planHost)
			}
			if len(e.pace) != 1 || e.pace[0] != "claude" || len(e.ssh) != 0 {
				t.Fatalf("the shared account was not measured on its local holder: pacelever=%v ssh=%v", e.pace, e.ssh)
			}
		})
	}
}

// The restriction still decides placement: a restricted holder measures,
// it is never judged or chosen.
func TestHubOnlyHostRestrictedHolderIsNotPlaced(t *testing.T) {
	e := hubOnlyFixture(t)
	e.s.Host = hubOnlyHost
	sel, err := e.a.selectProfile(context.Background(), e.s)
	chosenOn(t, sel, err)
	for _, c := range sel.cands {
		for _, j := range c.judgments {
			if j.Host != hubOnlyHost {
				t.Fatalf("%s/%s judged on restricted host %s", c.seatName, c.accountKey, j.Host)
			}
		}
	}
	// Held only by HOST, an unmeasurable account still refuses, and
	// the host refusal stays scoped to --host.
	e = hubOnlyFixture(t)
	e.s.Host = hubOnlyHost
	e.src.presence = e.src.presence[1:]
	if _, err := e.a.selectProfile(context.Background(), e.s); exitCode(err) != exitPlacement || !strings.Contains(err.Error(), "--host host-hub restricts accounts") {
		t.Fatalf("unmeasurable only holder: %v", err)
	}
	if len(e.ssh) != 1 || !strings.Contains(e.ssh[0], "hub-only-ssh panewire quota refresh 'claude'") {
		t.Fatalf("only holder was not measured over its transport: %v", e.ssh)
	}
}

// Refusal 3: `-m codex-terra61 --host HOST` named no_usable_presence
// although HOST holds the account. The pinned seat's default effort
// (low) has no placed rung, so the pool lookup failed and the account was
// never looked up; the refusal now names that cause.
func TestHubOnlyHostPinnedProfileNamesUnplacedRung(t *testing.T) {
	e := hubOnlyFixture(t)
	e.s.Profile, e.s.Host, e.s.OverrideReason, e.s.DryRun = "codex-terra61", hubOnlyHost, "hk 2647", true
	e.s.Prompt, e.s.Workspace, e.s.Label, e.s.Containment = writeBrief(t, e.a), "workers", "job-a", "hk:12"
	e.q.LaunchErr = fail(exitConfig, "quota catalog: Error: e6_arm_required: rung is not placed")
	err := e.a.Spawn(context.Background(), e.s)
	if err == nil || strings.Contains(err.Error(), "no_usable_presence") || !strings.Contains(err.Error(), "codex-terra61@low unresolved") || !strings.Contains(err.Error(), "e6_arm_required") {
		t.Fatalf("pinned unplaced rung: %v", err)
	}
	// The placed rung reaches the host's shared account and its gate.
	e = hubOnlyFixture(t)
	e.s.Profile, e.s.Host, e.s.OverrideReason, e.s.DryRun, e.s.Effort = "codex-terra61", hubOnlyHost, "hk 2647", true, "high"
	e.s.Prompt, e.s.Workspace, e.s.Label, e.s.Containment = writeBrief(t, e.a), "workers", "job-a", "hk:12"
	e.q.LaunchValue = Launch{Profile: "codex-terra61", ModelID: "terra-m", Effort: "high", Grade: "A+", Gate: "default", Pool: "codex"}
	e.a.Config.Hosts[hubOnlyHost]["cwd_map"] = map[string]any{e.s.CWD: "/remote/repo"}
	if err := e.a.Spawn(context.Background(), e.s); err != nil {
		t.Fatalf("placed pinned rung refused: %v", err)
	}
	if out := e.a.Out.(interface{ String() string }).String(); !strings.Contains(out, "dry-run host=host-hub profile=codex-terra61") {
		t.Fatalf("dry run did not reach %s: %s", hubOnlyHost, out)
	}
	if got := e.q.Calls[len(e.q.Calls)-1]; got.AccountKey != hubOnlyCodex {
		t.Fatalf("gate judged %q, want the host's shared account", got.AccountKey)
	}
}
