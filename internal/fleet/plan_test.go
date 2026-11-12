package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// planFixture is a live-shaped plan: the claude rung at waste risk against a
// free SWE-2 rung, local spawning disabled and one remote host holding the
// claude account, so the plan has to ask the remote host's quota gate.
func planFixture(t *testing.T) (*App, *fakeQuota, *fakeRunner, *fakeJobStore, *bytes.Buffer) {
	t.Helper()
	// codex has a window already used up: it is excluded and plan prints it
	// as dropped. (A pool merely on pace to use up its week stays a paced
	// candidate; see select_credit_pacing_t2844_test.go.)
	pools := map[string]PoolView{"devin": {Status: "ok", Class: "spend", Blocking: ptrF(4)}, "claude": riskPool(32.6, 8), "codex": {Status: "ok", Class: "spend", Blocking: ptrF(100), Projected: ptrF(120)}}
	rows := append(riskRows(), CatalogRow{Profile: "codex-luna", Effort: "high", Grade: "A", Pool: "codex", Gate: "default", ModelID: "m"})
	a, _, q := selFixture(t, "A", rows, pools, []string{"devin", "claude", "codex"})
	const key = "claude:00000000000000aa"
	src := a.Source.(*fakePlacementSource)
	src.hosts = []HostView{{Name: "host-b", Connected: true}}
	src.presence = append(src.presence, PresenceRecord{HostID: "presence-b", AccountKey: key, LastSeen: a.Now()})
	// All three pools are present on an eligible host. Local is disabled.
	for _, rec := range append([]PresenceRecord{}, src.presence...) {
		if rec.HostID == "local" {
			rec.HostID = "presence-b"
			src.presence = append(src.presence, rec)
		}
	}
	src.accounts[key] = SharedAccount{Key: key, Observed: true, Fresh: true}
	a.Config.Local["spawn"] = false
	a.Config.Hosts = map[string]map[string]any{"host-b": {"presence_id": "presence-b"}}
	a.Config.HostOrder = []string{"host-b"}
	q.LaunchValue = Launch{Profile: "sonnet", ModelID: "m", Effort: "high", Grade: "A", Gate: "default", Pool: "claude"}
	q.GateDo = func(r QuotaRequest) (string, error) {
		pool := map[string]string{"devin-swe2": "devin", "sonnet": "claude"}[firstNonempty(r.GateProfile, r.Profile)]
		if r.AccountKey != "" {
			return `{"schema":"scopefuel.gate.v1","provider_id":"` + pool + `","account_key":"` + r.AccountKey + `","ok":true,"exit_code":0}`, nil
		}
		return "pool=" + pool + "\n", nil
	}
	fr := &fakeRunner{}
	a.Runner = fr
	store := newFakeJobStore()
	a.JobStore = store
	out := &bytes.Buffer{}
	a.Out = out
	return a, q, fr, store, out
}

func TestPlanPrintsHostAndModelWithoutSideEffects(t *testing.T) {
	a, q, fr, store, out := planFixture(t)
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1"}); e != nil {
		t.Fatalf("%v\n%s", e, out.String())
	}
	text := out.String()
	if !strings.Contains(text, "chosen host=host-b seat=sonnet@high model=m pool=claude") || !strings.Contains(text, "waste_risk=true") {
		t.Fatalf("plan prints the chosen host and model:\n%s", text)
	}
	if !strings.Contains(text, "runner-up devin-swe2@high model=swe-2") {
		t.Fatalf("plan prints the runner-up:\n%s", text)
	}
	if !strings.Contains(text, "dropped codex-luna@high pool=codex reason=window_exhausted") {
		t.Fatalf("plan prints a pool-excluded rung as dropped:\n%s", text)
	}
	remote := 0
	for _, c := range q.Calls {
		if c.AccountKey != "" {
			remote++
		}
	}
	if remote == 0 {
		t.Fatalf("plan must ask the remote candidate's quota gate: %+v", q.Calls)
	}
	if len(store.calls) != 0 {
		t.Fatalf("plan must not touch the job store: %v", store.calls)
	}
	// The only host contact is the read-only preflight: one probe script per
	// remote host and the version of the local panewire.
	probes := 0
	for _, c := range fr.Calls {
		switch {
		case c.Name == "ssh" && len(c.Args) > 0 && c.Args[len(c.Args)-1] == probeScript:
			probes++
		case c.Name == "panewire" && strings.Join(c.Args, " ") == "--version":
		case c.Name == "handoffkeep" || c.Name == "herdr" || c.Name == "panewire" || c.Name == "ssh":
			t.Fatalf("plan must not spawn or write: %v", fr.Calls)
		}
	}
	if probes != 1 {
		t.Fatalf("plan probes each remote host once, got %d: %v", probes, fr.Calls)
	}
	if _, e := os.Stat(a.ledger().DB); e == nil {
		t.Fatal("plan must not touch the ledger")
	}
	if entries, _ := os.ReadDir(a.Jobs); len(entries) != 0 {
		t.Fatalf("plan must not write job state: %v", entries)
	}
}

func TestPlanJSONAndExitCodes(t *testing.T) {
	a, _, _, _, out := planFixture(t)
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1", "--json"}); e != nil {
		t.Fatal(e)
	}
	var doc map[string]any
	if e := json.Unmarshal(out.Bytes(), &doc); e != nil {
		t.Fatalf("--json output: %v\n%s", e, out.String())
	}
	chosen, _ := doc["chosen"].(map[string]any)
	if chosen["host"] != "host-b" || chosen["model_id"] != "m" || chosen["seat"] != "sonnet" {
		t.Fatalf("chosen: %v", doc["chosen"])
	}
	if doc["placement"] == nil || doc["selection"].(map[string]any)["mode"] != "plan" {
		t.Fatalf("plan carries both records: %v", doc)
	}

	// The remote host refuses: no host admits the seat → exit 80 (local
	// spawning disabled, no remote fallback).
	a, q, _, _, _ := planFixture(t)
	gate := q.GateDo
	q.GateDo = func(r QuotaRequest) (string, error) {
		if r.AccountKey != "" && r.Profile == "sonnet" && len(q.Calls) > 1 {
			return "", fail(3, "quota exhausted")
		}
		return gate(r)
	}
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1"}); exitCode(e) != exitLocalDisabled {
		t.Fatalf("want exit %d, got %v", exitLocalDisabled, e)
	}

	// Nothing admitted by selection → 77.
	a, q, _, _, _ = planFixture(t)
	q.GateDo = func(QuotaRequest) (string, error) {
		return "", &gateRefusal{cause: fail(3, "refused"), outcome: outcomeNoRoleCandidate}
	}
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1"}); exitCode(e) != exitSeatRefused {
		t.Fatalf("want exit %d, got %v", exitSeatRefused, e)
	}

	for _, args := range [][]string{{"--t", "T1"}, {"--grade", "A"}, {"--grade", "Z", "--t", "T1"}, {"--grade", "A", "--t", "T1", "--role", "tester"}} {
		if e := a.planCmd(context.Background(), args); exitCode(e) != exitUsage {
			t.Fatalf("%v: want usage exit, got %v", args, e)
		}
	}
}

// A disabled local host is excluded by the shared plan() before planHosts
// walks the ordered candidates, so even a local launch and gate that would
// admit never yield a local placement: the remote host is chosen, and the
// local row records the exclusion.
func TestPlanNeverPlacesOnDisabledLocal(t *testing.T) {
	a, _, _, _, out := planFixture(t)
	if e := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1", "--json"}); e != nil {
		t.Fatal(e)
	}
	var doc map[string]any
	if e := json.Unmarshal(out.Bytes(), &doc); e != nil {
		t.Fatal(e)
	}
	if host := doc["chosen"].(map[string]any)["host"]; host != "host-b" {
		t.Fatalf("a disabled local host must not be chosen: %v", host)
	}
	found := false
	for _, c := range doc["placement"].(map[string]any)["candidates"].([]any) {
		row := c.(map[string]any)
		if row["local"] == true {
			found = true
			if row["outcome"] != "spawn_disabled" || row["chosen"] == true {
				t.Fatalf("local row: %v", row)
			}
		}
	}
	if !found {
		t.Fatal("placement record lacks the local candidate")
	}
}

func TestPlanPoolCapReportsLiveCount(t *testing.T) {
	a, _, _, _, out := planFixture(t)
	a.Config.Quota = map[string]any{"pool_max_concurrent": map[string]any{"devin-free": int64(2)}}
	a.LiveSpawnProfiles = func(context.Context, string) (map[string]int, error) {
		return map[string]int{"devin-swe2": 2}, nil
	}
	if err := a.planCmd(context.Background(), []string{"--grade", "A", "--t", "T1", "--json"}); err != nil {
		t.Fatalf("plan: %v\n%s", err, out.String())
	}
	var doc struct {
		Selection struct {
			Candidates []map[string]any `json:"candidates"`
		} `json:"selection"`
	}
	if err := json.Unmarshal(out.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	for _, row := range doc.Selection.Candidates {
		if row["profile"] == "devin-swe2" && row["outcome"] == "dropped:pool_at_cap" {
			if row["cap"] != "devin-free" || row["cap_live"] != float64(2) || row["cap_limit"] != float64(2) {
				t.Fatalf("free seat cap evidence = %v, want devin-free with 2 live and limit 2", row)
			}
			return
		}
	}
	t.Fatalf("missing dropped free seat: %v", doc.Selection.Candidates)
}
