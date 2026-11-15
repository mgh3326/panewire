package fleet

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

// This file uses only what the code had before the placement shadow (hk 3650),
// so it compiles and runs on the base commit too. testdata/
// placement_shadow_live_base.json holds the live output of every scenario
// below as the BASE produced it; TestPlacementShadowLiveMatchesBase_T3650
// compares the head with it. Regenerate only from the base commit:
//
//	go test -run PlacementShadowLiveMatchesBase ./internal/fleet/ -args -write-live-base
const liveBaseGolden = "testdata/placement_shadow_live_base.json"

var writeLiveBase = flag.Bool("write-live-base", false, "write the live-output golden (run on the base commit only)")

// shadowEvents collects the fleet events of an App.
func shadowEvents(a *App) *[]fleetevent.Event {
	var events []fleetevent.Event
	a.EventSink = func(e fleetevent.Event) { events = append(events, e) }
	return &events
}

// workerFleet is the host-first fixture for a worker, every fit host holding
// account B, with the hub's score for each host (a fleet-state reading: no
// Decided).
func workerFleet(t *testing.T, scores map[string]float64) (*App, Spawn, *fakePlacementSource) {
	t.Helper()
	a, s, _, src := hostFirstFixture(t, "implementer", "B", workerRows,
		map[string]string{"mac-b": testClaudeB, "cloud-small": testClaudeB, "cloud-tiny": testClaudeB, "pi": testClaudeB}, nil)
	for i := range src.hosts {
		if sc, ok := scores[src.hosts[i].Name]; ok {
			src.hosts[i].Score = ptrF(sc)
		}
	}
	return a, s, src
}

func sigs(power float64) PlacementSignals {
	return PlacementSignals{Power: ptrF(power), LiveJobs: ptrI(0), BridgedJobs: ptrI(0), MemAvailable: ptrF(0.8), Load1PerCore: ptrF(0.1)}
}

// liveRecord is everything a caller can observe about one selection except
// the placement_shadow event: the error, the chosen pair, the full host/rung
// trace and the other fleet events.
func liveRecord(t *testing.T, a *App, s Spawn, events *[]fleetevent.Event) string {
	t.Helper()
	sel, err := a.selectProfile(context.Background(), s)
	out := map[string]any{}
	if err != nil {
		out["err"] = err.Error()
	}
	if sel != nil {
		out["record"] = sel.record()
		if sel.chosen != nil {
			out["chosen"] = sel.chosen.host + "/" + sel.chosen.accountKey
		}
	}
	var others []fleetevent.Event
	for _, e := range *events {
		if e.Kind != "placement_shadow" {
			e.TS, e.Host = "", ""
			others = append(others, e)
		}
	}
	out["events"] = others
	b, e := json.Marshal(out)
	if e != nil {
		t.Fatal(e)
	}
	return string(b)
}

type liveScenario struct {
	name, role, grade, tags string
	rows                    []CatalogRow
	place                   map[string]string
	load                    map[string]float64
	gate                    string // "", "refuseA", "refuseAll"
	// tune edits the fixture after it is built (hub readings, local host).
	tune func(a *App, src *fakePlacementSource)
}

// liveScenarios are the existing host-first and host-fit fixtures
// (TestHostFirstPlacement, TestHostFitPlacement, TestHostFirstRanksByHubScoreWithLocal)
// plus scored fleets where the shadow arms order hosts differently.
func liveScenarios() []liveScenario {
	both := map[string]string{"cloud-small": testClaudeA, "mac-b": testClaudeB}
	allB := map[string]string{"mac-b": testClaudeB, "cloud-small": testClaudeB, "cloud-tiny": testClaudeB, "pi": testClaudeB}
	scores := func(m map[string]float64) func(*App, *fakePlacementSource) {
		return func(_ *App, src *fakePlacementSource) {
			for i := range src.hosts {
				if sc, ok := m[src.hosts[i].Name]; ok {
					src.hosts[i].Score = ptrF(sc)
				}
			}
		}
	}
	return []liveScenario{
		{name: "account only on a busy host", role: "consult", grade: "A", rows: consultRows, place: both, load: map[string]float64{"cloud-small": 1.6, "mac-b": 0.2}},
		{name: "unity lead skips the waste account", role: "consult", grade: "A", tags: "unity", rows: consultRows, place: both, load: map[string]float64{"cloud-small": 0.1, "mac-b": 0.5}},
		{name: "refused accounts skip the host", role: "consult", grade: "A", rows: consultRows, place: map[string]string{"mac-b": testClaudeA, "cloud-small": testClaudeB}, load: map[string]float64{"mac-b": 0.1, "cloud-small": 0.5}, gate: "refuseA"},
		{name: "all hosts refused", role: "consult", grade: "A", rows: consultRows, place: both, load: map[string]float64{"cloud-small": 0.1, "mac-b": 0.5}, gate: "refuseAll"},
		{name: "no host declares the cap", role: "consult", grade: "A", tags: "gpu", rows: consultRows, place: both},
		{name: "no single host declares every cap", role: "consult", grade: "A", tags: "unity,linux", rows: consultRows, place: both},
		{name: "pi takes a light worker task", role: "implementer", grade: "B", rows: workerRows, place: map[string]string{"pi": testClaudeB}},
		{name: "pi never takes a lead", role: "consult", grade: "B", rows: consultRows, place: map[string]string{"pi": testClaudeB}},
		{name: "an idle pi does not draw a lead", role: "consult", grade: "B", rows: consultRows, place: map[string]string{"pi": testClaudeB, "mac-b": testClaudeB}, load: map[string]float64{"pi": 0.0, "mac-b": 0.8}},
		{name: "one-core vm never takes a heavy grade", role: "implementer", grade: "S", rows: workerRows, place: map[string]string{"cloud-tiny": testClaudeB}},
		{name: "heavy grade goes to a capable host", role: "implementer", grade: "S", rows: workerRows, place: map[string]string{"cloud-tiny": testClaudeB, "mac-b": testClaudeB}, load: map[string]float64{"cloud-tiny": 0.0, "mac-b": 0.9}},
		{name: "one-core vm takes light work", role: "implementer", grade: "B", rows: workerRows, place: map[string]string{"cloud-tiny": testClaudeB, "mac-b": testClaudeB}, load: map[string]float64{"cloud-tiny": 0.1, "mac-b": 0.9}},
		{name: "heavy-go work needs the cap", role: "implementer", grade: "B", tags: "heavy-go", rows: workerRows, place: map[string]string{"cloud-tiny": testClaudeB}},
		{name: "hub score ranks the local host too", role: "implementer", grade: "B", rows: workerRows, place: map[string]string{"pi": testClaudeB, "mac-b": testClaudeB},
			tune: func(a *App, src *fakePlacementSource) {
				a.Config.Local["spawn"] = true
				a.Config.Local["machine_id"] = "local-box"
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
				src.hosts = append(src.hosts, HostView{Name: "local-box", Connected: true, LoadRatio: ptrF(0.1), Score: ptrF(100)})
			}},
		{name: "local load5 over threshold, remote busy under it (Defect A)", role: "implementer", grade: "B", rows: workerRows, place: allB,
			tune: func(a *App, src *fakePlacementSource) {
				scores(map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 40, "pi": 30})(a, src)
				a.Config.Local["spawn"] = true
				a.Config.Local["machine_id"] = "local-box"
				a.LoadRatio = func(context.Context) (float64, error) { return 1.4, nil }
				src.hosts[0].LoadRatio = ptrF(0.5)
				src.hosts = append(src.hosts, HostView{Name: "local-box", Connected: true, LoadRatio: ptrF(0.3), Score: ptrF(300), Signals: sigs(100)})
				src.presence = append(src.presence, PresenceRecord{HostID: "local-box", AccountKey: testClaudeB, LastSeen: a.Now()})
			}},
		{name: "challenger inside the hysteresis margin (Defect B)", role: "implementer", grade: "B", rows: workerRows, place: allB,
			tune: scores(map[string]float64{"mac-b": 100, "cloud-small": 120, "cloud-tiny": 50, "pi": 40})},
		{name: "challenger clears the hysteresis margin", role: "implementer", grade: "B", rows: workerRows, place: allB,
			tune: scores(map[string]float64{"mac-b": 100, "cloud-small": 130, "cloud-tiny": 50, "pi": 40})},
		{name: "host at its job cap stays a live candidate", role: "implementer", grade: "B", rows: workerRows, place: allB,
			tune: func(a *App, src *fakePlacementSource) {
				scores(map[string]float64{"mac-b": 100, "cloud-small": 50, "cloud-tiny": 40, "pi": 30})(a, src)
				src.hosts[0].Reason = "active_jobs>=max"
			}},
		{name: "hub memory floor drops a host", role: "implementer", grade: "B", rows: workerRows, place: allB,
			tune: func(a *App, src *fakePlacementSource) {
				scores(map[string]float64{"mac-b": 100, "cloud-small": 90, "cloud-tiny": 1, "pi": 1})(a, src)
				src.hosts[0].Reason = "not_accepting(memory_pressure)"
				src.hosts[0].Signals = sigs(100)
			}},
		{name: "scored hosts with signals", role: "implementer", grade: "B", rows: workerRows, place: allB,
			tune: func(a *App, src *fakePlacementSource) {
				scores(map[string]float64{"mac-b": 100, "cloud-small": 90, "cloud-tiny": 1, "pi": 1})(a, src)
				src.hosts[0].Signals, src.hosts[1].Signals = sigs(100), sigs(100)
			}},
	}
}

// liveOutput runs one scenario and returns everything a caller can observe of
// the live selection (see liveRecord), without the placement_shadow event.
func liveOutput(t *testing.T, sc liveScenario) string {
	t.Helper()
	a, s, q, src := hostFirstFixture(t, sc.role, sc.grade, sc.rows, sc.place, sc.load)
	s.Tags = sc.tags
	gate := q.GateDo
	switch sc.gate {
	case "refuseA":
		q.GateDo = func(r QuotaRequest) (string, error) {
			if r.AccountKey == testClaudeA {
				return "", fail(3, "quota exhausted")
			}
			return gate(r)
		}
	case "refuseAll":
		q.GateDo = func(QuotaRequest) (string, error) { return "", fail(3, "quota exhausted") }
	}
	if sc.tune != nil {
		sc.tune(a, src)
	}
	return liveRecord(t, a, s, shadowEvents(a))
}

// The head's live decisions, traces and events equal what the base produced on
// the same fixtures.
func TestPlacementShadowLiveMatchesBase_T3650(t *testing.T) {
	got := map[string]string{}
	for _, sc := range liveScenarios() {
		got[sc.name] = liveOutput(t, sc)
	}
	if *writeLiveBase {
		b, err := json.MarshalIndent(got, "", " ")
		if err != nil || os.WriteFile(liveBaseGolden, append(b, '\n'), 0o644) != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	raw, err := os.ReadFile(liveBaseGolden)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]string
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if len(want) != len(got) {
		t.Fatalf("golden has %d scenarios, test has %d", len(want), len(got))
	}
	for name, g := range got {
		if liveBaseKey(t, want[name]) != liveBaseKey(t, g) {
			t.Errorf("%s: live output differs from the base:\nbase: %s\nhead: %s", name, want[name], g)
		}
	}
	t.Logf("%d scenarios identical to the base", len(got))
}

// liveBaseKey is a live record without the policy text a pool-order version
// change rewrites, without the consult stamp the consult grade floor
// added (pinned by consult_floor_test.go), without the ranked pool seat
// fields (seat_source, pool_waste_urgency; hk 4529, pinned by
// select_ranked_seat_t4529_test.go) and without the quota-drop attribution
// fields (evidence host on non-chosen rows, reading_age_s, stale; hk 4626,
// pinned by select_account_attribution_t4626_test.go). The decisions, candidate rows, host
// traces and events are what the base pins; the version, bands, rule sentence
// and host rule are the policy's own words and change with the policy.
func liveBaseKey(t *testing.T, raw string) string {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	if rec, ok := out["record"].(map[string]any); ok {
		for _, k := range []string{"policy_version", "tiers", "rule", "decision", "host_rule", "consult"} {
			delete(rec, k)
		}
		cands, _ := rec["candidates"].([]any)
		for _, c := range cands {
			if row, ok := c.(map[string]any); ok {
				delete(row, "seat_source")
				delete(row, "pool_waste_urgency")
				delete(row, "reading_age_s")
				delete(row, "stale")
				if row["outcome"] != "chosen" {
					// A drop now names the host its account bound through;
					// the base left it empty on every non-chosen row.
					delete(row, "host")
				}
			}
		}
	}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

var _ = fleetevent.Event{}
