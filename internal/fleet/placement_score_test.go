package fleet

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/mgh3326/panewire/internal/fleetevent"
)

func ptrI(v int) *int { return &v }

// Acceptance 3 (placement): one kind=placement event per decision carries the
// hub spawn request id, the job, every candidate host's terms, score and
// outcome, the chosen host and the reason, and nothing else. A fake writer
// receives it.
func TestPlacementEventCarriesRequestTermsScoreChosenAndReason(t *testing.T) {
	a := testApp(t)
	a.RequestID = "11111111-2222-4333-8444-555555555555"
	var got []fleetevent.Event
	a.EventSink = func(ev fleetevent.Event) { got = append(got, ev) }
	terms := func(cpu, mem, io, load, jobs float64) *PlacementTerms {
		return &PlacementTerms{CPU: cpu, Mem: mem, IO: io, Load: load, Jobs: jobs}
	}
	plan := &placePlan{candidates: []placeCandidate{
		{name: "m1b", score: ptrF(61.5), decided: true, decision: "challenger", signals: PlacementSignals{Terms: terms(.9, 1, .8, .9, .95), Power: ptrF(100), BridgedJobs: ptrI(1)}},
		{name: "desktop", score: ptrF(30), signals: PlacementSignals{Terms: terms(.9, .5, 1, 1, 1), Power: ptrF(39), BridgedJobs: ptrI(0)}},
		{name: "oci", outcome: "disconnected"},
	}, ordered: []*placeCandidate{{name: "m1b", score: ptrF(61.5)}, {name: "desktop", score: ptrF(30)}}}
	s := Spawn{Job: "job-a", placement: &placementDecision{plan: plan}}
	a.placementEvent(s, map[string]any{"chose": "m1b", "ranking": "score", "candidates": []any{}}, nil)
	if len(got) != 1 {
		t.Fatalf("want one event, got %d", len(got))
	}
	ev := got[0]
	if ev.Kind != "placement" || ev.Tool != "panewire" || ev.Job != "job-a" || ev.RequestID != a.RequestID || ev.Chosen != "m1b" || ev.Reason != "challenger" || ev.Outcome != "placed" {
		t.Fatalf("event: %+v", ev)
	}
	if len(ev.Candidates) != 3 {
		t.Fatalf("every candidate host must be listed: %+v", ev.Candidates)
	}
	m1b, desktop, oci := ev.Candidates[0], ev.Candidates[1], ev.Candidates[2]
	if m1b.Host != "m1b" || *m1b.Score != 61.5 || *m1b.CPU != .9 || *m1b.Mem != 1 || *m1b.IO != .8 || *m1b.Load != .9 || *m1b.Jobs != .95 || *m1b.Power != 100 || *m1b.Bridge != 1 || m1b.Outcome != "chosen" {
		t.Fatalf("m1b: %+v", m1b)
	}
	// free is the product of the terms, for the floor to be replayed from the line.
	if m1b.Free == nil || math.Abs(*m1b.Free-.9*1*.8*.9*.95) > 1e-12 || desktop.Free == nil || *desktop.Free != .45 || *desktop.Free < 0.05 || oci.Free != nil {
		t.Fatalf("free: m1b=%v desktop=%v oci=%v", m1b.Free, desktop.Free, oci.Free)
	}
	if *desktop.Mem != .5 || *desktop.Power != 39 || *desktop.Bridge != 0 || desktop.Outcome != "not_chosen" {
		t.Fatalf("desktop: %+v", desktop)
	}
	if oci.Outcome != "disconnected" || oci.Score != nil || oci.CPU != nil {
		t.Fatalf("a host with no hub reading keeps its refusal and no terms: %+v", oci)
	}
	// The line is numbers and host ids: no screen text, brief or argv field.
	raw, _ := json.Marshal(ev)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	for k := range fields {
		switch k {
		case "ts", "host", "tool", "kind", "job", "request_id", "outcome", "reason", "detail", "chosen", "candidates":
		default:
			t.Fatalf("unexpected field %q in %s", k, raw)
		}
	}
}

// A chosen host the hub did not name (a fallback after a refusal) keeps the
// ranking metric as its reason; the policy-order winner reads policy_order.
func TestPlacementEventReasonFollowsHubDecision(t *testing.T) {
	cases := []struct {
		name         string
		decided      bool
		decision     string
		chosen, want string
	}{
		{"policy-order winner", true, "", "m1b", "policy_order"},
		{"all loaded", true, "all_loaded", "m1b", "all_loaded"},
		{"fell back past the hub's pick", true, "challenger", "desktop", "score"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := testApp(t)
			var got fleetevent.Event
			a.EventSink = func(ev fleetevent.Event) { got = ev }
			plan := &placePlan{candidates: []placeCandidate{{name: "m1b", score: ptrF(1), decided: tc.decided, decision: tc.decision}, {name: "desktop", score: ptrF(1)}},
				ordered: []*placeCandidate{{name: "m1b", score: ptrF(1)}, {name: "desktop", score: ptrF(1)}}}
			a.placementEvent(Spawn{Job: "job-a", placement: &placementDecision{plan: plan}}, map[string]any{"chose": tc.chosen, "ranking": "score"}, nil)
			if got.Reason != tc.want {
				t.Fatalf("reason=%q, want %q", got.Reason, tc.want)
			}
		})
	}
}

// The hub's decision reaches both rankers: within a bucket the host the hub
// named ranks ahead of a higher-scoring rival, so its hysteresis margin holds.
func TestHubDecisionRanksAheadOfHigherScoreWithinBucket(t *testing.T) {
	raw := `{"decision":"first","reason":"","candidates":[{"machine":"first","score":50,"connected":true},{"machine":"second","score":60,"connected":true}]}`
	views, err := ParseHubHosts(raw)
	if err != nil || len(views) != 2 || !views[0].Decided || views[1].Decided || views[0].DecisionReason != "" {
		t.Fatalf("views=%+v err=%v", views, err)
	}
	src := &fakePlacementSource{hosts: views}
	a := placementApp(t, src)
	a.Config.Local = map[string]any{"spawn": false}
	a.Config.Hosts = map[string]map[string]any{"first": {}, "second": {}}
	a.Config.HostOrder = []string{"first", "second"}
	p, err := a.plan(context.Background(), Spawn{Profile: "claude"}, "", 0)
	if err != nil || len(p.ordered) < 2 || p.ordered[0].name != "first" {
		t.Fatalf("decided host must lead: %+v err=%v", p.ordered, err)
	}
	views[0].Decided, views[1].Decided = false, true
	views[1].DecisionReason = "challenger"
	src.hosts = views
	if p, _ = a.plan(context.Background(), Spawn{Profile: "claude"}, "", 0); p.ordered[0].name != "second" {
		t.Fatalf("a new decision must lead: %+v", p.ordered)
	}
	views[1].Decided = false
	src.hosts = views
	if p, _ = a.plan(context.Background(), Spawn{Profile: "claude"}, "", 0); p.ordered[0].name != "second" {
		t.Fatalf("with no decision the higher score leads: %+v", p.ordered)
	}
}

func TestRankHostsPrefersTheHubDecidedHost(t *testing.T) {
	hosts := []selHost{{name: "a", awake: true, load: ptrF(.1), score: ptrF(40), decided: true}, {name: "b", awake: true, load: ptrF(.1), score: ptrF(90)}}
	if got := rankHosts(hosts); got[0].name != "a" {
		t.Fatalf("decided host must lead: %+v", got)
	}
	hosts[0].decided = false
	if got := rankHosts(hosts); got[0].name != "b" {
		t.Fatalf("without a decision the score leads: %+v", got)
	}
}

func TestPlacementEventPreservesHubScoreSignals(t *testing.T) {
	for _, source := range []string{"prometheus", "hub-only"} {
		t.Run(source, func(t *testing.T) {
			raw := `{"source":"` + source + `","decision":"host-a","candidates":[{"machine":"host-a","connected":true,"score":12,"signals":{"terms":{"cpu":0.2,"mem":0.3,"io":0.4,"load":0.5,"jobs":0.6},"power":70,"bridged_jobs":0,"mem_available":0.3}}]}`
			views, err := parseHubHosts(raw)
			if err != nil || len(views) != 1 {
				t.Fatalf("views=%+v err=%v", views, err)
			}
			a := placementApp(t, &fakePlacementSource{hosts: views})
			a.Config.Local = map[string]any{"spawn": false}
			a.Config.Hosts = map[string]map[string]any{"host-a": {}}
			a.Config.HostOrder = []string{"host-a"}
			plan, err := a.plan(context.Background(), Spawn{Profile: "claude"}, "", 0)
			if err != nil {
				t.Fatal(err)
			}
			var event fleetevent.Event
			a.EventSink = func(e fleetevent.Event) { event = e }
			a.placementEvent(Spawn{Job: "job-a", placement: &placementDecision{plan: plan}}, map[string]any{"chose": "host-a", "ranking": "score"}, nil)
			for _, c := range event.Candidates {
				if c.Host != "host-a" {
					continue
				}
				if c.Source != source || c.Score == nil || *c.Score != 12 || c.Power == nil || *c.Power != 70 || c.Bridge == nil || *c.Bridge != 0 {
					t.Fatalf("candidate: %+v", c)
				}
				if source == "prometheus" && (c.CPU == nil || *c.CPU != .2 || c.Mem == nil || *c.Mem != .3 || c.IO == nil || *c.IO != .4 || c.Load == nil || *c.Load != .5 || c.Jobs == nil || *c.Jobs != .6) {
					t.Fatalf("missing score terms: %+v", c)
				}
				if source == "hub-only" && (c.CPU != nil || c.Mem == nil || *c.Mem != .3 || c.IO != nil || c.Load != nil) {
					t.Fatalf("unknown terms present: %+v", c)
				}
				return
			}
			t.Fatalf("host-a missing: %+v", event.Candidates)
		})
	}
}

func TestFleetSourceRequestsScoreSignals(t *testing.T) {
	a := testApp(t)
	a.Config.Hub = map[string]any{"hub_url": "https://hub.invalid", "operator_token_env": "fixture"}
	a.Runner = &fakeRunner{Do: func(c Command) Result {
		if c.Name != "panewire" {
			return Result{}
		}
		for _, arg := range c.Args {
			if arg == "--signals" {
				return Result{Out: `{"source":"prometheus","candidates":[{"machine":"host-a","score":12,"signals":{"terms":{"cpu":0.2,"mem":0.3,"io":0.4,"load":0.5,"jobs":0.6},"power":70,"bridged_jobs":0}}]}`}
			}
		}
		return Result{Out: `{"source":"prometheus","candidates":[{"machine":"host-a","score":12}]}`}
	}}
	hosts, err := (fleetSource{a}).Hosts(context.Background(), "repo")
	if err != nil || len(hosts) != 1 || hosts[0].Signals.Terms == nil || hosts[0].Signals.Terms.CPU != .2 {
		t.Fatalf("direct placement lost score terms: hosts=%+v err=%v", hosts, err)
	}
}
