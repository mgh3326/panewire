package panewire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"
)

// hk 4448: a placement decision counts its job against the chosen host at
// once, and no decision ranks on readings older than the decision.

// countFixtureProm answers the placement queries from tables a test may change
// between decisions.
type countFixtureProm struct {
	mu     sync.Mutex
	tables map[string]map[string]string
	server *httptest.Server
}

func newCountFixtureProm(t *testing.T, tables map[string]map[string]string) *countFixtureProm {
	t.Helper()
	p := &countFixtureProm{tables: tables}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		defer p.mu.Unlock()
		result := []any{}
		for machine, value := range p.tables[r.URL.Query().Get("query")] {
			result = append(result, map[string]any{"metric": map[string]string{"machine_id": machine}, "value": []any{float64(1), value}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": result}})
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *countFixtureProm) set(query, machine, value string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tables[query][machine] = value
}

// countFixtureHub is the 2026-11-13 03:59Z set of hosts: oci is saturated
// (cpu busy 99.8 %, power 30) and idle of claims, so it scores +0.05; rpi,
// desktop and mac-dev carry 2, 2 and 3 claims and score -17.4, -19.1, -27.9.
func countFixtureHub(t *testing.T) (*HubServer, *countFixtureProm) {
	t.Helper()
	busy := map[string]string{"oci": "0.99833", "rpi": "0.9675", "desktop": "0.98875", "mac-dev": "0.9685"}
	mem := map[string]string{"oci": "0.9", "rpi": "0.9", "desktop": "0.9", "mac-dev": "0.9"}
	zero := map[string]string{"oci": "0", "rpi": "0", "desktop": "0", "mac-dev": "0"}
	one := map[string]string{"oci": "1", "rpi": "1", "desktop": "1", "mac-dev": "1"}
	prom := newCountFixtureProm(t, map[string]map[string]string{
		placementCPUBusyQuery: maps2(busy), placementCPUBusy1mQuery: maps2(busy), placementMemAvailableQuery: mem,
		placementSwapInQuery: zero, placementThermalQuery: one,
	})
	policy := PlacementPolicy{LocalMachine: "mac-dev", SpillTargets: []string{"rpi", "desktop", "oci"}, MaxActiveJobs: 50, Power: map[string]float64{"oci": 30, "rpi": 100, "desktop": 100, "mac-dev": 100}}
	hub := placementPowerHub(t, prom.server.URL, policy)
	now := time.Now().UTC()
	for machine, claims := range map[string]int{"oci": 0, "rpi": 2, "desktop": 2, "mac-dev": 3} {
		hub.connect(machine, "test", "fixture", &hubAgent{}, true)
		jobs := make([]HubActiveJob, claims)
		for i := range jobs {
			jobs[i] = HubActiveJob{JobID: "claimed-" + machine + string(rune('a'+i)), AgentLabel: "agent", Epoch: 1, StartedAt: now.Add(-time.Minute).Format(time.RFC3339)}
		}
		hub.observeActiveJobs(machine, jobs, now)
	}
	return hub, prom
}

func maps2(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// countedDecision is one spawn decision as the spawn engine takes it: read the
// hub's fleet state, pick the usable host with the best published score, and
// tell the hub which host it is trying (the app.PlacementTry hook).
func countedDecision(t *testing.T, hub *HubServer, job string) (string, map[string]float64) {
	t.Helper()
	if _, err := (directSpawnState{hub}).Snapshot(t.Context()); err != nil {
		t.Error(err)
		return "", nil
	}
	snap := hub.fleetSnapshot(time.Now().UTC())
	scores := map[string]float64{}
	best := ""
	for _, state := range snap.States {
		if state.Kind != "load" || state.Load == nil || !state.Load.Usable || state.Load.SlotFull {
			continue
		}
		scores[state.Host] = state.Load.Score
		if best == "" || state.Load.Score > scores[best] {
			best = state.Host
		}
	}
	if best == "" {
		t.Error("no usable host in the fleet state")
		return "", nil
	}
	hub.countPlacement(job, best)
	return best, scores
}

// The 03:59Z burst. Six jobs are decided within a minute, none claimed yet.
// Each decision must rank on scores that carry the jobs decided before it, so
// the burst spreads the way the scores say: the model below is the documented
// formula (a bridged job costs claim_cost) applied to the first decision's
// scores, and is independent of the hub's counting.
func TestPlacementBurstSpreadsOnDecidedJobsNotClaims(t *testing.T) {
	hub, _ := countFixtureHub(t)
	// The sweep's publish, as the hub does it every 15 s.
	hub.publishFleetLoad(t.Context(), time.Now().UTC())

	var got, want []string
	var base map[string]float64
	counted := map[string]int{}
	for i := range 6 {
		host, scores := countedDecision(t, hub, "burst-job-"+string(rune('a'+i)))
		if base == nil {
			base = scores
			if base["oci"] <= 0 || base["oci"] > 1 || base["rpi"] > -10 {
				t.Fatalf("fixture does not reproduce the 03:59Z scores: %v", base)
			}
		}
		// What the model says this decision ranks, from the base scores and
		// the jobs it has decided so far.
		modelled := ""
		for _, machine := range []string{"oci", "rpi", "desktop", "mac-dev"} {
			score := base[machine] - float64(counted[machine])*placementDefaultClaimCost
			if modelled == "" || score > base[modelled]-float64(counted[modelled])*placementDefaultClaimCost {
				modelled = machine
			}
		}
		for machine, score := range scores {
			if diff := score - (base[machine] - float64(counted[machine])*placementDefaultClaimCost); diff > 0.002 || diff < -0.002 {
				t.Fatalf("decision %d: %s published %v, want %v with %d jobs decided onto it", i+1, machine, score, base[machine]-float64(counted[machine])*placementDefaultClaimCost, counted[machine])
			}
		}
		got, want = append(got, host), append(want, modelled)
		counted[host]++
	}
	t.Logf("burst placed %v", got)
	if !slices.Equal(got, want) {
		t.Fatalf("burst went %v, the scores say %v", got, want)
	}
	if slices.Equal(got, []string{"oci", "oci", "oci", "oci", "oci", "oci"}) || counted["oci"] > 3 {
		t.Fatalf("the burst piled onto oci: %v", got)
	}
}

// A decision ranks on the readings of its own moment: a host whose load fell
// between two decisions is seen as it is now, with no wait for the sweep's
// next publish or the value's republish.
func TestPlacementDecisionReadsHostsFresh(t *testing.T) {
	hub, prom := countFixtureHub(t)
	hub.publishFleetLoad(t.Context(), time.Now().UTC())
	if host, _ := countedDecision(t, hub, "fresh-job-a"); host != "oci" {
		t.Fatalf("first decision = %s, want oci (+0.05 beats the claimed hosts)", host)
	}
	// oci's saturating job ends and rpi's load spikes; nothing republishes.
	prom.set(placementCPUBusyQuery, "oci", "0.1")
	prom.set(placementCPUBusy1mQuery, "oci", "0.1")
	prom.set(placementCPUBusyQuery, "rpi", "0.9999")
	prom.set(placementCPUBusy1mQuery, "rpi", "0.9999")
	// oci: 0.9 free * power 30 = 27, less the 10 of the job decided onto it.
	host, scores := countedDecision(t, hub, "fresh-job-b")
	if host != "oci" || scores["oci"] < 15 || scores["rpi"] > -19.5 {
		t.Fatalf("second decision = %s on %v: it ranked on the readings of the first", host, scores)
	}
}

// The same read through the fallback the hub offers the engine when its fleet
// state is unusable: it must not serve the 30 s placement cache to a spawn.
func TestSpawnPlacementHostsBypassTheCache(t *testing.T) {
	hub, prom := countFixtureHub(t)
	before := hub.placement(t.Context(), "worker", "/work/project")
	prom.set(placementCPUBusyQuery, "oci", "0.1")
	prom.set(placementCPUBusy1mQuery, "oci", "0.1")
	cached, _ := t607Candidate(hub.placement(t.Context(), "worker", "/work/project"), "oci")
	if was, _ := t607Candidate(before, "oci"); cached.Score != was.Score {
		t.Fatalf("the operator read is cached for 30 s by design; got %v then %v", was.Score, cached.Score)
	}
	fresh, _ := t607Candidate(hub.placementFresh(t.Context(), "worker", "/work/project"), "oci")
	if fresh.Score < 20 {
		t.Fatalf("spawn placement served the cached reading: oci scores %v", fresh.Score)
	}
}

// countedHub is a hub with a spawn store and one connected node, for the
// release rules.
func countedHub(t *testing.T) *HubServer {
	t.Helper()
	hub := asyncSpawnHub(t)
	hub.connect("machine-a", "test", "fixture", &hubAgent{}, true)
	return hub
}

func countedBridged(hub *HubServer) int {
	hub.mu.Lock()
	policy := DefaultPlacementPolicy()
	hub.mu.Unlock()
	policy.LocalMachine, policy.SpillTargets = "machine-a", nil
	for _, candidate := range hub.placementCandidates(policy, placementMetrics{}, "hub-only", time.Now().UTC()) {
		if candidate.Machine == "machine-a" {
			return candidate.Signals.BridgedJobs
		}
	}
	return -1
}

func countedTask(t *testing.T, hub *HubServer) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

// A request that ends without a launch gives its count back: refused by the
// node, a node that never started it, a request that ran out of time.
func TestRefusedOrNotStartedRequestReleasesItsCount(t *testing.T) {
	cases := []struct {
		name    string
		outcome hubSpawnOutcome
	}{
		{"refused by the node", hubSpawnOutcome{RC: 85, Stage: "launch", Reason: "spawn_failed:checkout", Host: "machine-a", NodeResult: true}},
		{"never started by the node", hubSpawnOutcome{RC: 124, Stage: "claim", Reason: "deadline_not_started", Host: "machine-a", Retryable: true, NodeResult: true}},
		{"out of time before any node was bound", hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hub := countedHub(t)
			task := countedTask(t, hub)
			hub.countPlacement(task.Job, "machine-a")
			if got := countedBridged(hub); got != 1 {
				t.Fatalf("decided job counts %d, want 1", got)
			}
			if changed, err := hub.completeSpawn(t.Context(), task, tc.outcome); err != nil || !changed {
				t.Fatalf("completeSpawn = %v, %v", changed, err)
			}
			if got := countedBridged(hub); got != 0 {
				t.Fatalf("ended request still counts %d", got)
			}
		})
	}
}

// A launched job keeps its count until its claim shows, and the claim is the
// count from then on: one job, one cost.
func TestClaimConfirmsTheCountInsteadOfAddingToIt(t *testing.T) {
	hub := countedHub(t)
	task := countedTask(t, hub)
	hub.countPlacement(task.Job, "machine-a")
	if changed, err := hub.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: "machine-a", NodeResult: true}); err != nil || !changed {
		t.Fatalf("completeSpawn = %v, %v", changed, err)
	}
	if got := countedBridged(hub); got != 1 {
		t.Fatalf("launched, unclaimed job counts %d, want 1", got)
	}
	// The claim arrives. The count and the claim are the same job.
	now := time.Now().UTC()
	hub.observeActiveJobs("machine-a", []HubActiveJob{{JobID: task.Job, AgentLabel: "agent", Epoch: 1, StartedAt: now.Format(time.RFC3339)}}, now)
	if got := countedBridged(hub); got != 1 {
		t.Fatalf("claimed job counts %d, want 1 (counted twice)", got)
	}
	hub.mu.Lock()
	left := len(hub.placementCounts)
	hub.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d placement counts outlive their claim", left)
	}
	// And a claim that lands before the count is read (heartbeat first) is
	// still one job.
	hub.countPlacement(task.Job, "machine-a")
	if got := countedBridged(hub); got != 1 {
		t.Fatalf("claimed job counted again by a late decision: %d", got)
	}
}

// A request still reconciling may have started: it keeps its count. A count
// whose claim never shows ends with the bridge window, as a claim would.
func TestReconcilingKeepsCountAndTheBridgeWindowEndsIt(t *testing.T) {
	hub := countedHub(t)
	task := countedTask(t, hub)
	hub.countPlacement(task.Job, "machine-a")
	if _, err := hub.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 75, Stage: "landing", Reason: "unknown", Host: "machine-a"}); err != nil {
		t.Fatal(err)
	}
	if got := countedBridged(hub); got != 1 {
		t.Fatalf("reconciling request counts %d, want 1", got)
	}
	hub.mu.Lock()
	held := hub.placementCounts[task.Job]
	held.at = held.at.Add(-placementDefaultSpawnBridge - time.Second)
	hub.placementCounts[task.Job] = held
	hub.mu.Unlock()
	if got := countedBridged(hub); got != 0 {
		t.Fatalf("count past the bridge window still weighs %d", got)
	}
}

// The next candidate after a refusal takes the count off the refused host.
func TestNextCandidateMovesTheCount(t *testing.T) {
	hub, _ := countFixtureHub(t)
	hub.countPlacement("moving-job", "oci")
	hub.countPlacement("moving-job", "rpi")
	hub.mu.Lock()
	held := hub.placementCounts["moving-job"]
	n := len(hub.placementCounts)
	hub.mu.Unlock()
	if held.machine != "rpi" || n != 1 {
		t.Fatalf("job counts on %q, %d entries; want one entry on rpi", held.machine, n)
	}
}

// Jobs submitted together, each taking the gate the way the engine does: one
// decision at a time reads, ranks and counts, so the burst spreads as the
// sequential one does whatever order the workers arrive in.
func TestSimultaneousDecisionsSpreadThroughTheGate(t *testing.T) {
	hub, _ := countFixtureHub(t)
	hub.publishFleetLoad(t.Context(), time.Now().UTC())
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	counts := map[string]int{}
	for i := range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release, err := hub.placementGate.acquire(t.Context())
			if err != nil {
				t.Error(err)
				return
			}
			defer release()
			host, _ := countedDecision(t, hub, "together-job-"+string(rune('a'+i)))
			mu.Lock()
			counts[host]++
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if counts["oci"] != 3 || counts["rpi"] != 2 || counts["desktop"] != 1 {
		t.Fatalf("simultaneous burst placed %v, the scores say oci 3, rpi 2, desktop 1", counts)
	}
}

// A report from a node the job is not decided onto is presence only: it must
// not un-weigh the host the job is bound for.
func TestClaimFromAnotherNodeKeepsTheCount(t *testing.T) {
	hub, _ := countFixtureHub(t)
	hub.countPlacement("bound-job", "oci")
	now := time.Now().UTC()
	hub.observeActiveJobs("rpi", []HubActiveJob{{JobID: "bound-job", AgentLabel: "agent", Epoch: 1, StartedAt: now.Format(time.RFC3339)}}, now)
	hub.mu.Lock()
	held, still := hub.placementCounts["bound-job"]
	hub.mu.Unlock()
	if !still || held.machine != "oci" {
		t.Fatalf("a report from rpi erased the count for oci: %v %v", held, still)
	}
	hub.observeActiveJobs("oci", []HubActiveJob{{JobID: "bound-job", AgentLabel: "agent", Epoch: 1, StartedAt: now.Format(time.RFC3339)}}, now)
	hub.mu.Lock()
	_, still = hub.placementCounts["bound-job"]
	hub.mu.Unlock()
	if still {
		t.Fatal("the decided host's claim did not confirm the count")
	}
}

// A request bound to a node that runs out of time may still be launching: the
// store keeps the binding and takes a later node result. Its count stays until
// the claim, a definitive failure or the bridge window.
func TestBoundTimeoutKeepsTheCountUntilAClaim(t *testing.T) {
	hub := countedHub(t)
	task := countedTask(t, hub)
	dispatch := hubSpawnRequest{RequestID: t14RequestID(448), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", "test-job"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	task, err := hub.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	hub.countPlacement(task.Job, "machine-a")
	changed, err := hub.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Host: "machine-a"})
	if err != nil || !changed {
		t.Fatalf("completeSpawn = %v, %v", changed, err)
	}
	saved, err := hub.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "deadline_exceeded" {
		t.Fatalf("status = %q, %v; the fixture is meant to leave a bound timeout", saved.Status, err)
	}
	if got := countedBridged(hub); got != 1 {
		t.Fatalf("bound timeout counts %d, want 1", got)
	}
	// A later definitive refusal from the node ends it.
	if _, err := hub.completeSpawn(t.Context(), saved, hubSpawnOutcome{RC: 85, Stage: "launch", Reason: "spawn_failed:checkout", Host: "machine-a", NodeResult: true}); err != nil {
		t.Fatal(err)
	}
	if got := countedBridged(hub); got != 0 {
		t.Fatalf("a definitive failure left %d", got)
	}
}

// The hub's own engine is told which machines were left out for a full task
// cap, so an unlisted host is not read as an unreported one.
func TestPlacementResultNamesTheSlotFullMachines(t *testing.T) {
	hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"desktop": {TaskSlots: 1}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	now := time.Now().UTC()
	hub.observeActiveJobs("desktop", []HubActiveJob{{JobID: "held-job", AgentLabel: "agent", Epoch: 1, OwnerLane: "task-a", StartedAt: now.Format(time.RFC3339)}}, now)
	result := hub.placementFresh(t.Context(), "worker", "repo")
	if !slices.Equal(result.slotFull, []string{"desktop"}) {
		t.Fatalf("slot-full machines = %v, want [desktop]", result.slotFull)
	}
	if _, listed := t607Candidate(result, "desktop"); listed {
		t.Fatal("the API still lists a slot-full machine")
	}
}
