package panewire

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// t607PolicyHub builds a hub-only (no Prometheus) test hub whose placement
// policy optionally carries a per-machine task_slots map.
func t607PolicyHub(t *testing.T, machines map[string]PlacementMachineSlots) (*HubServer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placement.json")
	policy := PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5, Machines: machines}
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	hub, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": r6OperatorToken, "mac-work": r6NodeAToken, "desktop": r6NodeBToken}, PlacementPolicyPath: path})
	if err != nil {
		t.Fatal(err)
	}
	return hub, path
}

// t607Task writes count distinct owner_lane task groups onto a machine. Each
// group is a builder job plus a tester job sharing the group owner_lane,
// matching the wrk --lane/--owner claim contract.
func t607Task(hub *HubServer, machine string, groups, jobsPerGroup int) {
	jobs := make([]HubActiveJob, 0, groups*jobsPerGroup)
	for g := 0; g < groups; g++ {
		owner := "b-owner-" + string(rune('a'+g))
		for j := 0; j < jobsPerGroup; j++ {
			jobs = append(jobs, HubActiveJob{JobID: "job-" + machine + "-" + string(rune('a'+g)) + string(rune('0'+j)), AgentLabel: "wrk", Epoch: 1, OwnerLane: owner})
		}
	}
	hub.observeActiveJobs(machine, jobs, time.Now().UTC())
}

func t607Candidate(result PlacementResult, machine string) (PlacementCandidate, bool) {
	for _, candidate := range result.Candidates {
		if candidate.Machine == machine {
			return candidate, true
		}
	}
	return PlacementCandidate{}, false
}

func TestPlacementTaskSlotsExcludesFullMachine(t *testing.T) {
	hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"desktop": {TaskSlots: 2}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	t607Task(hub, "desktop", 2, 2)
	result := hub.placement(t.Context(), "worker", "repo-slots")
	if result.Decision != "mac-work" {
		t.Fatalf("decision=%q want mac-work: %+v", result.Decision, result)
	}
	if _, found := t607Candidate(result, "desktop"); found {
		t.Fatalf("slot-full desktop still a candidate: %+v", result.Candidates)
	}
	if _, found := t607Candidate(result, "mac-work"); !found {
		t.Fatalf("local missing from candidates: %+v", result.Candidates)
	}
}

// TestPlacementTaskSlotsCountsOwnerLaneBundle is the mutant discriminator:
// two jobs of one task (builder + its tester) must count as one slot. A
// job-counting mutant fills slots=2 and wrongly excludes the machine.
func TestPlacementTaskSlotsCountsOwnerLaneBundle(t *testing.T) {
	hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"desktop": {TaskSlots: 2}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	t607Task(hub, "desktop", 1, 2)
	result := hub.placement(t.Context(), "worker", "repo-slots")
	candidate, found := t607Candidate(result, "desktop")
	if !found {
		t.Fatalf("one task on a slots=2 machine was excluded: %+v", result)
	}
	if candidate.Tasks != 1 || candidate.TaskSlots != 2 {
		t.Fatalf("tasks=%d slots=%d, want 1/2", candidate.Tasks, candidate.TaskSlots)
	}
}

func TestPlacementTaskSlotsBoundary(t *testing.T) {
	for _, groups := range []int{1, 2, 3} {
		hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"desktop": {TaskSlots: 2}})
		hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
		hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
		t607Task(hub, "desktop", groups, 1)
		result := hub.placement(t.Context(), "worker", "repo-slots")
		_, found := t607Candidate(result, "desktop")
		if found != (groups < 2) {
			t.Fatalf("groups=%d candidate present=%t", groups, found)
		}
	}
}

// TestPlacementTaskSlotsExhaustedResponse exercises the real wire: every
// machine slot-full must answer an explicit empty candidate list, a null
// decision, and a reason — never an implicit "place anywhere".
func TestPlacementTaskSlotsExhaustedResponse(t *testing.T) {
	hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"mac-work": {TaskSlots: 1}, "desktop": {TaskSlots: 1}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	t607Task(hub, "mac-work", 1, 1)
	t607Task(hub, "desktop", 1, 1)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/placement?class=worker", nil)
	req.Header.Set("Authorization", "Bearer "+r6OperatorToken)
	hub.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"decision":null`) || !strings.Contains(recorder.Body.String(), `"candidates":[]`) {
		t.Fatalf("wire shape not explicit-empty: %s", recorder.Body.String())
	}
	var result PlacementResult
	if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Decision != "unavailable" || len(result.Candidates) != 0 || result.Reason != "task_slots_exhausted" {
		t.Fatalf("decoded=%+v", result)
	}
	if !validPlacementCLIResult(result, "", "") {
		t.Fatal("CLI validator rejected a legitimate exhausted response")
	}
}

// A quota-allowed pool can still be slot-exhausted: the validator must accept
// unavailable+task_slots_exhausted under an allow/boost quota, while an
// unavailable decision under allow with any other reason stays malformed.
func TestPlacementCLIValidatorQuotaAllowSlotExhausted(t *testing.T) {
	exhausted := PlacementResult{
		Decision: "unavailable",
		Source:   "hub-only",
		Reason:   "task_slots_exhausted",
		Quota:    &PlacementQuotaDecision{Pool: "claude", AccountFP: "a1b2c3d4", Decision: "allow", Reason: "quota_allowed", PolicyStatus: "current"},
	}
	if !validPlacementCLIResult(exhausted, "claude", "a1b2c3d4") {
		t.Fatal("quota-allowed slots-exhausted response rejected as malformed")
	}
	bogus := exhausted
	bogus.Reason = "other_reason"
	if validPlacementCLIResult(bogus, "claude", "a1b2c3d4") {
		t.Fatal("unavailable under quota allow without slot exhaustion accepted")
	}
}

func TestPlacementTaskSlotsAbsentPolicyIsNoop(t *testing.T) {
	hub, _ := t607PolicyHub(t, nil)
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	t607Task(hub, "desktop", 3, 1)
	result := hub.placement(t.Context(), "worker", "repo-slots")
	candidate, found := t607Candidate(result, "desktop")
	if !found || candidate.Tasks != 3 || candidate.TaskSlots != 0 {
		t.Fatalf("no machines map must not gate: %+v", result.Candidates)
	}
}

// Unattributable jobs must over-count, never under-count: an empty or
// "default" owner_lane cannot be proven to share a task.
func TestPlacementTaskSlotsUnownedJobsCountIndividually(t *testing.T) {
	hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"desktop": {TaskSlots: 2}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	hub.observeActiveJobs("desktop", []HubActiveJob{
		{JobID: "job-loose-a", AgentLabel: "wrk", Epoch: 1},
		{JobID: "job-loose-b", AgentLabel: "wrk", Epoch: 1, OwnerLane: "default"},
	}, time.Now().UTC())
	result := hub.placement(t.Context(), "worker", "repo-slots")
	if _, found := t607Candidate(result, "desktop"); found {
		t.Fatalf("two unowned jobs should fill slots=2: %+v", result.Candidates)
	}
}

// A tester living on a different machine than its builder still spends a slot
// where it actually runs.
func TestPlacementTaskSlotsCrossMachineTester(t *testing.T) {
	hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"desktop": {TaskSlots: 1}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	hub.observeActiveJobs("mac-work", []HubActiveJob{{JobID: "job-builder", AgentLabel: "wrk", Epoch: 1, OwnerLane: "b-x"}}, time.Now().UTC())
	hub.observeActiveJobs("desktop", []HubActiveJob{{JobID: "job-tester", AgentLabel: "wrk", Epoch: 1, OwnerLane: "b-x"}}, time.Now().UTC())
	result := hub.placement(t.Context(), "worker", "repo-slots")
	if _, found := t607Candidate(result, "desktop"); found {
		t.Fatalf("remote tester should fill desktop's slot: %+v", result.Candidates)
	}
}

// A hot-reloaded policy file picks up a newly added machines map without a
// restart, and the placement cache must not hide the flip.
func TestPlacementTaskSlotsHotReload(t *testing.T) {
	hub, path := t607PolicyHub(t, nil)
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	t607Task(hub, "desktop", 1, 1)
	if _, found := t607Candidate(hub.placement(t.Context(), "worker", "repo-slots"), "desktop"); !found {
		t.Fatal("baseline: desktop should be a candidate before the policy gains slots")
	}
	policy := PlacementPolicy{LocalMachine: "mac-work", SpillTargets: []string{"desktop"}, MaxActiveJobs: 5, LoadRatio: .5, Machines: map[string]PlacementMachineSlots{"desktop": {TaskSlots: 1}}}
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	if _, found := t607Candidate(hub.placement(t.Context(), "worker", "repo-slots"), "desktop"); found {
		t.Fatal("reloaded task_slots=1 did not exclude the full machine")
	}
}

func TestPlacementSlotsEndpoint(t *testing.T) {
	hub, _ := t607PolicyHub(t, map[string]PlacementMachineSlots{"desktop": {TaskSlots: 2}})
	hub.connect("mac-work", "test", "fixture", &hubAgent{}, true)
	hub.connect("desktop", "test", "fixture", &hubAgent{}, true)
	t607Task(hub, "desktop", 1, 2)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/placement/slots", nil)
	hub.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d, want 401", recorder.Code)
	} // Mutant: dropping authorizeOperator turns this RED.
	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/v1/placement/slots", nil)
	req.Header.Set("Authorization", "Bearer "+r6OperatorToken)
	hub.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d", recorder.Code)
	}
	var body struct {
		Machines []struct {
			Machine   string `json:"machine"`
			TasksUsed int    `json:"tasks_used"`
			TaskSlots *int   `json:"task_slots"`
		} `json:"machines"`
		PolicyStatus string `json:"policy_status"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.PolicyStatus != "current" || len(body.Machines) != 2 {
		t.Fatalf("slots body=%s", recorder.Body.String())
	}
	var desktop, macWork *int
	used := map[string]int{}
	for _, machine := range body.Machines {
		used[machine.Machine] = machine.TasksUsed
		if machine.Machine == "desktop" {
			desktop = machine.TaskSlots
		} else {
			macWork = machine.TaskSlots
		}
	}
	if used["desktop"] != 1 || used["mac-work"] != 0 || desktop == nil || *desktop != 2 || macWork != nil {
		t.Fatalf("slots body=%s", recorder.Body.String())
	}
}

func TestParsePlacementPolicyMachines(t *testing.T) {
	base := `{"local_machine":"mac-work","spill_targets":["desktop"],"max_active_jobs":5,"load_ratio":0.5%s}`
	valid, err := ParsePlacementPolicy([]byte(fmt.Sprintf(base, `,"machines":{"desktop":{"task_slots":2}}`)))
	if err != nil || valid.Machines["desktop"].TaskSlots != 2 {
		t.Fatalf("valid machines rejected: %v %+v", err, valid.Machines)
	}
	for name, machines := range map[string]string{
		"bad id":      `"Bad-Id":{"task_slots":1}`,
		"operator id": `"operator":{"task_slots":1}`,
		"zero slots":  `"desktop":{"task_slots":0}`,
		"over cap":    `"desktop":{"task_slots":101}`,
		"extra field": `"desktop":{"task_slots":1,"extra":1}`,
	} {
		if _, err := ParsePlacementPolicy([]byte(fmt.Sprintf(base, `,"machines":{`+machines+`}`))); err == nil {
			t.Fatalf("%s machines entry accepted", name)
		}
	}
	if _, err := ParsePlacementPolicy([]byte(fmt.Sprintf(base, ""))); err != nil {
		t.Fatalf("absent machines rejected: %v", err)
	}
}
