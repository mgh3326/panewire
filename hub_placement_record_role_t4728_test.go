package panewire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 4728: the hub builds its own placement record from the node's terminal
// outcome, and that record must obey handoffkeep's record contract. hk decodes
// the post with unknown fields rejected (internal/api/api.go decodeLimited) and
// then validates it (internal/store/placements.go checkPlacement); the role it
// accepts is the work class, while the outcome carries the spawn role
// (implementer, reviewer, consult). A record hk refuses is quarantined and its
// snapshot sealed, so the decision never reaches the hk log.

// hkPlacementDecision mirrors handoffkeep's store.PlacementDecision, field for
// field. The mirror is the point of the test: hk rejects a field this copy does
// not know, so an extra panewire field fails here exactly as it fails in hk.
type hkPlacementDecision struct {
	JobID       string                 `json:"job_id"`
	Attempt     int                    `json:"attempt"`
	TaskID      *int64                 `json:"task_id,omitempty"`
	DecidedAt   time.Time              `json:"decided_at"`
	ToolVersion string                 `json:"tool_version"`
	Source      string                 `json:"source"`
	Ranking     string                 `json:"ranking"`
	StateSource string                 `json:"state_source"`
	ChosenHost  *string                `json:"chosen_host,omitempty"`
	Outcome     string                 `json:"outcome"`
	Reason      string                 `json:"reason"`
	Profile     string                 `json:"profile"`
	Pool        string                 `json:"pool"`
	AccountKey  string                 `json:"account_key"`
	Family      string                 `json:"family"`
	Model       string                 `json:"model"`
	Role        string                 `json:"role"`
	PaneID      string                 `json:"pane_id"`
	Repo        string                 `json:"repo"`
	Candidates  []hkPlacementCandidate `json:"candidates"`
}

type hkPlacementCandidate struct {
	Host         string            `json:"host"`
	Outcome      string            `json:"outcome"`
	Reason       string            `json:"reason"`
	Score        *float64          `json:"score,omitempty"`
	SignalAgeS   *float64          `json:"signal_age_s,omitempty"`
	CPUBusyShort *float64          `json:"cpu_busy_short,omitempty"`
	CPUBusy5m    *float64          `json:"cpu_busy_5m,omitempty"`
	MemAvailable *float64          `json:"mem_available,omitempty"`
	SwapInMBPS   *float64          `json:"swap_in_mbps,omitempty"`
	PowerSource  string            `json:"power_source,omitempty"`
	BridgedJobs  *int              `json:"bridged_jobs,omitempty"`
	ActiveJobs   *int              `json:"active_jobs,omitempty"`
	Tasks        *int              `json:"tasks,omitempty"`
	TaskSlots    *int              `json:"task_slots,omitempty"`
	Power        *float64          `json:"power,omitempty"`
	LiveJobs     *int              `json:"live_jobs,omitempty"`
	Load1PerCore *float64          `json:"load1_per_core,omitempty"`
	Terms        *hkPlacementTerms `json:"terms,omitempty"`
}

type hkPlacementTerms struct {
	CPU  float64 `json:"cpu"`
	Mem  float64 `json:"mem"`
	IO   float64 `json:"io"`
	Load float64 `json:"load"`
	Jobs float64 `json:"jobs"`
}

// hkPlacementRefusal applies handoffkeep's placement contract to a posted
// record: the strict decode first, then the allowed sets of checkPlacement.
func hkPlacementRefusal(raw []byte) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var d hkPlacementDecision
	if err := dec.Decode(&d); err != nil {
		// hk answers this with {"error":"invalid_context"} and no reason, which
		// is exactly how a record carrying an unknown field fails in place.
		return fmt.Errorf("invalid_context (no reason): %v", err)
	}
	switch {
	case d.Source != "auto" && d.Source != "forced":
		return errors.New("invalid_context: source must be auto or forced")
	case d.Outcome != "placed" && d.Outcome != "refused" && d.Outcome != "failed":
		return errors.New("invalid_context: outcome must be placed, refused or failed")
	case d.Ranking == "":
		return errors.New("invalid_context: ranking empty")
	case d.StateSource == "":
		return errors.New("invalid_context: state_source empty")
	case d.Candidates == nil:
		return errors.New("invalid_context: candidates missing (send [] when there are none)")
	}
	if d.Role != "" && d.Role != "lead" && d.Role != "worker" && d.Role != "review" && d.Role != "consult" {
		return errors.New("invalid_context: role must be lead, worker, review or consult")
	}
	if d.Outcome == "placed" && (d.TaskID == nil || d.ChosenHost == nil || d.Family == "" || d.Role == "") {
		return errors.New("invalid_context: placed decisions require task_id, chosen_host, family and role")
	}
	for i, c := range d.Candidates {
		if c.Host == "" {
			return fmt.Errorf("invalid_context: candidates[%d].host", i)
		}
		if c.Outcome == "" {
			return fmt.Errorf("invalid_context: candidates[%d].outcome", i)
		}
	}
	return nil
}

// hubRecordForRole builds the record the hub posts when a node answers rc 0
// with the given spawn role, for a trace whose candidates were under the free
// floor — the state the hub ranks after every ineligible host and still picks
// from (hk 4465).
func hubRecordForRole(t *testing.T, outcomeRole, traceRole string) fleet.PlacementRecord {
	t.Helper()
	task := hubSpawnTask{Job: "job-4728", Attempt: 1}
	task.Outcome = hubSpawnOutcome{RC: 0, NodeResult: true, Host: "ncp", Role: outcomeRole, Profile: "worker", Pool: "claude"}
	tr := fleet.PlacementTrace{
		DecidedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Source:      "auto",
		Ranking:     "score",
		StateSource: "prometheus",
		StateReason: "available",
		Family:      "anthropic",
		Profile:     "worker",
		Role:        traceRole,
		TaskID:      42,
		Chosen:      "ncp",
		Candidates: []fleet.PlacementCandidateRecord{{
			Host:    "ncp",
			Outcome: "chosen",
			Reason:  "available",
			Score:   new(0.5),
			PlacementSignals: fleet.PlacementSignals{
				Terms:          &fleet.PlacementTerms{CPU: 0.01, Mem: 1, IO: 1, Load: 1, Jobs: 1},
				Power:          new(100.0),
				BelowFreeFloor: true,
			},
		}},
	}
	return hubPlacementRecord(task, tr, 1)
}

// The hub record names hk's work class, not the spawn role the node answered
// with. Without the mapping hk refuses the record with a reason, and the
// attempt is sealed as quarantined.
func TestHubPlacementRecordRoleIsHKWorkClass_t4728(t *testing.T) {
	cases := []struct {
		outcomeRole, traceRole, want string
	}{
		{"implementer", "", "worker"},
		{"reviewer", "", "review"},
		{"consult", "", "consult"},
		{"builder", "", "worker"},
		// A superseded attempt rebuilds from the trace alone: the trace role is
		// already a work class and must stay one.
		{"", "review", "review"},
		{"", "", "worker"},
	}
	for _, tc := range cases {
		name := tc.outcomeRole + "+" + tc.traceRole
		if tc.outcomeRole == "" {
			name = "trace_" + tc.traceRole
		}
		t.Run(name, func(t *testing.T) {
			record := hubRecordForRole(t, tc.outcomeRole, tc.traceRole)
			if record.Role != tc.want {
				t.Fatalf("role=%q, want hk's work class %q", record.Role, tc.want)
			}
			raw, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if refusal := hkPlacementRefusal(raw); refusal != nil {
				t.Fatalf("hk refuses the record: %v\nposted: %s", refusal, raw)
			}
		})
	}
}

// A candidate under the hub's free floor carries that verdict for the hub's own
// ranking; hk has no such field and rejects the whole record at decode, before
// any of its rules can name a reason.
func TestHubPlacementRecordCandidateWireMatchesHK_t4728(t *testing.T) {
	record := hubRecordForRole(t, "implementer", "")
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	var posted struct {
		Candidates []map[string]json.RawMessage `json:"candidates"`
	}
	if err := json.Unmarshal(raw, &posted); err != nil {
		t.Fatal(err)
	}
	if len(posted.Candidates) != 1 {
		t.Fatalf("candidates=%d, want 1", len(posted.Candidates))
	}
	var keys []string
	for key := range posted.Candidates[0] {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !contains(keys, "score") || !contains(keys, "terms") {
		t.Fatalf("the ranking evidence left the record: %v", keys)
	}
	if contains(keys, "below_free_floor") {
		t.Fatalf("the hub's floor verdict rides the record, which hk decodes strictly: %v", keys)
	}
	if refusal := hkPlacementRefusal(raw); refusal != nil {
		t.Fatalf("hk refuses the record: %v", refusal)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
