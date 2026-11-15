package fleet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// hk 4728: both placement record producers map a spawn role to hk's work
// class through PlacementWorkClass, and a record's candidate carries only the
// fields hk decodes.

func TestPlacementWorkClass_t4728(t *testing.T) {
	cases := []struct{ role, routing, want string }{
		{"reviewer", "", "review"},
		{"implementer", "judge", "review"},
		{"implementer", "", "worker"},
		{"builder", "", "worker"},
		{"consult", "", "consult"},
		{"", "", "worker"},
		// A work class maps to itself, so a record rebuilt from a trace role
		// and remapped by a replay keeps its class.
		{"worker", "", "worker"},
		{"review", "", "review"},
		{"consult", "", "consult"},
	}
	for _, tc := range cases {
		if got := PlacementWorkClass(tc.role, tc.routing); got != tc.want {
			t.Fatalf("role=%q routing=%q mapped to %q, want %q", tc.role, tc.routing, got, tc.want)
		}
	}
}

// The free-floor verdict is the hub's ranking input, so it must still decode
// from the hub's placement answer; it must not ride the hk record.
func TestPlacementCandidateRecordDropsFloorVerdict_t4728(t *testing.T) {
	var decoded PlacementSignals
	if err := json.Unmarshal([]byte(`{"below_free_floor":true,"terms":{"cpu":0.01,"mem":1,"io":1,"load":1,"jobs":1}}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.BelowFreeFloor || decoded.Terms == nil || decoded.Terms.CPU != 0.01 {
		t.Fatalf("the hub's floor verdict no longer decodes: %+v", decoded)
	}
	raw, err := json.Marshal(PlacementCandidateRecord{
		Host:             "ncp",
		Outcome:          "chosen",
		Reason:           "available",
		PlacementSignals: PlacementSignals{BelowFreeFloor: true, Power: new(100.0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for key := range wire {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	if slices.Contains(keys, "below_free_floor") {
		t.Fatalf("the floor verdict rides the record: %v", keys)
	}
	if !slices.Contains(keys, "power") {
		t.Fatalf("the candidate lost its ranking evidence: %v", keys)
	}
}

// A quarantined record whose role is a spawn role is re-posted as hk's work
// class, without the floor verdict, and leaves the quarantine once hk takes
// it.
func TestRequeueRejectedPlacementsRepostsWorkClass_t4728(t *testing.T) {
	a := testApp(t)
	h := placementServer(t, a)
	rejected := filepath.Join(a.stateDir(), "placements-rejected")
	if err := os.MkdirAll(rejected, 0700); err != nil {
		t.Fatal(err)
	}
	record := PlacementRecord{
		JobID: "job-4728", Attempt: 1, TaskID: 42,
		DecidedAt: "2026-01-01T00:00:00Z", ToolVersion: "test",
		Source: "auto", Ranking: "score", StateSource: "prometheus",
		Outcome: "placed", Reason: "available",
		Candidates: []PlacementCandidateRecord{{
			Host: "test-host", Outcome: "chosen", Reason: "available",
			PlacementSignals: PlacementSignals{
				Terms:          &PlacementTerms{CPU: 0.01, Mem: 1, IO: 1, Load: 1, Jobs: 1},
				BelowFreeFloor: true,
			},
		}},
		PlacementExecution: PlacementExecution{Host: "test-host", Family: "anthropic", Role: "implementer", Profile: "worker", Pool: "claude"},
	}
	body, err := json.Marshal(pendingPlacement{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	name := "job-4728.1.json"
	if err := os.WriteFile(filepath.Join(rejected, name), body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rejected, name+".reason"), []byte(`{"status":400,"error":"invalid_context"}`), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := a.RequeueRejectedPlacements(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 {
		t.Fatalf("replayed %d records, want 1", len(result.Records))
	}
	if row := result.Records[0]; row.Outcome != "delivered" || row.Role != "worker" || row.Job != "job-4728" {
		t.Fatalf("replay row: %+v", row)
	}
	requests, rows := h.snapshot()
	if len(requests) != 1 || len(rows) != 1 {
		t.Fatalf("hk saw %d requests and holds %d rows", len(requests), len(rows))
	}
	var posted struct {
		Role       string                       `json:"role"`
		Candidates []map[string]json.RawMessage `json:"candidates"`
	}
	if err := json.Unmarshal(requests[0], &posted); err != nil {
		t.Fatal(err)
	}
	if posted.Role != "worker" {
		t.Fatalf("posted role=%q, want worker", posted.Role)
	}
	if len(posted.Candidates) != 1 {
		t.Fatalf("posted candidates=%d", len(posted.Candidates))
	}
	if _, found := posted.Candidates[0]["below_free_floor"]; found {
		t.Fatal("the replayed record carries the floor verdict")
	}
	if _, found := posted.Candidates[0]["terms"]; !found {
		t.Fatal("the replayed record lost its ranking evidence")
	}
	for _, path := range []string{filepath.Join(rejected, name), filepath.Join(rejected, name+".reason")} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("%s survived a delivered replay", filepath.Base(path))
		}
	}
	if spooled := placementSpool(t, a); len(spooled) != 0 {
		t.Fatalf("the replay left %v spooled", spooled)
	}
}

// hk's refusal reason names the failing field (hk 197); the producer's
// warning must carry it, bounded, and the quarantine sidecar must keep it.
func TestPlacementRefusalLogsHKsReason_t4728(t *testing.T) {
	a := testApp(t)
	h := placementServer(t, a)
	h.statusFirst = http.StatusBadRequest
	h.refusalBody = fmt.Sprintf(`{"error":"invalid_context","reason":%q}`,
		"role must be lead, worker, review or consult "+strings.Repeat("x", 300))
	record := PlacementRecord{
		JobID: "job-4728", Attempt: 1, DecidedAt: "2026-01-01T00:00:00Z",
		ToolVersion: "test", Source: "auto", Ranking: "score",
		StateSource: "prometheus", Outcome: "refused", Reason: "exit_placement",
		Candidates: []PlacementCandidateRecord{},
	}
	body, err := json.Marshal(pendingPlacement{Record: record})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(a.stateDir(), "placements", "job-4728.1.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := atomicFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	a.flushPlacements()
	logs := a.Err.(*bytes.Buffer).String()
	if !strings.Contains(logs, "placement record job-4728.1.json rejected: hk HTTP 400 invalid_context: role must be lead, worker, review or consult") {
		t.Fatalf("the refusal line lost hk's reason: %s", logs)
	}
	if strings.Contains(logs, strings.Repeat("x", 200)) || !strings.Contains(logs, "…") {
		t.Fatalf("hk's reason is not bounded to one line: %s", logs)
	}
	note, err := os.ReadFile(filepath.Join(a.stateDir(), "placements-rejected", "job-4728.1.json.reason"))
	if err != nil {
		t.Fatal(err)
	}
	var sidecar struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(note, &sidecar); err != nil || !strings.Contains(sidecar.Reason, "role must be lead") {
		t.Fatalf("the sidecar lost hk's reason: %s", note)
	}
}
