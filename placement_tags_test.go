package panewire

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func taggedPlacementHub(t *testing.T) *HubServer {
	policy := measuredLoadPolicy()
	policy.Tags = map[string][]string{"mac-work": {"dagu", "macos"}, "desktop": {"dagu", "linux"}}
	busy := map[string]string{"mac-work": "0.9", "desktop": "0.1"}
	h := measuredLoadHub(t, map[string]map[string]string{placementCPUBusyQuery: busy, placementCPUBusy1mQuery: busy, placementThermalQuery: idleThermal()}, policy)
	t.Cleanup(func() { h.Close() })
	return h
}
func TestPlacementTagsFilterAndUseHighestScore(t *testing.T) {
	h := taggedPlacementHub(t)
	if plain := h.placement(t.Context(), "worker", "repo"); plain.Decision != "mac-work" {
		t.Fatalf("legacy policy order changed: %+v", plain)
	}
	if tagged := h.placement(t.Context(), "worker", "repo", "dagu"); tagged.Decision != "desktop" || len(tagged.Candidates) != 2 || tagged.Candidates[0].Score <= tagged.Candidates[1].Score {
		t.Fatalf("tagged score ranking %+v", tagged)
	}
	if mac := h.placement(t.Context(), "worker", "repo", "dagu", "macos"); mac.Decision != "mac-work" || len(mac.Candidates) != 1 {
		t.Fatalf("AND filter or cache leaked: %+v", mac)
	}
	if missing := h.placement(t.Context(), "worker", "repo", "gpu"); missing.Decision != "unavailable" || len(missing.Candidates) != 0 || missing.Reason != "no_tagged_capacity" {
		t.Fatalf("unknown capabilities widened pool: %+v", missing)
	}
}
func TestPlacementTagsKeepAcceptanceAndCapacityGuards(t *testing.T) {
	for _, guard := range []string{"not accepting", "slot full", "disconnected"} {
		t.Run(guard, func(t *testing.T) {
			h := taggedPlacementHub(t)
			h.mu.Lock()
			switch guard {
			case "not accepting":
				h.nodes["desktop"].accepting = false
			case "disconnected":
				h.nodes["desktop"].agent = nil
				h.nodes["desktop"].state = "disconnected"
			case "slot full":
				h.placementPolicy.Machines = map[string]PlacementMachineSlots{"desktop": {TaskSlots: 1}}
				h.nodes["desktop"].activeJobs = map[string]HubActiveJob{"job": {JobID: "job", AgentLabel: "worker", Epoch: 1}}
			}
			h.mu.Unlock()
			got := h.placement(t.Context(), "worker", "repo", "linux")
			if got.Decision != "unavailable" || len(got.Candidates) != 0 {
				t.Fatalf("guard bypassed or local fallback selected: %+v", got)
			}
		})
	}
}
func TestPlacementTagsRejectMalformedPolicyAndQuery(t *testing.T) {
	policy := measuredLoadPolicy()
	for _, tags := range []map[string][]string{{"operator": {"dagu"}}, {"bad host": {"dagu"}}, {"desktop": {"GPU"}}, {"desktop": {"dagu", "dagu"}}, {"unconfigured": {"dagu"}}, {"unconfigured": {}}} {
		policy.Tags = tags
		raw, _ := json.Marshal(policy)
		if _, err := ParsePlacementPolicy(raw); err == nil {
			t.Fatalf("invalid tags accepted %v", tags)
		}
	}
	h := taggedPlacementHub(t)
	for _, query := range []string{"tags=a,,b", "tags=GPU", "tags=a&tags=b"} {
		request := httptest.NewRequest("GET", "/v1/placement?class=worker&"+query, nil)
		request.Header.Set("Authorization", "Bearer "+r6OperatorToken)
		reply := httptest.NewRecorder()
		h.Handler().ServeHTTP(reply, request)
		if reply.Code != 400 {
			t.Fatalf("query=%s code=%d", query, reply.Code)
		}
	}
}

func TestPlacementTagsAcceptMaximumRequest(t *testing.T) {
	tags := make([]string, 16)
	for i := range tags {
		tags[i] = strings.Repeat(string(rune('a'+i)), 64)
	}
	raw := strings.Join(tags, ",")
	parsed, err := parsePlacementTags(raw)
	if err != nil || len(parsed) != 16 {
		t.Fatalf("maximum documented request refused: tags=%d err=%v", len(parsed), err)
	}
	for _, invalid := range []string{raw + "x", raw + ",extra"} {
		if _, err := parsePlacementTags(invalid); err == nil {
			t.Fatalf("oversized request accepted: %s", invalid)
		}
	}
}

func TestPlacementTagsRequirePolicyCandidates(t *testing.T) {
	policy := measuredLoadPolicy()
	policy.Tags = map[string][]string{policy.LocalMachine: {"dagu"}, policy.SpillTargets[0]: {"linux"}}
	raw, _ := json.Marshal(policy)
	if _, err := ParsePlacementPolicy(raw); err != nil {
		t.Fatalf("local/spill tag keys refused: %v", err)
	}
	policy.SpillTargets = nil
	raw, _ = json.Marshal(policy)
	if _, err := ParsePlacementPolicy(raw); err == nil {
		t.Fatal("removed candidate's stale tag key accepted")
	}
}
func TestPlaceCLISendsCanonicalTags(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("tags"); got != "dagu,macos" {
			t.Errorf("tags=%q", got)
		}
		json.NewEncoder(w).Encode(PlacementResult{Decision: "desktop", Source: "hub-only", Candidates: []PlacementCandidate{{Machine: "desktop", Connected: true, Reason: "available"}}})
	}))
	defer server.Close()
	token := filepath.Join(t.TempDir(), "operator.env")
	if err := os.WriteFile(token, []byte("HUB_MACHINE_ID=operator\nHUB_TOKEN="+r6OperatorToken+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--class", "worker", "--tags", "macos,dagu,macos", "--hub-url", server.URL, "--hub-token-env", token}
	var out, stderr bytes.Buffer
	if code := runPlaceCLI(args, &out, &stderr, hubCLIDeps{HTTPClient: server.Client(), AllowInsecureForTests: true}); code != ExitOK {
		t.Fatalf("code=%d error=%s", code, stderr.String())
	}
	if !strings.Contains(out.String(), `"decision":"desktop"`) {
		t.Fatal(out.String())
	}
}
