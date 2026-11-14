package panewire

import (
	"net/http"
	"path/filepath"
	"testing"
)

// hk 4654: a --parent-job successor replaces its predecessor's job, and the
// lane its task's findings are addressed to must follow the work onto the
// successor's machine. The hub moves the row the predecessor's pane holds,
// whatever its name; a route no such job holds is never moved this way.

func takeoverHub(t *testing.T, lanes string, jobs map[string]*hubJobRecord) (*HubServer, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lanes.json")
	lanesProjectionWrite(t, path, lanes)
	hub := lanesNodeHub(t, path, nil)
	for id, record := range jobs {
		hub.jobs[id] = record
	}
	return hub, path
}

func takeoverJob(id, node, pane string) *hubJobRecord {
	return &hubJobRecord{HubActiveJob: HubActiveJob{JobID: id, AgentLabel: id, Epoch: 1, Pane: pane}, Node: node}
}

func TestLaneTakeoverMovesThePredecessorRowAcrossMachines(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"mw-recheck-coverage":{"machine":"machine-a","pane":"w1:p1","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		map[string]*hubJobRecord{"mw-recheck-coverage": takeoverJob("mw-recheck-coverage", "machine-a", "w1:p1")})

	// Another machine cannot simply replace the row: that is the refusal the
	// takeover path exists for.
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/mw-recheck-coverage", lanesNodeBToken, "machine-b", `{"machine":"machine-b","pane":"w2:p5"}`); writer.Code != http.StatusForbidden {
		t.Fatalf("plain cross-machine replace status=%d body=%s", writer.Code, writer.Body.String())
	}

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/mw-recheck-coverage", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","parent":"mergewake","takeover":"mw-recheck-coverage"}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("takeover status=%d body=%s", writer.Code, writer.Body.String())
	}
	var projection hubLaneProjection
	if err := decodeLanesJSON(writer.Body, &projection); err != nil || projection.Lane != "mw-recheck-coverage" || projection.Machine != "machine-b" || projection.Pane != "w2:p5" || projection.Parent != "mergewake" {
		t.Fatalf("takeover projection=%+v err=%v", projection, err)
	}
	routes := lanesNodeFile(t, path)
	route, found := routes["mw-recheck-coverage"]
	if len(routes) != 2 || !found || route.Machine != "machine-b" || route.Pane != "w2:p5" {
		t.Fatalf("routes after takeover: %+v", routes)
	}

	// A retried takeover converges on the row this pane already holds instead
	// of adding a second name.
	retry := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/mw-recheck-coverage", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","parent":"mergewake","takeover":"mw-recheck-coverage"}`)
	if retry.Code != http.StatusOK {
		t.Fatalf("retried takeover status=%d body=%s", retry.Code, retry.Body.String())
	}
	var held hubLaneProjection
	if err := decodeLanesJSON(retry.Body, &held); err != nil || held.Lane != "mw-recheck-coverage" || held.Machine != "machine-b" {
		t.Fatalf("retried takeover projection=%+v err=%v", held, err)
	}
	if routes := lanesNodeFile(t, path); len(routes) != 2 {
		t.Fatalf("retried takeover changed the row count: %+v", routes)
	}
}

// The successor's requested name does not decide what moves: the predecessor's
// row keeps its name, which is the lane its task's findings are addressed to.
func TestLaneTakeoverKeepsThePredecessorLaneName(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"author-lane":{"machine":"machine-a","pane":"w1:p1","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		map[string]*hubJobRecord{"fix-job-1": takeoverJob("fix-job-1", "machine-a", "w1:p1")})

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/fix-job-2", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","parent":"mergewake","takeover":"fix-job-1"}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("takeover status=%d body=%s", writer.Code, writer.Body.String())
	}
	var projection hubLaneProjection
	if err := decodeLanesJSON(writer.Body, &projection); err != nil || projection.Lane != "author-lane" {
		t.Fatalf("projection=%+v err=%v", projection, err)
	}
	routes := lanesNodeFile(t, path)
	if _, extra := routes["fix-job-2"]; extra || len(routes) != 2 {
		t.Fatalf("takeover must move the row, not rename it: %+v", routes)
	}
	if route := routes["author-lane"]; route.Machine != "machine-b" || route.Pane != "w2:p5" {
		t.Fatalf("author row: %+v", route)
	}
}

func TestLaneTakeoverRefusals(t *testing.T) {
	lanes := `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","parent":"mergewake"},"pinned":{"machine":"machine-a","pane":"w3:p3","protected":true},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`
	hub, path := takeoverHub(t, lanes, map[string]*hubJobRecord{
		// A job the hub knows, but on another pane than the row.
		"job-elsewhere": takeoverJob("job-elsewhere", "machine-a", "w8:p8"),
		// The row a job holds may still be one the node may not take.
		"job-pinned": takeoverJob("job-pinned", "machine-a", "w3:p3"),
	})

	body := func(takeover string) string {
		return `{"machine":"machine-b","pane":"w2:p5","takeover":"` + takeover + `"}`
	}
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", lanesNodeBToken, "machine-b", body("job-unknown")); writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("unknown job status=%d body=%q", writer.Code, writer.Body.String())
	}
	// The named job does not hold the row, so the write falls back to the
	// ordinary scope rules and the foreign row answers the shared refusal.
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", lanesNodeBToken, "machine-b", body("job-elsewhere")); writer.Code != http.StatusForbidden {
		t.Fatalf("unproven takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", lanesNodeBToken, "machine-b", body("bad job id")); writer.Code != http.StatusBadRequest {
		t.Fatalf("malformed takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/pinned", lanesNodeBToken, "machine-b", body("job-pinned")); writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_protected"}`+"\n" {
		t.Fatalf("protected takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["lane-a"].Machine != "machine-a" {
		t.Fatalf("a refused takeover changed the row: %+v", routes)
	}
}
