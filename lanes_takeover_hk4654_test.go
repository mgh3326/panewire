package panewire

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
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
	hub.inbox = &hubInbox{store: openSpawnStore(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))}
	for id, record := range jobs {
		hub.jobs[id] = record
	}
	return hub, path
}

func acceptedTakeoverSuccessor(t *testing.T, hub *HubServer, job, predecessor string) {
	t.Helper()
	if prior := hub.jobs[predecessor]; prior != nil {
		if _, err := hub.inbox.store.latestSpawnForJob(t.Context(), predecessor); err != nil {
			seedTakeoverPredecessor(t, hub.inbox.store, predecessor, prior.Node, prior.Pane)
		}
	}
	request := durableSpawnRequest()
	request.RequestID = uuid.NewString()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--owner", "mergewake", "--role", "implementer", "--host", "auto", "--parent-job", predecessor}
	task, _, err := hub.inbox.store.claimSpawn(t.Context(), request, job, "mergewake", time.Now())
	if err != nil {
		t.Fatalf("claim accepted takeover spawn: %v", err)
	}
	if _, err := hub.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, hubSpawnRequest{RequestID: uuid.NewString(), Machine: "machine-b", Args: request.Args}, time.Now()); err != nil {
		t.Fatalf("bind accepted takeover spawn: %v", err)
	}
}

func takeoverJob(id, node, pane string) *hubJobRecord {
	return &hubJobRecord{HubActiveJob: HubActiveJob{JobID: id, AgentLabel: id, Epoch: 1, Pane: pane}, Node: node}
}

func TestLaneTakeoverMovesThePredecessorRowAcrossMachines(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"mw-recheck-coverage":{"machine":"machine-a","pane":"w1:p1","job":"mw-recheck-coverage","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		map[string]*hubJobRecord{"mw-recheck-coverage": takeoverJob("mw-recheck-coverage", "machine-a", "w1:p1")})
	acceptedTakeoverSuccessor(t, hub, "successor-job", "mw-recheck-coverage")

	// Another machine cannot simply replace the row: that is the refusal the
	// takeover path exists for.
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/mw-recheck-coverage", lanesNodeBToken, "machine-b", `{"machine":"machine-b","pane":"w2:p5"}`); writer.Code != http.StatusForbidden {
		t.Fatalf("plain cross-machine replace status=%d body=%s", writer.Code, writer.Body.String())
	}

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/mw-recheck-coverage", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"successor-job","parent":"mergewake","takeover":"mw-recheck-coverage"}`)
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
		`{"machine":"machine-b","pane":"w2:p5","job":"successor-job","parent":"mergewake","takeover":"mw-recheck-coverage"}`)
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
		`{"lanes":{"author-lane":{"machine":"machine-a","pane":"w1:p1","job":"fix-job-1","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		map[string]*hubJobRecord{"fix-job-1": takeoverJob("fix-job-1", "machine-a", "w1:p1")})
	acceptedTakeoverSuccessor(t, hub, "fix-job-2", "fix-job-1")

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/fix-job-2", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"fix-job-2","parent":"mergewake","takeover":"fix-job-1"}`)
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
	lanes := `{"lanes":{"lane-a":{"machine":"machine-a","pane":"w1:p1","job":"unrelated-job","parent":"mergewake"},"pinned":{"machine":"machine-a","pane":"w3:p3","job":"job-pinned","protected":true},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`
	hub, path := takeoverHub(t, lanes, map[string]*hubJobRecord{
		// A job the hub knows, but on another pane than the row.
		"job-elsewhere": takeoverJob("job-elsewhere", "machine-a", "w8:p8"),
		// The row a job holds may still be one the node may not take.
		"job-pinned": takeoverJob("job-pinned", "machine-a", "w3:p3"),
	})

	body := func(takeover, successor string) string {
		return `{"machine":"machine-b","pane":"w2:p5","job":"` + successor + `","takeover":"` + takeover + `"}`
	}
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", lanesNodeBToken, "machine-b", body("job-unknown", "successor-unknown")); writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("unknown job status=%d body=%q", writer.Code, writer.Body.String())
	}
	acceptedTakeoverSuccessor(t, hub, "successor-elsewhere", "job-elsewhere")
	// A recorded predecessor on another pane does not authorize moving lane-a.
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", lanesNodeBToken, "machine-b", body("job-elsewhere", "successor-elsewhere")); writer.Code != http.StatusConflict {
		t.Fatalf("unproven takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-a", lanesNodeBToken, "machine-b", body("bad job id", "successor-elsewhere")); writer.Code != http.StatusBadRequest {
		t.Fatalf("malformed takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	delete(hub.jobs, "successor-elsewhere")
	acceptedTakeoverSuccessor(t, hub, "successor-pinned", "job-pinned")
	if writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/pinned", lanesNodeBToken, "machine-b", body("job-pinned", "successor-pinned")); writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_protected"}`+"\n" {
		t.Fatalf("protected takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["lane-a"].Machine != "machine-a" {
		t.Fatalf("a refused takeover changed the row: %+v", routes)
	}
}

// The client authorizes a takeover with the job the task was bound to, which
// may be an ancestor of the successor's --parent-job after rebindInherited.
// The hub proves the chain from its own durable spawn records.
func seedAcceptedChainJob(t *testing.T, store *laneInboxStore, job, parentJob, machine, pane string) {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = uuid.NewString()
	args := []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--owner", "mergewake", "--role", "implementer", "--host", "auto", "--parent-job", parentJob}
	if parentJob == "" {
		args[len(args)-1] = "chain-root"
	}
	request.Args = args
	task, _, err := store.claimSpawn(t.Context(), request, job, "mergewake", time.Now())
	if err != nil {
		t.Fatalf("claim chain job %s: %v", job, err)
	}
	if _, err := store.completeSpawn(t.Context(), task, hubSpawnOutcome{RC: 0, Stage: "landing", Host: machine, Pane: pane}, time.Now()); err != nil {
		t.Fatalf("record chain job %s route: %v", job, err)
	}
}

func bindChainSuccessor(t *testing.T, store *laneInboxStore, job, parentJob, machine string) {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = uuid.NewString()
	request.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", job, "--job", job, "--t", "T1", "--owner", "mergewake", "--role", "implementer", "--host", "auto", "--parent-job", parentJob}
	task, _, err := store.claimSpawn(t.Context(), request, job, "mergewake", time.Now())
	if err != nil {
		t.Fatalf("claim chain successor %s: %v", job, err)
	}
	if _, err := store.bindSpawn(t.Context(), request.RequestID, task.Attempt, hubSpawnRequest{RequestID: uuid.NewString(), Machine: machine, Args: request.Args}, time.Now()); err != nil {
		t.Fatalf("bind chain successor %s: %v", job, err)
	}
}

func TestLaneTakeoverAncestorTwoHopsMovesLane(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","job":"ancestor-a","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "ancestor-a", "", "machine-a", "w1:p1")
	seedAcceptedChainJob(t, hub.inbox.store, "mid-b", "ancestor-a", "machine-a", "w1:p2")
	bindChainSuccessor(t, hub.inbox.store, "successor-s", "mid-b", "machine-b")

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"successor-s","parent":"mergewake","takeover":"ancestor-a"}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("ancestor takeover status=%d body=%s", writer.Code, writer.Body.String())
	}
	routes := lanesNodeFile(t, path)
	route, found := routes["task-lane"]
	if !found || route.Machine != "machine-b" || route.Pane != "w2:p5" || route.Job != "successor-s" {
		t.Fatalf("routes after ancestor takeover: %+v", routes)
	}
}

func TestLaneTakeoverOffChainRefused(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","job":"ancestor-a","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "ancestor-a", "", "machine-a", "w1:p1")
	seedAcceptedChainJob(t, hub.inbox.store, "mid-b", "ancestor-a", "machine-a", "w1:p2")
	seedAcceptedChainJob(t, hub.inbox.store, "off-chain", "", "machine-a", "w3:p3")
	bindChainSuccessor(t, hub.inbox.store, "successor-s", "mid-b", "machine-b")

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"successor-s","parent":"mergewake","takeover":"off-chain"}`)
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("off-chain takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["task-lane"].Machine != "machine-a" || routes["task-lane"].Job != "ancestor-a" {
		t.Fatalf("refused off-chain takeover changed the row: %+v", routes)
	}
}

func TestLaneTakeoverBeyondBoundRefused(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"lane-far":{"machine":"machine-a","pane":"w1:p1","job":"bound-base","parent":"mergewake"},"lane-near":{"machine":"machine-a","pane":"w1:p2","job":"bound-c0","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "bound-base", "", "machine-a", "w1:p1")
	parent := "bound-base"
	for _, job := range []string{"bound-c0", "bound-c1", "bound-c2", "bound-c3", "bound-c4", "bound-c5", "bound-c6", "bound-c7", "bound-c8"} {
		pane := "w1:p1"
		if job == "bound-c0" {
			pane = "w1:p2"
		}
		seedAcceptedChainJob(t, hub.inbox.store, job, parent, "machine-a", pane)
		parent = job
	}
	bindChainSuccessor(t, hub.inbox.store, "bound-successor", "bound-c8", "machine-b")

	beyond := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-far", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"bound-successor","parent":"mergewake","takeover":"bound-base"}`)
	if beyond.Code != http.StatusConflict || beyond.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("beyond-bound takeover status=%d body=%q", beyond.Code, beyond.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["lane-far"].Machine != "machine-a" {
		t.Fatalf("refused beyond-bound takeover changed the row: %+v", routes)
	}

	within := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-near", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p6","job":"bound-successor","parent":"mergewake","takeover":"bound-c0"}`)
	if within.Code != http.StatusOK {
		t.Fatalf("within-bound takeover status=%d body=%s", within.Code, within.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["lane-near"].Machine != "machine-b" || routes["lane-near"].Job != "bound-successor" {
		t.Fatalf("routes after within-bound takeover: %+v", routes)
	}
}

func TestLaneTakeoverSurvivesUnrelatedWrites(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","job":"job-j","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "job-j", "", "machine-a", "w1:p1")

	operator := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeOperatorToken, "",
		`{"machine":"machine-a","pane":"w1:p1","parent":"mergewake"}`)
	if operator.Code != http.StatusOK {
		t.Fatalf("operator re-add status=%d body=%s", operator.Code, operator.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["task-lane"].Job != "job-j" {
		t.Fatalf("operator re-add erased the binding: %+v", routes)
	}

	reregister := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeAToken, "machine-a",
		`{"machine":"machine-a","pane":"w1:p1","parent":"mergewake"}`)
	if reregister.Code != http.StatusOK {
		t.Fatalf("node re-register status=%d body=%s", reregister.Code, reregister.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["task-lane"].Job != "job-j" {
		t.Fatalf("node re-register erased the binding: %+v", routes)
	}

	bindChainSuccessor(t, hub.inbox.store, "successor-j", "job-j", "machine-b")
	takeover := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"successor-j","parent":"mergewake","takeover":"job-j"}`)
	if takeover.Code != http.StatusOK {
		t.Fatalf("takeover status=%d body=%s", takeover.Code, takeover.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["task-lane"].Machine != "machine-b" || routes["task-lane"].Pane != "w2:p5" || routes["task-lane"].Job != "successor-j" {
		t.Fatalf("routes after takeover: %+v", routes)
	}
}

// A row bound to the takeover job at any pane other than its accepted
// placement is never moved: the binding may have been written while the
// job's spawn was still pending, at a pane that never hosted it.
func TestLaneTakeoverRefusesRowOffAcceptedPlacement(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w9:p9","job":"placed-job","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p8"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "placed-job", "", "machine-a", "w1:p1")
	bindChainSuccessor(t, hub.inbox.store, "successor-p", "placed-job", "machine-b")

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"successor-p","parent":"mergewake","takeover":"placed-job"}`)
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("off-placement takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["task-lane"].Machine != "machine-a" || routes["task-lane"].Pane != "w9:p9" {
		t.Fatalf("refused off-placement takeover changed the row: %+v", routes)
	}
}

// The takeover moves the predecessor's row even when the successor already
// holds its own row at the target pane: the retry convergence must not
// pre-empt a predecessor row that still needs moving.
func TestLaneTakeoverMovesDespiteOwnRowAtPane(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","job":"pred-t","parent":"mergewake"},"succ-lane":{"machine":"machine-b","pane":"w2:p5","job":"succ-own","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "pred-t", "", "machine-a", "w1:p1")
	bindChainSuccessor(t, hub.inbox.store, "succ-own", "pred-t", "machine-b")

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"succ-own","parent":"mergewake","takeover":"pred-t"}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("takeover status=%d body=%s", writer.Code, writer.Body.String())
	}
	routes := lanesNodeFile(t, path)
	if route := routes["task-lane"]; route.Machine != "machine-b" || route.Pane != "w2:p5" || route.Job != "succ-own" {
		t.Fatalf("predecessor row not moved: %+v", routes)
	}
	moved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	retry := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"succ-own","parent":"mergewake","takeover":"pred-t"}`)
	if retry.Code != http.StatusOK {
		t.Fatalf("retried takeover status=%d body=%s", retry.Code, retry.Body.String())
	}
	again, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(moved) {
		t.Fatalf("retried takeover changed the file:\nbefore %s\nafter %s", moved, again)
	}

}

// Two rows bound to the predecessor job refuse the takeover with a distinct
// code, even when the successor already holds its own row at the target pane:
// moving one would strand the other's reports.
func TestLaneTakeoverAmbiguousPredecessorRefused(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"t1":{"machine":"machine-a","pane":"w1:p1","job":"pred-a","parent":"mergewake"},"t2":{"machine":"machine-a","pane":"w1:p2","job":"pred-a","parent":"mergewake"},"succ-lane":{"machine":"machine-b","pane":"w2:p5","job":"succ-a","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "pred-a", "", "machine-a", "w1:p1")
	bindChainSuccessor(t, hub.inbox.store, "succ-a", "pred-a", "machine-b")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/t1", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"succ-a","parent":"mergewake","takeover":"pred-a"}`)
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_ambiguous"}`+"\n" {
		t.Fatalf("ambiguous takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("refused ambiguous takeover changed the file:\nbefore %s\nafter %s", before, after)
	}
}

// No row bound to the predecessor job refuses the takeover, even when the
// successor holds an unrelated lane at the target pane: that row proves
// nothing about this takeover.
func TestLaneTakeoverMissingPredecessorWithUnrelatedRowRefused(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","parent":"mergewake"},"succ-lane":{"machine":"machine-b","pane":"w2:p5","job":"succ-m","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "pred-m", "", "machine-a", "w1:p1")
	bindChainSuccessor(t, hub.inbox.store, "succ-m", "pred-m", "machine-b")

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"succ-m","parent":"mergewake","takeover":"pred-m"}`)
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("missing-predecessor takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["task-lane"].Machine != "machine-a" || routes["task-lane"].Pane != "w1:p1" {
		t.Fatalf("refused takeover changed the row: %+v", routes)
	}
}

// The requested lane itself sitting at the successor pane bound to the
// successor job proves nothing without the takeover stamp: it may be the
// successor's own lane, never moved by this takeover. The retry refuses.
func TestLaneTakeoverUnstampedOwnRowRefused(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"succ-lane":{"machine":"machine-b","pane":"w2:p5","job":"succ-u","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "pred-u", "", "machine-a", "w1:p1")
	bindChainSuccessor(t, hub.inbox.store, "succ-u", "pred-u", "machine-b")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/succ-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"succ-u","parent":"mergewake","takeover":"pred-u"}`)
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("unstamped takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != string(before) {
		t.Fatalf("refused takeover changed the file: %q", after)
	}
}

// A lost response retries the identical PUT under a lane name that was never
// created: the move kept the predecessor row's own name. The retry converges
// on that moved row and changes nothing.
func TestLaneTakeoverRetryDifferentNameConverges(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"lane-old":{"machine":"machine-a","pane":"w1:p1","job":"pred-r","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "pred-r", "", "machine-a", "w1:p1")
	bindChainSuccessor(t, hub.inbox.store, "succ-r", "pred-r", "machine-b")
	body := `{"machine":"machine-b","pane":"w2:p5","job":"succ-r","parent":"mergewake","takeover":"pred-r"}`

	first := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-new", lanesNodeBToken, "machine-b", body)
	if first.Code != http.StatusOK {
		t.Fatalf("takeover status=%d body=%s", first.Code, first.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["lane-old"].Machine != "machine-b" || routes["lane-old"].Job != "succ-r" {
		t.Fatalf("predecessor row not moved: %+v", routes)
	}
	moved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	retry := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-new", lanesNodeBToken, "machine-b", body)
	if retry.Code != http.StatusOK {
		t.Fatalf("retried takeover status=%d body=%s", retry.Code, retry.Body.String())
	}
	var projection hubLaneProjection
	if err := decodeLanesJSON(retry.Body, &projection); err != nil || projection.Lane != "lane-old" {
		t.Fatalf("retry projection=%+v err=%v", projection, err)
	}
	again, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(moved) {
		t.Fatalf("retried takeover changed the file:\nbefore %s\nafter %s", moved, again)
	}
}

// A --purpose value that looks like --parent-job= never forges a takeover
// parent: flag reads walk strict pairs. A successor with no real --parent-job
// is refused even when the forgery names an accepted job with a bound row.
func TestLaneTakeoverPurposeForgeryRefused(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","job":"victim-f","parent":"mergewake"},"lane-p":{"machine":"machine-a","pane":"w1:p2","job":"real-p","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "victim-f", "", "machine-a", "w1:p1")
	seedAcceptedChainJob(t, hub.inbox.store, "real-p", "", "machine-a", "w1:p2")

	forged := durableSpawnRequest()
	forged.RequestID = uuid.NewString()
	forged.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "succ-f", "--job", "succ-f", "--t", "T1", "--owner", "mergewake", "--role", "implementer", "--host", "auto", "--task", "43", "--purpose", "--parent-job=victim-f"}
	forgedTask, _, err := hub.inbox.store.claimSpawn(t.Context(), forged, "succ-f", "mergewake", time.Now())
	if err != nil {
		t.Fatalf("claim forged successor: %v", err)
	}
	if _, err := hub.inbox.store.bindSpawn(t.Context(), forged.RequestID, forgedTask.Attempt, hubSpawnRequest{RequestID: uuid.NewString(), Machine: "machine-b", Args: forged.Args}, time.Now()); err != nil {
		t.Fatalf("bind forged successor: %v", err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/task-lane", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p5","job":"succ-f","parent":"mergewake","takeover":"victim-f"}`)
	if writer.Code != http.StatusConflict || writer.Body.String() != `{"error":"lane_takeover_unconfirmed"}`+"\n" {
		t.Fatalf("forged takeover status=%d body=%q", writer.Code, writer.Body.String())
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != string(before) {
		t.Fatalf("refused forgery changed the file: %q", after)
	}
}

// A real --parent-job still wins when a --purpose value forges another one
// earlier in the argv: the forgery is the purpose's value, not a flag.
func TestLaneTakeoverRealParentBeatsForgery(t *testing.T) {
	hub, path := takeoverHub(t,
		`{"lanes":{"task-lane":{"machine":"machine-a","pane":"w1:p1","job":"victim-f","parent":"mergewake"},"lane-p":{"machine":"machine-a","pane":"w1:p2","job":"real-p","parent":"mergewake"},"mergewake":{"machine":"machine-a","pane":"w9:p9"}}}`,
		nil)
	seedAcceptedChainJob(t, hub.inbox.store, "victim-f", "", "machine-a", "w1:p1")
	seedAcceptedChainJob(t, hub.inbox.store, "real-p", "", "machine-a", "w1:p2")

	mixed := durableSpawnRequest()
	mixed.RequestID = uuid.NewString()
	mixed.Args = []string{"-c", "/work/project", "-p", "/work/brief.md", "-m", "auto", "-l", "succ-m", "--job", "succ-m", "--t", "T1", "--owner", "mergewake", "--role", "implementer", "--host", "auto", "--purpose", "--parent-job=victim-f", "--parent-job", "real-p"}
	mixedTask, _, err := hub.inbox.store.claimSpawn(t.Context(), mixed, "succ-m", "mergewake", time.Now())
	if err != nil {
		t.Fatalf("claim mixed successor: %v", err)
	}
	if _, err := hub.inbox.store.bindSpawn(t.Context(), mixed.RequestID, mixedTask.Attempt, hubSpawnRequest{RequestID: uuid.NewString(), Machine: "machine-b", Args: mixed.Args}, time.Now()); err != nil {
		t.Fatalf("bind mixed successor: %v", err)
	}
	writer := lanesNodeRequest(t, hub, http.MethodPut, "/v1/lanes/lane-p", lanesNodeBToken, "machine-b",
		`{"machine":"machine-b","pane":"w2:p6","job":"succ-m","parent":"mergewake","takeover":"real-p"}`)
	if writer.Code != http.StatusOK {
		t.Fatalf("takeover status=%d body=%s", writer.Code, writer.Body.String())
	}
	if routes := lanesNodeFile(t, path); routes["lane-p"].Machine != "machine-b" || routes["lane-p"].Job != "succ-m" {
		t.Fatalf("real-parent takeover did not move: %+v", routes)
	}
	if routes := lanesNodeFile(t, path); routes["task-lane"].Machine != "machine-a" {
		t.Fatalf("forgery moved the victim row: %+v", routes)
	}
}
