package panewire

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
)

func startProofHub(t *testing.T, path string) *HubServer {
	t.Helper()
	return startProofHubWindow(t, path, 200*time.Millisecond, 100*time.Millisecond)
}

// startProofHubWindow sets the start_by and fence margin. A test in which the
// grant must win uses a long window, so a slow -race runner cannot fence it.
func startProofHubWindow(t *testing.T, path string, startBy, margin time.Duration) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: path, SpawnStartBy: startBy, SpawnStartByMargin: margin})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	return h
}

// startProofNode connects machine with the given capabilities.
func startProofNode(t *testing.T, h *HubServer, server *httptest.Server, machine string, capabilities []string) *websocket.Conn {
	t.Helper()
	headers := http.Header{}
	headers.Set(hubMachineIDHeader, machine)
	headers.Set(hubAuthorizationHeader, "Bearer node-token")
	connection, _, err := websocket.Dial(t.Context(), t14WSURL(server.URL, "/v1/agent"), &websocket.DialOptions{HTTPHeader: headers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	if err := wsjson.Write(t.Context(), connection, map[string]any{"type": "hello", "machine_id": machine, "version": "t14", "accepting": true, "capabilities": capabilities}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, "the node to connect", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		record := h.nodes[machine]
		return record != nil && record.agent != nil && record.state == "connected"
	})
	return connection
}

// startProofTask binds the durable request to desktop as the placement would.
func startProofTask(t *testing.T, h *HubServer, id int, grantRequired bool) (hubSpawnTask, hubSpawnRequest) {
	t.Helper()
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(task, id, "desktop")
	node.GrantRequired = grantRequired
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return task, node
}

func startProofClaim(t *testing.T, connection *websocket.Conn, nodeID string) bool {
	t.Helper()
	if err := wsjson.Write(t.Context(), connection, map[string]string{"type": "job.spawn.claim", "request_id": nodeID}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, payload, err := connection.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := parseHubOutbound(payload)
	if !ok || message.Type != "job.spawn.grant" || message.RequestID != nodeID {
		t.Fatalf("claim answer %s", payload)
	}
	return message.Granted
}

// Grant and fence are one durable decision per node request id: whichever is
// written first wins, a grant is idempotent, and only the bound,
// grant-required attempt on its own machine can be granted.
func TestSpawnGrantAndFenceExcludeEachOther_t4612(t *testing.T) {
	h := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	task, node := startProofTask(t, h, 461210, true)
	store := h.inbox.store
	if granted, err := store.decideSpawnGrant(t.Context(), node.RequestID, "desktop", time.Now()); err != nil || granted {
		t.Fatalf("grant before send: %v %v", granted, err)
	}
	sentSpawnFixture(t, store, task)
	if granted, _ := store.decideSpawnGrant(t.Context(), node.RequestID, "machine-b", time.Now()); granted {
		t.Fatal("another machine was granted the attempt")
	}
	if granted, err := store.decideSpawnGrant(t.Context(), node.RequestID, "desktop", time.Now()); err != nil || !granted {
		t.Fatalf("grant: %v %v", granted, err)
	}
	if granted, _ := store.decideSpawnGrant(t.Context(), node.RequestID, "desktop", time.Now()); !granted {
		t.Fatal("a repeated claim lost its grant")
	}
	if fenced, err := store.fenceSpawnAttempt(t.Context(), task.Request.RequestID, node.RequestID, time.Now()); err != nil || fenced {
		t.Fatalf("fence after grant: %v %v", fenced, err)
	}
	other := t14RequestID(461211)
	if fenced, err := store.fenceSpawnAttempt(t.Context(), task.Request.RequestID, other, time.Now()); err != nil || !fenced {
		t.Fatalf("fence: %v %v", fenced, err)
	}
	if fenced, _ := store.fenceSpawnAttempt(t.Context(), task.Request.RequestID, other, time.Now()); !fenced {
		t.Fatal("a repeated fence lost")
	}

	fencedHub := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	fencedTask, fencedNode := startProofTask(t, fencedHub, 461213, true)
	sentSpawnFixture(t, fencedHub.inbox.store, fencedTask)
	if fenced, err := fencedHub.inbox.store.fenceSpawnAttempt(t.Context(), fencedTask.Request.RequestID, fencedNode.RequestID, time.Now()); err != nil || !fenced {
		t.Fatalf("fence of a sent attempt: %v %v", fenced, err)
	}
	if granted, err := fencedHub.inbox.store.decideSpawnGrant(t.Context(), fencedNode.RequestID, "desktop", time.Now()); err != nil || granted {
		t.Fatalf("a fenced sent attempt was granted: %v %v", granted, err)
	}

	legacy := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	legacyTask, legacyNode := startProofTask(t, legacy, 461212, false)
	sentSpawnFixture(t, legacy.inbox.store, legacyTask)
	if granted, _ := legacy.inbox.store.decideSpawnGrant(t.Context(), legacyNode.RequestID, "desktop", time.Now()); granted {
		t.Fatal("an attempt sent without the grant requirement was granted")
	}
}

// A grant-required attempt the node never claimed is fenced at start_by plus
// the margin and goes to the next host; a late claim is then refused. On main
// a silent node kept the request until its landing deadline.
func TestSpawnUngrantedAttemptIsFencedAndReplaced_t4612(t *testing.T) {
	retryHK(t)
	h := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	task, node := startProofTask(t, h, 461220, true)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (&directSpawnTransport{h: h, task: task}).dispatch(ctx, node)
		done <- err
	}()
	frame := t14ReadNodeSpawn(t, desktop)
	if frame.StartByMS == 0 {
		t.Fatal("grant-required frame carried no start_by_ms")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("silent attempt was not fenced")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil || len(saved.Exclusions) != 1 || saved.Exclusions[0].Machine != "desktop" || saved.Exclusions[0].Reason != "deadline_not_started" {
		t.Fatalf("fenced attempt: status=%s node=%v exclusions=%+v err=%v", saved.Status, saved.Node != nil, saved.Exclusions, err)
	}
	if startProofClaim(t, desktop, node.RequestID) {
		t.Fatal("a late claim for a fenced attempt was granted")
	}
}

// A granted attempt is never released on silence: past the fence time it
// keeps waiting, and its landing timeout leaves it deadline_exceeded (owned,
// as on main) with the reservation, and a restarted hub cannot fence it either.
func TestSpawnGrantedSilentAttemptKeepsOwnership_t4612(t *testing.T) {
	retryHK(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h := startProofHubWindow(t, path, time.Second, 500*time.Millisecond)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	task, node := startProofTask(t, h, 461230, true)
	waiting, triggerDeadline := context.WithCancel(t.Context())
	defer triggerDeadline()
	done := make(chan error, 1)
	go func() {
		_, err := (&directSpawnTransport{h: h, task: task}).dispatch(triggeredDeadlineContext{Context: waiting}, node)
		done <- err
	}()
	t14ReadNodeSpawn(t, desktop)
	if !startProofClaim(t, desktop, node.RequestID) {
		t.Fatal("the bound node was refused its grant")
	}
	// Window proving nothing more happens: past start_by + margin.
	time.Sleep(2 * time.Second)
	select {
	case err := <-done:
		t.Fatalf("granted attempt ended on silence: %v", err)
	default:
	}
	triggerDeadline()
	<-done
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "deadline_exceeded" || saved.Node == nil || saved.Node.RequestID != node.RequestID {
		t.Fatalf("granted silent attempt: status=%s err=%v", saved.Status, err)
	}
	if stored := startProofStoredStatus(t, h, task.Request.RequestID); stored != "deadline_exceeded_grant" {
		t.Fatalf("stored status=%q, want deadline_exceeded_grant", stored)
	}
	if reserved, err := h.inbox.store.spawnJobReserved(t.Context(), "test-job"); err != nil || !reserved {
		t.Fatalf("reservation released: %v %v", reserved, err)
	}
	_ = h.Close()
	restarted := startProofHub(t, path)
	if fenced, err := restarted.inbox.store.fenceSpawnAttempt(t.Context(), task.Request.RequestID, node.RequestID, time.Now()); err != nil || fenced {
		t.Fatalf("restarted hub fenced a granted attempt: %v %v", fenced, err)
	}
}

// A node without the grant capability gets no start_by and is never fenced:
// its silence keeps the request exactly as before.
func TestSpawnLegacyNodeIsNeverFenced_t4612(t *testing.T) {
	retryHK(t)
	h := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1"})
	task, node := startProofTask(t, h, 461240, false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := (&directSpawnTransport{h: h, task: task}).dispatch(ctx, node)
		done <- err
	}()
	if frame := t14ReadNodeSpawn(t, desktop); frame.StartByMS != 0 {
		t.Fatalf("legacy frame carried start_by_ms=%d", frame.StartByMS)
	}
	// Window proving nothing more happens: three times start_by + margin.
	time.Sleep(900 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("legacy attempt ended on silence: %v", err)
	default:
	}
	cancel()
	<-done
	if saved, _ := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID); saved.Status != "dispatching" {
		t.Fatalf("legacy attempt status=%s, want dispatching", saved.Status)
	}
}

// A grant-required binding is never sent to a node that lost the
// capability (a downgrade): it goes back to placement unsent.
func TestSpawnGrantRequiredSkipsNodeWithoutCapability_t4612(t *testing.T) {
	retryHK(t)
	h := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1"})
	task, node := startProofTask(t, h, 461250, true)
	if _, err := (&directSpawnTransport{h: h, task: task}).dispatch(t.Context(), node); err == nil {
		t.Fatal("dispatch sent a grant-required attempt")
	}
	if saved, _ := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID); saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("status=%s node=%v, want unsent and unbound", saved.Status, saved.Node != nil)
	}
}

// After a hub restart the coordinator's replay of a sent, ungranted attempt
// carries a fresh start_by and fences it on silence like the first send.
func TestSpawnRestartReplayFencesUngrantedAttempt_t4612(t *testing.T) {
	retryHK(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	first := startProofHub(t, path)
	task, _ := startProofTask(t, first, 461260, true)
	sentSpawnFixture(t, first.inbox.store, task)
	_ = first.Close()
	h := startProofHub(t, path)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "dispatching" {
		t.Fatalf("restored status=%s err=%v", saved.Status, err)
	}
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(t.Context(), saved) }()
	if frame := t14ReadNodeSpawn(t, desktop); frame.StartByMS == 0 {
		t.Fatal("replay carried no start_by_ms")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("replayed silent attempt was not fenced")
	}
	after, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || after.Status != "accepted" || len(after.Exclusions) != 1 || after.Exclusions[0].Reason != "deadline_not_started" {
		t.Fatalf("after replay: status=%s exclusions=%+v err=%v", after.Status, after.Exclusions, err)
	}
}

// End to end through the coordinator and a real node client: placement on a
// node advertising the capability binds a grant-required attempt, the hub
// records the grant, and the node's real read loop delivers it, so the node
// goes on past the claim to its next step (here the brief fetch, which this
// fixture node cannot do) instead of answering deadline_not_started at its
// start_by.
func TestSpawnGrantEndToEndWithRealNode_t4612(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h := startProofHubWindow(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), 5*time.Second, time.Second)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	client, err := NewHubClient(HubClientConfig{URL: t14WSURL(server.URL, ""), MachineID: "desktop", Token: "node-token", Accepting: true, AllowInsecureForTests: true, SpawnConfigPath: t3482SpawnConfig(t, filepath.Join(t.TempDir(), "work")), SpawnJournalDir: t.TempDir(), PingInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() { defer close(stopped); client.Run(ctx) }()
	defer func() { cancel(); <-stopped }()
	eventually(t, 3*time.Second, "the real node to connect with the grant capability", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		record := h.nodes["desktop"]
		return record != nil && record.state == "connected" && record.capabilities[hubCapabilitySpawnGrant]
	})
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) { return retryDispatchApp(t, out, errout), nil }
	h.startSpawnCoordinator(t.Context())
	request := durableSpawnRequest()
	if status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request); status != http.StatusAccepted {
		t.Fatalf("submit returned HTTP %d", status)
	}
	var status, result string
	read := func() bool {
		return h.inbox.store.db.QueryRow(`SELECT status,result FROM spawn_operations WHERE request_id=?`, request.RequestID).Scan(&status, &result) == nil && status == "retryable"
	}
	if !waitUntil(4*time.Second, read) {
		t.Fatalf("request did not settle before the node's start_by: status=%s result=%.300s", status, result)
	}
	var state string
	if err := h.inbox.store.db.QueryRow(`SELECT state FROM spawn_attempt_grants`).Scan(&state); err != nil || state != "granted" {
		t.Fatalf("grant row state=%s err=%v", state, err)
	}
	if strings.Contains(result, "deadline_not_started") || !strings.Contains(result, "brief_doc_unavailable") {
		t.Fatalf("node did not go on past its grant: %.300s", result)
	}
}

// A fence committed before a hub stop, with the attempt still dispatching,
// settles on the next hub's replay without sending the frame again.
func TestSpawnCommittedFenceSettlesOnReplayWithoutResend_t4612(t *testing.T) {
	retryHK(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	first := startProofHub(t, path)
	task, node := startProofTask(t, first, 461261, true)
	sentSpawnFixture(t, first.inbox.store, task)
	if fenced, err := first.inbox.store.fenceSpawnAttempt(t.Context(), task.Request.RequestID, node.RequestID, time.Now()); err != nil || !fenced {
		t.Fatalf("fence=%v err=%v", fenced, err)
	}
	_ = first.Close()
	h := startProofHub(t, path)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "dispatching" {
		t.Fatalf("restored status=%s err=%v", saved.Status, err)
	}
	h.executeSpawnTask(t.Context(), saved)
	after, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || after.Status != "accepted" || len(after.Exclusions) != 1 || after.Exclusions[0].Reason != "deadline_not_started" {
		t.Fatalf("after replay: status=%s exclusions=%+v err=%v", after.Status, after.Exclusions, err)
	}
	// Window that proves nothing more happens: no job.spawn reaches the node.
	readCtx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	var raw json.RawMessage
	if wsjson.Read(readCtx, desktop, &raw) == nil && strings.Contains(string(raw), `"job.spawn"`) {
		t.Fatalf("fenced attempt was sent again: %s", raw)
	}
}

// A store error at the fence instant does not end fencing: the hub retries
// and re-places the silent, ungranted attempt once the store is back.
func TestSpawnFenceRetriesAfterStoreError_t4612(t *testing.T) {
	retryHK(t)
	h := startProofHubWindow(t, filepath.Join(t.TempDir(), "inbox.sqlite3"), 200*time.Millisecond, 100*time.Millisecond)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	task, _ := startProofTask(t, h, 461262, true)
	if _, err := h.inbox.store.db.Exec(`ALTER TABLE spawn_attempt_grants RENAME TO spawn_attempt_grants_away`); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(t.Context(), task) }()
	t14ReadNodeSpawn(t, desktop)
	// Fixture pacing: let the first fence attempt fail on the missing table.
	time.Sleep(600 * time.Millisecond)
	if _, err := h.inbox.store.db.Exec(`ALTER TABLE spawn_attempt_grants_away RENAME TO spawn_attempt_grants`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("fence was not retried after the store error")
	}
	after, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || after.Status != "accepted" || len(after.Exclusions) != 1 {
		t.Fatalf("after retry: status=%s exclusions=%+v err=%v", after.Status, after.Exclusions, err)
	}
}

func startProofStoredStatus(t *testing.T, h *HubServer, requestID string) string {
	t.Helper()
	var stored string
	if err := h.inbox.store.db.QueryRow(`SELECT status FROM spawn_operations WHERE request_id=?`, requestID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	return stored
}

// A sent grant-required attempt is stored under a status no hub before the
// start proof lists (v0.0.404, 54345a76), so after a rollback such a hub never
// replays, sweeps or releases it. Every read, the HTTP status surface
// included, shows the sent status it refines.
func TestSpawnGrantRequiredStatusHiddenFromOlderHubsAndClients_t4612(t *testing.T) {
	retryHK(t)
	h := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	task, _ := startProofTask(t, h, 461270, true)
	if stored := startProofStoredStatus(t, h, task.Request.RequestID); stored != "bound_grant" || task.Status != "bound" {
		t.Fatalf("bound: stored=%q read=%q, want bound_grant and bound", stored, task.Status)
	}
	startProofOlderHubMatchesNone(t, h, task.Request.RequestID)
	sent := sentSpawnFixture(t, h.inbox.store, task)
	if stored := startProofStoredStatus(t, h, task.Request.RequestID); stored != "dispatching_grant" {
		t.Fatalf("stored status=%q, want dispatching_grant", stored)
	}
	if n, err := h.inbox.store.spawnGrantRowCount(t.Context()); err != nil || n != 1 {
		t.Fatalf("grant rows=%d err=%v, want 1", n, err)
	}
	if sent.Status != "dispatching" {
		t.Fatalf("store read status=%q, want dispatching", sent.Status)
	}
	startProofOlderHubMatchesNone(t, h, task.Request.RequestID)
	status, _, body := t14HTTP(t, http.MethodGet, server.URL+"/v1/spawns/"+task.Request.RequestID, t14OperatorToken, nil)
	var reply struct {
		Status string `json:"status"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &reply) != nil || reply.Status != "dispatching" {
		t.Fatalf("GET /v1/spawns/<id>: HTTP %d %s, want status dispatching", status, body)
	}
}

// startProofOlderHubMatchesNone runs the status predicate of every v0.0.404
// spawn_operations query that binds, sends, replays, sweeps, reports or
// releases against the row.
func startProofOlderHubMatchesNone(t *testing.T, h *HubServer, requestID string) {
	t.Helper()
	for _, where := range []string{
		`status IN ('accepted','bound','dispatching','reconciling')`,
		`status='deadline_exceeded'`,
		`status IN ('accepted','bound','dispatching','reconciling','deadline_exceeded')`,
		`status IN ('bound','dispatching','reconciling','deadline_exceeded')`,
		`(status IN ('bound','dispatching','reconciling','succeeded') OR status='deadline_exceeded')`,
		`status IN ('accepted','bound','dispatching') OR status='succeeded'`,
		`status IN ('succeeded','failed')`,
		`status IN ('staged','retryable')`,
	} {
		var n int
		if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_operations WHERE request_id=? AND (`+where+`)`, requestID).Scan(&n); err != nil || n != 0 {
			t.Fatalf("an older hub's %s matches the grant-required row: n=%d err=%v", where, n, err)
		}
	}
}

// A grant-required row that an older hub moved to a plain sent status (it
// sent the frame with no start_by) is never treated as grant-required again:
// the replay carries no start_by, the claim is refused, it is never fenced and
// a later write keeps the plain status.
func TestSpawnLegacySentGrantRowIsNeverFenced_t4612(t *testing.T) {
	retryHK(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	first := startProofHub(t, path)
	task, node := startProofTask(t, first, 461271, true)
	// What v0.0.404 does to a bound row it sends.
	if _, err := first.inbox.store.db.Exec(`UPDATE spawn_operations SET status='dispatching' WHERE request_id=?`, task.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	h := startProofHub(t, path)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "dispatching" || saved.Node == nil || saved.Node.GrantRequired {
		t.Fatalf("legacy-sent row read as %+v err=%v, want dispatching without grant_required", saved.Node, err)
	}
	if granted, err := h.inbox.store.decideSpawnGrant(t.Context(), node.RequestID, "desktop", time.Now()); err != nil || granted {
		t.Fatalf("legacy-sent row granted=%v err=%v", granted, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); h.executeSpawnTask(ctx, saved) }()
	if frame := t14ReadNodeSpawn(t, desktop); frame.StartByMS != 0 {
		t.Fatalf("legacy-sent replay carried start_by_ms=%d", frame.StartByMS)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// Window that proves nothing more happens: 200 ms start_by + 100 ms
		// margin passed many times over without a fence.
		cancel()
		<-done
	}
	if fenced, err := h.inbox.store.spawnAttemptFenced(t.Context(), node.RequestID); err != nil || fenced {
		t.Fatalf("legacy-sent row fenced=%v err=%v", fenced, err)
	}
	if stored := startProofStoredStatus(t, h, task.Request.RequestID); stored != "dispatching" {
		t.Fatalf("legacy-sent row stored=%q, want plain dispatching", stored)
	}
}

// With --spawn-grant-drain, placement goes on, but a node with the grant
// capability gets a plain binding and a frame with no start_by: no new
// *_grant row appears.
func TestSpawnGrantDrainBindsNoGrantRows_t4612(t *testing.T) {
	retryHK(t)
	h := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	h.spawnGrantDrain = true
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	desktop := startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) { return retryDispatchApp(t, out, errout), nil }
	h.startSpawnCoordinator(t.Context())
	request := durableSpawnRequest()
	if status, _, _ := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request); status != http.StatusAccepted {
		t.Fatalf("submit returned HTTP %d", status)
	}
	frame := t14ReadNodeSpawn(t, desktop)
	if frame.StartByMS != 0 {
		t.Fatalf("draining hub sent start_by_ms=%d", frame.StartByMS)
	}
	if stored := startProofStoredStatus(t, h, request.RequestID); stored != "dispatching" {
		t.Fatalf("draining hub stored %q, want plain dispatching", stored)
	}
	if n, err := h.inbox.store.spawnGrantRowCount(t.Context()); err != nil || n != 0 {
		t.Fatalf("grant rows=%d err=%v, want 0", n, err)
	}
}

// With --spawn-grant-drain a quota_stop starts no hand-off.
func TestSpawnGrantDrainStartsNoQuotaHandOff_t4612(t *testing.T) {
	// The logger is set at construction: the fixture's node session reads it
	// on another goroutine (hk 5282).
	logs := &syncBuffer{}
	f := newQuotaReplaceFixtureLogged(t, "reviewer", "mwr-1-p1", nil, logs)
	f.hub.spawnGrantDrain = true
	event := quotaStopEvent(f.job, f.stopPool)
	event.Reason = "quota_stop"
	f.hub.startQuotaReplacement("machine-a", event)
	f.hub.spawnWorkers.Wait()
	if !strings.Contains(logs.String(), "hub quota hand-off skipped while draining grant rows") {
		t.Fatalf("no drain skip logged: %s", logs.String())
	}
	if reserved, err := f.hub.inbox.store.spawnJobReserved(t.Context(), "mwr-1-p1-q1"); err != nil || reserved {
		t.Fatalf("draining hub reserved a successor: %v %v", reserved, err)
	}
}

// A replay with a short context (a recovery reprobe) fences an ungranted
// attempt whose latest start_by plus the margin has passed, before sending
// it again, and re-places it.
func TestSpawnShortReplayFencesPastStartBy_t4612(t *testing.T) {
	retryHK(t)
	h := startProofHub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	startProofNode(t, h, server, "desktop", []string{"spawn_v2", "spawn_ops_v1", hubCapabilitySpawnGrant})
	task, node := startProofTask(t, h, 461290, true)
	sent := sentSpawnFixture(t, h.inbox.store, task)
	if err := h.inbox.store.recordSpawnStartBy(t.Context(), node.RequestID, time.Now().Add(-time.Second).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	h.executeSpawnTask(ctx, sent)
	if fenced, err := h.inbox.store.spawnAttemptFenced(t.Context(), node.RequestID); err != nil || !fenced {
		t.Fatalf("short replay fenced=%v err=%v", fenced, err)
	}
	after, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || after.Status != "accepted" || len(after.Exclusions) != 1 || after.Exclusions[0].Reason != "deadline_not_started" {
		t.Fatalf("after short replay: status=%s exclusions=%+v err=%v", after.Status, after.Exclusions, err)
	}
}
