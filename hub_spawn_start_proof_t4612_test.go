package panewire

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func startProofHub(t *testing.T, path string) *HubServer {
	t.Helper()
	t.Setenv(crashReportDSNEnv, "")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: path, SpawnStartBy: 200 * time.Millisecond, SpawnStartByMargin: 100 * time.Millisecond})
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
// keeps waiting, its landing timeout keeps it owned (deadline_exceeded or
// reconciling) with the reservation, and a restarted hub cannot fence it either.
func TestSpawnGrantedSilentAttemptStaysReconciling_t4612(t *testing.T) {
	retryHK(t)
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h := startProofHub(t, path)
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
	// Window proving nothing more happens: three times start_by + margin.
	time.Sleep(900 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("granted attempt ended on silence: %v", err)
	default:
	}
	triggerDeadline()
	<-done
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || (saved.Status != "reconciling" && saved.Status != "deadline_exceeded") || saved.Node == nil || saved.Node.RequestID != node.RequestID {
		t.Fatalf("granted silent attempt: status=%s err=%v", saved.Status, err)
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
