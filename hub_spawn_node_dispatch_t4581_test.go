package panewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/fleetevent"
)

// boundDispatchFixture creates a distinct durable job already assigned to a node.
func boundDispatchFixture(t *testing.T, h *HubServer, n int, machine string) hubSpawnTask {
	t.Helper()
	request := durableSpawnRequest()
	request.RequestID = t14RequestID(458100 + n)
	request.Args = append([]string(nil), request.Args...)
	job := fmt.Sprintf("dispatch-%d", n)
	for i, arg := range request.Args {
		switch arg {
		case "test-job":
			request.Args[i] = job
		case "42":
			request.Args[i] = fmt.Sprint(458100 + n)
		}
	}
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, job, "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(task, 458200+n, machine)
	node.CWDKey = "repo-a"
	node.Args = []string{"--job", job, "-m", "sonnet"}
	task, err = h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, node, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func TestNodeDispatcherRunsOnlyOneSpawnAtATime_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	first := boundDispatchFixture(t, h, 1, "machine-a")
	second := boundDispatchFixture(t, h, 2, "machine-a")
	h.startSpawnCoordinator(t.Context())
	message := t14ReadNodeSpawn(t, node)
	if message.RequestID != first.Node.RequestID && message.RequestID != second.Node.RequestID {
		t.Fatalf("unexpected first request %s", message.RequestID)
	}
	next := make(chan hubOutboundMessage, 1)
	go func() { next <- t14ReadNodeSpawn(t, node) }()
	// Window: a second frame must not be sent while the first is unresolved.
	select {
	case got := <-next:
		t.Fatalf("node received a second spawn while %s was unresolved: %s", message.RequestID, got.RequestID)
	case <-time.After(250 * time.Millisecond):
	}
	if err := wsjson.Write(t.Context(), node, nodeRefusal(message.RequestID, "spawn_disabled", false)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-next:
		if got.RequestID == message.RequestID {
			t.Fatalf("next slot replayed completed request %s", got.RequestID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("node did not receive its queued spawn after the slot was released")
	}
}

func TestQueuedBoundDeadlineExpiresWithNodeSlotBusy_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	for n := 0; n < 40; n++ {
		task := boundDispatchFixture(t, h, 10+n, "machine-a")
		if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET created_ms=? WHERE request_id=?`, time.Now().Add(time.Duration(n)*time.Millisecond).UnixMilli(), task.Request.RequestID); err != nil {
			t.Fatal(err)
		}
	}
	expired := boundDispatchFixture(t, h, 99, "machine-a")
	if _, err := h.inbox.store.db.Exec(`UPDATE spawn_operations SET created_ms=?,deadline_ms=? WHERE request_id=?`, time.Now().Add(time.Hour).UnixMilli(), time.Now().Add(-time.Second).UnixMilli(), expired.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	h.startSpawnCoordinator(t.Context())
	t14ReadNodeSpawn(t, node)
	var saved hubSpawnTask
	eventually(t, 3*time.Second, "queued unsent deadline to release its reservation", func() bool {
		var err error
		saved, err = h.inbox.store.spawnTask(t.Context(), expired.Request.RequestID)
		return err == nil && saved.Status == "retryable" && saved.Outcome.Reason == "timeout"
	})
	var reserved int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_reservations WHERE request_id=?`, expired.Request.RequestID).Scan(&reserved); err != nil || reserved != 0 {
		t.Fatalf("expired queued request kept reservation: count=%d err=%v", reserved, err)
	}
}

func TestRecoveredDefinitiveRefusalReturnsToPlacement_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	task := boundDispatchFixture(t, h, 101, "machine-a")
	task = sentSpawnFixture(t, h.inbox.store, task)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(boundedContext(t), *task.Node)
	}()
	message := t14ReadNodeSpawn(t, node)
	if err := wsjson.Write(t.Context(), node, nodeRefusal(message.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	<-done
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("recovered definitive refusal did not return to placement: status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
}

// A saturated node must not hide an idle node behind a page of queued rows.
func TestIdleNodeProgressesPastBusyNodeQueue_t4581(t *testing.T) {
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	busy := t14NamedNode(t, h, server, "machine-a", true, true)
	idle := t14NamedNode(t, h, server, "machine-b", true, true)
	boundDispatchFixture(t, h, 200, "machine-a")
	h.startSpawnCoordinator(t.Context())
	t14ReadNodeSpawn(t, busy)
	for n := 0; n < 40; n++ {
		boundDispatchFixture(t, h, 201+n, "machine-a")
	}
	want := boundDispatchFixture(t, h, 250, "machine-b")
	kickSpawnCoordinator(h)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, payload, err := idle.Read(ctx)
	if err != nil {
		t.Fatalf("idle node received no spawn behind the busy node's queue: %v", err)
	}
	message, valid := parseHubOutbound(payload)
	if !valid || message.Type != "job.spawn" || message.RequestID != want.Node.RequestID {
		t.Fatalf("idle node received %s; want request %s", payload, want.Node.RequestID)
	}
}

func TestRefusalStoreFailureRetainsDispatchOwnership_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	task := sentSpawnFixture(t, h.inbox.store, boundDispatchFixture(t, h, 300, "machine-a"))
	if _, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_refusal_unbind BEFORE UPDATE ON spawn_operations WHEN NEW.status='accepted' BEGIN SELECT RAISE(FAIL, 'injected unbind failure'); END`); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(boundedContext(t), *task.Node)
	}()
	message := t14ReadNodeSpawn(t, node)
	if err := wsjson.Write(t.Context(), node, nodeRefusal(message.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	<-done
	saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "dispatching" || saved.Node == nil || saved.Node.RequestID != task.Node.RequestID {
		t.Fatalf("failed refusal transaction lost dispatch ownership: status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
	var reserved int
	if err := h.inbox.store.db.QueryRow(`SELECT count(*) FROM spawn_reservations WHERE request_id=?`, task.Request.RequestID).Scan(&reserved); err != nil || reserved != 1 {
		t.Fatalf("failed refusal transaction changed reservation: count=%d err=%v", reserved, err)
	}
	if _, err := h.inbox.store.db.Exec(`DROP TRIGGER fail_refusal_unbind`); err != nil {
		t.Fatal(err)
	}
	done = make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: saved}).dispatch(boundedContext(t), *saved.Node)
	}()
	replayed := t14ReadNodeSpawn(t, node)
	if replayed.RequestID != task.Node.RequestID {
		t.Fatalf("store recovery replayed %s, want %s", replayed.RequestID, task.Node.RequestID)
	}
	if err := wsjson.Write(t.Context(), node, nodeRefusal(replayed.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	<-done
	recovered, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || recovered.Status != "accepted" || recovered.Node != nil {
		t.Fatalf("refusal did not recover after store repair: status=%s node=%+v err=%v", recovered.Status, recovered.Node, err)
	}
}

func TestRefusalExclusionSurvivesStoreRestartAndResetsOnRetry_t4581(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	task := sentSpawnFixture(t, h.inbox.store, boundDispatchFixture(t, h, 400, "machine-a"))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = (&directSpawnTransport{h: h, task: task}).dispatch(boundedContext(t), *task.Node)
	}()
	message := t14ReadNodeSpawn(t, node)
	if err := wsjson.Write(t.Context(), node, nodeRefusal(message.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	<-done
	_ = node.CloseNow()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	server.Close()
	h, err = NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server = httptest.NewServer(h.Handler())
	defer server.Close()
	node = t14Node(t, h, server, true)
	recovered := h.inbox.store
	saved, err := recovered.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("refusal did not survive recovery as an unbound request: status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
	transport := &directSpawnTransport{h: h, task: saved}
	request := refusalNodeRequest(saved.Request, 458401, "machine-a")
	reply, err := transport.Spawn(t.Context(), request)
	if err != nil || !reply.NextHost || reply.Error != "cwd_unmapped" {
		t.Fatalf("recovered placement reused refused host: reply=%+v err=%v", reply, err)
	}
	if changed, err := recovered.completeSpawn(t.Context(), saved, hubSpawnOutcome{RC: 124, Stage: "landing", Reason: "timeout", Retryable: true}, time.Now()); err != nil || !changed {
		t.Fatalf("finish first attempt: changed=%v err=%v", changed, err)
	}
	finished, err := recovered.spawnTask(t.Context(), saved.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := recovered.resumeCleanedSpawn(t.Context(), finished, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = (&directSpawnTransport{h: h, task: retry}).Spawn(t.Context(), request)
	rebound, err := recovered.spawnTask(t.Context(), retry.Request.RequestID)
	if err != nil || rebound.Status != "bound" || rebound.Node == nil || rebound.Node.RequestID != request.RequestID {
		t.Fatalf("new attempt did not clear the old host exclusion: status=%s node=%+v err=%v", rebound.Status, rebound.Node, err)
	}
}

func TestSubmissionProgressesPastStuckNode_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	var eventsMu sync.Mutex
	var events []fleetevent.Event
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.EventSink = func(event fleetevent.Event) {
			eventsMu.Lock()
			defer eventsMu.Unlock()
			events = append(events, event)
		}
		return app, nil
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	stuck := t14NamedNode(t, h, server, "machine-a", true, true)
	healthy := t14NamedNode(t, h, server, "desktop", true, true)
	for n := 0; n < hubSpawnWorkerCount; n++ {
		boundDispatchFixture(t, h, 500+n, "machine-a")
	}
	h.startSpawnCoordinator(t.Context())
	t14ReadNodeSpawn(t, stuck)
	request := durableSpawnRequest()
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("submission rejected: status=%d body=%s", status, body)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, payload, err := healthy.Read(ctx)
	if err != nil {
		t.Fatalf("accepted submission did not reach healthy node while another node was stuck: %v", err)
	}
	message, valid := parseHubOutbound(payload)
	if !valid || message.Type != "job.spawn" || hubSpawnArg(message.Args, "--job") != "test-job" {
		t.Fatalf("healthy node received %s, want submitted job", payload)
	}
	eventsMu.Lock()
	defer eventsMu.Unlock()
	for _, event := range events {
		if event.Job == "test-job" && (event.Kind == "spawn" || event.Kind == "placement") && event.Outcome == "refused" {
			t.Errorf("an in-flight binding produced a refusal event: %+v", event)
		}
	}
}

func TestPoolCountsCurrentBoundProfileOnRetry_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	task := liveOp(t, h, 458190, "profile-retry", "auto", time.Now())
	outcome := hubSpawnOutcome{RC: 70, Stage: "launch", Reason: "spawn_failed", Host: "machine-a", Profile: "sonnet", Retryable: true}
	if changed, err := h.inbox.store.completeSpawn(t.Context(), task, outcome, time.Now()); err != nil || !changed {
		t.Fatalf("finish first attempt: changed=%v err=%v", changed, err)
	}
	finished, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := h.inbox.store.resumeCleanedSpawn(t.Context(), finished, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	node := boundaryTestNode(retry, 458191, "machine-a")
	node.Args = []string{"--job", retry.Job, "-m", "builder-sol"}
	if _, err := h.inbox.store.bindSpawn(t.Context(), retry.Request.RequestID, retry.Attempt, node, time.Now()); err != nil {
		t.Fatal(err)
	}
	counts, err := h.liveSpawnProfiles(t.Context(), "")
	if err != nil || counts["builder-sol"] != 1 || len(counts) != 1 {
		t.Fatalf("bound retry counted its previous attempt's pool: profiles=%v err=%v", counts, err)
	}
}

func TestSubmittedWorkProgressesWhileStaleQuotaWriterIsHeld_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	hk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/tasks/"):
			fmt.Fprintf(w, `{"id":%s,"title":"work (grade B, T1)","state":"ready","refs":{}}`, strings.TrimPrefix(r.URL.Path, "/v1/tasks/"))
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/documents/"):
			fmt.Fprint(w, `{"document":{},"changed":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(hk.Close)
	t.Setenv("HANDOFFKEEP_URL", hk.URL)
	t.Setenv("HANDOFFKEEP_TOKEN_SVC_PANEWIRE", "test-only-token")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "desktop": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	var fresh atomic.Bool
	fresh.Store(true)
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Config.Hosts["machine-b"] = map[string]any{}
		app.Config.HostOrder = []string{"desktop", "machine-b", "machine-a"}
		app.Quota = staleRefreshQuota{}
		app.Source = staleRefreshSource{now: app.Now(), hosts: []string{"desktop", "machine-b"}, fresh: &fresh, freshKey: "claude:0000000000000002"}
		return app, nil
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	stuck := t14NamedNode(t, h, server, "machine-a", true, true)
	t14NamedNode(t, h, server, "desktop", true, true)
	healthy := t14NamedNode(t, h, server, "machine-b", true, true)
	h.mu.Lock()
	writer := h.nodes["desktop"].agent
	h.mu.Unlock()
	writer.writeMu.Lock()
	defer writer.writeMu.Unlock()
	for n := 0; n < hubSpawnWorkerCount; n++ {
		boundDispatchFixture(t, h, 600+n, "machine-a")
	}
	h.startSpawnCoordinator(t.Context())
	t14ReadNodeSpawn(t, stuck)
	stale := durableSpawnRequest()
	stale.RequestID = t14RequestID(458699)
	stale.Args = append([]string(nil), stale.Args...)
	for i, arg := range stale.Args {
		switch arg {
		case "test-job":
			stale.Args[i] = "stale-job"
		case "42":
			stale.Args[i] = "999"
		case "auto":
			if i > 0 && stale.Args[i-1] == "--host" {
				stale.Args[i] = "desktop"
			}
		}
	}
	stale.Args = append(stale.Args, "--containment", "hk:12", "--override-reason", "test")
	if _, _, err := h.inbox.store.claimSpawn(t.Context(), stale, "stale-job", "parent", time.Now()); err != nil {
		t.Fatal(err)
	}
	kickSpawnCoordinator(h)
	eventually(t, 3*time.Second, "stale quota refresh queued behind the held writer", func() bool {
		h.quotaRefresh.mu.Lock()
		defer h.quotaRefresh.mu.Unlock()
		return h.quotaRefresh.keys[hubQuotaRefreshKey{machine: "desktop", pool: "claude"}]
	})
	request := durableSpawnRequest()
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("healthy submission rejected: status=%d body=%s", status, body)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	_, payload, err := healthy.Read(ctx)
	if err != nil {
		t.Fatalf("healthy submission stalled behind stale quota writer: %v", err)
	}
	message, valid := parseHubOutbound(payload)
	if !valid || message.Type != "job.spawn" || hubSpawnArg(message.Args, "--job") != "test-job" {
		t.Fatalf("healthy node received %s, want submitted job", payload)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), stale.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("stale quota did not preserve unbound ownership: status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
}

func TestQueuedBindingMakesOverCapSubmissionRefuse_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.LiveSpawnProfiles = h.liveSpawnProfiles
		app.Config.Quota = map[string]any{"pool_max_concurrent": map[string]any{"p": int64(2)}}
		return app, nil
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	busy := t14NamedNode(t, h, server, "machine-a", true, true)
	t14NamedNode(t, h, server, "desktop", true, true)
	a := boundDispatchFixture(t, h, 700, "machine-a")
	b := boundDispatchFixture(t, h, 701, "machine-a")
	h.startSpawnCoordinator(t.Context())
	first := t14ReadNodeSpawn(t, busy)
	queued := a
	if first.RequestID == a.Node.RequestID {
		queued = b
	}
	request := durableSpawnRequest()
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("submission rejected before placement: status=%d body=%s", status, body)
	}
	var saved hubSpawnTask
	eventually(t, 3*time.Second, "over-cap placement refusal", func() bool {
		var err error
		saved, err = h.inbox.store.spawnTask(t.Context(), request.RequestID)
		return err == nil && saved.Status == "retryable"
	})
	if !strings.Contains(saved.Outcome.Reason, "pool_at_cap") || saved.Node != nil {
		t.Fatalf("over-cap placement was not refused: status=%s reason=%q node=%+v", saved.Status, saved.Outcome.Reason, saved.Node)
	}
	waiting, err := h.inbox.store.spawnTask(t.Context(), queued.Request.RequestID)
	if err != nil || waiting.Status != "bound" {
		t.Fatalf("queued job was not counted while unsent: status=%s err=%v", waiting.Status, err)
	}
}

func TestRefusalWakesRealPlacementOnAnotherNode_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = func(out, errout io.Writer) (*fleet.App, error) { return retryDispatchApp(t, out, errout), nil }
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	first := t14NamedNode(t, h, server, "machine-a", true, true)
	second := t14NamedNode(t, h, server, "desktop", true, true)
	request := durableSpawnRequest()
	task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	original := boundaryTestNode(task, 458800, "machine-a")
	if _, err := h.inbox.store.bindSpawn(t.Context(), request.RequestID, task.Attempt, original, time.Now()); err != nil {
		t.Fatal(err)
	}
	h.startSpawnCoordinator(t.Context())
	message := t14ReadNodeSpawn(t, first)
	if err := wsjson.Write(t.Context(), first, nodeRefusal(message.RequestID, "cwd_unmapped", true)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancel()
	_, payload, err := second.Read(ctx)
	if err != nil {
		t.Fatalf("refusal did not wake placement on another node: %v", err)
	}
	next, valid := parseHubOutbound(payload)
	if !valid || next.Type != "job.spawn" || next.RequestID == original.RequestID || hubSpawnArg(next.Args, "--job") != "test-job" {
		t.Fatalf("next node received %s after first request %s", payload, original.RequestID)
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
	if err != nil || saved.Attempt != task.Attempt || saved.Node == nil || saved.Node.Machine != "desktop" || saved.Node.RequestID != next.RequestID {
		t.Fatalf("re-placement lost logical ownership: attempt=%d node=%+v err=%v", saved.Attempt, saved.Node, err)
	}
}

type alternateHarnessQuota struct{ retryQuota }

func (alternateHarnessQuota) Catalog(context.Context) ([]fleet.CatalogRow, error) {
	return []fleet.CatalogRow{
		{Profile: "codex-sol", Effort: "high", Grade: "B", ModelID: "codex-model", Pool: "p", Gate: "default"},
		{Profile: "sonnet", Effort: "high", Grade: "B", ModelID: "claude-model", Pool: "p", Gate: "default"},
	}, nil
}

func (alternateHarnessQuota) Launch(_ context.Context, request fleet.QuotaRequest) (fleet.Launch, error) {
	return fleet.Launch{Profile: request.Profile, ModelID: "model", Effort: "high", Grade: "B", Pool: "p", Gate: "default"}, nil
}

func TestLaunchProofReplacesHarnessOnSameNode_t4581(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	retryHK(t)
	config := HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "desktop": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")}
	h, err := NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	newApp := func(out, errout io.Writer) (*fleet.App, error) {
		app := retryDispatchApp(t, out, errout)
		app.Quota = alternateHarnessQuota{}
		return app, nil
	}
	h.spawnNewApp = newApp
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14NamedNode(t, h, server, "desktop", true, true)
	h.startSpawnCoordinator(t.Context())
	request := durableSpawnRequest()
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/spawns", t14OperatorToken, request)
	if status != http.StatusAccepted {
		t.Fatalf("submission rejected: status=%d body=%s", status, body)
	}
	first := t14ReadNodeSpawn(t, node)
	firstProfile := hubSpawnArg(first.Args, "-m")
	if firstProfile != "sonnet" && firstProfile != "codex-sol" {
		t.Fatalf("unexpected initial harness: %q", firstProfile)
	}
	// Restart after the send boundary so the refusal is handled by recovery,
	// with no original placement worker left to hold the candidate loop.
	_ = node.CloseNow()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	server.Close()
	h, err = NewHubServer(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	h.spawnNewApp = newApp
	server = httptest.NewServer(h.Handler())
	defer server.Close()
	node = t14NamedNode(t, h, server, "desktop", true, true)
	h.startSpawnCoordinator(t.Context())
	replayed := t14ReadNodeSpawn(t, node)
	if replayed.RequestID != first.RequestID {
		t.Fatalf("recovery sent %s, want original request %s", replayed.RequestID, first.RequestID)
	}
	if err := wsjson.Write(t.Context(), node, map[string]any{"type": "job.spawn.result", "request_id": replayed.RequestID, "rc": 70, "job_id": "", "pane": "", "stdout_tail": "", "error": "launch_proof_failed", "stage": "launch", "retryable": true}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
	defer cancel()
	_, payload, err := node.Read(ctx)
	if err != nil {
		t.Fatalf("launch-proof refusal did not try an alternate harness on the same node: %v", err)
	}
	next, valid := parseHubOutbound(payload)
	nextProfile := hubSpawnArg(next.Args, "-m")
	if !valid || next.Type != "job.spawn" || next.RequestID == first.RequestID || nextProfile == "" || nextProfile == firstProfile {
		t.Fatalf("fallback after %s used %q: %s", firstProfile, nextProfile, payload)
	}
}

func TestContainmentUnbindsQueuedRowsDespiteStoreFailure_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	release := make(chan struct{})
	defer close(release)
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		<-release
		return nil, errors.New("fixture stops re-placement")
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	var tasks []hubSpawnTask
	for n := 0; n < hubSpawnWorkerCount; n++ {
		tasks = append(tasks, boundDispatchFixture(t, h, 900+n, "machine-a"))
	}
	h.startSpawnCoordinator(t.Context())
	first := t14ReadNodeSpawn(t, node)
	var queued hubSpawnTask
	for _, task := range tasks {
		if task.Node.RequestID != first.RequestID {
			queued = task
			break
		}
	}
	target := boundDispatchFixture(t, h, 999, "machine-a")
	_, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_contained_unbind BEFORE UPDATE ON spawn_operations WHEN OLD.request_id='` + target.Request.RequestID + `' AND NEW.status='accepted' BEGIN SELECT RAISE(FAIL, 'injected unbind failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body := t14HTTP(t, http.MethodPost, server.URL+"/v1/nodes/machine-a/accepting", t14OperatorToken, map[string]string{"mode": "off", "containment": "hk:12"})
	if status != http.StatusOK {
		t.Fatalf("containment override: status=%d body=%s", status, body)
	}
	eventually(t, 3*time.Second, "another queued binding unbound by containment", func() bool {
		saved, err := h.inbox.store.spawnTask(t.Context(), queued.Request.RequestID)
		return err == nil && saved.Status == "accepted" && saved.Node == nil
	})
	retained, err := h.inbox.store.spawnTask(t.Context(), target.Request.RequestID)
	if err != nil || retained.Status != "bound" || retained.Node == nil || retained.Node.RequestID != target.Node.RequestID {
		t.Fatalf("failed unbind lost the unsent binding: status=%s node=%+v err=%v", retained.Status, retained.Node, err)
	}
	if _, err := h.inbox.store.db.Exec(`DROP TRIGGER fail_contained_unbind`); err != nil {
		t.Fatal(err)
	}
	kickSpawnCoordinator(h)
	eventually(t, 3*time.Second, "failed containment unbind to recover", func() bool {
		saved, err := h.inbox.store.spawnTask(t.Context(), target.Request.RequestID)
		return err == nil && saved.Status == "accepted" && saved.Node == nil
	})
}

func TestDisconnectUnbindsOnlyUnsentRowsWhenWriterIsStuck_t4581(t *testing.T) {
	h := asyncSpawnHub(t)
	release := make(chan struct{})
	defer close(release)
	h.spawnNewApp = func(io.Writer, io.Writer) (*fleet.App, error) {
		<-release
		return nil, errors.New("fixture stops re-placement")
	}
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	h.mu.Lock()
	agent := h.nodes["machine-a"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	defer agent.writeMu.Unlock()
	var tasks []hubSpawnTask
	for n := 0; n < hubSpawnWorkerCount; n++ {
		tasks = append(tasks, boundDispatchFixture(t, h, 1000+n, "machine-a"))
	}
	h.startSpawnCoordinator(t.Context())
	var sent hubSpawnTask
	eventually(t, 3*time.Second, "one durable send transition behind held writer", func() bool {
		for _, task := range tasks {
			current, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
			if err == nil && current.Status == "dispatching" {
				sent = current
				return true
			}
		}
		return false
	})
	target := boundDispatchFixture(t, h, 1099, "machine-a")
	if _, err := h.inbox.store.db.Exec(`CREATE TRIGGER fail_disconnect_unbind BEFORE UPDATE ON spawn_operations WHEN OLD.request_id='` + target.Request.RequestID + `' AND NEW.status='accepted' BEGIN SELECT RAISE(FAIL, 'injected unbind failure'); END`); err != nil {
		t.Fatal(err)
	}
	_ = node.CloseNow()
	eventually(t, 3*time.Second, "node disconnect", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.nodes["machine-a"].state == "disconnected"
	})
	eventually(t, 3*time.Second, "another unsent binding unbound on disconnect", func() bool {
		for _, task := range tasks {
			if task.Request.RequestID == sent.Request.RequestID {
				continue
			}
			current, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
			if err == nil && current.Status == "accepted" && current.Node == nil {
				return true
			}
		}
		return false
	})
	retained, err := h.inbox.store.spawnTask(t.Context(), target.Request.RequestID)
	if err != nil || retained.Status != "bound" || retained.Node == nil {
		t.Fatalf("failed disconnect unbind lost unsent proof: status=%s node=%+v err=%v", retained.Status, retained.Node, err)
	}
	uncertain, err := h.inbox.store.spawnTask(t.Context(), sent.Request.RequestID)
	if err != nil || uncertain.Status != "dispatching" || uncertain.Node == nil || uncertain.Node.RequestID != sent.Node.RequestID {
		t.Fatalf("disconnect treated a committed send as unsent: status=%s node=%+v err=%v", uncertain.Status, uncertain.Node, err)
	}
	if _, err := h.inbox.store.db.Exec(`DROP TRIGGER fail_disconnect_unbind`); err != nil {
		t.Fatal(err)
	}
	kickSpawnCoordinator(h)
	eventually(t, 3*time.Second, "failed disconnect unbind to recover", func() bool {
		saved, err := h.inbox.store.spawnTask(t.Context(), target.Request.RequestID)
		return err == nil && saved.Status == "accepted" && saved.Node == nil
	})
}

func TestDispatcherRunsRealNodeEnginesIndependently_t4581(t *testing.T) {
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token", "machine-b": "node-token"}, InboxStorePath: filepath.Join(t.TempDir(), "inbox.sqlite3")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	firstNode := t14NamedNode(t, h, server, "machine-a", true, true)
	secondNode := t14NamedNode(t, h, server, "machine-b", true, true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	release := make(chan struct{})
	releaseEngines := sync.OnceFunc(func() { close(release) })
	defer releaseEngines()
	var runningA, runningB, peakA, startedA atomic.Int32
	framesA := make(chan string, 4)
	serve := func(conn *websocket.Conn, running *atomic.Int32, isA bool) {
		client := &HubClient{
			spawnConfigPath: t14SpawnConfig(t, t.TempDir()),
			spawnJournalDir: t.TempDir(),
			fleetEvents:     (&fleetEventRecorder{}).write,
			spawnEngine: func(context.Context, string, string, string, string, []string) (*hubSpawnOutput, fleet.Execution) {
				count := running.Add(1)
				if isA {
					startedA.Add(1)
					for old := peakA.Load(); count > old && !peakA.CompareAndSwap(old, count); old = peakA.Load() {
					}
				}
				<-release
				running.Add(-1)
				return &hubSpawnOutput{}, fleet.Execution{RC: 1, Stage: "launch"}
			},
		}
		peer := &hubClientConnection{connection: conn}
		go func() {
			for {
				_, payload, err := conn.Read(ctx)
				if err != nil {
					return
				}
				message, valid := parseHubOutbound(payload)
				if !valid || message.Type != "job.spawn" {
					continue
				}
				if isA {
					framesA <- message.RequestID
				}
				go client.handleHubSpawn(ctx, peer, message)
			}
		}()
	}
	serve(firstNode, &runningA, true)
	serve(secondNode, &runningB, false)
	boundDispatchFixture(t, h, 1200, "machine-a")
	boundDispatchFixture(t, h, 1201, "machine-a")
	boundDispatchFixture(t, h, 1202, "machine-b")
	h.startSpawnCoordinator(t.Context())
	eventually(t, 3*time.Second, "both nodes to run their engines", func() bool { return runningA.Load() == 1 && runningB.Load() == 1 })
	first := <-framesA
	// Window: the busy node's second request must stay at the hub.
	select {
	case second := <-framesA:
		t.Fatalf("node received %s while engine for %s was running", second, first)
	case <-time.After(250 * time.Millisecond):
	}
	releaseEngines()
	eventually(t, 3*time.Second, "the second node-a engine to start", func() bool { return startedA.Load() == 2 })
	if got := peakA.Load(); got != 1 {
		t.Fatalf("node-a ran %d engines at once", got)
	}
}

func TestShutdownCancelsDispatcherWaitingForNodeWriter_t4581(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite3")
	h, err := NewHubServer(HubServerConfig{Tokens: map[string]string{"operator": t14OperatorToken, "machine-a": "node-token"}, InboxStorePath: path})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close() })
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	t14Node(t, h, server, true)
	h.mu.Lock()
	agent := h.nodes["machine-a"].agent
	h.mu.Unlock()
	agent.writeMu.Lock()
	task := boundDispatchFixture(t, h, 1300, "machine-a")
	h.startSpawnCoordinator(t.Context())
	eventually(t, 3*time.Second, "durable send transition before waiting for the writer", func() bool {
		saved, err := h.inbox.store.spawnTask(t.Context(), task.Request.RequestID)
		return err == nil && saved.Status == "dispatching"
	})
	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()
	var closeErr error
	var closedWhileHeld bool
	select {
	case closeErr = <-closed:
		closedWhileHeld = true
	case <-time.After(time.Second):
	}
	agent.writeMu.Unlock()
	if !closedWhileHeld {
		<-closed
		t.Fatal("hub shutdown waited for an unrelated node writer lock")
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	reopened := openSpawnStore(t, path)
	saved, err := reopened.spawnTask(t.Context(), task.Request.RequestID)
	if err != nil || saved.Node == nil || saved.Node.RequestID != task.Node.RequestID || (saved.Status != "dispatching" && saved.Status != "reconciling") {
		t.Fatalf("shutdown lost uncertain send ownership: status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
}
