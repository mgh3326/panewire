package panewire

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/mgh3326/panewire/internal/fleet"
)

// hk 6073: a spawn bound to a (host, kind) is re-checked right before it is
// sent. A verified login-screen refusal arms that pair's launch backoff at
// once, so a second spawn bound to the same pair behind it is unbound and
// re-placed instead of being refused the same way.

// recheckBound claims and binds one request to machine-a with a claude seat
// (profile sonnet) and answers the node request it was bound with.
func recheckBound(t *testing.T, h *HubServer, n int) (hubSpawnTask, hubSpawnRequest) {
	t.Helper()
	task := claimParkRequest(t, h, n, time.Now())
	node := hubSpawnRequest{RequestID: t14RequestID(607300 + n), Machine: "machine-a", CWDKey: "project", Brief: hubSpawnBrief{Inline: "task"}, Args: []string{"--job", task.Job, "-l", task.Job, "-m", "sonnet"}, WaitSeconds: 600, DeadlineMS: task.Deadline.UnixMilli()}
	bound, err := h.inbox.store.bindSpawn(t.Context(), task.Request.RequestID, task.Attempt, node, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return bound, node
}

func recheckLoginRefusal(requestID string) map[string]any {
	return firstRunRefusal(requestID, loginTail, fleet.ExitAuthUnavailable)
}

func recheckClaudeBackoff(h *HubServer) time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	if entry := h.launchBackoffs[launchKey{"machine-a", "claude"}]; entry != nil {
		return entry.until
	}
	return time.Time{}
}

// nodeSentNothing proves no job.spawn frame reaches the node. The window only
// shows that nothing more happens, so the sleep inside Read is the proof.
func nodeSentNothing(t *testing.T, node *websocket.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, payload, err := node.Read(ctx); err == nil {
		t.Fatalf("a frame reached the node: %s", payload)
	}
}

func TestAsyncSpawnBoundRecheckUnbindsAfterLoginRefusal_t6073(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	node := t14Node(t, h, server, true)
	first, firstNode := recheckBound(t, h, 1)
	second, secondNode := recheckBound(t, h, 2)

	done := make(chan error, 1)
	go func() {
		_, err := (&directSpawnTransport{h: h, task: first}).dispatch(boundedContext(t), firstNode)
		done <- err
	}()
	sent := t14ReadNodeSpawn(t, node)
	if err := wsjson.Write(t.Context(), node, recheckLoginRefusal(sent.RequestID)); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if recheckClaudeBackoff(h).IsZero() {
		t.Fatal("a verified login-screen refusal did not arm the launch backoff")
	}

	_, err := (&directSpawnTransport{h: h, task: second}).dispatch(boundedContext(t), secondNode)
	if err == nil {
		t.Fatal("the second spawn was sent to the kind that just refused at the login screen")
	}
	nodeSentNothing(t, node)
	saved, err := h.inbox.store.spawnTask(t.Context(), second.Request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("second spawn was not unbound for re-placement: status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
}

// The coordinator's pass over a bound row that waits for the node's slot
// unbinds it as soon as its pair is benched, without waiting for the slot.
func TestAsyncSpawnQueuedBindingRecheckedForReadiness_t6073(t *testing.T) {
	h := asyncSpawnHub(t)
	server := httptest.NewServer(h.Handler())
	defer server.Close()
	_ = t14Node(t, h, server, true)
	bound, _ := recheckBound(t, h, 3)

	if h.handleQueuedBinding(t.Context(), bound) {
		t.Fatal("a ready pair's bound row was handled before its turn")
	}
	h.observeLaunchResult("machine-a", "sonnet", hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: loginTail}, "spawn_failed:auth_unavailable", time.Now().UTC())
	if !h.handleQueuedBinding(t.Context(), bound) {
		t.Fatal("a benched pair's bound row was left to be sent")
	}
	saved, err := h.inbox.store.spawnTask(t.Context(), bound.Request.RequestID)
	if err != nil || saved.Status != "accepted" || saved.Node != nil {
		t.Fatalf("status=%s node=%+v err=%v", saved.Status, saved.Node, err)
	}
}

func TestLoginMissingNeedDropsBoundSpawnUnlessProfileNamed_t6073(t *testing.T) {
	h := asyncSpawnHub(t)
	auto, autoNode := recheckBound(t, h, 4)
	named := claimParkRequest(t, h, 5, time.Now())
	named.Request.Args = append([]string(nil), named.Request.Args...)
	for i, arg := range named.Request.Args {
		if arg == "-m" {
			named.Request.Args[i+1] = "sonnet"
		}
	}
	h.loginNeedMu.Lock()
	h.loginNeeds = map[string][]string{"machine-a": {"claude"}}
	h.loginNeedMu.Unlock()
	if got := h.boundSpawnReadinessDrop(auto, autoNode); got != fleet.ReadyLoginMissing {
		t.Fatalf("auto spawn: drop=%q want %q", got, fleet.ReadyLoginMissing)
	}
	if got := h.boundSpawnReadinessDrop(named, autoNode); got != "" {
		t.Fatalf("a named profile ignores the standing login need, got %q", got)
	}
}

func TestLaunchBackoffLoginScreenArmsAtOnce_t6073(t *testing.T) {
	hub, server := fsHub(t, "")
	fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
	now := time.Date(2026, 11, 21, 15, 21, 43, 0, time.UTC)
	until := func() map[string]time.Time { return readinessState(t, hub, "node-a").BackoffUntil }

	hub.observeLaunchResult("node-a", "sonnet", hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: loginTail}, "spawn_failed:auth_unavailable", now)
	if got := until(); !got["claude"].Equal(now.Add(30*time.Minute)) || len(got) != 1 {
		t.Fatalf("one verified login refusal: until=%v want claude at %v", got, now.Add(30*time.Minute))
	}
}

// Only the verified login screen skips the threshold. An ordinary failure, and
// an rc 88 for an account the host cannot confirm, still need two.
func TestLaunchBackoffOrdinaryFailureStillNeedsTwo_t6073(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 21, 43, 0, time.UTC)
	for _, test := range []struct {
		name   string
		result hubSpawnResult
		reason string
	}{
		{"ordinary spawn failure", hubSpawnResult{RC: 1, StdoutTail: "stderr: boom"}, "spawn_failed"},
		{"unconfirmed account", hubSpawnResult{RC: fleet.ExitAuthUnavailable, StdoutTail: "stderr: AUTH_UNAVAILABLE: account not confirmed"}, "spawn_failed:auth_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			hub, server := fsHub(t, "")
			fsConnectNode(t, hub, server, "node-a", fsNodeAToken)
			until := func() map[string]time.Time { return readinessState(t, hub, "node-a").BackoffUntil }
			hub.observeLaunchResult("node-a", "sonnet", test.result, test.reason, now)
			if got := until(); len(got) != 0 {
				t.Fatalf("one failure benched the pair: %v", got)
			}
			hub.observeLaunchResult("node-a", "sonnet", test.result, test.reason, now.Add(time.Second))
			if got := until(); len(got) != 1 {
				t.Fatalf("two failures did not bench the pair: %v", got)
			}
		})
	}
}
