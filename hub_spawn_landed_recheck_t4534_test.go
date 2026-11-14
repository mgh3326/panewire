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

// landingUnconfirmedTail is the stdout tail of a landing the node refused rc 70
// because its cleanup could not prove the pane it launched: the cleanup
// warning the node writes to stderr, which the node copies into the tail.
const landingUnconfirmedTail = "stderr: panewire: warning: pane cleanup failed; inspect with panewire find test-job before retry: failed spawn pane identity or status is unconfirmed; inspect panewire find test-job on the executing host before cleanup"

// readNodeCommand returns the node.command the hub sent next, or false when
// none arrived within the window: the hub asked the node nothing.
func readNodeCommand(t *testing.T, node *websocket.Conn) (map[string]any, bool) {
	t.Helper()
	// The window bounds a wait for a frame that may never come; it is not a
	// pacing sleep.
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var command map[string]any
	if err := wsjson.Read(ctx, node, &command); err != nil {
		return nil, false
	}
	return command, true
}

// A landing refused rc 70 because the node could not confirm its pane is the
// one it launched. The hub asks the node whether a pane with the job's name and
// cwd exists: if it does, the request settles as landed with no second launch;
// otherwise the refusal stands. Before the re-check the request failed at once,
// so no pane.read reached the node and the first case failed.
func TestSpawnLandingRefusalRechecksPaneBeforeFailing(t *testing.T) {
	cases := []struct {
		name   string
		answer map[string]any // the node's pane.read answer
		status string         // the request's settled status
		rc     int            // the rc the submitter sees
	}{
		{"pane with the job name and cwd is still running", map[string]any{"outcome": "read", "agent_status": "working", "focused": false, "composer": "empty"}, "succeeded", 0},
		{"no pane with the job name and cwd", map[string]any{"outcome": "pane-gone"}, "failed", 70},
		{"the pane now belongs to another cwd", map[string]any{"outcome": "pane-changed"}, "failed", 70},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := asyncSpawnHub(t)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, "test-job", "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			transport := &directSpawnTransport{h: h, task: task}
			type answer struct {
				reply fleet.HubSpawnReply
				err   error
			}
			done := make(chan answer, 1)
			go func() {
				reply, err := dispatchPlacedSpawn(boundedContext(t), transport, refusalNodeRequest(request, 910, "machine-a"))
				done <- answer{reply, err}
			}()
			sent := t14ReadNodeSpawn(t, node)
			refusal := map[string]any{
				"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 70, "job_id": "test-job", "pane": "w1:p9",
				"stdout_tail": landingUnconfirmedTail, "error": "spawn_failed", "stage": "landing", "retryable": false,
				"receipt": map[string]any{"job": "test-job", "host": "machine-a", "pane": "w1:p9", "profile": "sonnet", "model": "m", "role": "worker", "cwd": "/work/project.test-job", "label": "test-job", "kind": "claude"},
			}
			if err = wsjson.Write(t.Context(), node, refusal); err != nil {
				t.Fatal(err)
			}
			command, asked := readNodeCommand(t, node)
			if !asked || command["op"] != "pane.read" {
				t.Fatalf("hub did not re-check the pane: asked=%v command=%v", asked, command)
			}
			if err = wsjson.Write(t.Context(), node, map[string]any{"type": "node.command.result", "request_id": command["request_id"], "op": "pane.read", "result": tc.answer}); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if got.err != nil || got.reply.RC != tc.rc {
				t.Fatalf("submitter saw %+v err=%v, want rc %d", got.reply, got.err, tc.rc)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != tc.status || saved.Outcome.RC != tc.rc {
				t.Fatalf("saved status=%s rc=%d err=%v, want %s rc %d", saved.Status, saved.Outcome.RC, err, tc.status, tc.rc)
			}
		})
	}
}
