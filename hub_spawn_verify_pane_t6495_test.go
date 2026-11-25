package panewire

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket/wsjson"
	"github.com/mgh3326/panewire/internal/fleet"
	"github.com/mgh3326/panewire/internal/spawnargs"
)

// TestVerifySpawnPaneNamesLauncherJob_t6495 lands a spawn whose argv carries
// --purpose before the names and checks the hub verifies the pane against the
// launcher's parse: a flag's value is never the job, and a spawn without
// --job runs under its -l label. A lax pair scan reads "--job" (the purpose's
// value) as the flag and misses the label fallback, so the legitimate landing
// settles unknown instead of succeeded.
func TestVerifySpawnPaneNamesLauncherJob_t6495(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		job   string
		label string
	}{
		{
			name:  "purpose value is not the job",
			args:  []string{"-m", "sonnet", "--purpose", "--job", "-w", "workers", "-l", "review-a", "--job", "job-real", "--role", "implementer"},
			job:   "job-real",
			label: "review-a",
		},
		{
			name:  "label is the job without --job",
			args:  []string{"-m", "sonnet", "--purpose", "nightly", "-w", "workers", "-l", "review-b", "--role", "implementer"},
			job:   "review-b",
			label: "review-b",
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The argv is wire-plausible: the node admits it, so the hub must
			// verify the pane it names rather than refuse the frame.
			if refusal := spawnargs.Refusal(tc.args, true); refusal != "" {
				t.Fatalf("the wire refuses %q: %s", tc.args, refusal)
			}
			if identity, err := fleet.ParseSpawnIdentity(tc.args); err != nil || identity.Job != tc.job || identity.Label != tc.label {
				t.Fatalf("ParseSpawnIdentity=%+v err=%v, want job %q label %q", identity, err, tc.job, tc.label)
			}
			h := asyncSpawnHub(t)
			server := httptest.NewServer(h.Handler())
			defer server.Close()
			node := t14Node(t, h, server, true)
			request := autoSpawnRequest()
			task, _, err := h.inbox.store.claimSpawn(t.Context(), request, tc.job, "parent", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			transport := &directSpawnTransport{h: h, task: task}
			type answer struct {
				reply fleet.HubSpawnReply
				err   error
			}
			done := make(chan answer, 1)
			nodeRequest := fleet.HubSpawnRequest{RequestID: t14RequestID(649501 + i), Machine: "machine-a", CWDKey: "project", Brief: fleet.HubBrief{Doc: "briefs/test-job.md"}, Checkout: request.Repository.Checkout, Args: tc.args, WaitSeconds: 60}
			go func() {
				reply, err := dispatchPlacedSpawn(boundedContext(t), transport, nodeRequest)
				done <- answer{reply, err}
			}()
			sent := t14ReadNodeSpawn(t, node)
			if sent.RequestID != nodeRequest.RequestID {
				t.Fatalf("node received %s, want %s", sent.RequestID, nodeRequest.RequestID)
			}
			landed := map[string]any{
				"type": "job.spawn.result", "request_id": sent.RequestID, "rc": 0, "job_id": tc.job, "pane": "w1:p9", "stdout_tail": "",
				"receipt": map[string]any{"job": tc.job, "host": "machine-a", "pane": "w1:p9", "profile": "sonnet", "model": "m", "role": "implementer", "cwd": "/work/project.test-job", "label": tc.label, "kind": "claude"},
			}
			if err = wsjson.Write(t.Context(), node, landed); err != nil {
				t.Fatal(err)
			}
			command, asked := readNodeCommand(t, node)
			if !asked || command["op"] != "pane.read" {
				t.Fatalf("the hub did not verify the landed pane: asked=%v command=%v", asked, command)
			}
			raw, _ := json.Marshal(command["args"])
			if !strings.Contains(string(raw), `"name":"`+tc.label+`"`) {
				t.Fatalf("pane.read names %s, want the label %q", raw, tc.label)
			}
			if err = wsjson.Write(t.Context(), node, map[string]any{"type": "node.command.result", "request_id": command["request_id"], "op": "pane.read", "result": map[string]any{"outcome": "read", "agent_status": "working", "focused": false, "composer": "empty"}}); err != nil {
				t.Fatal(err)
			}
			got := <-done
			if got.err != nil || got.reply.RC != 0 || got.reply.JobID != tc.job {
				t.Fatalf("landing saw %+v err=%v, want rc 0 for job %q", got.reply, got.err, tc.job)
			}
			saved, err := h.inbox.store.spawnTask(t.Context(), request.RequestID)
			if err != nil || saved.Status != "succeeded" || saved.Outcome.RC != 0 {
				t.Fatalf("saved status=%s rc=%d err=%v, want succeeded rc 0", saved.Status, saved.Outcome.RC, err)
			}
		})
	}
}
