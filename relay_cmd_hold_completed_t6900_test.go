package panewire

// hk 6900: a job that completed while its cmd pane lives keeps its held row. The
// row is typed when the pane turns idle, as for a live job; only a close, a lost
// pane or a reap drops it job_ended (relayJobPaneEndKind).

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCmdHoldKeptWhenJobCompletedPaneLives_t6900(t *testing.T) {
	rig := t3276Start(t)
	root := t.TempDir()
	rig.node.jobsInboxRoot = root
	pane := t3384Install(t, "cmd", "working")
	held := t3384Hold(t, rig, relayDeliverDirective)
	events := filepath.Join(root, "jobs", t3276Worker, "events")
	if err := os.MkdirAll(events, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, kind := range map[string]string{"00001-job.claim.json": "job.claim", "00002-job.completed.json": "job.completed"} {
		body, _ := json.Marshal(map[string]any{"kind": kind, "job_id": t3276Worker, "payload": map[string]any{"agent_label": t3276Worker}})
		if err := os.WriteFile(filepath.Join(events, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pane.setStatus(t, "idle")

	rig.node.relayBusyManager().release(t.Context(), "w1:pW", false)

	kinds, payloads := rig.drainEvents()
	if t3276Count(kinds, "relay.dropped") != 0 || t3276Count(kinds, "relay.delivered") != 1 {
		t.Fatalf("events=%v dropped=%+v, want the completed job's held row delivered when the pane is idle", kinds, payloads["relay.dropped"])
	}
	if got := pane.calls(t, "pane send-text"); got != 1 {
		t.Fatalf("pane send-text called %d times after idle, want 1", got)
	}
	if row := rig.row(held[0].EventID); row.DeliveredAt == "" {
		t.Fatalf("the delivered row was not marked delivered: %+v", row)
	}
}
