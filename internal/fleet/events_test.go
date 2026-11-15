package fleet

import (
	"testing"
)

// A revival clears the terminal time with the rest of the terminal state, so
// At always dates the job's current end and never one it left behind: a
// reprompted implementer, or one respawned after a quota stop, is live again.
func TestJobRevivalClearsTheTerminalTime(t *testing.T) {
	for _, kind := range []string{"job.claim", "job.spawned", "job.reprompted"} {
		t.Run(kind, func(t *testing.T) {
			a := testApp(t)
			putEvent(t, a, "w", "job.claim", map[string]any{"owner_lane": "owner", "task_id": "42", "role": "implementer"})
			putEvent(t, a, "w", "job.spawned", map[string]any{"pane_id": "p-w", "tab_id": "t-p-w", "label": "agent-w"})
			putEvent(t, a, "w", "job.completed", map[string]any{})
			j, err := a.job("w")
			if err != nil {
				t.Fatal(err)
			}
			if j.At.IsZero() || !j.Terminal {
				t.Fatalf("completion did not end the job: at=%v terminal=%v", j.At, j.Terminal)
			}
			switch kind {
			case "job.claim":
				putEvent(t, a, "w", kind, map[string]any{"owner_lane": "owner", "task_id": "42", "role": "implementer"})
			case "job.spawned":
				putEvent(t, a, "w", kind, map[string]any{"pane_id": "p-w2", "tab_id": "t-p-w2", "label": "agent-w"})
			default:
				putEvent(t, a, "w", kind, map[string]any{"reason": "quota_stop_resumed"})
			}
			j, err = a.job("w")
			if err != nil {
				t.Fatal(err)
			}
			if !j.At.IsZero() || j.Terminal || j.Ended != "" || j.Outcome != "" {
				t.Fatalf("%s left the old end in place: at=%v terminal=%v ended=%q outcome=%q", kind, j.At, j.Terminal, j.Ended, j.Outcome)
			}
		})
	}
}
