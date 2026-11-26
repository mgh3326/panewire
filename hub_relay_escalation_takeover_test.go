package panewire

import (
	"encoding/json"
	"os"
	"testing"
)

func TestUnconfirmedNoticeMootUsesTakeoverRouteJob_t6816(t *testing.T) {
	const successor = "hk6816-successor"
	for _, tc := range []struct {
		name      string
		completed bool
		want      string
	}{
		{name: "live successor keeps notice"},
		{name: "ended successor keeps moot result", completed: true, want: "job_ended"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := t3431Start(t, t3431Jobs{records: map[string]map[string]any{
				successor: {"state": "running", "task_id": float64(3431), "label": "successor-label"},
			}})
			routes := loadReportRelayRoutes(rig.hub.reportRelayPath)
			route := routes[t3431Worker]
			route.Job = successor
			route.TakenOverFrom = t3431Job
			routes[t3431Worker] = route
			contents, err := json.Marshal(map[string]any{"lanes": routes})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(rig.hub.reportRelayPath, contents, 0o600); err != nil {
				t.Fatal(err)
			}

			rig.job(t3431Job, true, false)
			rig.hub.mu.Lock()
			rig.hub.jobs[successor] = &hubJobRecord{
				HubActiveJob: HubActiveJob{JobID: successor, AgentLabel: "successor-label", Epoch: 1},
				Node:         "host-b", LastSeen: rig.clock.Now(), Completed: tc.completed,
			}
			rig.hub.mu.Unlock()
			rig.task(t3431Task, "in_progress", "started")

			if got := rig.hub.unconfirmedNoticeMoot(t3431Worker); got != tc.want {
				t.Fatalf("unconfirmed notice moot reason = %q, want %q", got, tc.want)
			}
		})
	}
}
