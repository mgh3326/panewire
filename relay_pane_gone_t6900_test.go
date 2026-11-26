package panewire

// hk 6900: the hub records a completed job's pane as gone on a reap that
// closed it, on a job close and on a final lost pane (relay_route_pane.go).

import (
	"net/http"
	"testing"
	"time"
)

func TestJobsReapRecordsPaneGone_t6900(t *testing.T) {
	cases := []struct {
		name    string
		outcome string
		gone    bool
	}{
		{name: "a reap that closed the pane", outcome: "closed", gone: true},
		{name: "a reap that found the pane gone", outcome: "pane-gone", gone: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			hub, server := ownJobsHub(t)
			connection := t14NamedNodeToken(t, hub, server, "node-b", pw2NodeBToken, false, true)
			hub.jobs["job-b"] = &hubJobRecord{HubActiveJob: HubActiveJob{JobID: "job-b", OwnerLane: "lane-a", Pane: "w2:p1"}, Node: "node-b", Completed: true}
			results := ownJobsCaller.doAsync(t, server, http.MethodPost, "/v1/jobs/reap", `{"lane":"lane-a","job":"job-b","force":false}`)
			nodeCommandReplyTo(t, connection, nodeCommandRead(t, connection), map[string]any{"result": map[string]any{"outcome": test.outcome, "pane": "w2:p1"}})
			ownJobsResult(t, results)
			hub.mu.Lock()
			gone := hub.jobs["job-b"].PaneGone
			hub.mu.Unlock()
			if gone != test.gone {
				t.Fatalf("PaneGone=%t after reap outcome %q, want %t", gone, test.outcome, test.gone)
			}
		})
	}
}

func TestJobCloseAndLostPaneRecordPaneGone_t6900(t *testing.T) {
	cases := []struct {
		name  string
		apply func(h *HubServer, event hubJobEventPayload) bool
	}{
		{name: "a job close", apply: func(h *HubServer, event hubJobEventPayload) bool {
			return h.observeJobRevocation("node-a", event, time.Now())
		}},
		{name: "a final lost pane", apply: func(h *HubServer, event hubJobEventPayload) bool {
			return h.observeJobLost("node-a", event, time.Now())
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			h, _, _ := deliveryStageHub(t)
			h.mu.Lock()
			h.jobs = map[string]*hubJobRecord{"job-x": {HubActiveJob: HubActiveJob{JobID: "job-x", AgentLabel: "lane-a", Pane: "live-pane", Epoch: 1}, Node: "node-a", Completed: true}}
			h.mu.Unlock()
			if !test.apply(h, hubJobEventPayload{JobID: "job-x", Epoch: 1}) {
				t.Fatal("the node's event was not accepted for the completed job")
			}
			h.mu.Lock()
			gone := h.jobs["job-x"].PaneGone
			h.mu.Unlock()
			if !gone {
				t.Fatal("the recorded end did not mark the job's pane gone")
			}
		})
	}
}
