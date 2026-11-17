package panewire

import (
	"testing"
	"time"

	"github.com/mgh3326/panewire/internal/fleet"
)

// localLostJobApp supplies the hub identity without reading node configuration.
type localLostJobApp struct{ *fleet.App }

func (localLostJobApp) NodeID() (string, bool) { return "machine-a", true }

func TestHubLocalLostWriteReportsEscalatedAsTerminal(t *testing.T) {
	jobs := &quotaJobs{records: map[string]map[string]any{"parent": {"state": "escalated", "end_reason": "independent escalation"}}}
	app := localLostJobApp{&fleet.App{JobStore: jobs, Now: time.Now}}
	holder := hubSpawnTask{Job: "parent", Outcome: hubSpawnOutcome{Host: "machine-a"}}
	lost, why := (&HubServer{}).markHolderJobLostBecause(t.Context(), app, holder, "parent", "ladder step")
	if !lost || why != nodeJobMarkLostCompleted {
		t.Fatalf("local terminal result=%v/%q; want completed so escalation rechecks the record", lost, why)
	}
}
