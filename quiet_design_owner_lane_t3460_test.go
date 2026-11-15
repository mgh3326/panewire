package panewire

// hk 3460: a worker's routine lane.event is quiet only in the design lane that
// owns the worker's job. The same event received by any other design lane is
// not attributable to that lane's worker, so it is delivered as before.

import (
	"path/filepath"
	"testing"
	"time"
)

const (
	t3460OtherDesign = "other-design"
	t3460Job         = "hk3460-other-worker"
	t3460Pane        = "w1:other-worker"
)

// t3460Hub is the t3434 fixture plus one live worker on host-a whose job is
// owned by t3460OtherDesign rather than t3426Design. observeActiveJobs replaces
// the node's whole active set, so the t3434 worker is listed again.
func t3460Hub(t *testing.T, path string) (*HubServer, *hubAgent, *fakeHandoffkeep) {
	t.Helper()
	h, agent, fake := t3434Hub(t, path)
	h.observeActiveJobs("host-a", []HubActiveJob{
		{JobID: t3434WorkerJob, AgentLabel: t3434WorkerJob, Epoch: 1, OwnerLane: t3426Design, Pane: t3434WorkerPane, Role: "implementer"},
		{JobID: t3460Job, AgentLabel: t3460Job, Epoch: 1, OwnerLane: t3460OtherDesign, Pane: t3460Pane, Role: "implementer"},
	}, time.Now())
	return h, agent, fake
}

// (a) A routine worker event owned by another design lane is not quiet in
// t3426Design: the worker is not that lane's, so the lane is woken as for any
// unattributed event.
func TestQuietDesignWorkerOfAnotherDesignLaneWakesReceivingLane(t *testing.T) {
	h, agent, _ := t3460Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	e := t3434Event(t3426Design, t3460Pane, "", "hk 3460: PR #230 open")
	t3434Woken(t, h, agent, agent, e, t3434DesignPane, "info")
}

// (b) The same event received by its owner lane stays quiet, as before.
func TestQuietDesignWorkerOfAnotherDesignLaneStaysQuietInItsOwnLane(t *testing.T) {
	h, agent, fake := t3460Hub(t, filepath.Join(t.TempDir(), "inbox.sqlite3"))
	t3434Quiet(t, h, agent, fake, t3434Event(t3460OtherDesign, t3460Pane, "", "hk 3460: PR #230 open"))
}
