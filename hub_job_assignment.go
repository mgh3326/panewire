package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/fleet"
)

// hubOwnerAssignment is an assignment actually issued by this hub, not an
// owner inferred from an unverified heartbeat after restart.
type hubOwnerAssignment struct {
	Job   string `json:"job"`
	Node  string `json:"node"`
	Epoch uint64 `json:"epoch"`
	Ended bool   `json:"ended,omitempty"`
}

// seedLandedOwner follows a durable landing without making the spawn answer
// depend on hk availability. Startup and heartbeat retry a missed seed.
func (h *HubServer) seedLandedOwner(job, node string) {
	go func() {
		ctx, cancel := context.WithTimeout(h.closeCtx, nodeLoopHKTimeout)
		defer cancel()
		h.seedOwnerAssignment(ctx, hubOwnerAssignment{Job: job, Node: node, Epoch: 1})
	}()
}

// seedOwnerAssignment only adopts a live legacy record from a binding the hub
// persisted itself. A node heartbeat cannot establish that authority.
func (h *HubServer) seedOwnerAssignment(ctx context.Context, assignment hubOwnerAssignment) {
	if assignment.Ended {
		return
	}
	unlock, err := h.poolActionLocks.lock(ctx, assignment.Job)
	if err != nil {
		return
	}
	defer unlock()
	current, found, err := h.ownerAssignment(assignment.Job)
	if err != nil || !found || current != assignment {
		return
	}
	record, exists, err := h.readJobRecord(ctx, assignment.Job)
	if err != nil || !exists {
		return
	}
	state, _ := record["state"].(string)
	owner, supported, err := fleet.JobOwnerFromRecord(record)
	if err != nil || !supported || hkTerminalState(state) {
		return
	}
	if owner != (fleet.JobOwner{}) {
		if owner.Node != assignment.Node || owner.Epoch != assignment.Epoch {
			h.logger.Warn("handoffkeep owner differs from hub assignment", "job", assignment.Job)
		}
		return
	}
	to := fleet.JobOwner{Node: assignment.Node, Epoch: assignment.Epoch}
	supportedWrite, err := h.writeJobOwner(ctx, assignment.Job, fleet.JobOwner{}, to)
	if err != nil {
		h.logger.Warn("handoffkeep owner adoption failed", "job", assignment.Job, "error", err)
	} else {
		h.noteJobFenceSupport(supportedWrite)
	}
}

func (h *HubServer) sweepOwnerAssignments(ctx context.Context) {
	if h.ownerTransferDir == "" {
		return
	}
	entries, err := os.ReadDir(filepath.Join(h.ownerTransferDir, "_assignments"))
	if err != nil {
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		job := strings.TrimSuffix(entry.Name(), ".json")
		assignment, found, err := h.ownerAssignment(job)
		if err == nil && found {
			attempt, cancel := context.WithTimeout(ctx, nodeLoopHKTimeout)
			h.seedOwnerAssignment(attempt, assignment)
			cancel()
		}
	}
}

func (h *HubServer) ownerAssignmentPath(job string) (string, error) {
	if h.ownerTransferDir == "" || !hubJobIDPattern.MatchString(job) {
		return "", errors.New("job assignment store unavailable")
	}
	return filepath.Join(h.ownerTransferDir, "_assignments", job+".json"), nil
}

func (h *HubServer) ownerAssignment(job string) (hubOwnerAssignment, bool, error) {
	path, err := h.ownerAssignmentPath(job)
	if err != nil {
		return hubOwnerAssignment{}, false, err
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return hubOwnerAssignment{}, false, nil
	}
	if err != nil {
		return hubOwnerAssignment{}, false, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, 4097))
	var assignment hubOwnerAssignment
	if err != nil || len(body) > 4096 || json.Unmarshal(body, &assignment) != nil || assignment.Job != job || !machineIDPattern.MatchString(assignment.Node) || assignment.Epoch == 0 {
		return hubOwnerAssignment{}, false, errors.New("invalid job assignment record")
	}
	return assignment, true, nil
}

// saveOwnerAssignment is called under the per-job lock. A fresh spawn may
// change its epoch-1 binding only through the durable spawn placement path.
func (h *HubServer) saveOwnerAssignment(assignment hubOwnerAssignment, rebind bool) error {
	if !machineIDPattern.MatchString(assignment.Node) || assignment.Epoch == 0 {
		return errors.New("invalid job assignment")
	}
	path, err := h.ownerAssignmentPath(assignment.Job)
	if err != nil {
		return err
	}
	old, found, err := h.ownerAssignment(assignment.Job)
	if err != nil {
		return err
	}
	if found && (old.Epoch > assignment.Epoch || old.Epoch == assignment.Epoch && old.Node != assignment.Node && !(rebind && assignment.Epoch == 1)) {
		return errors.New("job assignment changed")
	}
	if found && old.Ended && !assignment.Ended && assignment.Epoch <= old.Epoch {
		return errors.New("job assignment ended")
	}
	if found && old == assignment {
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	durable.SyncDirectoryBestEffort(filepath.Dir(h.ownerTransferDir))
	durable.SyncDirectoryBestEffort(h.ownerTransferDir)
	body, err := json.Marshal(assignment)
	if err != nil {
		return err
	}
	return durable.ReplaceFileAtomically(path, ".assignment-*", body, 0600)
}

// endOwnerAssignment is called under the per-job lock before accepting a
// terminal event. A restart must not seed a job the hub already saw end.
func (h *HubServer) endOwnerAssignment(job, node string, epoch uint64) error {
	if h.ownerTransferDir == "" {
		return nil
	}
	assignment, found, err := h.ownerAssignment(job)
	if err != nil || !found || assignment.Node != node || assignment.Epoch != epoch || assignment.Ended {
		return err
	}
	assignment.Ended = true
	return h.saveOwnerAssignment(assignment, false)
}
