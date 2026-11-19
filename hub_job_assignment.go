package panewire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/mgh3326/panewire/internal/durable"
	"github.com/mgh3326/panewire/internal/fleet"
)

// hubOwnerAssignment is an assignment actually issued by this hub, not an
// owner inferred from an unverified heartbeat after restart.
type hubOwnerAssignment struct {
	Job    string   `json:"job"`
	Node   string   `json:"node"`
	Epoch  uint64   `json:"epoch"`
	From   string   `json:"from,omitempty"`
	Fenced []string `json:"fenced,omitempty"`
	Ended  bool     `json:"ended,omitempty"`
	// Lost marks an end the node settled lost (a final job.lost, hk 5351), so
	// a restarted hub still knows the holder's role may be released.
	Lost bool `json:"lost,omitempty"`
}

const hubOwnerAssignmentMaxBytes = 64 * 1024

func sameOwnerAssignment(a, b hubOwnerAssignment) bool {
	return a.Job == b.Job && a.Node == b.Node && a.Epoch == b.Epoch && a.From == b.From && a.Ended == b.Ended && a.Lost == b.Lost && slices.Equal(a.Fenced, b.Fenced)
}

// seedLandedOwner follows a durable landing without making the spawn answer
// depend on hk availability. Startup and heartbeat retry a missed seed.
func (h *HubServer) seedLandedOwner(job, node string) {
	h.scheduleOwnerSeed(hubOwnerAssignment{Job: job, Node: node, Epoch: 1})
}

// seedOwnerAssignment only adopts a live legacy record from a binding the hub
// persisted itself. A node heartbeat cannot establish that authority.
func (h *HubServer) seedOwnerAssignment(ctx context.Context, assignment hubOwnerAssignment) bool {
	if assignment.Ended {
		return false
	}
	unlock, err := h.poolActionLocks.lock(ctx, assignment.Job)
	if err != nil {
		return false
	}
	defer unlock()
	current, found, err := h.ownerAssignment(assignment.Job)
	if err != nil || !found || current.Ended || current.Node != assignment.Node || current.Epoch != assignment.Epoch {
		return false
	}
	record, exists, err := h.readJobRecord(ctx, assignment.Job)
	if err != nil || !exists {
		return false
	}
	state, _ := record["state"].(string)
	owner, supported, err := fleet.JobOwnerFromRecord(record)
	if err != nil || !supported || hkTerminalState(state) {
		return false
	}
	if owner != (fleet.JobOwner{}) {
		if owner.Node != assignment.Node || owner.Epoch != assignment.Epoch {
			h.logger.Warn("handoffkeep owner differs from hub assignment", "job", assignment.Job)
			return false
		}
		return true
	}
	to := fleet.JobOwner{Node: assignment.Node, Epoch: assignment.Epoch}
	supportedWrite, err := h.writeJobOwner(ctx, assignment.Job, fleet.JobOwner{}, to)
	if err != nil {
		h.logger.Warn("handoffkeep owner adoption failed", "job", assignment.Job, "error", err)
	} else {
		h.noteJobFenceSupport(supportedWrite)
	}
	return err == nil && supportedWrite
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
			h.scheduleOwnerSeed(assignment)
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
	body, err := io.ReadAll(io.LimitReader(file, hubOwnerAssignmentMaxBytes+1))
	var assignment hubOwnerAssignment
	if err != nil || len(body) > hubOwnerAssignmentMaxBytes || json.Unmarshal(body, &assignment) != nil || assignment.Job != job || !validOwnerAssignment(assignment) {
		return hubOwnerAssignment{}, false, errors.New("invalid job assignment record")
	}
	return assignment, true, nil
}

func validOwnerAssignment(assignment hubOwnerAssignment) bool {
	if !machineIDPattern.MatchString(assignment.Node) || assignment.Epoch == 0 || (assignment.From != "" && (!machineIDPattern.MatchString(assignment.From) || assignment.From == assignment.Node)) {
		return false
	}
	for _, node := range assignment.Fenced {
		if !machineIDPattern.MatchString(node) || node == assignment.Node {
			return false
		}
	}
	return true
}

// successorOwnerAssignment carries every predecessor forward so journal
// cleanup cannot erase pending revocations after a later hub restart.
func (h *HubServer) successorOwnerAssignment(job, from, to string, epoch uint64) (hubOwnerAssignment, error) {
	old, found, err := h.ownerAssignment(job)
	if err != nil {
		return hubOwnerAssignment{}, err
	}
	nodes := make(map[string]struct{})
	if found && (old.Node == from && old.Epoch == epoch || old.Node == to && old.Epoch == epoch+1) {
		if old.From != "" {
			nodes[old.From] = struct{}{}
		}
		for _, node := range old.Fenced {
			nodes[node] = struct{}{}
		}
	}
	h.mu.Lock()
	current := h.jobs[job]
	if current != nil && (current.Node == from && current.Epoch == epoch || current.Node == to && current.Epoch == epoch+1) {
		for node := range current.FencedNodes {
			nodes[node] = struct{}{}
		}
	}
	h.mu.Unlock()
	nodes[from] = struct{}{}
	delete(nodes, to)
	fenced := make([]string, 0, len(nodes))
	for node := range nodes {
		fenced = append(fenced, node)
	}
	slices.Sort(fenced)
	return hubOwnerAssignment{Job: job, Node: to, Epoch: epoch + 1, From: from, Fenced: fenced}, nil
}

// saveOwnerAssignment is called under the per-job lock. A fresh spawn may
// change its epoch-1 binding only through the durable spawn placement path.
func (h *HubServer) saveOwnerAssignment(assignment hubOwnerAssignment, rebind bool) error {
	if !validOwnerAssignment(assignment) {
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
	if found && old.Node == assignment.Node && old.Epoch == assignment.Epoch && assignment.From == "" {
		assignment.From = old.From
	}
	if found && old.Node == assignment.Node && old.Epoch == assignment.Epoch && len(assignment.Fenced) == 0 {
		assignment.Fenced = slices.Clone(old.Fenced)
	}
	if found && sameOwnerAssignment(old, assignment) {
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
	if len(body) > hubOwnerAssignmentMaxBytes {
		return errors.New("job assignment record too large")
	}
	return durable.ReplaceFileAtomically(path, ".assignment-*", body, 0600)
}

// endOwnerAssignment is called under the per-job lock before accepting a
// terminal event. A restart must not seed a job the hub already saw end.
func (h *HubServer) endOwnerAssignment(job, node string, epoch uint64) error {
	return h.endOwnerAssignmentAs(job, node, epoch, false)
}

// endOwnerAssignmentAs is endOwnerAssignment for an end the node settled lost
// (a final job.lost, hk 5351). lost is persisted with the end, so a restarted
// hub can tell the holder's role is free without the in-memory job record.
func (h *HubServer) endOwnerAssignmentAs(job, node string, epoch uint64, lost bool) error {
	if h.ownerTransferDir == "" {
		return nil
	}
	assignment, found, err := h.ownerAssignment(job)
	if err != nil {
		return err
	}
	if !found {
		if !lost {
			return nil
		}
		// A final loss the hub accepted with no assignment on disk (a job it
		// learned from a heartbeat before the seed landed) still records its
		// end, so the lost end outlives a restart (hk 5351).
		return h.saveOwnerAssignment(hubOwnerAssignment{Job: job, Node: node, Epoch: epoch, Ended: true, Lost: true}, false)
	}
	if assignment.Node != node || assignment.Epoch != epoch {
		return nil
	}
	if assignment.Ended && (assignment.Lost || !lost) {
		return nil
	}
	assignment.Ended, assignment.Lost = true, assignment.Lost || lost
	return h.saveOwnerAssignment(assignment, false)
}
