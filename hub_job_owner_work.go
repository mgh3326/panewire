package panewire

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/mgh3326/panewire/internal/fleet"
)

type hubOwnerGrant struct {
	job, node string
	epoch     uint64
}

type hubOwnerWork struct {
	mu         sync.Mutex
	running    sync.WaitGroup
	closed     bool
	confirmed  map[string]hubOwnerGrant
	seeding    map[string]bool
	recovering map[string]bool
}

// scheduleOwnerSeed keeps HK I/O off the node's heartbeat reader and makes
// overlapping landings and heartbeats share one attempt per job.
func (h *HubServer) scheduleOwnerSeed(assignment hubOwnerAssignment) {
	if assignment.Ended {
		return
	}
	grant := hubOwnerGrant{job: assignment.Job, node: assignment.Node, epoch: assignment.Epoch}
	work := &h.ownerWork
	work.mu.Lock()
	if work.closed || work.confirmed[assignment.Job] == grant || work.seeding[assignment.Job] {
		work.mu.Unlock()
		return
	}
	if work.seeding == nil {
		work.seeding = make(map[string]bool)
	}
	work.seeding[assignment.Job] = true
	work.running.Add(1)
	work.mu.Unlock()
	go func() {
		defer work.running.Done()
		ctx, cancel := context.WithTimeout(h.closeCtx, nodeLoopHKTimeout)
		confirmed := h.seedOwnerAssignment(ctx, assignment)
		cancel()
		work.mu.Lock()
		delete(work.seeding, assignment.Job)
		if confirmed {
			if work.confirmed == nil {
				work.confirmed = make(map[string]hubOwnerGrant)
			}
			work.confirmed[assignment.Job] = grant
		}
		work.mu.Unlock()
	}()
}

func (h *HubServer) scheduleOwnerRecovery(machine string, agent *hubAgent) {
	work := &h.ownerWork
	work.mu.Lock()
	if work.closed || work.recovering[machine] {
		work.mu.Unlock()
		return
	}
	if work.recovering == nil {
		work.recovering = make(map[string]bool)
	}
	work.recovering[machine] = true
	work.running.Add(1)
	work.mu.Unlock()
	go func() {
		defer work.running.Done()
		defer func() {
			work.mu.Lock()
			delete(work.recovering, machine)
			work.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(h.closeCtx, nodeLoopHKTimeout)
		defer cancel()
		recovered := h.recoverOwnerTransfers(ctx, machine)
		if h.restoreOwnerAssignments(ctx, machine) || recovered {
			h.queueHeartbeatAssignments(machine, agent)
		}
	}()
}

func (h *HubServer) closeOwnerWork() {
	work := &h.ownerWork
	work.mu.Lock()
	work.closed = true
	work.mu.Unlock()
	work.running.Wait()
}

// restoreOwnerAssignments rebuilds a successor from its durable assignment if
// the hub stopped after retiring the transfer journal but before delivery.
func (h *HubServer) restoreOwnerAssignments(ctx context.Context, machine string) bool {
	if h.ownerTransferDir == "" || !machineIDPattern.MatchString(machine) {
		return false
	}
	entries, err := os.ReadDir(filepath.Join(h.ownerTransferDir, "_assignments"))
	if err != nil {
		return false
	}
	restored := false
	for _, entry := range entries {
		if ctx.Err() != nil {
			break
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		job := strings.TrimSuffix(entry.Name(), ".json")
		assignment, found, err := h.ownerAssignment(job)
		if err != nil || !found || assignment.Ended || assignment.From == "" || assignment.Node != machine {
			continue
		}
		if h.restoreOwnerAssignment(ctx, assignment) {
			restored = true
		}
	}
	return restored
}

func (h *HubServer) restoreOwnerAssignment(ctx context.Context, assignment hubOwnerAssignment) bool {
	unlock, err := h.poolActionLocks.lock(ctx, assignment.Job)
	if err != nil {
		return false
	}
	defer unlock()
	current, found, err := h.ownerAssignment(assignment.Job)
	if err != nil || !found || !sameOwnerAssignment(current, assignment) {
		return false
	}
	pending, err := h.hasPendingOwnerTransfer(assignment.Job)
	if err != nil || pending {
		return false
	}
	h.mu.Lock()
	if known := h.jobs[assignment.Job]; known != nil {
		if known.Node != assignment.Node || known.Epoch != assignment.Epoch {
			h.mu.Unlock()
			return false
		}
		h.restoreAssignmentFencesLocked(known, assignment)
		predecessors := make([]string, 0, len(known.FencedNodes))
		for node := range known.FencedNodes {
			predecessors = append(predecessors, node)
		}
		h.mu.Unlock()
		for _, node := range predecessors {
			h.tryPendingRevocations(node)
		}
		return false
	}
	h.mu.Unlock()
	record, exists, err := h.readJobRecord(ctx, assignment.Job)
	if err != nil || !exists {
		return false
	}
	state, _ := record["state"].(string)
	if hkTerminalState(state) {
		assignment.Ended = true
		_ = h.saveOwnerAssignment(assignment, false)
		return false
	}
	owner, supported, err := fleet.JobOwnerFromRecord(record)
	if err != nil || (supported && (owner.Node != assignment.Node || owner.Epoch != assignment.Epoch)) {
		return false
	}
	h.mu.Lock()
	if h.jobs[assignment.Job] != nil {
		h.mu.Unlock()
		return false
	}
	job := &hubJobRecord{HubActiveJob: HubActiveJob{JobID: assignment.Job, Epoch: assignment.Epoch}, Node: assignment.Node, LastSeen: h.now()}
	h.restoreAssignmentFencesLocked(job, assignment)
	h.jobs[assignment.Job] = job
	predecessors := make([]string, 0, len(job.FencedNodes))
	for node := range job.FencedNodes {
		predecessors = append(predecessors, node)
	}
	h.mu.Unlock()
	for _, node := range predecessors {
		h.tryPendingRevocations(node)
	}
	return true
}

// restoreAssignmentFencesLocked reinstates every prior node at the new epoch.
// The caller holds h.mu, as it does for the job and pending-revocation maps.
func (h *HubServer) restoreAssignmentFencesLocked(job *hubJobRecord, assignment hubOwnerAssignment) {
	if job.FencedNodes == nil {
		job.FencedNodes = make(map[string]uint64)
	}
	delete(job.FencedNodes, assignment.Node)
	if pending := h.pendingRevocations[assignment.Node]; pending != nil {
		delete(pending, assignment.Job)
		if len(pending) == 0 {
			delete(h.pendingRevocations, assignment.Node)
		}
	}
	for _, node := range append(append([]string(nil), assignment.Fenced...), assignment.From) {
		if node == "" || node == assignment.Node {
			continue
		}
		if job.FencedNodes[node] >= assignment.Epoch {
			continue
		}
		job.FencedNodes[node] = assignment.Epoch
		h.enqueueRevocationLocked(node, hubJobRevokedEvent{Type: "job.revoked", JobID: assignment.Job, Epoch: assignment.Epoch})
	}
}

// The transfer journal is written before HK can advance, so an unresolved
// intent prevents an old durable assignment from authorizing an outage fallback.
// The caller holds the per-job action lock while checking and admitting it.
func (h *HubServer) hasPendingOwnerTransfer(job string) (bool, error) {
	entries, err := os.ReadDir(h.ownerTransferDir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == "_assignments" {
			continue
		}
		dir := filepath.Join(h.ownerTransferDir, entry.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			return false, err
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasPrefix(file.Name(), job+"--") || !strings.HasSuffix(file.Name(), ".json") {
				continue
			}
			intent, err := readHubOwnerTransfer(filepath.Join(dir, file.Name()))
			if err != nil {
				return false, err
			}
			if intent.Job == job {
				return true, nil
			}
		}
	}
	return false, nil
}
