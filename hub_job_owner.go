package panewire

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/mgh3326/panewire/internal/fleet"
)

// transferJobOwner fences the predecessor in hk before the hub exposes the
// successor. An unreadable job record fails closed; only a server that
// explicitly omits the owner fields gets the legacy behavior.
func (h *HubServer) transferJobOwner(ctx context.Context, id, from, to string, epoch uint64) error {
	old := fleet.JobOwner{Node: from, Epoch: epoch}
	next := fleet.JobOwner{Node: to, Epoch: epoch + 1}
	current, supported, exists, err := h.readJobOwner(ctx, id)
	if err != nil {
		return fmt.Errorf("job %s cannot be reassigned: %w", id, err)
	}
	if !exists {
		return fmt.Errorf("job %s cannot be reassigned: job not_found", id)
	}
	if !supported {
		h.noteJobFenceSupport(false)
		return nil
	}
	intent := hubOwnerTransfer{Job: id, From: from, To: to, Epoch: epoch}
	known, err := h.hasOwnerTransfer(intent)
	if err != nil {
		return fmt.Errorf("job %s cannot be reassigned: %w", id, err)
	}
	if known && current == next {
		h.noteJobFenceSupport(true)
		return nil
	}
	if current == old || (current == (fleet.JobOwner{}) && epoch == 1) {
		if !known {
			if err := h.saveOwnerTransfer(intent); err != nil {
				return fmt.Errorf("job %s cannot be reassigned: transfer journal unavailable", id)
			}
		}
	}
	switch {
	case h.spawnNewApp != nil || h.handoffkeep == nil:
		var app *fleet.App
		app, err = h.newSpawnApp(io.Discard, io.Discard)
		if err == nil {
			supported, err = app.TransferJobOwner(ctx, id, old, next)
		}
	case h.handoffkeep != nil:
		var store fleet.JobStore
		hk := h.handoffkeep.env
		store, err = fleet.NewJobStore(hk.URL, hk.Token, hk.CFID, hk.CFSecret)
		if err == nil {
			supported, err = fleet.TransferJobOwner(ctx, store, id, old, next)
		}
	}
	if err != nil {
		var backend *fleet.BackendError
		if fleet.IsJobOwnerMismatch(err) || (errors.As(err, &backend) && backend.Code == "job_state_conflict") {
			_ = h.removeOwnerTransfer(intent)
		}
		return fmt.Errorf("job %s cannot be reassigned: %w", id, err)
	}
	h.noteJobFenceSupport(supported)
	return nil
}

func (h *HubServer) noteJobFenceSupport(supported bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if supported {
		h.jobFenceLegacy = false
	} else if !h.jobFenceLegacy {
		h.jobFenceLegacy = true
		h.logger.Warn("handoffkeep job owner fencing unavailable; using legacy reassignment")
	}
}

// poolActionOwnerMoved uses hk's durable owner even when a restarted hub has
// no in-memory job or has heard only the predecessor's stale heartbeat.
func poolActionOwnerMoved(action poolAction, record map[string]any) (bool, error) {
	owner, supported, err := fleet.JobOwnerFromRecord(record)
	if err != nil {
		return false, err
	}
	return supported && owner.Node != "" && (owner.Node != action.Node || owner.Epoch != action.Epoch), nil
}

// firstSeenJobOwned admits a restarted hub's first observation only when hk
// confirms the owner. Older servers and absent job records retain epoch-1
// registration; a supporting server's stored grant is authoritative.
func (h *HubServer) readJobOwner(ctx context.Context, id string) (fleet.JobOwner, bool, bool, error) {
	var record map[string]any
	var exists bool
	var err error
	switch {
	case h.spawnNewApp != nil || h.handoffkeep == nil:
		var app *fleet.App
		app, err = h.newSpawnApp(io.Discard, io.Discard)
		if err == nil {
			record, exists, err = app.ExistingJob(ctx, id)
		}
	case h.handoffkeep != nil:
		var store fleet.JobStore
		hk := h.handoffkeep.env
		store, err = fleet.NewJobStore(hk.URL, hk.Token, hk.CFID, hk.CFSecret)
		if err == nil {
			record, err = store.Get(ctx, id)
			if errors.Is(err, fleet.ErrJobNotFound) {
				err = nil
			} else {
				exists = err == nil
			}
		}
	}
	if err != nil {
		return fleet.JobOwner{}, false, false, err
	}
	if !exists {
		return fleet.JobOwner{}, false, false, nil
	}
	owner, supported, err := fleet.JobOwnerFromRecord(record)
	return owner, supported, true, err
}

func (h *HubServer) firstSeenJobOwned(ctx context.Context, id, node string, epoch uint64) bool {
	owner, supported, exists, err := h.readJobOwner(ctx, id)
	if err != nil {
		// Without an explicit hk client the historical epoch-1 fallback is
		// retained; reassignment itself still requires the owner record.
		return h.spawnNewApp == nil && h.handoffkeep == nil && epoch == 1
	}
	if !exists || !supported || owner.Node == "" {
		return epoch == 1
	}
	return owner.Node == node && owner.Epoch == epoch
}
