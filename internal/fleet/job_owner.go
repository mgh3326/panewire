package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/mgh3326/panewire/internal/jobowner"
)

type jobFenceWarnings struct {
	mu     sync.Mutex
	legacy bool
}

var jobFenceInit sync.Mutex

// JobOwner is the handoffkeep fence: only this node and epoch may write.
type JobOwner struct {
	Node  string `json:"node"`
	Epoch uint64 `json:"epoch"`
}

// JobOwnerFromRecord distinguishes an older server, which omits both fields,
// from a supporting server's explicit empty owner. A partial answer fails closed.
func JobOwnerFromRecord(record map[string]any) (JobOwner, bool, error) {
	nodeValue, hasNode := record["owner_node"]
	epochValue, hasEpoch := record["owner_epoch"]
	if !hasNode && !hasEpoch {
		return JobOwner{}, false, nil
	}
	node, nodeOK := nodeValue.(string)
	var epoch uint64
	var err error
	switch value := epochValue.(type) {
	case float64:
		if value < 0 || value >= math.Exp2(53) || value != math.Trunc(value) {
			err = errors.New("invalid job owner epoch")
		} else {
			epoch = uint64(value)
		}
	case json.Number:
		epoch, err = strconv.ParseUint(string(value), 10, 64)
	case int:
		if value < 0 {
			err = errors.New("invalid job owner epoch")
		} else {
			epoch = uint64(value)
		}
	case int64:
		if value < 0 {
			err = errors.New("invalid job owner epoch")
		} else {
			epoch = uint64(value)
		}
	case uint64:
		epoch = value
	default:
		err = errors.New("invalid job owner epoch")
	}
	if !hasNode || !hasEpoch || !nodeOK || err != nil || (node == "") != (epoch == 0) || (node != "" && !nodeIDRE.MatchString(node)) {
		return JobOwner{}, true, errors.New("invalid job owner record")
	}
	return JobOwner{Node: node, Epoch: epoch}, true, nil
}

// IsJobOwnerMismatch reports hk's conditional-write refusal.
func IsJobOwnerMismatch(err error) bool {
	var backend *BackendError
	return errors.As(err, &backend) && backend.Status == http.StatusConflict && backend.Code == "job_owner_mismatch"
}

var errJobReassigned = errors.New("job reassigned by the hub")

// IsJobReassigned includes a new hk refusal and a previously recorded one.
func IsJobReassigned(err error) bool {
	return IsJobOwnerMismatch(err) || errors.Is(err, errJobReassigned)
}

type jobOwnerGrant struct {
	job   string
	epoch uint64
}

type jobOwnerGrantKey struct{}

func withJobEpoch(ctx context.Context, job Job) context.Context {
	if grant, ok := ctx.Value(jobOwnerGrantKey{}).(jobOwnerGrant); ok && grant.job == job.ID {
		return ctx
	}
	if job.OwnerEpoch == 0 {
		return ctx
	}
	return context.WithValue(ctx, jobOwnerGrantKey{}, jobOwnerGrant{job: job.ID, epoch: job.OwnerEpoch})
}

func (a *App) sameJobOwnerEpoch(job Job) bool {
	if job.OwnerEpoch == 0 {
		return true
	}
	epoch, err := jobowner.Epoch(a.Jobs, job.ID)
	return err == nil && epoch == job.OwnerEpoch
}

// CaptureJobOwner binds an operation to the grant it began under. A later
// A→B→A reassignment cannot promote an in-flight A operation to A's new epoch.
func CaptureJobOwner(ctx context.Context, jobs, id string) (context.Context, error) {
	if grant, ok := ctx.Value(jobOwnerGrantKey{}).(jobOwnerGrant); ok && grant.job == id {
		return ctx, nil
	}
	epoch, err := jobowner.Epoch(jobs, id)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, jobOwnerGrantKey{}, jobOwnerGrant{job: id, epoch: epoch}), nil
}

// PutOwnedJob applies the node's persisted grant to a write. An older hk gets
// the original update, while a supporting hk rejects a stale node atomically.
// The local marker uses the same path as a hub revocation, so all subsequent
// writers and the existing pane sweep observe the reassignment.
func PutOwnedJob(ctx context.Context, store JobStore, jobs, node, id string, fields map[string]any, capability func(bool)) error {
	if jobs != "" {
		var err error
		ctx, err = CaptureJobOwner(ctx, jobs, id)
		if err != nil {
			return err
		}

		events, err := (&App{Jobs: jobs}).events(id)
		if err == nil && viewLedger(events).Reassigned {
			return errJobReassigned
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	record, err := store.Get(ctx, id)
	if err != nil {
		return err
	}
	current, supported, err := JobOwnerFromRecord(record)
	if err != nil {
		return err
	}
	if capability != nil {
		capability(supported)
	}
	if !supported {
		return store.Put(ctx, id, fields)
	}
	if !nodeIDRE.MatchString(node) {
		return errors.New("job owner node unavailable")
	}
	grant, ok := ctx.Value(jobOwnerGrantKey{}).(jobOwnerGrant)
	if !ok || grant.job != id || grant.epoch == 0 {
		return errors.New("job owner epoch unavailable")
	}
	epoch := grant.epoch
	self := JobOwner{Node: node, Epoch: epoch}
	if current != (JobOwner{}) && current != self {
		refusal := &BackendError{Status: http.StatusConflict, Code: "job_owner_mismatch", OwnerNode: current.Node, OwnerEpoch: current.Epoch}
		return revokeJobOwnerMismatch(jobs, id, epoch, current, fmt.Errorf("job_owner_mismatch: observed owner %s epoch %d: %w", current.Node, current.Epoch, refusal))
	}
	update := make(map[string]any, len(fields)+2)
	for key, value := range fields {
		update[key] = value
	}
	// Empty is a condition, never a node-side claim. A seed that lands while
	// this write is in flight must make the old empty-owner PUT fail.
	update["expected_owner"] = current
	err = store.Put(ctx, id, update)
	if !IsJobOwnerMismatch(err) {
		return err
	}

	// A seed for this same grant is the one safe retry. Never retry a write
	// using another owner's condition, even when the node ID comes back later.
	var backend *BackendError
	var observed JobOwner
	if errors.As(err, &backend) {
		observed = JobOwner{Node: backend.OwnerNode, Epoch: backend.OwnerEpoch}
	}
	refreshed, readErr := store.Get(ctx, id)
	if readErr == nil {
		owner, available, ownerErr := JobOwnerFromRecord(refreshed)
		if ownerErr == nil && available {
			observed = owner
			if owner == self {
				update["expected_owner"] = owner
				err = store.Put(ctx, id, update)
				if !IsJobOwnerMismatch(err) {
					return err
				}
				observed = JobOwner{}
				if errors.As(err, &backend) {
					observed = JobOwner{Node: backend.OwnerNode, Epoch: backend.OwnerEpoch}
				}
			}
		}
	}
	return revokeJobOwnerMismatch(jobs, id, epoch, observed, err)
}

func revokeJobOwnerMismatch(jobs, id string, epoch uint64, current JobOwner, err error) error {
	if current.Node == "" || current.Epoch == 0 {
		return fmt.Errorf("job owner unavailable after mismatch: %w", err)
	}
	revocationEpoch := epoch + 1
	if current.Epoch > revocationEpoch {
		revocationEpoch = current.Epoch
	}
	if markerErr := jobowner.RevokeIfEpoch(jobs, id, epoch, revocationEpoch); markerErr != nil {
		return errors.Join(errJobReassigned, fmt.Errorf("local revocation unavailable: %w", markerErr))
	}
	return err
}

// TransferJobOwner moves a fence with one conditional update. The bool says
// whether the server understands the fence; legacy servers receive no PUT.
func TransferJobOwner(ctx context.Context, store JobStore, id string, from, to JobOwner) (bool, error) {
	record, err := store.Get(ctx, id)
	if err != nil {
		return false, err
	}
	current, supported, err := JobOwnerFromRecord(record)
	if err != nil || !supported {
		return supported, err
	}
	expected := from
	if current == (JobOwner{}) {
		expected = JobOwner{}
	}
	err = store.Put(ctx, id, map[string]any{"expected_owner": expected, "owner": to})
	var backend *BackendError
	ambiguous := !errors.As(err, &backend) || backend.Status >= http.StatusInternalServerError
	if err != nil && ambiguous && current == expected {
		// The request may have committed even if its reply or deadline was
		// lost. Give the confirming read its own bounded context.
		checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		confirmed, readErr := store.Get(checkCtx, id)
		cancel()
		if readErr == nil {
			owner, available, ownerErr := JobOwnerFromRecord(confirmed)
			if ownerErr == nil && available && owner == to {
				return true, nil
			}
		}
	}
	if IsJobOwnerMismatch(err) {
		if errors.As(err, &backend) {
			node := backend.OwnerNode
			if node != "" && !nodeIDRE.MatchString(node) {
				node = "unknown"
			}
			return true, fmt.Errorf("job_owner_mismatch: current owner %s epoch %d: %w", node, backend.OwnerEpoch, err)
		}
	}
	if errors.As(err, &backend) && backend.Code == "job_state_conflict" {
		return true, fmt.Errorf("job_state_conflict: terminal record refuses ownership transfer: %w", err)
	}
	return true, err
}

// TransferJobOwner uses this app's configured job store for a hub transfer.
func (a *App) TransferJobOwner(ctx context.Context, id string, from, to JobOwner) (bool, error) {
	store, err := a.jobStore()
	if err != nil {
		return false, err
	}
	return TransferJobOwner(ctx, store, id, from, to)
}
